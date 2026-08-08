use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use kernel_api::{Object, ResourceKind, ServiceId, ServiceSpec, ServiceStatus};
use kernel_controller::{
    Action, ControllerRuntime, FencedStore, ReconcileContext, ReconcileError, Reconciler,
    RuntimeConfig,
};
use kernel_store::{Clock, Keyspace, StorePrefix};

use crate::{AssignmentWriteError, Scheduler, SchedulerError, SchedulerSettings, TimestampClock};

const CONFLICT_RETRY: Duration = Duration::from_millis(100);

/// Watch-driven scheduler operator whose primary resource is `Service`.
pub struct SchedulerReconciler {
    scheduler: Scheduler,
    timestamp_clock: Arc<dyn TimestampClock>,
    service_prefix: StorePrefix,
    trigger_prefix: StorePrefix,
}

impl SchedulerReconciler {
    /// Constructs one scheduler operator without starting background work.
    pub fn new(
        cluster_id: kernel_api::ClusterId,
        settings: SchedulerSettings,
        timestamp_clock: Arc<dyn TimestampClock>,
    ) -> Result<Self, SchedulerError> {
        let keyspace = Keyspace::new(&cluster_id);
        let service_kind = ResourceKind::new(<Self as Reconciler>::KIND)?;
        Ok(Self {
            scheduler: Scheduler::new(cluster_id, settings)?,
            timestamp_clock,
            service_prefix: keyspace.resource_kind(&service_kind),
            trigger_prefix: keyspace.cluster(),
        })
    }

    /// Wraps this operator in the shared watch, resync, backoff, and finalizer runtime.
    pub fn runtime(
        self: Arc<Self>,
        fenced_store: Arc<FencedStore>,
        monotonic_clock: Arc<dyn Clock>,
        config: RuntimeConfig,
    ) -> ControllerRuntime<Self> {
        ControllerRuntime::new_with_trigger_prefix(
            self.clone(),
            self.service_prefix.clone(),
            self.trigger_prefix.clone(),
            fenced_store,
            monotonic_clock,
            config,
        )
    }

    async fn converge(
        &self,
        context: &ReconcileContext,
        service_id: &ServiceId,
        finalizing: Option<&ServiceId>,
    ) -> Result<Action, ReconcileError> {
        let report = self
            .scheduler
            .reconcile_service_once(context.store(), service_id, self.timestamp_clock.now())
            .await
            .map_err(classify_error)?;
        if report.conflict || finalizing.is_some_and(|id| report.has_assignments(id)) {
            Ok(Action::Requeue(CONFLICT_RETRY))
        } else {
            Ok(Action::Done)
        }
    }
}

#[async_trait]
impl Reconciler for SchedulerReconciler {
    type Id = ServiceId;
    type Spec = ServiceSpec;
    type Status = ServiceStatus;

    const KIND: &'static str = "Service";
    const FINALIZER: Option<&'static str> = Some("scheduler.maestro.dev/assignments");

    async fn reconcile(
        &self,
        resource: Object<Self::Id, Self::Spec, Self::Status>,
        context: ReconcileContext,
    ) -> Result<Action, ReconcileError> {
        self.converge(&context, &resource.meta.id, None).await
    }

    async fn finalize(
        &self,
        resource: Object<Self::Id, Self::Spec, Self::Status>,
        context: ReconcileContext,
    ) -> Result<Action, ReconcileError> {
        self.converge(&context, &resource.meta.id, Some(&resource.meta.id))
            .await
    }
}

fn classify_error(error: SchedulerError) -> ReconcileError {
    match error {
        SchedulerError::Controller(error) => ReconcileError::Infrastructure(error),
        SchedulerError::AssignmentWrite(AssignmentWriteError::Controller(error)) => {
            ReconcileError::Infrastructure(error)
        }
        error => ReconcileError::Terminal {
            reason: terminal_reason(&error).to_string(),
            message: error.to_string(),
        },
    }
}

fn terminal_reason(error: &SchedulerError) -> &'static str {
    match error {
        SchedulerError::ZeroReplacementGrace => "InvalidSchedulerSettings",
        SchedulerError::ZeroDeploymentDrainGrace => "InvalidSchedulerSettings",
        SchedulerError::InvalidIdentifier(_) => "InvalidResourceIdentity",
        SchedulerError::MalformedResource { .. } => "MalformedResource",
        SchedulerError::ResourceIdentityMismatch { .. } => "ResourceIdentityMismatch",
        SchedulerError::DuplicateResource { .. } => "DuplicateResource",
        SchedulerError::DuplicateNodeNetwork { .. } => "DuplicateNodeNetwork",
        SchedulerError::AssignmentWrite(error) => match error {
            AssignmentWriteError::MalformedResource { .. } => "MalformedAssignment",
            AssignmentWriteError::ResourceIdentityMismatch { .. } => "AssignmentIdentityMismatch",
            AssignmentWriteError::DuplicateAssignment { .. } => "DuplicateAssignment",
            AssignmentWriteError::IdentityCollision { .. } => "AssignmentIdentityCollision",
            AssignmentWriteError::Serialize { .. } => "AssignmentSerializationFailed",
            AssignmentWriteError::SerializeObservation { .. } => {
                "SchedulerObservationSerializationFailed"
            }
            AssignmentWriteError::MalformedObservation { .. } => "MalformedSchedulerObservation",
            AssignmentWriteError::AtomicGroupTooLarge { .. } => "SchedulerTransactionTooLarge",
            AssignmentWriteError::Controller(_) => "SchedulerInfrastructureFailed",
        },
        SchedulerError::Controller(_) => "SchedulerInfrastructureFailed",
    }
}
