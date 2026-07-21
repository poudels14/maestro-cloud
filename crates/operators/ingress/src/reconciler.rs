use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use kernel_api::{Object, ResourceKind, ServiceId, ServiceSpec, ServiceStatus, Timestamp};
use kernel_controller::{
    Action, ControllerRuntime, FencedStore, ReconcileContext, ReconcileError, Reconciler,
    RuntimeConfig, TimestampClock,
};
use kernel_store::{Clock, Keyspace, StorePrefix};

use crate::writer::IngressWriteError;
use crate::{IngressBackend, IngressController, IngressError, IngressSettings};

const CONFLICT_RETRY: Duration = Duration::from_millis(100);

/// Watch-driven ingress operator whose primary resource is `Service`.
pub struct IngressReconciler {
    controller: IngressController,
    timestamp_clock: Arc<dyn TimestampClock>,
    service_prefix: StorePrefix,
    trigger_prefix: StorePrefix,
}

impl IngressReconciler {
    /// Constructs one ingress operator without starting background work.
    pub fn new(
        cluster_id: kernel_api::ClusterId,
        settings: IngressSettings,
        backend: Arc<dyn IngressBackend>,
        timestamp_clock: Arc<dyn TimestampClock>,
    ) -> Result<Self, IngressError> {
        let keyspace = Keyspace::new(&cluster_id);
        let service_kind = ResourceKind::new(<Self as Reconciler>::KIND)?;
        Ok(Self {
            controller: IngressController::new(cluster_id, settings, backend)?,
            timestamp_clock,
            service_prefix: keyspace.resource_kind(&service_kind),
            trigger_prefix: keyspace.cluster(),
        })
    }

    /// Wraps this operator in the shared watch, resync, backoff, and finalizer runtime.
    pub fn runtime(
        self: Arc<Self>,
        store: Arc<FencedStore>,
        monotonic_clock: Arc<dyn Clock>,
        config: RuntimeConfig,
    ) -> ControllerRuntime<Self> {
        ControllerRuntime::new_with_trigger_prefix(
            self.clone(),
            self.service_prefix.clone(),
            self.trigger_prefix.clone(),
            store,
            monotonic_clock,
            config,
        )
    }

    async fn converge(
        &self,
        context: &ReconcileContext,
        finalizing: Option<&ServiceId>,
    ) -> Result<Action, ReconcileError> {
        let now = self.timestamp_clock.now();
        let report = self
            .controller
            .reconcile_once(context.store(), now)
            .await
            .map_err(classify_error)?;
        if report.conflict {
            return Ok(Action::Requeue(CONFLICT_RETRY));
        }
        if let Some(deadline) = report.requeue_at {
            return Ok(Action::Requeue(until(now, deadline)));
        }
        if finalizing.is_some_and(|id| report.has_generations(id)) {
            Ok(Action::Requeue(CONFLICT_RETRY))
        } else {
            Ok(Action::Done)
        }
    }
}

#[async_trait]
impl Reconciler for IngressReconciler {
    type Id = ServiceId;
    type Spec = ServiceSpec;
    type Status = ServiceStatus;

    const KIND: &'static str = "Service";
    const FINALIZER: Option<&'static str> = Some("ingress.maestro.dev/traffic");

    async fn reconcile(
        &self,
        _resource: Object<Self::Id, Self::Spec, Self::Status>,
        context: ReconcileContext,
    ) -> Result<Action, ReconcileError> {
        self.converge(&context, None).await
    }

    async fn finalize(
        &self,
        resource: Object<Self::Id, Self::Spec, Self::Status>,
        context: ReconcileContext,
    ) -> Result<Action, ReconcileError> {
        self.converge(&context, Some(&resource.meta.id)).await
    }
}

fn until(now: Timestamp, deadline: Timestamp) -> Duration {
    let millis = deadline.0.saturating_sub(now.0).max(1);
    Duration::from_millis(u64::try_from(millis).unwrap_or(u64::MAX))
}

fn classify_error(error: IngressError) -> ReconcileError {
    match error {
        IngressError::Controller(error) => ReconcileError::Infrastructure(error),
        IngressError::Write(IngressWriteError::Controller(error)) => {
            ReconcileError::Infrastructure(error)
        }
        IngressError::Backend(error) => ReconcileError::Retryable {
            message: error.to_string(),
        },
        error => ReconcileError::Terminal {
            reason: terminal_reason(&error).to_string(),
            message: error.to_string(),
        },
    }
}

fn terminal_reason(error: &IngressError) -> &'static str {
    match error {
        IngressError::InvalidIdentifier(_) => "InvalidResourceIdentity",
        IngressError::Plan(crate::IngressPlanError::ZeroRetirementGrace) => {
            "InvalidIngressSettings"
        }
        IngressError::Plan(_) => "InvalidIngressState",
        IngressError::MalformedResource { .. } => "MalformedResource",
        IngressError::ResourceIdentityMismatch { .. } => "ResourceIdentityMismatch",
        IngressError::DuplicateResource { .. } => "DuplicateResource",
        IngressError::Write(_) => "IngressMutationFailed",
        IngressError::Backend(_) => "IngressBackendFailed",
        IngressError::Controller(_) => "IngressInfrastructureFailed",
    }
}
