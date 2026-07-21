use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use kernel_api::{Object, ResourceKind, ServiceId, ServiceSpec, ServiceStatus};
use kernel_controller::{
    Action, ControllerRuntime, FencedStore, ReconcileContext, ReconcileError, Reconciler,
    RuntimeConfig, TimestampClock,
};
use kernel_store::{Clock, Keyspace, StorePrefix};

use crate::writer::DeploymentWriteError;
use crate::{DeploymentController, DeploymentError, LifecycleSettings};

const CONFLICT_RETRY: Duration = Duration::from_millis(100);

/// Watch-driven deployment operator whose primary resource is `Service`.
pub struct DeploymentReconciler {
    controller: DeploymentController,
    timestamp_clock: Arc<dyn TimestampClock>,
    service_prefix: StorePrefix,
    trigger_prefix: StorePrefix,
}

impl DeploymentReconciler {
    /// Constructs one deployment operator without starting background work.
    pub fn new(
        cluster_id: kernel_api::ClusterId,
        settings: LifecycleSettings,
        timestamp_clock: Arc<dyn TimestampClock>,
    ) -> Result<Self, DeploymentError> {
        let keyspace = Keyspace::new(&cluster_id);
        let service_kind = ResourceKind::new(<Self as Reconciler>::KIND)?;
        Ok(Self {
            controller: DeploymentController::new(cluster_id, settings)?,
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
        let report = self
            .controller
            .reconcile_once(context.store(), self.timestamp_clock.now())
            .await
            .map_err(classify_error)?;
        if report.conflict || finalizing.is_some_and(|id| report.has_children(id)) {
            Ok(Action::Requeue(CONFLICT_RETRY))
        } else {
            Ok(Action::Done)
        }
    }
}

#[async_trait]
impl Reconciler for DeploymentReconciler {
    type Id = ServiceId;
    type Spec = ServiceSpec;
    type Status = ServiceStatus;

    const KIND: &'static str = "Service";
    const FINALIZER: Option<&'static str> = Some("deployment.maestro.dev/children");

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

fn classify_error(error: DeploymentError) -> ReconcileError {
    match error {
        DeploymentError::Controller(error) => ReconcileError::Infrastructure(error),
        DeploymentError::Write(DeploymentWriteError::Controller(error)) => {
            ReconcileError::Infrastructure(error)
        }
        error => ReconcileError::Terminal {
            reason: terminal_reason(&error).to_string(),
            message: error.to_string(),
        },
    }
}

fn terminal_reason(error: &DeploymentError) -> &'static str {
    match error {
        DeploymentError::InvalidIdentifier(_) => "InvalidResourceIdentity",
        DeploymentError::Plan(crate::DeploymentPlanError::ZeroDrainGrace) => {
            "InvalidDeploymentSettings"
        }
        DeploymentError::Plan(_) => "InvalidDeploymentState",
        DeploymentError::MalformedResource { .. } => "MalformedResource",
        DeploymentError::ResourceIdentityMismatch { .. } => "ResourceIdentityMismatch",
        DeploymentError::DuplicateResource { .. } => "DuplicateResource",
        DeploymentError::Write(_) => "DeploymentMutationFailed",
        DeploymentError::Controller(_) => "DeploymentInfrastructureFailed",
    }
}
