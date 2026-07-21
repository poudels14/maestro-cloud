use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use kernel_api::{Object, ResourceKind, ServiceId, ServiceSpec, ServiceStatus};
use kernel_controller::{
    Action, ControllerRuntime, FencedStore, ReconcileContext, ReconcileError, Reconciler,
    RuntimeConfig,
};
use kernel_store::{Clock, Keyspace, StorePrefix};

use crate::writer::DnsWriteError;
use crate::{DnsController, DnsError, DnsSettings};

const CONFLICT_RETRY: Duration = Duration::from_millis(100);

/// Watch-driven DNS operator whose primary resource is `Service`.
pub struct DnsReconciler {
    controller: DnsController,
    service_prefix: StorePrefix,
    trigger_prefix: StorePrefix,
}

impl DnsReconciler {
    /// Constructs one DNS operator without starting background work.
    pub fn new(cluster_id: kernel_api::ClusterId, settings: DnsSettings) -> Result<Self, DnsError> {
        let keyspace = Keyspace::new(&cluster_id);
        let service_kind = ResourceKind::new(<Self as Reconciler>::KIND)?;
        Ok(Self {
            controller: DnsController::new(cluster_id, settings)?,
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
            .reconcile_once(context.store())
            .await
            .map_err(classify_error)?;
        if report.conflict || finalizing.is_some_and(|id| report.has_records(id)) {
            Ok(Action::Requeue(CONFLICT_RETRY))
        } else {
            Ok(Action::Done)
        }
    }
}

#[async_trait]
impl Reconciler for DnsReconciler {
    type Id = ServiceId;
    type Spec = ServiceSpec;
    type Status = ServiceStatus;

    const KIND: &'static str = "Service";
    const FINALIZER: Option<&'static str> = Some("dns.maestro.dev/records");

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

fn classify_error(error: DnsError) -> ReconcileError {
    match error {
        DnsError::Controller(error) => ReconcileError::Infrastructure(error),
        DnsError::Write(DnsWriteError::Controller(error)) => ReconcileError::Infrastructure(error),
        error => ReconcileError::Terminal {
            reason: terminal_reason(&error).to_string(),
            message: error.to_string(),
        },
    }
}

fn terminal_reason(error: &DnsError) -> &'static str {
    match error {
        DnsError::InvalidIdentifier(_) => "InvalidResourceIdentity",
        DnsError::Plan(crate::DnsPlanError::ZeroTtl) => "InvalidDnsSettings",
        DnsError::Plan(_) => "InvalidDnsState",
        DnsError::MalformedResource { .. } => "MalformedResource",
        DnsError::ResourceIdentityMismatch { .. } => "ResourceIdentityMismatch",
        DnsError::DuplicateResource { .. } => "DuplicateResource",
        DnsError::Write(_) => "DnsMutationFailed",
        DnsError::Controller(_) => "DnsInfrastructureFailed",
    }
}
