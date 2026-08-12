use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use kernel_api::{
    FirewallPolicyId, FirewallPolicySpec, FirewallPolicyStatus, NodeNetworkId, NodeNetworkSpec,
    NodeNetworkStatus, Object, ResourceKind,
};
use kernel_controller::{
    Action, ControllerRuntime, FencedStore, ReconcileContext, ReconcileError, Reconciler,
    RuntimeConfig,
};
use kernel_store::{Clock, StorePrefix};

use crate::writer::FirewallWriteError;
use crate::{FirewallController, FirewallError};

const CONFLICT_RETRY: Duration = Duration::from_millis(100);
const ACKNOWLEDGEMENT_RETRY: Duration = Duration::from_secs(1);

/// Watch-driven policy reconciler that owns FirewallPolicy finalization.
pub struct FirewallPolicyReconciler {
    controller: Arc<FirewallController>,
    policy_prefix: StorePrefix,
    trigger_prefix: StorePrefix,
}

impl FirewallPolicyReconciler {
    /// Binds policy lifecycle to a shared whole-bundle controller.
    pub fn new(controller: Arc<FirewallController>) -> Result<Self, FirewallError> {
        let policy_kind = ResourceKind::new(<Self as Reconciler>::KIND)?;
        Ok(Self {
            policy_prefix: controller.keyspace().resource_kind(&policy_kind),
            trigger_prefix: controller.keyspace().cluster(),
            controller,
        })
    }

    /// Wraps policy reconciliation in shared watches, fencing, finalizers, and backoff.
    pub fn runtime(
        self: Arc<Self>,
        store: Arc<FencedStore>,
        monotonic_clock: Arc<dyn Clock>,
        config: RuntimeConfig,
    ) -> ControllerRuntime<Self> {
        ControllerRuntime::new_with_trigger_prefix(
            self.clone(),
            self.policy_prefix.clone(),
            self.trigger_prefix.clone(),
            store,
            monotonic_clock,
            config,
        )
    }

    async fn converge(
        &self,
        context: &ReconcileContext,
        finalizing: bool,
    ) -> Result<Action, ReconcileError> {
        let report = self
            .controller
            .reconcile_once(context.store())
            .await
            .map_err(classify_error)?;
        Ok(if report.conflict {
            Action::Requeue(CONFLICT_RETRY)
        } else if finalizing && (report.desired_state_changed || report.pending_rulesets > 0) {
            Action::Requeue(ACKNOWLEDGEMENT_RETRY)
        } else {
            Action::Done
        })
    }
}

#[async_trait]
impl Reconciler for FirewallPolicyReconciler {
    type Id = FirewallPolicyId;
    type Spec = FirewallPolicySpec;
    type Status = FirewallPolicyStatus;

    const KIND: &'static str = "FirewallPolicy";
    const FINALIZER: Option<&'static str> = Some("firewall.maestro.dev/rules");

    async fn reconcile(
        &self,
        _resource: Object<Self::Id, Self::Spec, Self::Status>,
        context: ReconcileContext,
    ) -> Result<Action, ReconcileError> {
        self.converge(&context, false).await
    }

    async fn finalize(
        &self,
        _resource: Object<Self::Id, Self::Spec, Self::Status>,
        context: ReconcileContext,
    ) -> Result<Action, ReconcileError> {
        self.converge(&context, true).await
    }
}

/// NodeNetwork primary that installs host and DNS guards without user policies.
pub struct FirewallBaselineReconciler {
    controller: Arc<FirewallController>,
    network_prefix: StorePrefix,
    trigger_prefix: StorePrefix,
}

impl FirewallBaselineReconciler {
    /// Binds baseline lifecycle to the same serialized whole-bundle controller.
    pub fn new(controller: Arc<FirewallController>) -> Result<Self, FirewallError> {
        let network_kind = ResourceKind::new(<Self as Reconciler>::KIND)?;
        Ok(Self {
            network_prefix: controller.keyspace().resource_kind(&network_kind),
            trigger_prefix: controller.keyspace().cluster(),
            controller,
        })
    }

    /// Wraps baseline reconciliation in shared watches, fencing, and backoff.
    pub fn runtime(
        self: Arc<Self>,
        store: Arc<FencedStore>,
        monotonic_clock: Arc<dyn Clock>,
        config: RuntimeConfig,
    ) -> ControllerRuntime<Self> {
        ControllerRuntime::new_with_trigger_prefix(
            self.clone(),
            self.network_prefix.clone(),
            self.trigger_prefix.clone(),
            store,
            monotonic_clock,
            config,
        )
    }
}

#[async_trait]
impl Reconciler for FirewallBaselineReconciler {
    type Id = NodeNetworkId;
    type Spec = NodeNetworkSpec;
    type Status = NodeNetworkStatus;

    const KIND: &'static str = "NodeNetwork";

    async fn reconcile(
        &self,
        _resource: Object<Self::Id, Self::Spec, Self::Status>,
        context: ReconcileContext,
    ) -> Result<Action, ReconcileError> {
        let report = self
            .controller
            .reconcile_once(context.store())
            .await
            .map_err(classify_error)?;
        Ok(if report.conflict {
            Action::Requeue(CONFLICT_RETRY)
        } else {
            Action::Done
        })
    }
}

fn classify_error(error: FirewallError) -> ReconcileError {
    match error {
        FirewallError::Controller(error) => ReconcileError::Infrastructure(error),
        FirewallError::Write(FirewallWriteError::Controller(error)) => {
            ReconcileError::Infrastructure(error)
        }
        error => ReconcileError::Terminal {
            reason: terminal_reason(&error).to_string(),
            message: error.to_string(),
        },
    }
}

fn terminal_reason(error: &FirewallError) -> &'static str {
    match error {
        FirewallError::InvalidIdentifier(_) => "InvalidResourceIdentity",
        FirewallError::Plan(_) => "InvalidFirewallState",
        FirewallError::MalformedResource { .. } => "MalformedResource",
        FirewallError::ResourceIdentityMismatch { .. } => "ResourceIdentityMismatch",
        FirewallError::DuplicateResource { .. } => "DuplicateResource",
        FirewallError::Write(_) => "FirewallMutationFailed",
        FirewallError::Controller(_) => "FirewallInfrastructureFailed",
    }
}
