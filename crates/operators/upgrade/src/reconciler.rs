use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use kernel_api::{
    ConditionState, NodeId, Object, ResourceKind, Timestamp, UpgradePhase, UpgradeRun,
    UpgradeRunId, UpgradeRunSpec, UpgradeRunStatus,
};
use kernel_controller::{
    Action, ControllerRuntime, FencedStore, ReconcileContext, ReconcileError, Reconciler,
    RuntimeConfig, TimestampClock,
};
use kernel_store::{Clock, Keyspace, StorePrefix};

use crate::conditions::set_ready_condition;
use crate::snapshot::UpgradeSnapshot;
use crate::writer::{UpgradeWriteOutcome, UpgradeWriter};
use crate::{
    NodeUpgradeBackend, NodeUpgradeBackendError, UpgradeDispatchOutcome, UpgradePlan,
    UpgradePlanAction, UpgradePlanError, UpgradeSettings, UpgradeSettingsError, plan_upgrade,
    record_dispatch_outcome,
};

const CONFLICT_RETRY: Duration = Duration::from_millis(100);

/// Reconciles UpgradeRun resources into fenced node maintenance and idempotent dispatches.
pub struct UpgradeReconciler {
    keyspace: Keyspace,
    prefix: StorePrefix,
    monotonic_clock: Arc<dyn Clock>,
    timestamp_clock: Arc<dyn TimestampClock>,
    settings: UpgradeSettings,
    backend: Arc<dyn NodeUpgradeBackend>,
    writer: UpgradeWriter,
}

impl UpgradeReconciler {
    /// Constructs an upgrade reconciler without starting background work.
    pub fn new(
        cluster_id: kernel_api::ClusterId,
        monotonic_clock: Arc<dyn Clock>,
        timestamp_clock: Arc<dyn TimestampClock>,
        settings: UpgradeSettings,
        backend: Arc<dyn NodeUpgradeBackend>,
    ) -> Result<Self, UpgradeError> {
        let keyspace = Keyspace::new(&cluster_id);
        let kind = ResourceKind::new("UpgradeRun")?;
        Ok(Self {
            prefix: keyspace.resource_kind(&kind),
            writer: UpgradeWriter::new(&cluster_id)?,
            keyspace,
            monotonic_clock,
            timestamp_clock,
            settings: settings.validate()?,
            backend,
        })
    }

    /// Wraps the operator in the shared watch, resync, and retry runtime.
    pub fn runtime(
        self: Arc<Self>,
        store: Arc<FencedStore>,
        config: RuntimeConfig,
    ) -> ControllerRuntime<Self> {
        ControllerRuntime::new_with_trigger_prefix(
            self.clone(),
            self.prefix.clone(),
            self.keyspace.cluster(),
            store,
            self.monotonic_clock.clone(),
            config,
        )
    }

    async fn converge(
        &self,
        mut run: UpgradeRun,
        context: &ReconcileContext,
        mode: ReconcileMode,
    ) -> Result<Action, ReconcileError> {
        run.meta.revision = context.observed_version().resource_revision();
        if mode == ReconcileMode::Finalizing {
            terminate_for_deletion(&mut run, self.timestamp_clock.now());
        }
        let snapshot = UpgradeSnapshot::load(context.store(), &self.keyspace)
            .await
            .map_err(classify)?;
        let input = snapshot.input(
            run.clone(),
            context.store().token().identity().node_id.clone(),
            self.timestamp_clock.now(),
        );
        let planned = plan_upgrade(input.clone(), self.settings).map_err(classify_plan)?;
        if let UpgradePlanAction::Dispatch(request) = &planned.action {
            match self
                .writer
                .apply(
                    context.store(),
                    context.observed_version(),
                    &run,
                    &snapshot,
                    &planned,
                )
                .await
                .map_err(classify)?
            {
                UpgradeWriteOutcome::Conflict => return Ok(Action::Requeue(CONFLICT_RETRY)),
                UpgradeWriteOutcome::Noop | UpgradeWriteOutcome::Applied => {}
            }
            context.store().verify_leadership().await?;
            let outcome = match self.backend.apply(request).await {
                Ok(()) => UpgradeDispatchOutcome::Accepted,
                Err(NodeUpgradeBackendError::Unavailable { message }) => {
                    UpgradeDispatchOutcome::Retryable { message }
                }
                Err(NodeUpgradeBackendError::Rejected { message }) => {
                    UpgradeDispatchOutcome::Rejected { message }
                }
            };
            let outcome_plan =
                record_dispatch_outcome(input, self.settings, outcome).map_err(classify_plan)?;
            return self
                .persist(context, &run, &snapshot, &outcome_plan, mode)
                .await;
        }
        self.persist(context, &run, &snapshot, &planned, mode).await
    }

    async fn persist(
        &self,
        context: &ReconcileContext,
        observed: &UpgradeRun,
        snapshot: &UpgradeSnapshot,
        plan: &UpgradePlan,
        mode: ReconcileMode,
    ) -> Result<Action, ReconcileError> {
        let write = self
            .writer
            .apply(
                context.store(),
                context.observed_version(),
                observed,
                snapshot,
                plan,
            )
            .await
            .map_err(classify)?;
        match write {
            UpgradeWriteOutcome::Conflict => Ok(Action::Requeue(CONFLICT_RETRY)),
            UpgradeWriteOutcome::Applied if mode == ReconcileMode::Finalizing => {
                Ok(Action::Requeue(Duration::ZERO))
            }
            UpgradeWriteOutcome::Applied | UpgradeWriteOutcome::Noop => action(&plan.action),
        }
    }
}

#[async_trait]
impl Reconciler for UpgradeReconciler {
    type Id = UpgradeRunId;
    type Spec = UpgradeRunSpec;
    type Status = UpgradeRunStatus;

    const KIND: &'static str = "UpgradeRun";
    const FINALIZER: Option<&'static str> = Some("upgrade.maestro.dev/maintenance");

    async fn reconcile(
        &self,
        resource: Object<Self::Id, Self::Spec, Self::Status>,
        context: ReconcileContext,
    ) -> Result<Action, ReconcileError> {
        self.converge(resource, &context, ReconcileMode::Active)
            .await
    }

    async fn finalize(
        &self,
        resource: Object<Self::Id, Self::Spec, Self::Status>,
        context: ReconcileContext,
    ) -> Result<Action, ReconcileError> {
        self.converge(resource, &context, ReconcileMode::Finalizing)
            .await
    }
}

fn terminate_for_deletion(run: &mut UpgradeRun, now: Timestamp) {
    let target = match run.status.phase {
        UpgradePhase::Pending | UpgradePhase::Draining => UpgradePhase::Canceled,
        UpgradePhase::Applying | UpgradePhase::Restarting | UpgradePhase::Verifying => {
            UpgradePhase::Failed
        }
        UpgradePhase::Completed | UpgradePhase::Failed | UpgradePhase::Canceled => run.status.phase,
    };
    for status in &mut run.status.nodes {
        status.phase = match status.phase {
            UpgradePhase::Pending | UpgradePhase::Draining => UpgradePhase::Canceled,
            UpgradePhase::Applying | UpgradePhase::Restarting | UpgradePhase::Verifying => {
                UpgradePhase::Failed
            }
            UpgradePhase::Completed | UpgradePhase::Failed | UpgradePhase::Canceled => status.phase,
        };
        status.retry_at = None;
    }
    run.status.phase = target;
    set_ready_condition(
        run,
        ConditionState::False,
        "UpgradeCanceled",
        "upgrade resource deletion restored node scheduling",
        now,
    );
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReconcileMode {
    Active,
    Finalizing,
}

fn action(action: &UpgradePlanAction) -> Result<Action, ReconcileError> {
    match action {
        UpgradePlanAction::Done => Ok(Action::Done),
        UpgradePlanAction::Requeue(delay) => Ok(Action::Requeue(*delay)),
        UpgradePlanAction::Dispatch(_) => Err(ReconcileError::Terminal {
            reason: "UpgradeDispatchUnrecorded".to_string(),
            message: "upgrade dispatch action reached persistence without an outcome".to_string(),
        }),
    }
}

fn classify(error: UpgradeError) -> ReconcileError {
    match error {
        UpgradeError::Controller(error) => ReconcileError::Infrastructure(error),
        error => ReconcileError::Terminal {
            reason: "UpgradeReconcileFailed".to_string(),
            message: error.to_string(),
        },
    }
}

fn classify_plan(error: UpgradePlanError) -> ReconcileError {
    ReconcileError::Terminal {
        reason: "UpgradePlanFailed".to_string(),
        message: error.to_string(),
    }
}

/// Upgrade construction, snapshot, planning, and atomic write failures.
#[derive(Debug, thiserror::Error)]
pub enum UpgradeError {
    /// A built-in resource identity was invalid.
    #[error(transparent)]
    InvalidIdentifier(#[from] kernel_api::InvalidIdentifier),
    /// Static retry or observation settings were invalid.
    #[error(transparent)]
    InvalidSettings(#[from] UpgradeSettingsError),
    /// The leadership fence or backing store rejected an operation.
    #[error(transparent)]
    Controller(#[from] kernel_controller::ControllerError),
    /// A relevant stored resource could not be decoded.
    #[error("malformed {kind} resource at `{key}`: {message}")]
    MalformedResource {
        kind: &'static str,
        key: String,
        message: String,
    },
    /// Typed metadata identity did not match its canonical store key.
    #[error("{kind} `{resource_id}` does not match store key `{key}`")]
    ResourceIdentityMismatch {
        kind: &'static str,
        resource_id: String,
        key: String,
    },
    /// One typed identity occurred under more than one key.
    #[error("{kind} `{resource_id}` occurs more than once")]
    DuplicateResource {
        kind: &'static str,
        resource_id: String,
    },
    /// A planned node mutation referenced a missing snapshot node.
    #[error("planned upgrade mutation references missing node `{node_id}`")]
    PlannedNodeMissing { node_id: NodeId },
    /// One plan attempted to mutate the same node more than once.
    #[error("planned upgrade mutates node `{node_id}` more than once")]
    DuplicateNodeUpdate { node_id: NodeId },
    /// Desired state could not be encoded for its fenced transaction.
    #[error("failed to serialize upgrade state: {message}")]
    Serialize { message: String },
}
