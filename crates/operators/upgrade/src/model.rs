use std::collections::BTreeSet;
use std::time::Duration;

use kernel_api::{
    Assignment, Node, NodeId, NodeInstanceId, Timestamp, UpgradeOperation, UpgradeRun, UpgradeRunId,
};

/// Retry and observation cadence for one upgrade state machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UpgradeSettings {
    /// Delay after a retryable node-upgrade dispatch failure.
    pub retry_delay: Duration,
    /// Delay while waiting for drain, restart, or version observations.
    pub observation_interval: Duration,
    /// Maximum dispatch attempts before the run fails and restores scheduling.
    pub max_attempts: u32,
}

impl UpgradeSettings {
    /// Validates bounded, non-hot-loop upgrade settings.
    pub fn new(
        retry_delay: Duration,
        observation_interval: Duration,
        max_attempts: u32,
    ) -> Result<Self, UpgradeSettingsError> {
        Self {
            retry_delay,
            observation_interval,
            max_attempts,
        }
        .validate()
    }

    /// Revalidates settings assembled by a caller or configuration adapter.
    pub fn validate(self) -> Result<Self, UpgradeSettingsError> {
        if self.retry_delay.is_zero() || self.observation_interval.is_zero() {
            return Err(UpgradeSettingsError::ZeroDelay);
        }
        if self.max_attempts == 0 {
            return Err(UpgradeSettingsError::ZeroAttempts);
        }
        Ok(self)
    }
}

/// Invalid static upgrade settings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum UpgradeSettingsError {
    /// A zero delay would create a hot reconciliation loop.
    #[error("upgrade retry and observation delays must be greater than zero")]
    ZeroDelay,
    /// A zero attempt budget can never dispatch an upgrade.
    #[error("upgrade maximum attempts must be greater than zero")]
    ZeroAttempts,
}

/// Complete immutable input for one pure upgrade planning pass.
#[derive(Debug, Clone)]
pub struct UpgradeInput {
    /// UpgradeRun being reconciled.
    pub run: UpgradeRun,
    /// Known cluster nodes.
    pub nodes: Vec<Node>,
    /// Current workload assignments used to prove rolling drain completion.
    pub assignments: Vec<Assignment>,
    /// Nodes whose liveness sessions are currently present.
    pub live_nodes: BTreeSet<NodeId>,
    /// Node holding the leadership fence for this pass.
    pub leader_id: NodeId,
    /// Injected wall-clock time.
    pub now: Timestamp,
}

/// One node in an idempotent upgrade dispatch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeUpgradeTarget {
    /// Stable node identity.
    pub node_id: NodeId,
    /// Daemon identity that must change after the request is accepted.
    pub previous_instance_id: NodeInstanceId,
}

/// Store membership recovery authorized only for a planned all-control-plane reboot.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PlannedStoreRecovery {
    /// Configured master whose persisted store is retained as the source of truth.
    pub canonical_node_id: NodeId,
    /// Complete voter set that must be rebuilt after the planned reboot.
    pub expected_members: BTreeSet<NodeId>,
}

/// Stable request emitted by the pure planner for a side-effect adapter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeUpgradeRequest {
    /// Idempotency key shared by every replay of this run.
    pub run_id: UpgradeRunId,
    /// Host mutation requested from each node.
    pub operation: UpgradeOperation,
    /// Minimum semantic version, or the restart sentinel for restart-only runs.
    pub target_version: String,
    /// Rolling singleton or all-node target batch.
    pub targets: Vec<NodeUpgradeTarget>,
    /// Explicit recovery plan when this batch restarts every store voter together.
    pub store_recovery: Option<PlannedStoreRecovery>,
}

/// Result returned by an idempotent node-upgrade adapter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UpgradeDispatchOutcome {
    /// Every target accepted the request or had already accepted its replay.
    Accepted,
    /// No permanent policy error occurred; retry after the configured delay.
    Retryable { message: String },
    /// The request cannot succeed without changing desired state or configuration.
    Rejected { message: String },
}

/// Scheduling decision from one pure upgrade planning pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UpgradePlanAction {
    /// Desired and observed state agree.
    Done,
    /// Reconcile after a bounded observation or retry delay.
    Requeue(Duration),
    /// Verify leadership, perform this idempotent side effect, then record its outcome.
    Dispatch(NodeUpgradeRequest),
}

/// Atomic desired state and optional side effect from one planning pass.
#[derive(Debug, Clone)]
pub struct UpgradePlan {
    /// Desired UpgradeRun status.
    pub run: UpgradeRun,
    /// Node condition updates committed with the run status.
    pub node_updates: Vec<Node>,
    /// Next scheduling or side-effect action.
    pub action: UpgradePlanAction,
}
