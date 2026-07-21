use std::time::Duration;

use kernel_api::{ClusterId, NodeId, Timestamp};
use runtime::NetworkSpec;

/// Node-scoped assignment reconciliation settings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssignmentAgentSettings {
    /// Cluster whose assignment and runtime ownership labels are reconciled.
    pub cluster_id: ClusterId,
    /// Local node; assignments for other nodes are never mutated.
    pub node_id: NodeId,
    /// Node-local runtime bridge and exact host-owned IPAM range.
    pub network: NetworkSpec,
    /// Graceful workload shutdown deadline before forced termination.
    pub stop_timeout: Duration,
    /// Level-triggered full reconciliation interval.
    pub resync_interval: Duration,
    /// Delay before the first restart attempt after an observed exit.
    pub restart_backoff_base: Duration,
    /// Maximum exponential delay between an exit and its restart attempt.
    pub restart_backoff_max: Duration,
}

/// Results of one complete desired/runtime-state comparison.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AssignmentReconcileReport {
    /// Active local assignments observed in the store snapshot.
    pub desired: usize,
    /// Assignments confirmed running after reconciliation.
    pub running: usize,
    /// Exited workloads restored using a durably accounted restart attempt.
    pub restarted: usize,
    /// Earliest wall-clock deadline at which pending work should be retried.
    pub requeue_at: Option<Timestamp>,
    /// Assignments left pending or failed with a status condition.
    pub unresolved: usize,
    /// Runtime workloads removed because no active local assignment owned them.
    pub garbage_collected: usize,
    /// Malformed resources skipped without crashing the agent loop.
    pub malformed_resources: usize,
}

pub(crate) fn earliest(
    current: Option<Timestamp>,
    candidate: Option<Timestamp>,
) -> Option<Timestamp> {
    match (current, candidate) {
        (Some(current), Some(candidate)) => Some(std::cmp::min(current, candidate)),
        (current, candidate) => current.or(candidate),
    }
}
