use async_trait::async_trait;
use kernel_api::{AssignmentId, BuildId, DeploymentId, NodeId, Timestamp};

/// One user-visible transition while an assignment is being published to a node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssignmentPublishingEvent {
    /// Build whose log stream receives this event.
    pub build_id: BuildId,
    /// Deployment being published.
    pub deployment_id: DeploymentId,
    /// Assignment that owns the replica.
    pub assignment_id: AssignmentId,
    /// Target node.
    pub node_id: NodeId,
    /// Zero-based replica slot.
    pub replica_index: u32,
    /// Time at which this transition was observed.
    pub occurred_at: Timestamp,
    /// Publication transition.
    pub state: AssignmentPublishingState,
}

/// User-visible assignment publication state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AssignmentPublishingState {
    /// The node accepted a newly created replica assignment.
    Started,
    /// Publication cannot currently advance.
    Waiting {
        /// Stable machine-readable reason from assignment reconciliation.
        reason: String,
        /// Human-readable diagnostic detail.
        message: String,
        /// Whether the assignment has exhausted its retry policy.
        failed: bool,
    },
    /// The artifact is available and its workload has started.
    Completed,
}

/// Best-effort boundary for exposing assignment publication progress.
#[async_trait]
pub trait AssignmentPublishingSink: Send + Sync {
    /// Records one publication transition without affecting assignment correctness.
    async fn record(&self, event: AssignmentPublishingEvent);
}
