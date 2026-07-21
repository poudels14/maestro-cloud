use kernel_api::{NodeId, UpgradePhase};

/// Invalid desired state or unsafe topology discovered by the upgrade planner.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum UpgradePlanError {
    /// No non-deleting nodes exist.
    #[error("cluster upgrade requires at least one node")]
    NoNodes,
    /// One node identity occurred more than once.
    #[error("node `{node_id}` occurs more than once")]
    DuplicateNode { node_id: NodeId },
    /// One selected or persisted node no longer exists.
    #[error("upgrade node `{node_id}` does not exist")]
    NodeMissing { node_id: NodeId },
    /// The active leader is absent from the node snapshot.
    #[error("upgrade leader `{node_id}` does not exist")]
    LeaderMissing { node_id: NodeId },
    /// Every known node must be live before coordinated maintenance.
    #[error("all nodes must be live before upgrade; offline: {node_ids:?}")]
    OfflineNodes { node_ids: Vec<NodeId> },
    /// Explicit selection contained the same node more than once.
    #[error("upgrade node selection contains duplicates")]
    DuplicateSelection,
    /// A node is already held by another maintenance owner.
    #[error("node `{node_id}` is already in maintenance")]
    NodeAlreadyMaintained { node_id: NodeId },
    /// Two control-plane voters cannot preserve quorum during rolling maintenance.
    #[error("rolling upgrade cannot safely restart a two-voter control plane")]
    UnsafeTwoVoterRollingUpgrade,
    /// Target semantic version was malformed.
    #[error("invalid upgrade target version `{value}`: {message}")]
    InvalidTargetVersion { value: String, message: String },
    /// A node reported a malformed semantic version.
    #[error("node `{node_id}` reported invalid version `{value}`: {message}")]
    InvalidNodeVersion {
        node_id: NodeId,
        value: String,
        message: String,
    },
    /// No node requires the requested minimum version.
    #[error("every selected node already meets target version `{target}`")]
    TargetAlreadySatisfied { target: String },
    /// Pending state unexpectedly contained persisted progress.
    #[error("pending upgrade run already contains node progress")]
    UnexpectedPendingProgress,
    /// A persisted aggregate phase had no matching node batch.
    #[error("upgrade phase {phase:?} has no matching node batch")]
    EmptyBatch { phase: UpgradePhase },
    /// No pending node was available for the next batch.
    #[error("upgrade has no pending node for its next batch")]
    EmptyPendingBatch,
    /// Dispatch outcome was applied outside the Applying phase.
    #[error("cannot record upgrade dispatch while run is {phase:?}")]
    UnexpectedDispatchPhase { phase: UpgradePhase },
    /// Applying progress lost the daemon identity captured before drain.
    #[error("upgrade node `{node_id}` has no previous daemon identity")]
    PreviousInstanceMissing { node_id: NodeId },
    /// A persisted per-node transition violated the shared phase matrix.
    #[error("invalid upgrade transition from {from:?} to {to:?}")]
    InvalidTransition {
        from: UpgradePhase,
        to: UpgradePhase,
    },
    /// An internal status index did not resolve to a node entry.
    #[error("upgrade status index no longer resolves")]
    CorruptStatusIndex,
}
