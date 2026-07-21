use async_trait::async_trait;

use crate::NodeUpgradeRequest;

/// Idempotent node-maintenance boundary used by the upgrade operator.
#[async_trait]
pub trait NodeUpgradeBackend: Send + Sync {
    /// Stages and applies one rolling singleton or all-node batch.
    ///
    /// Replays with the same run ID and targets must not start a second host
    /// mutation after a prior call was accepted.
    async fn apply(&self, request: &NodeUpgradeRequest) -> Result<(), NodeUpgradeBackendError>;
}

/// Matchable failure returned by a node-upgrade adapter.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum NodeUpgradeBackendError {
    /// Transport or node availability prevented a conclusive acceptance.
    #[error("node upgrade backend is unavailable: {message}")]
    Unavailable { message: String },
    /// Node policy or upgrade staging permanently rejected the request.
    #[error("node upgrade backend rejected the request: {message}")]
    Rejected { message: String },
}
