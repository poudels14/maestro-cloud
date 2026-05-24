use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::watch;

use super::types::{LeadershipState, NodeId};

#[async_trait]
pub trait LeaderElector: Send + Sync {
    /// Campaign for leadership. Returns when this node becomes the leader or
    /// when the campaign is canceled (e.g., via [`resign`]). Implementations
    /// typically loop internally so the caller can spawn this as a long-lived task.
    async fn campaign(&self) -> Result<()>;

    /// Voluntarily step down. No-op if this node is not currently the leader.
    async fn resign(&self) -> Result<()>;

    /// Current leadership state as observed by this elector. Returns [`LeadershipState::Unknown`]
    /// before the first observation has been recorded.
    fn state(&self) -> LeadershipState;

    /// Watch leadership state changes.
    fn subscribe(&self) -> watch::Receiver<LeadershipState>;

    fn this_node(&self) -> &NodeId;
}
