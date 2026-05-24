use anyhow::Result;
use async_trait::async_trait;

use super::types::{NodeId, NodeInfo};

#[async_trait]
pub trait NodeRegistry: Send + Sync {
    /// Register this node with the cluster. Returns an opaque registration handle
    /// that must be kept alive (via [`keep_alive`]) for the node to stay listed.
    async fn register(&self, info: &NodeInfo) -> Result<()>;

    /// Refresh the registration so the node continues to appear in the registry.
    async fn keep_alive(&self) -> Result<()>;

    /// Voluntarily remove this node from the registry.
    async fn deregister(&self) -> Result<()>;

    async fn list_nodes(&self) -> Result<Vec<NodeInfo>>;

    async fn get_node(&self, node_id: &NodeId) -> Result<Option<NodeInfo>>;

    /// Set the `unschedulable` flag on this node so the scheduler stops
    /// placing new replicas here.
    async fn set_unschedulable(&self, unschedulable: bool) -> Result<()>;
}
