use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{ClusterId, NodeId, NodeRole};

/// Secret-free cluster configuration returned to authenticated operators.
///
/// This is deliberately a separate wire contract from the daemon's parsed
/// configuration so adding a secret-bearing daemon field cannot expose it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct MaskedClusterConfig {
    /// Stable cluster identity.
    pub cluster_id: ClusterId,
    /// Human-readable DNS-safe cluster name.
    pub name: String,
    /// Fixed private address pool for tunnels and workload networks.
    pub cluster_cidr: String,
    /// Maximum stable node indexes supported by the address pool.
    pub node_limit: u32,
    /// Prefix allocated to each node workload network.
    pub node_prefix: u8,
    /// Node serving this view.
    pub local_node_id: NodeId,
    /// Declared cluster members in stable node-ID order.
    pub nodes: Vec<MaskedClusterConfigNode>,
    /// Private networks allowed to initiate control-plane traffic.
    pub control_allow_cidrs: Vec<String>,
    /// Cluster-wide service ports.
    pub ports: MaskedClusterConfigPorts,
}

/// Secret-free topology for one configured cluster member.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct MaskedClusterConfigNode {
    /// Stable node identity.
    pub node_id: NodeId,
    /// Configured node hostname.
    pub hostname: String,
    /// Scheduling and control-plane capability.
    pub role: NodeRole,
    /// Private control-plane address advertised to peers.
    pub host_address: String,
    /// Public API port for this node.
    pub api_port: u16,
    /// Private workload subnet assigned to this node.
    pub workload_subnet: String,
}

/// Secret-free cluster-wide port allocation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct MaskedClusterConfigPorts {
    /// Cluster gateway listener port.
    pub gateway: u16,
    /// Internal store client port.
    pub store_client: u16,
    /// Internal store membership port.
    pub store_peer: u16,
    /// WireGuard mesh UDP port.
    pub wireguard: u16,
}
