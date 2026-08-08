use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{ClusterId, NodeId, NodeRole, SecretValue};

/// Exact cluster and node target for one local preview launch-config update.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PreviewLaunchConfigUpdateRequest {
    /// Cluster the caller expects the contacted Admin endpoint to serve.
    pub cluster_id: ClusterId,
    /// Node the caller expects the contacted Admin endpoint to serve.
    pub node_id: NodeId,
    /// DNS suffix used for stable preview hostnames.
    pub domain: String,
    /// GitHub token with pull-request read and deployment read/write access.
    pub github_token: SecretValue,
    /// Maximum previews retained cluster-wide, including close grace periods.
    pub max_concurrent_previews: usize,
}

/// Secret-free receipt for one idempotent local launch-config update.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PreviewLaunchConfigUpdateResponse {
    /// Node whose protected launch document was inspected.
    pub node_id: NodeId,
    /// Whether the protected launch document changed.
    pub changed: bool,
}

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
    /// Node serving this view.
    pub local_node_id: NodeId,
    /// Declared cluster members in stable node-ID order.
    pub nodes: Vec<MaskedClusterConfigNode>,
    /// Private networks allowed to initiate control-plane traffic.
    pub control_allow_cidrs: Vec<String>,
    /// Cluster-wide service ports.
    pub ports: MaskedClusterConfigPorts,
    /// Optional Tailscale gateway settings with the credential omitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tailscale: Option<MaskedTailscaleConfig>,
    /// Optional Cloudflare Tunnel settings with the connector token omitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cloudflare: Option<MaskedCloudflareConfig>,
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

/// Secret-free view of the managed Tailscale gateway fleet.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct MaskedTailscaleConfig {
    /// Effective routes advertised to the tailnet.
    pub advertise_routes: Vec<String>,
    /// Cluster bridge resolvers reachable through the advertised routes.
    pub dns_nameservers: Vec<String>,
    /// Tailnet policy tags applied during authentication.
    pub tags: Vec<String>,
    /// Explicit remote cluster suffixes forwarded through the managed gateways.
    pub cross_cluster_dns: Vec<MaskedCrossClusterDnsRoute>,
}

/// Secret-free view of the remotely managed Cloudflare Tunnel connector fleet.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct MaskedCloudflareConfig {
    /// Remotely managed tunnel connector settings.
    pub tunnel: MaskedCloudflareTunnelConfig,
}

/// Secret-free view of one Cloudflare Tunnel connector fleet.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct MaskedCloudflareTunnelConfig {
    /// Desired number of highly available connector workloads per workload-capable node.
    pub replicas: u32,
}

/// Secret-free view of one scoped remote DNS route.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct MaskedCrossClusterDnsRoute {
    /// Remote cluster identity embedded in its authoritative DNS suffix.
    pub cluster_id: ClusterId,
    /// Remote bridge resolver addresses reachable through Tailscale.
    pub nameservers: Vec<String>,
}
