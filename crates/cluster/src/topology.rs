mod validation;

use std::collections::BTreeMap;
use std::net::Ipv4Addr;

use kernel_api::{ClusterId, NodeId, NodeRole, SecretValue};
use serde::{Deserialize, Serialize};

use crate::{
    CloudflareTunnelConfig, CloudflareTunnelConfigError, ClusterPorts, ClusterPortsError, Ipv4Cidr,
    TailscaleConfigError, TailscaleGatewayConfig,
};

/// WireGuard MTU applied consistently to the mesh and workload interfaces.
pub const WIREGUARD_MTU_BYTES: u16 = 1_420;

/// A node address reachable by the other cluster members.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NodeEndpoint {
    /// Stable private host address used for cluster control traffic.
    pub host_address: Ipv4Addr,
    /// Port of the node's public control API.
    pub api_port: u16,
}

/// Operator-facing definition of one cluster member.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NodeDefinition {
    /// Hostname configured on the node.
    pub hostname: String,
    /// Control-plane address advertised to cluster peers.
    pub endpoint: NodeEndpoint,
    /// Private `/24` allocated exclusively to workloads on this node.
    pub workload_subnet: Ipv4Cidr,
    /// Public scheduling and control-plane capability.
    pub role: NodeRole,
}

/// Persisted input required to form or join a cluster.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ClusterConfig {
    /// Stable cluster identity.
    pub cluster_id: ClusterId,
    /// Lowercase DNS label used in certificates and discovery.
    pub name: String,
    /// Desired members keyed by stable node identity.
    pub nodes: BTreeMap<NodeId, NodeDefinition>,
    /// Optional private networks allowed to initiate control traffic.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub control_allow_cidrs: Vec<Ipv4Cidr>,
    /// Cluster-wide service ports fixed during initialization.
    pub ports: ClusterPorts,
    /// Shared bootstrap credential; replaced by node certificates after join.
    pub join_secret: SecretValue,
    /// Optional managed Tailscale subnet-router fleet.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tailscale: Option<TailscaleGatewayConfig>,
    /// Optional remotely managed Cloudflare Tunnel connector fleet.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cloudflare: Option<CloudflareTunnelConfig>,
}

/// Stable facts derived by successful cluster preflight.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatedTopology {
    master: NodeId,
    control_plane_nodes: Vec<NodeId>,
}

impl ValidatedTopology {
    /// Returns the one node responsible for initial cluster formation.
    pub fn master(&self) -> &NodeId {
        &self.master
    }

    /// Returns the one or three nodes eligible to run cluster controllers.
    pub fn control_plane_nodes(&self) -> &[NodeId] {
        &self.control_plane_nodes
    }
}

/// Why a topology cannot safely form a cluster.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ClusterPreflightError {
    /// Cluster names must be safe in DNS and certificates.
    #[error("{field} `{value}` must be a lowercase DNS label")]
    InvalidDnsLabel {
        /// Operator-facing field name.
        field: &'static str,
        /// Rejected value.
        value: String,
    },
    /// Node identifiers have a narrower topology constraint than resource IDs.
    #[error("node ID `{node_id}` must be a lowercase DNS label")]
    InvalidNodeName { node_id: NodeId },
    /// Hostnames may contain several DNS labels.
    #[error("hostname `{hostname}` for node `{node_id}` is not a lowercase DNS hostname")]
    InvalidHostname { node_id: NodeId, hostname: String },
    /// At least the initializing node must be declared.
    #[error("cluster topology must contain at least one node")]
    NoNodes,
    /// One master initializes the cluster trust root.
    #[error("cluster topology must contain one master node")]
    MissingMaster,
    /// More than one trust initializer is ambiguous.
    #[error("cluster topology cannot contain more than one master node")]
    MultipleMasters,
    /// Control-plane quorum is intentionally limited to supported shapes.
    #[error(
        "cluster topology must contain exactly one or three control-plane nodes, found {count}"
    )]
    InvalidControlPlaneCount { count: usize },
    /// Control endpoints must use private, routable IPv4 addresses.
    #[error("node `{node_id}` endpoint `{address}` must be private and non-loopback")]
    InvalidEndpointAddress { node_id: NodeId, address: Ipv4Addr },
    /// Port zero would select a different ephemeral port on each start.
    #[error("node `{node_id}` API port must be non-zero")]
    ZeroApiPort { node_id: NodeId },
    /// An API endpoint must unambiguously identify one node.
    #[error("endpoint `{address}:{port}` is assigned to more than one node")]
    DuplicateEndpoint { address: Ipv4Addr, port: u16 },
    /// Multi-node allocations use `/24`; larger historical standalone ranges remain supported.
    #[error(
        "node `{node_id}` workload network `{network}` must be a private IPv4 /24, or /16-/24 in a one-node topology"
    )]
    InvalidWorkloadSubnet { node_id: NodeId, network: Ipv4Cidr },
    /// Per-node workload address spaces cannot collide.
    #[error("workload networks for nodes `{first}` and `{second}` overlap")]
    OverlappingWorkloadSubnets { first: NodeId, second: NodeId },
    /// Host control traffic cannot traverse a workload address space.
    #[error(
        "node `{endpoint_node}` endpoint `{address}` is inside node `{subnet_node}` workload network"
    )]
    EndpointInsideWorkloadSubnet {
        subnet_node: NodeId,
        endpoint_node: NodeId,
        address: Ipv4Addr,
    },
    /// Control allowlists are restricted to private address space.
    #[error("control network {index} `{network}` must be private IPv4 space")]
    NonPrivateControlNetwork { index: usize, network: Ipv4Cidr },
    /// Control and workload traffic use disjoint address spaces.
    #[error("control network {index} overlaps the workload network for node `{node_id}`")]
    ControlNetworkOverlapsWorkload { index: usize, node_id: NodeId },
    /// A non-empty allowlist must admit all declared members.
    #[error("node `{node_id}` endpoint `{address}` is absent from the control allowlist")]
    EndpointOutsideControlNetworks { node_id: NodeId, address: Ipv4Addr },
    /// Bootstrap credentials need sufficient entropy before certificate issue.
    #[error("cluster join secret must contain at least 32 characters")]
    WeakJoinSecret,
    /// Persisted port allocation is invalid.
    #[error(transparent)]
    InvalidPorts(#[from] ClusterPortsError),
    /// Optional Tailscale gateway settings are invalid.
    #[error(transparent)]
    InvalidTailscale(#[from] TailscaleConfigError),
    /// Cloudflare connector credentials and replica policy must be bounded.
    #[error("invalid Cloudflare tunnel configuration: {0}")]
    InvalidCloudflare(#[from] CloudflareTunnelConfigError),
}
