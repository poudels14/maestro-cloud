use std::collections::BTreeMap;
use std::net::{IpAddr, SocketAddr};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{
    ClusterId, Condition, DeploymentId, NodeFirewallId, NodeId, NodeInstanceId, NodeNetworkId,
    Object, ServiceId, Timestamp,
};

/// Operator-facing summary of one cluster's durable node topology.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ClusterInfo {
    /// Stable identity that namespaces every cluster resource.
    pub cluster_id: ClusterId,
    /// Number of declared node resources.
    pub node_count: u64,
    /// Nodes capable of running cluster controllers.
    pub control_plane_node_count: u64,
    /// Nodes eligible to receive workload assignments.
    pub workload_node_count: u64,
}

/// One requested replica slot for which the scheduler found no valid placement.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct UnschedulableReplica {
    /// Service owning the replica.
    pub service_id: ServiceId,
    /// Deployment owning the replica.
    pub deployment_id: DeploymentId,
    /// Stable replica slot within the deployment.
    pub replica_index: u32,
    /// Human-readable placement failure suitable for operator diagnostics.
    pub reason: String,
}

/// Scheduling and control-plane capability assigned to a node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum NodeRole {
    /// Initial control-plane node that can bootstrap a cluster and run workloads.
    Master,
    /// Control-plane node that also runs workloads.
    Hybrid,
    /// Control-plane node that does not run user workloads.
    ControlPlane,
    /// Workload node without control-plane responsibility.
    Worker,
}

impl NodeRole {
    /// Whether this role runs cluster controllers.
    pub fn is_control_plane(self) -> bool {
        matches!(self, Self::Master | Self::Hybrid | Self::ControlPlane)
    }

    /// Whether this role accepts workload assignments.
    pub fn runs_workloads(self) -> bool {
        matches!(self, Self::Master | Self::Hybrid | Self::Worker)
    }
}

/// Desired identity and connectivity of a cluster node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct NodeSpec {
    /// Operator-facing hostname.
    pub hostname: String,
    /// Stable host address used for control-plane traffic.
    pub host_address: IpAddr,
    /// Scheduling and control-plane capability.
    pub role: NodeRole,
    /// Arbitrary scheduling labels advertised by the node.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub scheduling_labels: BTreeMap<String, String>,
}

/// Current reachability and lifecycle of a cluster node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct NodeStatus {
    /// Identity of the currently running daemon instance.
    pub instance_id: NodeInstanceId,
    /// Semantic version reported by the currently running daemon.
    #[serde(default)]
    pub version: String,
    /// Last time the node renewed its liveness session.
    pub last_seen: Timestamp,
    /// Generic readiness, availability, drain, and maintenance evidence.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<Condition>,
}

/// A cluster node resource.
pub type Node = Object<NodeId, NodeSpec, NodeStatus>;

/// Immutable identity and topology evidence retained after node removal.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct NodeTombstoneSpec {
    /// Last private control-plane address owned by the removed node.
    pub host_address: IpAddr,
    /// Last cluster role held by the removed node.
    pub role: NodeRole,
    /// Time at which removal was requested.
    pub requested_at: Timestamp,
}

/// Observed completion of one irreversible node-identity removal.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct NodeTombstoneStatus {
    /// Time at which membership and durable node state were removed.
    pub removed_at: Timestamp,
}

/// Durable guard preventing reuse of a removed node identity.
pub type NodeTombstone = Object<NodeId, NodeTombstoneSpec, NodeTombstoneStatus>;

/// Desired WireGuard publication and workload subnet for one node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct NodeNetworkSpec {
    /// Node that owns the private key corresponding to this publication.
    pub node_id: NodeId,
    /// WireGuard public key; the private key is never stored in a resource.
    pub public_key: String,
    /// Directly reachable WireGuard UDP endpoint.
    pub endpoint: SocketAddr,
    /// Canonical CIDR reserved for workloads on the node.
    pub workload_subnet: String,
    /// Mesh interface MTU applied to both WireGuard and workload networking.
    pub mtu_bytes: u16,
}

/// Observed mesh programming state for one node network.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct NodeNetworkStatus {
    /// Desired generation most recently applied by the node agent.
    pub applied_generation: crate::Generation,
    /// Generic readiness and configuration evidence.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<Condition>,
}

/// A node network resource.
pub type NodeNetwork = Object<NodeNetworkId, NodeNetworkSpec, NodeNetworkStatus>;

/// Complete generated nftables input desired on one node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct NodeFirewallSpec {
    /// Node whose host and forwarding hooks own this ruleset.
    pub node_id: NodeId,
    /// Owned nftables table replaced by the script.
    pub table_name: String,
    /// Complete input applied in one atomic `nft -f` transaction.
    pub script: String,
    /// SHA-256 digest of the exact script bytes.
    pub digest: String,
}

/// Node-local application evidence for one desired firewall generation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct NodeFirewallStatus {
    /// Desired generation most recently applied by the node agent.
    pub applied_generation: crate::Generation,
    /// Digest of the exact script accepted by the backend.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub applied_digest: Option<String>,
    /// Generic validation and application evidence.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<Condition>,
}

/// Desired and observed firewall state for one node.
pub type NodeFirewall = Object<NodeFirewallId, NodeFirewallSpec, NodeFirewallStatus>;
