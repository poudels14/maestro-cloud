use std::collections::BTreeMap;
use std::net::{IpAddr, SocketAddr};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{Condition, NodeId, NodeInstanceId, NodeNetworkId, Object, Timestamp};

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
    /// Last time the node renewed its liveness session.
    pub last_seen: Timestamp,
    /// Generic readiness, availability, drain, and maintenance evidence.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<Condition>,
}

/// A cluster node resource.
pub type Node = Object<NodeId, NodeSpec, NodeStatus>;

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
