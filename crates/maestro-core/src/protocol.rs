use std::collections::BTreeMap;
use std::net::{IpAddr, SocketAddr};

use serde::{Deserialize, Serialize};

use crate::config::ClusterSettings;
use crate::deployment::ReplicaPlan;
use crate::ids::NodeId;
use crate::node::{NodeRole, WireGuardInfo};
use crate::resources::NodeCapacity;
use crate::routing::RouteTable;
use crate::status::ReplicaStatus;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum AgentMessage {
    Hello { node_id: NodeId },
    Replicas { statuses: Vec<ReplicaStatus> },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum ControlMessage {
    Plans {
        plans: Vec<ReplicaPlan>,
    },
    Routes {
        table: RouteTable,
        dns: BTreeMap<String, Vec<IpAddr>>,
    },
    Mesh {
        peers: Vec<MeshPeer>,
    },
    NotLeader {
        leader: Option<String>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MeshPeer {
    pub node_id: NodeId,
    pub public_key: String,
    pub endpoint: SocketAddr,
    pub subnet: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JoinRequest {
    pub secret: String,
    pub name: String,
    pub role: NodeRole,
    pub advertise: String,
    pub capacity: NodeCapacity,
    pub reserved: NodeCapacity,
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    pub wireguard: Option<WireGuardInfo>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JoinResponse {
    pub node_id: NodeId,
    pub subnet: String,
    pub cluster: ClusterSettings,
    pub voters: Vec<String>,
    pub ca_pem: String,
    pub cert_pem: String,
    pub key_pem: String,
    pub voter_secrets: Option<VoterSecrets>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VoterSecrets {
    pub ca_key_pem: String,
    pub encryption_key: Vec<u8>,
    pub api_key: Vec<u8>,
    pub join_secret: String,
}
