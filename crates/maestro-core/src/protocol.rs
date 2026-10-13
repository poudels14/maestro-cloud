use std::collections::BTreeMap;
use std::net::IpAddr;

use serde::{Deserialize, Serialize};

use crate::deployment::ReplicaPlan;
use crate::ids::NodeId;
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
    NotLeader {
        leader: Option<String>,
    },
}
