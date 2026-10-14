use std::collections::BTreeMap;
use std::net::SocketAddr;

use serde::{Deserialize, Serialize};

use crate::ids::NodeId;
use crate::resources::NodeCapacity;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum NodeRole {
    Voter,
    #[serde(alias = "agent")]
    Worker,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum NodeState {
    Active,
    Draining,
    Removed,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeRecord {
    pub id: NodeId,
    pub name: String,
    pub role: NodeRole,
    pub endpoint: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subnet: Option<String>,
    pub runs_workloads: bool,
    pub capacity: NodeCapacity,
    pub reserved: NodeCapacity,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub labels: BTreeMap<String, String>,
    pub state: NodeState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wireguard: Option<WireGuardInfo>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WireGuardInfo {
    pub public_key: String,
    pub endpoint: SocketAddr,
}

impl NodeRecord {
    pub fn allocatable(&self) -> NodeCapacity {
        NodeCapacity {
            cpu: self.capacity.cpu.saturating_sub(self.reserved.cpu),
            memory: self.capacity.memory.saturating_sub(self.reserved.memory),
        }
    }

    pub fn is_schedulable(&self) -> bool {
        self.runs_workloads && self.state == NodeState::Active
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_workers_stored_under_the_old_agent_name() {
        assert_eq!(
            serde_json::from_str::<NodeRole>("\"agent\"").unwrap(),
            NodeRole::Worker
        );
        assert_eq!(
            serde_json::to_string(&NodeRole::Worker).unwrap(),
            "\"worker\""
        );
    }
}
