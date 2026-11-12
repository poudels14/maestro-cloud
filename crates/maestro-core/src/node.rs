use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

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

/// The first address of a node's container subnet: its bridge gateway, where the node's DNS
/// server listens.
pub fn subnet_gateway(subnet: &str) -> Option<IpAddr> {
    let (network, _) = subnet.split_once('/')?;
    let [first, second, third, _] = network.parse::<Ipv4Addr>().ok()?.octets();
    Some(IpAddr::from([first, second, third, 1]))
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

    #[test]
    fn finds_the_gateway_of_a_node_subnet() {
        assert_eq!(
            subnet_gateway("10.50.3.0/24"),
            Some("10.50.3.1".parse().unwrap())
        );
        assert_eq!(subnet_gateway("10.50.3.0"), None);
        assert_eq!(subnet_gateway("fd00::/64"), None);
    }
}
