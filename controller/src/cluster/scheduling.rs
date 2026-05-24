//! Cluster-wide types for scheduling: ports, assignments, placement.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::types::NodeId;
use crate::deployment::types::NodeAffinity;

pub type Port = u16;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "camelCase")]
pub struct ReplicaSlot {
    pub service_id: String,
    pub replica_index: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Assignment {
    pub service_id: String,
    pub deployment_id: String,
    pub replica_index: u32,
    pub node_id: NodeId,
    pub port: Port,
    pub created_at_ms: u64,
}

impl Assignment {
    pub fn slot(&self) -> ReplicaSlot {
        ReplicaSlot {
            service_id: self.service_id.clone(),
            replica_index: self.replica_index,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ServiceScheduleSpec {
    pub service_id: String,
    pub deployment_id: String,
    pub desired_replicas: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_affinity: Option<NodeAffinity>,
    #[serde(default)]
    pub assigned_port: Option<Port>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct NodeCapacity {
    pub node_id: NodeId,
    pub labels: BTreeMap<String, String>,
    pub can_run_workloads: bool,
}

#[derive(Debug, Clone, Default)]
pub struct SchedulingError {
    pub messages: Vec<String>,
}

impl std::fmt::Display for SchedulingError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}", self.messages.join("; "))
    }
}

impl std::error::Error for SchedulingError {}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SchedulePlan {
    pub assignments: Vec<Assignment>,
    pub unschedulable: Vec<UnschedulableReplica>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnschedulableReplica {
    pub slot: ReplicaSlot,
    pub reason: String,
}
