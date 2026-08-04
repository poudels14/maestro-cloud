use std::collections::BTreeMap;
use std::net::IpAddr;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::{DeploymentPhase, initial_restart_generation};
use crate::{
    AssignmentId, Condition, DeploymentId, Generation, MaskedSecret, NodeId, Object,
    ReplicaStateId, ServiceId, Timestamp, WorkloadId,
};

/// Desired placement of one deployment replica on one node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct AssignmentSpec {
    /// Service being placed.
    pub service_id: ServiceId,
    /// Immutable deployment being placed.
    pub deployment_id: DeploymentId,
    /// Deployment workload generation this assignment realizes.
    #[serde(default = "initial_restart_generation")]
    #[schemars(default = "initial_restart_generation")]
    pub restart_generation: Generation,
    /// Zero-based replica slot within the deployment.
    pub replica_index: u32,
    /// Node selected by the scheduler.
    pub node_id: NodeId,
    /// Monotonic epoch incremented when a slot moves to another node.
    pub placement_epoch: u64,
    /// Cluster-routable address reserved before start, or absent for runtime IPAM.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workload_address: Option<IpAddr>,
    /// Assignment superseded by this placement, when one is draining.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub replaces_assignment_id: Option<AssignmentId>,
}

/// Runtime lifecycle of an assignment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum AssignmentPhase {
    /// Waiting for the node agent to create a workload.
    Pending,
    /// The assigned workload is running.
    Running,
    /// The workload is stopping without accepting new traffic.
    Draining,
    /// The workload stopped and no longer owns runtime state.
    Stopped,
    /// The node agent could not converge the assignment.
    Failed,
}

/// Observed workload identity and lifecycle for an assignment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct AssignmentStatus {
    /// Current runtime phase.
    pub phase: AssignmentPhase,
    /// Runtime workload identity created for this assignment.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workload_id: Option<WorkloadId>,
    /// Address observed after the runtime attached the workload.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workload_address: Option<IpAddr>,
    /// Generic runtime and drain evidence.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<Condition>,
}

/// A scheduled workload assignment resource.
pub type Assignment = Object<AssignmentId, AssignmentSpec, AssignmentStatus>;

/// Returns the runtime-observed address, falling back to scheduler-owned IPAM.
pub fn assignment_workload_address(assignment: &Assignment) -> Option<IpAddr> {
    assignment
        .status
        .workload_address
        .or(assignment.spec.workload_address)
}

/// Immutable placement identity retained after an assignment stops.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PlacementHistorySpec {
    /// Service whose replica occupied the placement.
    pub service_id: ServiceId,
    /// Immutable deployment whose replica occupied the placement.
    pub deployment_id: DeploymentId,
    /// Zero-based replica slot within the deployment.
    pub replica_index: u32,
    /// Node that hosted the workload.
    pub node_id: NodeId,
    /// Node API address captured when the workload started.
    pub cluster_host_address: IpAddr,
    /// Node API port captured when the workload started.
    pub cluster_api_port: u16,
    /// Runtime hostname captured when the workload started.
    pub container_hostname: String,
}

/// Start and terminal timing for one placement.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PlacementHistoryStatus {
    /// Time the workload first occupied the placement.
    pub started_at: Timestamp,
    /// Time the workload left the placement, or `None` while it remains active.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ended_at: Option<Timestamp>,
}

/// Durable audit record for one assignment placement.
pub type PlacementHistory = Object<AssignmentId, PlacementHistorySpec, PlacementHistoryStatus>;

/// Desired identity of one observable deployment replica slot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ReplicaStateSpec {
    /// Service owning the slot.
    pub service_id: ServiceId,
    /// Deployment owning the slot.
    pub deployment_id: DeploymentId,
    /// Current assignment for the slot.
    pub assignment_id: AssignmentId,
    /// Zero-based replica slot within the deployment.
    pub replica_index: u32,
}

/// Health and restart evidence observed for one replica slot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ReplicaStateStatus {
    /// Deployment lifecycle phase observed for the replica.
    pub phase: DeploymentPhase,
    /// Current node when assigned.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub node_id: Option<NodeId>,
    /// Current runtime workload identity.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workload_id: Option<WorkloadId>,
    /// Consecutive failed health probes.
    pub healthcheck_failures: u32,
    /// Restart attempts consumed by this assignment.
    pub restart_attempts: u32,
    /// Attempt durably reserved before a runtime restart and cleared after it is observed running.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub restart_pending_attempt: Option<u32>,
    /// Earliest UTC time at which the pending restart may be attempted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub restart_not_before: Option<Timestamp>,
    /// API-safe secret observations resolved and mounted for this replica.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolved_secrets: Option<BTreeMap<String, MaskedSecret>>,
    /// Generic health and exhaustion evidence.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<Condition>,
}

/// An observed deployment replica resource.
pub type ReplicaState = Object<ReplicaStateId, ReplicaStateSpec, ReplicaStateStatus>;
