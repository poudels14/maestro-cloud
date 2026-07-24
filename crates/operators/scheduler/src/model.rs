use std::collections::BTreeSet;
use std::fmt::{Display, Formatter};

use kernel_api::{
    Assignment, AssignmentId, ClusterId, DeploymentId, NodeId, NodeRole, PlacementConstraint,
    ServiceId, WorkloadNetworkMode,
};

/// One deployment whose replica slots must remain assigned.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeploymentGroup {
    /// Immutable deployment identity.
    pub deployment_id: DeploymentId,
    /// Desired workload generation for every assignment in the group.
    pub restart_generation: kernel_api::Generation,
    /// Desired slots for this deployment.
    pub replicas: u32,
}

/// Exact unhealthy observation that may trigger replacement of one assignment.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct UnhealthySlot {
    /// Deployment whose replica was observed.
    pub deployment_id: DeploymentId,
    /// Node on which the observation was made.
    pub node_id: NodeId,
    /// Stable replica slot within the deployment.
    pub replica_index: u32,
    /// Assignment identity that produced the observation.
    pub assignment_id: AssignmentId,
}

/// Desired scheduling view for one service and all retained rollout groups.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceSchedule {
    /// Service being placed.
    pub service_id: ServiceId,
    /// Ordered deployment groups; earlier groups establish rollout co-location preference.
    pub groups: Vec<DeploymentGroup>,
    /// Hard node and label selection constraints.
    pub placement: PlacementConstraint,
    /// Assignment-specific failed health observations.
    pub unhealthy_slots: BTreeSet<UnhealthySlot>,
    /// Slots whose restart budget is exhausted and may not be replaced automatically.
    pub exhausted_slots: BTreeSet<(DeploymentId, u32)>,
}

/// Whether a node may receive new assignments in this planning pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeSchedulingState {
    /// Node liveness, data plane, and administrative state permit placement.
    Available,
    /// Node is temporarily unreachable; grace-held assignments may remain.
    Unavailable,
    /// Node was explicitly drained or placed into maintenance.
    Unschedulable,
}

/// Scheduler-owned projection of node and node-network resources.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScheduleNode {
    /// Stable node identity.
    pub node_id: NodeId,
    /// Workload and control-plane role.
    pub role: NodeRole,
    /// Scheduling labels copied from the node resource.
    pub labels: std::collections::BTreeMap<String, String>,
    /// Address ownership advertised by the node.
    pub workload_network_mode: WorkloadNetworkMode,
    /// Canonical IPv4 subnet for cluster-routed nodes.
    pub workload_subnet: Option<String>,
    /// Current placement eligibility derived before invoking the pure planner.
    pub state: NodeSchedulingState,
}

/// Complete deterministic input for one scheduling pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScheduleInput {
    /// Cluster scope mixed into generated assignment identities.
    pub cluster_id: ClusterId,
    /// Service rollout groups requiring placements.
    pub services: Vec<ServiceSchedule>,
    /// Known node scheduling views.
    pub nodes: Vec<ScheduleNode>,
    /// Existing assignments from the same linearizable resource snapshot.
    pub current: Vec<Assignment>,
    /// Assignments retained during node-loss or drain grace periods.
    pub held: BTreeSet<AssignmentId>,
}

/// Stable reason a requested replica could not receive a new assignment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UnschedulableReason {
    /// A hard node or label constraint matched no known node.
    AffinityMatchesNoNode,
    /// No node currently permits new workloads.
    NoSchedulableNode,
    /// The failed assignment's node was the only eligible placement.
    NoAlternateNode,
    /// The selected node published a malformed or unsupported workload subnet.
    InvalidWorkloadSubnet {
        /// Node with the invalid network publication.
        node_id: NodeId,
        /// Rejected CIDR text.
        subnet: String,
    },
    /// Every workload address in the selected node subnet is already reserved.
    WorkloadAddressCapacityExhausted {
        /// Node whose address pool is full.
        node_id: NodeId,
    },
    /// Host-backed volumes in one service belong to different nodes.
    ConflictingHostVolumeNodes,
    /// An explicit placement node conflicts with the service's host-backed volume owner.
    HostVolumePlacementMismatch {
        /// Node required by the mounted host path.
        volume_node_id: NodeId,
        /// Different node required by service placement.
        placement_node_id: NodeId,
    },
}

impl Display for UnschedulableReason {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AffinityMatchesNoNode => formatter.write_str("affinity matches no node"),
            Self::NoSchedulableNode => formatter.write_str("no schedulable node"),
            Self::NoAlternateNode => {
                formatter.write_str("no alternate schedulable node for replacement")
            }
            Self::InvalidWorkloadSubnet { node_id, subnet } => write!(
                formatter,
                "node `{node_id}` published invalid workload subnet `{subnet}`"
            ),
            Self::WorkloadAddressCapacityExhausted { node_id } => write!(
                formatter,
                "workload address capacity exhausted on node `{node_id}`"
            ),
            Self::ConflictingHostVolumeNodes => {
                formatter.write_str("host-backed volumes require different nodes")
            }
            Self::HostVolumePlacementMismatch {
                volume_node_id,
                placement_node_id,
            } => write!(
                formatter,
                "placement node `{placement_node_id}` conflicts with host-volume node `{volume_node_id}`"
            ),
        }
    }
}

/// One replica slot that the planner could not place.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnschedulableReplica {
    /// Service owning the replica.
    pub service_id: ServiceId,
    /// Deployment owning the replica.
    pub deployment_id: DeploymentId,
    /// Stable replica slot within the deployment.
    pub replica_index: u32,
    /// Matchable placement failure.
    pub reason: UnschedulableReason,
}

/// Desired assignments and isolated placement failures from one pure pass.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SchedulePlan {
    /// Fully addressed assignments ready for an atomic store diff.
    pub assignments: Vec<Assignment>,
    /// Requested replica slots that could not be newly placed.
    pub unschedulable: Vec<UnschedulableReplica>,
}
