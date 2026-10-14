use maestro_core::{
    Assignment, ClusterFreeze, Deployment, DeploymentId, DeploymentStatus, NodeId, NodeRecord,
    ServiceControls, ServiceId, ServiceSpec,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum Mutation {
    PutService {
        id: ServiceId,
        spec: ServiceSpec,
    },
    DeleteService {
        id: ServiceId,
    },
    CreateDeployment {
        deployment: Deployment,
    },
    SetDeploymentStatus {
        service_id: ServiceId,
        deployment_id: DeploymentId,
        status: DeploymentStatus,
        image: Option<String>,
        failure: Option<String>,
        at_ms: u64,
    },
    DeleteDeployment {
        service_id: ServiceId,
        deployment_id: DeploymentId,
    },
    SetServiceControls {
        id: ServiceId,
        controls: ServiceControls,
    },
    SetClusterFreeze {
        freeze: Option<ClusterFreeze>,
    },
    PruneDeployments {
        service_id: ServiceId,
        keep: usize,
    },
    PutNode {
        node: NodeRecord,
    },
    RemoveNode {
        id: NodeId,
    },
    SetNodeAssignments {
        node_id: NodeId,
        assignments: Vec<Assignment>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum ChangeKey {
    Service {
        id: ServiceId,
    },
    Deployment {
        service_id: ServiceId,
        deployment_id: DeploymentId,
    },
    Node {
        id: NodeId,
    },
    NodeAssignments {
        node_id: NodeId,
    },
    ServiceControls {
        id: ServiceId,
    },
    ClusterFreeze,
    Everything,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Change {
    pub version: u64,
    pub keys: Vec<ChangeKey>,
}
