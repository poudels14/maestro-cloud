use maestro_core::{
    Assignment, ClusterFreeze, Deployment, DeploymentId, DeploymentStatus, GitCommit, NodeId,
    NodeRecord, PreviewSource, ServiceControls, ServiceId, ServiceSpec, SlackWebhook,
    UnschedulableReplica,
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
    SetDeploymentSpec {
        service_id: ServiceId,
        deployment_id: DeploymentId,
        spec: ServiceSpec,
    },
    SetDeploymentBuild {
        service_id: ServiceId,
        deployment_id: DeploymentId,
        commit: Option<GitCommit>,
        built_on: Option<NodeId>,
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
    SetUnschedulable {
        replicas: Vec<UnschedulableReplica>,
    },
    SetBlockedIp {
        ip: String,
        blocked: bool,
    },
    SetSlackWebhooks {
        webhooks: Vec<SlackWebhook>,
    },
    SetPreviewSource {
        id: ServiceId,
        source: Option<PreviewSource>,
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
    Unschedulable,
    Blocklist,
    SlackWebhooks,
    PreviewSource {
        id: ServiceId,
    },
    Everything,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Change {
    pub version: u64,
    pub keys: Vec<ChangeKey>,
}
