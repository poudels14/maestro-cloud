use crate::deployment::types::{
    DeploymentStatus, DeploymentWithReplicas, IngressConfig, PreviewConfig, ReplicaState,
    ServiceBuildConfig, ServiceConfig, ServiceDeployConfig, ServiceDeployment,
};

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RolloutServiceResponse {
    pub(crate) queued: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) replicas: Option<u32>,
    pub(crate) deployment_id: Option<String>,
    pub(crate) deployment_index: Option<usize>,
    pub(crate) service_id: String,
    pub(crate) status: Option<DeploymentStatus>,
    pub(crate) version: String,
}

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ServiceListItem {
    #[serde(flatten)]
    service: ServiceConfig,
    status: Option<DeploymentStatus>,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    system: bool,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    deploy_frozen: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    replicas_override: Option<u32>,
}

impl ServiceListItem {
    pub(crate) fn new(
        service: ServiceConfig,
        status: Option<DeploymentStatus>,
        system: bool,
        deploy_frozen: bool,
        replicas_override: Option<u32>,
    ) -> Self {
        Self {
            service: service.mask_secrets(),
            status,
            system,
            deploy_frozen,
            replicas_override,
        }
    }
}

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DeploymentListItem {
    #[serde(flatten)]
    deployment: ServiceDeployment,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    replicas: Vec<ReplicaState>,
}

impl DeploymentListItem {
    pub(crate) fn new(item: DeploymentWithReplicas) -> Self {
        let mut deployment = item.deployment;
        deployment.config = deployment.config.mask_secrets();
        Self {
            deployment,
            replicas: item.replicas,
        }
    }
}

#[derive(Debug, Clone, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RolloutServiceRequest {
    pub(crate) id: String,
    pub(crate) name: String,
    #[serde(default)]
    pub(crate) build: Option<ServiceBuildConfig>,
    #[serde(default)]
    pub(crate) image: Option<String>,
    pub(crate) deploy: ServiceDeployConfig,
    #[serde(default)]
    pub(crate) ingress: Option<IngressConfig>,
    #[serde(default)]
    pub(crate) preview: Option<PreviewConfig>,
}

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct UploadServiceResponse {
    pub(crate) service_id: String,
    pub(crate) deployment_id: String,
    pub(crate) version: String,
    pub(crate) name: String,
}

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CancelDeploymentResponse {
    pub(crate) canceled: bool,
    pub(crate) service_id: String,
    pub(crate) deployment_id: String,
    pub(crate) status: DeploymentStatus,
}

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RemoveDeploymentResponse {
    pub(crate) removed: bool,
    pub(crate) service_id: String,
    pub(crate) deployment_id: String,
    pub(crate) status: DeploymentStatus,
}

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RolloutDiffResponse {
    pub(crate) service_id: String,
    pub(crate) status: RolloutDiffStatus,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) changes: Vec<RolloutChange>,
}

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum RolloutDiffStatus {
    New,
    Changed,
    Unchanged,
}

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RolloutChange {
    pub(crate) field: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) from: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) to: Option<String>,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub(crate) struct FreezeRequest {
    pub(crate) frozen: bool,
}

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct FreezeResponse {
    pub(crate) service_id: String,
    pub(crate) deploy_frozen: bool,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub(crate) struct ReplicasOverrideRequest {
    pub(crate) replicas: u32,
}

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ReplicasResponse {
    pub(crate) service_id: String,
    pub(crate) replicas: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) replicas_override: Option<u32>,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub(crate) struct BlockedIpRequest {
    pub(crate) ip: String,
    pub(crate) blocked: bool,
}

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct BlockedIpsResponse {
    pub(crate) blocked_ips: Vec<String>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct UpgradeSystemRequest {
    pub(crate) version: String,
    #[serde(default)]
    pub(crate) run_id: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ClusterUpgradeRequest {
    pub(crate) target_version: String,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ClusterRestartRequest {
    #[serde(default)]
    pub(crate) node_id: Option<String>,
    #[serde(default)]
    pub(crate) all: bool,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ClusterUnfreezeRequest {
    pub(crate) upgrade_run_id: String,
}

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SlackWebhookView {
    pub(crate) id: String,
    pub(crate) name: String,
    pub(crate) url: String,
    pub(crate) categories: Vec<crate::slack::SlackCategory>,
    pub(crate) enabled: bool,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CreateSlackWebhookRequest {
    pub(crate) name: String,
    pub(crate) url: String,
    pub(crate) categories: Vec<crate::slack::SlackCategory>,
    #[serde(default)]
    pub(crate) enabled: Option<bool>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct UpdateSlackWebhookRequest {
    #[serde(default)]
    pub(crate) name: Option<String>,
    #[serde(default)]
    pub(crate) url: Option<String>,
    #[serde(default)]
    pub(crate) categories: Option<Vec<crate::slack::SlackCategory>>,
    #[serde(default)]
    pub(crate) enabled: Option<bool>,
}
