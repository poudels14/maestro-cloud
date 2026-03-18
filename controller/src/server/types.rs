use crate::deployment::types::{
    DeploymentStatus, IngressConfig, ServiceBuildConfig, ServiceConfig, ServiceDeployConfig,
    ServiceProvider,
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
    pub(crate) service: ServiceConfig,
    pub(crate) status: Option<DeploymentStatus>,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub(crate) system: bool,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub(crate) deploy_frozen: bool,
}

#[derive(Debug, Clone, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RolloutServiceRequest {
    pub(crate) id: String,
    pub(crate) name: String,
    #[serde(default)]
    pub(crate) provider: ServiceProvider,
    #[serde(default)]
    pub(crate) build: Option<ServiceBuildConfig>,
    #[serde(default)]
    pub(crate) image: Option<String>,
    pub(crate) deploy: ServiceDeployConfig,
    #[serde(default)]
    pub(crate) ingress: Option<IngressConfig>,
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
