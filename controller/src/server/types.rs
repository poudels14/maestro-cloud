use crate::deployment::types::{
    DeploymentStatus, ServiceBuildConfig, ServiceConfig, ServiceDeployConfig, ServiceProvider,
};

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PatchServiceResponse {
    pub(crate) queued: bool,
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
}

#[derive(Debug, Clone, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PatchServiceRequest {
    pub(crate) id: String,
    pub(crate) name: String,
    #[serde(default)]
    pub(crate) provider: ServiceProvider,
    #[serde(default)]
    pub(crate) build: Option<ServiceBuildConfig>,
    #[serde(default)]
    pub(crate) image: Option<String>,
    pub(crate) deploy: ServiceDeployConfig,
}

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CancelDeploymentResponse {
    pub(crate) canceled: bool,
    pub(crate) service_id: String,
    pub(crate) deployment_id: String,
    pub(crate) status: DeploymentStatus,
}
