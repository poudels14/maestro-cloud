use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::utils;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ServiceConfig {
    pub id: String,
    pub name: String,
    pub version: String,
    #[serde(default)]
    pub provider: ServiceProvider,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub build: Option<ServiceBuildConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image: Option<String>,
    pub deploy: ServiceDeployConfig,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ServiceProvider {
    #[default]
    Docker,
    Shell,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ServiceBuildConfig {
    pub repo: String,
    pub dockerfile_path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ServiceDeployConfig {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub flags: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ports: Vec<String>,
    pub command: Option<Command>,
    pub healthcheck_path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Command {
    pub command: String,
    #[serde(default)]
    pub args: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ActiveDeployment {
    pub deployment_id: String,
    pub version: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum DeploymentStatus {
    Queued,
    Building,
    Ready,
    Crashed,
    Stopped,
    Terminated,
    #[serde(alias = "CANCELLED")]
    Canceled,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ServiceDeployment {
    pub id: String,
    #[serde(default)]
    pub created_at: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deployed_at: Option<u64>,
    pub status: DeploymentStatus,
    pub config: ServiceConfig,
    pub git_commit: Option<GitCommitInfo>,
    pub build: Option<DeploymentBuildInfo>,
}

impl ServiceDeployment {
    pub fn new(config: ServiceConfig) -> Result<Self> {
        Ok(ServiceDeployment {
            id: utils::nanoid::unique_id(),
            created_at: utils::time::current_time_millis()?,
            deployed_at: None,
            status: DeploymentStatus::Queued,
            config,
            git_commit: None,
            build: None,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct GitCommitInfo {
    pub reference: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DeploymentBuildInfo {
    pub docker_image_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ServiceInfo {
    pub config: ServiceConfig,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<DeploymentStatus>,
}
