use serde::{Deserialize, Serialize};

/// Mirrors the controller's ServiceDeployment for JSON deserialization.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ServiceDeployment {
    pub id: String,
    #[serde(default)]
    pub created_at: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deployed_at: Option<u64>,
    pub status: String,
    pub config: ServiceConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ServiceConfig {
    pub id: String,
    pub name: String,
    pub version: String,
    #[serde(default)]
    pub provider: String,
    pub deploy: ServiceDeployConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ServiceDeployConfig {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ports: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub healthcheck_path: Option<String>,
}

/// Mirrors the controller's ServiceInfo for JSON round-tripping.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ServiceInfo {
    pub config: serde_json::Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
}
