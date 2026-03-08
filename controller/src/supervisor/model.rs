use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ServiceRuntimeConfig {
    pub command: String,
    pub restart_delay_ms: u64,
    pub max_restarts: u32,
    pub shutdown_grace_period_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamedServiceConfig {
    pub name: String,
    pub config: ServiceRuntimeConfig,
}
