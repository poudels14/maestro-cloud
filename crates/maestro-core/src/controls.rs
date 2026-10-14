use serde::{Deserialize, Serialize};

pub const MAX_REPLICAS_OVERRIDE: u32 = 25;

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ServiceControls {
    #[serde(default)]
    pub frozen: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replicas_override: Option<u32>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterFreeze {
    pub reason: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upgrade_run_id: Option<String>,
    pub at_ms: u64,
}

impl ServiceControls {
    pub fn effective_replicas(&self, configured: u32) -> u32 {
        self.replicas_override
            .map_or(configured, |replicas| replicas.max(configured))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UnschedulableReplica {
    pub service_id: crate::ServiceId,
    pub deployment_id: crate::DeploymentId,
    pub replica_index: u32,
    pub reason: String,
}
