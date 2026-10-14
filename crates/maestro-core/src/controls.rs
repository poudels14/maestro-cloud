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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SlackCategory {
    Info,
    Error,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SlackWebhook {
    pub id: String,
    pub name: String,
    pub url: String,
    pub categories: Vec<SlackCategory>,
    pub enabled: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PreviewSource {
    pub base_service_id: crate::ServiceId,
    pub pr_number: u64,
    pub head_ref: String,
    pub head_sha: String,
    pub title: String,
    pub created_at_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub closed_at_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comment_id: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub github_deployment_id: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reported: Option<String>,
}
