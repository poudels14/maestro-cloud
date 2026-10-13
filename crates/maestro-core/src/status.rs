use std::net::IpAddr;

use serde::{Deserialize, Serialize};

use crate::ids::{DeploymentId, ReplicaId, ServiceId};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ReplicaPhase {
    Starting,
    Running,
    Healthy,
    Unhealthy,
    Restarting,
    Failed,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplicaStatus {
    pub replica_id: ReplicaId,
    pub service_id: ServiceId,
    pub deployment_id: DeploymentId,
    pub phase: ReplicaPhase,
    pub ip: Option<IpAddr>,
    pub restarts: u32,
    pub consecutive_failures: u32,
    pub last_error: Option<String>,
}

impl ReplicaPhase {
    pub fn is_live(self) -> bool {
        matches!(self, Self::Running | Self::Healthy | Self::Unhealthy)
    }
}
