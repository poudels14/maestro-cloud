use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::ids::{DeploymentId, NodeId, ReplicaId, ServiceId};
use crate::resources::{EffectiveResources, OomPriority};
use crate::service::{Command, ServiceSpec};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum DeploymentStatus {
    Queued,
    Building,
    Deploying,
    Ready,
    Draining,
    Stopped,
    Failed,
    Cancelled,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Deployment {
    pub id: DeploymentId,
    pub service_id: ServiceId,
    pub spec: ServiceSpec,
    pub status: DeploymentStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure: Option<String>,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Assignment {
    pub replica_id: ReplicaId,
    pub service_id: ServiceId,
    pub deployment_id: DeploymentId,
    pub node_id: NodeId,
    pub index: u32,
    pub image: String,
    pub resources: EffectiveResources,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplicaPlan {
    pub replica_id: ReplicaId,
    pub service_id: ServiceId,
    pub deployment_id: DeploymentId,
    pub image: String,
    pub resources: EffectiveResources,
    pub oom_priority: OomPriority,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command: Option<Command>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub env: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub healthcheck: Option<HealthcheckPlan>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_restarts: Option<u32>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HealthcheckPlan {
    pub path: String,
    pub port: u16,
    pub interval_secs: u32,
}

impl ReplicaPlan {
    pub fn new(assignment: &Assignment, spec: &ServiceSpec) -> Self {
        let deploy = &spec.deploy;
        let port = spec
            .ingress
            .as_ref()
            .map(|ingress| ingress.port)
            .or_else(|| deploy.expose_ports.first().copied());
        let healthcheck = deploy
            .healthcheck_path
            .clone()
            .zip(port)
            .map(|(path, port)| HealthcheckPlan {
                path,
                port,
                interval_secs: deploy.healthcheck_interval,
            });
        Self {
            replica_id: assignment.replica_id.clone(),
            service_id: assignment.service_id.clone(),
            deployment_id: assignment.deployment_id.clone(),
            image: assignment.image.clone(),
            resources: assignment.resources,
            oom_priority: deploy.oom_priority,
            command: deploy.command.clone(),
            env: deploy.env.items.clone(),
            healthcheck,
            max_restarts: deploy.max_restarts,
        }
    }
}

impl DeploymentStatus {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Stopped | Self::Failed | Self::Cancelled)
    }
}
