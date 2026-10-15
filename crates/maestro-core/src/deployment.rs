use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::ids::{DeploymentId, NodeId, ReplicaId, ServiceId};
use crate::resources::{EffectiveResources, OomPriority};
use crate::routing::affinity_token;
use crate::service::{Command, EgressRule, SecretsSpec, ServiceSpec, VolumeMount};

const TEMPLATE_OPEN: &str = "${{";
const TEMPLATE_CLOSE: &str = "}}";

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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub git_commit: Option<GitCommit>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub built_on: Option<NodeId>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GitCommit {
    pub reference: String,
    pub message: String,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retiring_since_ms: Option<u64>,
}

impl Assignment {
    pub fn is_retiring(&self) -> bool {
        self.retiring_since_ms.is_some()
    }
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
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub volumes: Vec<VolumeMount>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secrets: Option<SecretsSpec>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image_source: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub egress: Vec<EgressRule>,
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
        let variables = spec.ingress_variables();
        Self {
            replica_id: assignment.replica_id.clone(),
            service_id: assignment.service_id.clone(),
            deployment_id: assignment.deployment_id.clone(),
            image: assignment.image.clone(),
            resources: assignment.resources,
            oom_priority: deploy.oom_priority,
            command: deploy.command.clone(),
            env: replica_env(assignment, &variables, &deploy.env.items),
            healthcheck,
            max_restarts: deploy.max_restarts,
            volumes: deploy.volumes.clone(),
            secrets: deploy
                .secrets
                .clone()
                .filter(|secrets| !secrets.items.is_empty())
                .map(|secrets| SecretsSpec {
                    source: None,
                    items: expand_all(&secrets.items, &variables),
                    ..secrets
                }),
            image_source: None,
            egress: deploy.egress.allow.clone(),
        }
    }
}

fn replica_env(
    assignment: &Assignment,
    variables: &BTreeMap<String, String>,
    items: &BTreeMap<String, String>,
) -> BTreeMap<String, String> {
    let mut env = BTreeMap::from([
        (
            "MAESTRO_NODE_ID".to_string(),
            assignment.node_id.to_string(),
        ),
        (
            "MAESTRO_REPLICA_INDEX".to_string(),
            assignment.index.to_string(),
        ),
        (
            "MAESTRO_REPLICA_ID".to_string(),
            assignment.replica_id.to_string(),
        ),
        (
            "MAESTRO_AFFINITY_TOKEN".to_string(),
            affinity_token(&assignment.replica_id),
        ),
    ]);
    env.extend(variables.clone());
    env.extend(expand_all(items, variables));
    env
}

fn expand_all(
    items: &BTreeMap<String, String>,
    variables: &BTreeMap<String, String>,
) -> BTreeMap<String, String> {
    items
        .iter()
        .map(|(key, value)| (key.clone(), expand_templates(value, variables)))
        .collect()
}

fn expand_templates(value: &str, variables: &BTreeMap<String, String>) -> String {
    let mut expanded = String::with_capacity(value.len());
    let mut remaining = value;
    while let Some(open) = remaining.find(TEMPLATE_OPEN) {
        let after = &remaining[open + TEMPLATE_OPEN.len()..];
        let replacement = after.find(TEMPLATE_CLOSE).and_then(|close| {
            variables
                .get(after[..close].trim())
                .map(|value| (value, close + TEMPLATE_CLOSE.len()))
        });
        match replacement {
            Some((value, consumed)) => {
                expanded.push_str(&remaining[..open]);
                expanded.push_str(value);
                remaining = &after[consumed..];
            }
            None => {
                expanded.push_str(&remaining[..open + TEMPLATE_OPEN.len()]);
                remaining = after;
            }
        }
    }
    expanded.push_str(remaining);
    expanded
}

impl DeploymentStatus {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Stopped | Self::Failed | Self::Cancelled)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fills_in_known_placeholders_and_leaves_the_rest() {
        let variables = BTreeMap::from([
            (
                "MAESTRO_INGRESS_HOST".to_string(),
                "pr-12.preview.example".to_string(),
            ),
            ("MAESTRO_INGRESS_PORT".to_string(), "8080".to_string()),
        ]);
        assert_eq!(
            expand_templates(
                "https://${{ MAESTRO_INGRESS_HOST }}:${{MAESTRO_INGRESS_PORT}}/",
                &variables
            ),
            "https://pr-12.preview.example:8080/"
        );
        assert_eq!(
            expand_templates("${{ UNKNOWN }} and ${{ MAESTRO_INGRESS_HOST", &variables),
            "${{ UNKNOWN }} and ${{ MAESTRO_INGRESS_HOST"
        );
        assert_eq!(
            expand_templates("plain ${HOME}", &variables),
            "plain ${HOME}"
        );
    }
}
