use std::collections::BTreeMap;
use std::net::Ipv4Addr;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct LegacyServiceInfo {
    pub(crate) config: LegacyServiceConfig,
    #[serde(default)]
    pub(crate) deploy_frozen: bool,
    #[serde(default)]
    pub(crate) replicas_override: Option<u32>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct LegacyServiceConfig {
    pub(crate) id: String,
    pub(crate) name: String,
    pub(crate) version: String,
    #[serde(default)]
    pub(crate) build: Option<LegacyBuildConfig>,
    #[serde(default)]
    pub(crate) image: Option<String>,
    pub(crate) deploy: LegacyDeployConfig,
    #[serde(default)]
    pub(crate) ingress: Option<LegacyIngressConfig>,
    #[serde(default)]
    pub(crate) preview: Option<LegacyPreviewConfig>,
    #[serde(default)]
    pub(crate) preview_source: Option<LegacyPreviewSource>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct LegacyBuildConfig {
    #[serde(default)]
    pub(crate) repo: Option<String>,
    #[serde(default)]
    pub(crate) branch: Option<String>,
    pub(crate) dockerfile: String,
    #[serde(default)]
    pub(crate) watch: bool,
    #[serde(default)]
    pub(crate) registry: Option<String>,
    #[serde(default)]
    pub(crate) depot: Option<LegacyDepotConfig>,
    #[serde(default)]
    pub(crate) env: LegacyEnvConfig,
    #[serde(default)]
    pub(crate) secrets: LegacyEnvConfig,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct LegacyDepotConfig {
    pub(crate) project: String,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct LegacyDeployConfig {
    #[serde(default)]
    pub(crate) flags: Vec<String>,
    #[serde(default)]
    pub(crate) expose_ports: Vec<u16>,
    #[serde(default)]
    pub(crate) command: Option<LegacyCommand>,
    #[serde(default)]
    pub(crate) healthcheck_path: Option<String>,
    #[serde(default = "default_healthcheck_interval")]
    pub(crate) healthcheck_interval: u32,
    #[serde(default = "default_replicas")]
    pub(crate) replicas: u32,
    #[serde(default = "default_true")]
    pub(crate) exec: bool,
    #[serde(default)]
    pub(crate) max_restarts: Option<u32>,
    #[serde(default)]
    pub(crate) env: LegacyEnvConfig,
    #[serde(default)]
    pub(crate) secrets: Option<LegacySecretsConfig>,
    #[serde(default)]
    pub(crate) volumes: Vec<LegacyVolumeMount>,
    #[serde(default)]
    pub(crate) node_affinity: Option<LegacyNodeAffinity>,
    #[serde(default)]
    pub(crate) egress: LegacyEgressConfig,
}

#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct LegacyEnvConfig {
    #[serde(default)]
    pub(crate) source: Option<String>,
    #[serde(default)]
    pub(crate) items: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct LegacySecretsConfig {
    pub(crate) mount_path: String,
    #[serde(default)]
    pub(crate) source: Option<String>,
    #[serde(default)]
    pub(crate) items: BTreeMap<String, String>,
    #[serde(default)]
    pub(crate) keys: BTreeMap<String, LegacySecretKeyMeta>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct LegacySecretKeyMeta {
    #[serde(default)]
    pub(crate) hash: String,
    #[serde(default)]
    pub(crate) changed: bool,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct LegacyCommand {
    pub(crate) command: String,
    #[serde(default)]
    pub(crate) args: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct LegacyVolumeMount {
    pub(crate) host_path: String,
    pub(crate) mount_path: String,
    #[serde(default)]
    pub(crate) read_only: bool,
    #[serde(default)]
    pub(crate) owner: Option<LegacyVolumeOwner>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct LegacyVolumeOwner {
    pub(crate) uid: u32,
    #[serde(default)]
    pub(crate) gid: Option<u32>,
}

#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub(crate) struct LegacyNodeAffinity {
    #[serde(default)]
    pub(crate) node_id: Option<String>,
    #[serde(default)]
    pub(crate) labels: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct LegacyEgressConfig {
    #[serde(default)]
    pub(crate) allow: Vec<LegacyEgressRule>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct LegacyEgressRule {
    pub(crate) cidr: String,
    #[serde(default)]
    pub(crate) ports: Vec<u16>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct LegacyIngressConfig {
    #[serde(default)]
    pub(crate) host: Option<String>,
    #[serde(default)]
    pub(crate) hosts: Vec<String>,
    #[serde(default)]
    pub(crate) port: Option<u16>,
    #[serde(default)]
    pub(crate) session_affinity: Option<LegacySessionAffinity>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct LegacySessionAffinity {
    pub(crate) header: String,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct LegacyPreviewConfig {
    #[serde(default)]
    pub(crate) enabled: bool,
    #[serde(default = "default_close_grace")]
    pub(crate) close_grace_period: String,
    #[serde(default = "default_replicas")]
    pub(crate) replicas: u32,
    #[serde(default)]
    pub(crate) env: LegacyEnvConfig,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct LegacyPreviewSource {
    pub(crate) base_service_id: String,
    pub(crate) pr_number: u64,
    pub(crate) head_ref: String,
    pub(crate) head_sha: String,
    pub(crate) title: String,
    #[serde(default)]
    pub(crate) created_at: u64,
    #[serde(default)]
    pub(crate) volumes_stripped: bool,
    #[serde(default)]
    pub(crate) closed_at: Option<u64>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct LegacyDeployment {
    pub(crate) id: String,
    #[serde(default)]
    pub(crate) created_at: u64,
    #[serde(default)]
    pub(crate) deployed_at: Option<u64>,
    #[serde(default)]
    pub(crate) drained_at: Option<u64>,
    pub(crate) status: LegacyDeploymentStatus,
    pub(crate) config: LegacyServiceConfig,
    #[serde(default)]
    pub(crate) git_commit: Option<LegacyGitCommit>,
    #[serde(default)]
    pub(crate) build: Option<LegacyDeploymentBuild>,
    #[serde(default)]
    pub(crate) upload_archive: Option<String>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct LegacyGitCommit {
    pub(crate) reference: String,
    pub(crate) message: String,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct LegacyDeploymentBuild {
    pub(crate) docker_image_id: String,
    #[serde(default)]
    pub(crate) source_node_id: Option<String>,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum LegacyDeploymentStatus {
    Queued,
    Building,
    PendingReady,
    Ready,
    Crashed,
    Terminated,
    Removed,
    Draining,
    #[serde(alias = "CANCELLED")]
    Canceled,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct LegacyAssignmentManifest {
    pub(crate) node_id: String,
    pub(crate) generation: u64,
    #[serde(default)]
    pub(crate) assignments: Vec<LegacyAssignment>,
    #[serde(default)]
    pub(crate) images: Vec<LegacyImageAssignment>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct LegacyAssignment {
    pub(crate) assignment_id: String,
    pub(crate) placement_epoch: u64,
    pub(crate) service_id: String,
    pub(crate) deployment_id: String,
    pub(crate) replica_index: u32,
    pub(crate) node_id: String,
    #[serde(default)]
    pub(crate) container_ip: Option<Ipv4Addr>,
    #[serde(default)]
    pub(crate) replaces_assignment_id: Option<String>,
    pub(crate) created_at_ms: i64,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct LegacyImageAssignment {
    pub(crate) service_id: String,
    pub(crate) deployment_id: String,
    pub(crate) image: String,
    pub(crate) source_node_id: String,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct LegacyReplicaState {
    #[serde(default)]
    pub(crate) service_id: Option<String>,
    #[serde(default)]
    pub(crate) deployment_id: Option<String>,
    #[serde(default)]
    pub(crate) replica_index: u32,
    pub(crate) status: LegacyDeploymentStatus,
    #[serde(default)]
    pub(crate) healthcheck_failures: u32,
    #[serde(default)]
    pub(crate) restart_attempts: u32,
    #[serde(default)]
    pub(crate) node_id: Option<String>,
    #[serde(default)]
    pub(crate) assignment_id: Option<String>,
    #[serde(default)]
    pub(crate) endpoint: Option<LegacyReplicaEndpoint>,
    #[serde(default)]
    pub(crate) error: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct LegacyReplicaEndpoint {
    pub(crate) container_ip: String,
    pub(crate) container_hostname: String,
    pub(crate) ingress_container_port: u16,
    pub(crate) gateway: LegacyNodeGatewayEndpoint,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct LegacyNodeGatewayEndpoint {
    pub(crate) host_ip: Ipv4Addr,
    pub(crate) port: u16,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct LegacyTrafficGeneration {
    pub(crate) service_id: String,
    pub(crate) deployment_id: String,
    pub(crate) traffic_epoch: u64,
    pub(crate) active_assignment_ids: Vec<String>,
    #[serde(default)]
    pub(crate) active_node_ids: Vec<String>,
    pub(crate) generation: String,
    #[serde(default)]
    pub(crate) routing_fingerprint: String,
    pub(crate) switched_at_ms: i64,
    pub(crate) drain_old_after_ms: i64,
}

const fn default_healthcheck_interval() -> u32 {
    60
}

const fn default_replicas() -> u32 {
    1
}

const fn default_true() -> bool {
    true
}

fn default_close_grace() -> String {
    "1d".to_owned()
}
