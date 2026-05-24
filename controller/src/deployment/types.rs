use std::collections::HashMap;
use std::path::PathBuf;

use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::logs::Logger;
use crate::utils;
use crate::utils::crypto::SecretString;
use crate::utils::secrets::SecretProvider;

#[derive(Debug, Clone)]
pub struct ControllerConfig {
    pub cluster_alias: String,
    pub cluster_name: String,
    pub node_id: String,
    pub node_role: crate::cluster::NodeRole,
    pub data_dir: PathBuf,
    pub etcd_port: u16,
    pub probe_port: Option<u16>,
    pub admin_port: Option<u16>,
    pub ingress_ports: Vec<u16>,
    pub project_dir: PathBuf,
    pub network: String,
    pub subnet: Option<String>,
    pub tailscale_authkey: Option<String>,
    pub encryption_key: SecretString,
    pub jwt_secret_key: Option<String>,
    pub build_command_env: HashMap<String, SecretString>,
    pub tags: Vec<String>,
    pub system_type: Option<crate::config::SystemType>,
    pub force: bool,
    pub disable_etcd_cert: bool,
    pub enable_ingress_access_logs: bool,
    pub maestro_config: String,
    pub cloudflare_tunnel_token: Option<SecretString>,
    pub cloudflare_tunnel_replicas: u32,
    pub slack_webhook_url: Option<SecretString>,
    pub cluster_bootstrap: ClusterBootstrap,
}

#[derive(Debug, Clone, derive_builder::Builder)]
#[builder(pattern = "owned", default)]
pub struct ClusterBootstrap {
    pub mode: ClusterBootstrapMode,
    /// Peer entries in the form `node-id=https://host:peer-port`. Required for
    /// the initial cluster bootstrap (--initial-cluster) and to find peer URLs
    /// when joining an existing cluster.
    pub peers: Vec<ClusterPeer>,
    /// Port other etcd peers should reach this node on.
    pub etcd_peer_port: u16,
    /// Address this node advertises to peers. Defaults to its hostname.
    pub advertise_host: Option<String>,
    /// Whether scheduler/leader-loop integration should be active. Single-node
    /// installs leave this false so the existing single-node path is unchanged.
    pub scheduling_enabled: bool,
    /// Shared container registry that all nodes push/pull built images through.
    /// Required for multi-node deployments where the build node and run node
    /// differ. Format: `host:port` or `host:port/namespace`.
    pub shared_registry: Option<String>,
}

impl ClusterBootstrap {
    /// When true, the DeploymentController defers replica lifecycle to the
    /// cluster AssignmentReconciler (which writes per-node assignments into
    /// etcd) rather than spawning containers itself. Builds + deployment
    /// status transitions still run; only the engine.start_replica calls are
    /// skipped.
    pub fn defers_replica_spawn(&self) -> bool {
        self.scheduling_enabled
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClusterBootstrapMode {
    /// Single-node cluster (one etcd member). Default; preserves legacy behavior.
    Single,
    /// Bootstrap a brand-new multi-node etcd cluster from this node + peers.
    NewCluster,
    /// Join an existing etcd cluster as a new member.
    JoinExisting,
}

impl Default for ClusterBootstrapMode {
    fn default() -> Self {
        ClusterBootstrapMode::Single
    }
}

impl Default for ClusterBootstrap {
    fn default() -> Self {
        Self {
            mode: ClusterBootstrapMode::Single,
            peers: Vec::new(),
            etcd_peer_port: 2380,
            advertise_host: None,
            scheduling_enabled: false,
            shared_registry: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ClusterPeer {
    /// Reachable address of the peer node — used both as the etcd peer URL
    /// host and (sanitized) as the etcd member name. Two nodes only agree on
    /// the cluster if they reference each other by the same host string.
    pub host: String,
    pub peer_port: Option<u16>,
}

impl ClusterPeer {
    /// Accepts `host`, `host:port`, or `http(s)://host:port`. The port falls
    /// through to `--etcd-peer-port` when omitted.
    pub fn parse(spec: &str) -> Option<Self> {
        let trimmed = spec
            .trim()
            .trim_start_matches("http://")
            .trim_start_matches("https://");
        if trimmed.is_empty() {
            return None;
        }
        match trimmed.split_once(':') {
            Some((host, port_str)) => {
                let port = port_str.parse::<u16>().ok()?;
                Some(Self {
                    host: host.to_string(),
                    peer_port: Some(port),
                })
            }
            None => Some(Self {
                host: trimmed.to_string(),
                peer_port: None,
            }),
        }
    }

    pub fn effective_peer_port(&self, default: u16) -> u16 {
        self.peer_port.unwrap_or(default)
    }

    pub fn peer_url(&self, scheme: &str, default_port: u16) -> String {
        format!(
            "{scheme}://{}:{}",
            self.host,
            self.effective_peer_port(default_port)
        )
    }

    /// Etcd member name derived from the host. Etcd requires alphanumeric +
    /// hyphen/underscore; we sanitize to that subset so an IP-as-host like
    /// `10.0.0.1` becomes `10-0-0-1`.
    pub fn etcd_member_name(&self) -> String {
        sanitize_member_name(&self.host)
    }
}

pub fn sanitize_member_name(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '-'
            }
        })
        .collect()
}

#[cfg(test)]
mod cluster_peer_tests {
    use super::*;

    #[test]
    fn host_only_uses_default_port() {
        let peer = ClusterPeer::parse("node-a").unwrap();
        assert_eq!(peer.host, "node-a");
        assert_eq!(peer.peer_port, None);
        assert_eq!(peer.effective_peer_port(2380), 2380);
        assert_eq!(peer.peer_url("http", 2380), "http://node-a:2380");
    }

    #[test]
    fn host_port_overrides_default() {
        let peer = ClusterPeer::parse("10.0.0.5:2381").unwrap();
        assert_eq!(peer.host, "10.0.0.5");
        assert_eq!(peer.peer_port, Some(2381));
        assert_eq!(peer.effective_peer_port(2380), 2381);
    }

    #[test]
    fn http_scheme_is_stripped() {
        let peer = ClusterPeer::parse("http://host-x:2380").unwrap();
        assert_eq!(peer.host, "host-x");
        assert_eq!(peer.peer_port, Some(2380));
    }

    #[test]
    fn https_scheme_is_stripped() {
        let peer = ClusterPeer::parse("https://host-y").unwrap();
        assert_eq!(peer.host, "host-y");
    }

    #[test]
    fn empty_input_is_rejected() {
        assert!(ClusterPeer::parse("").is_none());
        assert!(ClusterPeer::parse("   ").is_none());
    }

    #[test]
    fn ip_address_is_sanitized_into_member_name() {
        let peer = ClusterPeer::parse("10.0.0.1").unwrap();
        assert_eq!(peer.etcd_member_name(), "10-0-0-1");
    }

    #[test]
    fn dns_name_keeps_dashes() {
        let peer = ClusterPeer::parse("node-a-prod").unwrap();
        assert_eq!(peer.etcd_member_name(), "node-a-prod");
    }
}

impl ControllerConfig {
    #[inline]
    pub fn etcd_dir(&self) -> PathBuf {
        self.data_dir.join("system/etcd/")
    }

    #[inline]
    pub fn probe_dir(&self) -> PathBuf {
        self.data_dir.join("system/probe")
    }

    #[inline]
    pub fn certs_dir(&self) -> PathBuf {
        self.data_dir.join("system/certs")
    }
}

#[derive(Debug, Clone)]
pub struct Deployment {
    pub id: String,
    pub service_id: String,
    pub replica_index: u32,
}

#[derive(Debug, Clone)]
pub struct QueuedDeployment {
    pub service_id: String,
    pub key: String,
    pub mod_revision: u64,
    pub deployment: ServiceDeployment,
}

#[derive(Debug, Clone)]
pub struct ForceQueueOutcome {
    pub deployment_index: usize,
    pub deployment: ServiceDeployment,
}

#[derive(Debug, Clone)]
pub enum CancelDeploymentOutcome {
    Canceled(ServiceDeployment),
    NotCancelable(ServiceDeployment),
    NotFound,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ServiceConfig {
    pub id: String,
    pub name: String,
    pub version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub build: Option<ServiceBuildConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image: Option<String>,
    pub deploy: ServiceDeployConfig,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ingress: Option<IngressConfig>,
}

impl ServiceConfig {
    pub fn strip_secrets(&self, prev_keys: &HashMap<String, SecretKeyMeta>) -> Self {
        let mut config = self.clone();
        if let Some(secrets) = &config.deploy.secrets {
            config.deploy.secrets = Some(secrets.to_metadata(prev_keys));
        }
        config.deploy.env.items.clear();
        if let Some(build) = &mut config.build {
            build.env.items.clear();
            build.secrets.items.clear();
        }
        config
    }

    pub fn mask_secrets(&self) -> Self {
        let mut config = self.clone();
        for value in config.deploy.env.items.values_mut() {
            *value = SecretString::new(value.masked());
        }
        if let Some(build) = &mut config.build {
            for value in build.env.items.values_mut() {
                *value = SecretString::new(value.masked());
            }
            for value in build.secrets.items.values_mut() {
                *value = SecretString::new(value.masked());
            }
        }
        config
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct IngressConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub hosts: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub port: Option<u16>,
}

impl IngressConfig {
    pub fn hosts(&self) -> Vec<&str> {
        let mut hosts = Vec::with_capacity(1 + self.hosts.len());
        if let Some(host) = &self.host {
            hosts.push(host.as_str());
        }
        for host in &self.hosts {
            hosts.push(host.as_str());
        }
        hosts
    }
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct IngressRouting {
    pub service_id: String,
    pub rule: String,
    pub entry_points: Vec<String>,
    pub servers: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ServiceBuildConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repo: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub branch: Option<String>,
    pub dockerfile: String,
    #[serde(default)]
    pub watch: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub registry: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub depot: Option<DepotConfig>,
    #[serde(default, skip_serializing_if = "EnvConfig::is_empty")]
    pub env: EnvConfig,
    #[serde(default, skip_serializing_if = "EnvConfig::is_empty")]
    pub secrets: EnvConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DepotConfig {
    pub project: String,
}

impl ServiceBuildConfig {
    pub fn source(&self) -> crate::builder::GitSource {
        let mut env = self.env.items.clone();
        env.extend(self.secrets.items.clone());
        crate::builder::GitSource::new(
            self.repo.as_deref().unwrap_or(""),
            self.branch.as_deref(),
            env,
        )
    }

    pub async fn resolved_source(&self, logger: &Logger) -> Result<crate::builder::GitSource> {
        let mut env = self.env.resolved(logger).await?;
        env.extend(self.secrets.resolved(logger).await?);
        Ok(crate::builder::GitSource::new(
            self.repo.as_deref().unwrap_or(""),
            self.branch.as_deref(),
            env,
        ))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ServiceDeployConfig {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub flags: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub expose_ports: Vec<u16>,
    pub command: Option<Command>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub healthcheck_path: Option<String>,
    #[serde(default = "default_healthcheck_interval")]
    pub healthcheck_interval: u32,
    #[serde(default = "default_replicas")]
    pub replicas: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_restarts: Option<u32>,
    #[serde(default, skip_serializing_if = "EnvConfig::is_empty")]
    pub env: EnvConfig,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secrets: Option<SecretsConfig>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub volumes: Vec<VolumeMount>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_affinity: Option<NodeAffinity>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct NodeAffinity {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_id: Option<String>,
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub labels: std::collections::BTreeMap<String, String>,
}

impl NodeAffinity {
    pub fn matches(
        &self,
        node_id: &str,
        node_labels: &std::collections::BTreeMap<String, String>,
    ) -> bool {
        if let Some(required_id) = &self.node_id {
            if required_id != node_id {
                return false;
            }
        }
        for (key, value) in &self.labels {
            match node_labels.get(key) {
                Some(actual) if actual == value => {}
                _ => return false,
            }
        }
        true
    }

    pub fn is_pinned(&self) -> bool {
        self.node_id.is_some()
    }
}

pub const MIN_HEALTHCHECK_INTERVAL_SECS: u32 = 5;
pub const MAX_HEALTHCHECK_INTERVAL_SECS: u32 = 300;
pub const DEFAULT_HEALTHCHECK_INTERVAL_SECS: u32 = 60;

fn default_healthcheck_interval() -> u32 {
    DEFAULT_HEALTHCHECK_INTERVAL_SECS
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct VolumeMount {
    pub host_path: String,
    pub mount_path: String,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub read_only: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<VolumeOwner>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct VolumeOwner {
    pub uid: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gid: Option<u32>,
}

impl VolumeOwner {
    pub fn resolved_gid(&self) -> u32 {
        self.gid.unwrap_or(self.uid)
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct EnvConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub items: HashMap<String, SecretString>,
}

impl EnvConfig {
    pub fn is_empty(&self) -> bool {
        self.source.is_none() && self.items.is_empty()
    }

    pub async fn resolved(&self, logger: &Logger) -> Result<HashMap<String, SecretString>> {
        let mut items = self.items.clone();
        if let Some(source) = &self.source {
            let source_items = SecretProvider::new(source, logger)?.fetch_kv().await?;
            for (key, value) in source_items {
                items.entry(key).or_insert(SecretString::new(value));
            }
        }
        Ok(items)
    }
}

/// Secrets with resolved values — used during rollout and deployment.
/// Never stored in etcd or returned by API.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SecretsConfig {
    pub mount_path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub items: HashMap<String, String>,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub keys: HashMap<String, SecretKeyMeta>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SecretKeyMeta {
    pub hash: String,
    #[serde(default)]
    pub changed: bool,
}

impl SecretsConfig {
    pub fn compute_secrets_hash(&self) -> String {
        use sha2::{Digest, Sha256};
        let mut sorted_keys: Vec<&String> = self.items.keys().collect();
        sorted_keys.sort();
        let mut hasher = Sha256::new();
        for key in sorted_keys {
            hasher.update(key.as_bytes());
            hasher.update(b"=");
            hasher.update(self.items.get(key).unwrap().as_bytes());
            hasher.update(b"\n");
        }
        format!("{:x}", hasher.finalize())
    }

    pub fn compute_value_hash(value: &str) -> String {
        use sha2::{Digest, Sha256};
        format!("{:x}", Sha256::digest(value.as_bytes()))
    }

    pub fn to_metadata(&self, prev_keys: &HashMap<String, SecretKeyMeta>) -> SecretsConfig {
        let keys = self
            .items
            .iter()
            .map(|(k, v)| {
                let hash = Self::compute_value_hash(v);
                let changed = prev_keys.get(k).is_some_and(|prev| prev.hash != hash);
                (k.clone(), SecretKeyMeta { hash, changed })
            })
            .collect();
        SecretsConfig {
            mount_path: self.mount_path.clone(),
            source: self.source.clone(),
            items: HashMap::new(),
            keys,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Command {
    pub command: String,
    #[serde(default)]
    pub args: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum DeploymentStatus {
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

impl DeploymentStatus {
    /// Whether a transition from `self` to `target` is allowed.
    pub fn can_transition_to(&self, target: &DeploymentStatus) -> bool {
        match target {
            DeploymentStatus::PendingReady => matches!(self, DeploymentStatus::Building),
            DeploymentStatus::Ready => {
                matches!(
                    self,
                    DeploymentStatus::Building | DeploymentStatus::PendingReady
                )
            }
            DeploymentStatus::Crashed => !matches!(
                self,
                DeploymentStatus::Crashed
                    | DeploymentStatus::Canceled
                    | DeploymentStatus::Terminated
            ),
            DeploymentStatus::Draining => {
                matches!(
                    self,
                    DeploymentStatus::Ready
                        | DeploymentStatus::PendingReady
                        | DeploymentStatus::Building
                )
            }
            DeploymentStatus::Terminated => !matches!(self, DeploymentStatus::Terminated),
            _ => true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ReplicaState {
    #[serde(default)]
    pub replica_index: u32,
    pub status: DeploymentStatus,
    #[serde(default)]
    pub healthcheck_failures: u32,
    #[serde(default)]
    pub restart_attempts: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ServiceDeployment {
    pub id: String,
    #[serde(default)]
    pub created_at: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deployed_at: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub drained_at: Option<u64>,
    pub status: DeploymentStatus,
    pub config: ServiceConfig,
    pub git_commit: Option<GitCommitInfo>,
    pub build: Option<DeploymentBuildInfo>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upload_archive: Option<String>,
}

impl ServiceDeployment {
    pub fn hostname_for_replica(&self, replica_index: u32) -> String {
        let short_id: String = self.id.chars().take(6).collect();
        if replica_index == 0 {
            format!("{}-{short_id}", self.config.id)
        } else {
            format!("{}-{short_id}-{replica_index}", self.config.id)
        }
    }

    pub fn has_build_step(&self) -> bool {
        if self.config.build.is_some() {
            return true;
        }

        self.config
            .image
            .as_deref()
            .map(str::trim)
            .is_some_and(|image| !image.is_empty())
    }

    pub fn new(config: ServiceConfig) -> Result<Self> {
        Ok(ServiceDeployment {
            id: utils::nanoid::unique_id(10),
            created_at: utils::time::current_time_millis()?,
            deployed_at: None,
            drained_at: None,
            status: DeploymentStatus::Queued,
            config,
            git_commit: None,
            build: None,
            upload_archive: None,
        })
    }

    pub async fn resolve_build_secrets(&mut self, logger: &Logger) -> Result<()> {
        if let Some(build) = &mut self.config.build {
            build.env.items = build.env.resolved(logger).await?;
            build.secrets.items = build.secrets.resolved(logger).await?;
        }
        Ok(())
    }

    pub async fn resolve_deploy_secrets(
        &mut self,
        logger: &Logger,
    ) -> Result<Option<ResolvedSecret>> {
        self.config.deploy.env.items = self.config.deploy.env.resolved(logger).await?;
        if let Some(secrets) = &mut self.config.deploy.secrets {
            if let Some(source) = &secrets.source {
                let source_items = SecretProvider::new(source, logger)?.fetch_kv().await?;
                let resolved = ResolvedSecret {
                    source: source.clone(),
                    count: source_items.len(),
                };
                for (key, value) in source_items {
                    secrets.items.entry(key).or_insert(value);
                }
                return Ok(Some(resolved));
            }
        }
        Ok(None)
    }
}

pub struct ResolvedSecret {
    pub source: String,
    pub count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DeploymentWithReplicas {
    #[serde(flatten)]
    pub deployment: ServiceDeployment,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub replicas: Vec<ReplicaState>,
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
    #[serde(default)]
    pub deploy_frozen: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replicas_override: Option<u32>,
}

impl ServiceInfo {
    pub fn effective_replicas(&self) -> u32 {
        match self.replicas_override {
            Some(override_value) => override_value.max(self.config.deploy.replicas),
            None => self.config.deploy.replicas,
        }
    }
}

fn default_replicas() -> u32 {
    1
}
