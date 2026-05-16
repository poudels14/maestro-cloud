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
    pub jwt_secret: Option<String>,
    pub build_command_env: HashMap<String, SecretString>,
    pub tags: Vec<String>,
    pub system_type: Option<crate::config::SystemType>,
    pub force: bool,
    pub disable_etcd_cert: bool,
    pub enable_ingress_access_logs: bool,
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
    #[serde(default)]
    pub provider: ServiceProvider,
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
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct IngressConfig {
    pub host: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub port: Option<u16>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct IngressRouting {
    pub service_id: String,
    pub rule: String,
    pub entry_points: Vec<String>,
    pub servers: Vec<String>,
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
        crate::builder::GitSource::new(&self.repo, self.branch.as_deref(), env)
    }

    pub async fn resolved_source(&self, logger: &Logger) -> Result<crate::builder::GitSource> {
        let mut env = self.env.resolved(logger).await?;
        env.extend(self.secrets.resolved(logger).await?);
        Ok(crate::builder::GitSource::new(
            &self.repo,
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
    #[serde(default = "default_replicas")]
    pub replicas: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_restarts: Option<u32>,
    #[serde(default, skip_serializing_if = "EnvConfig::is_empty")]
    pub env: EnvConfig,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secrets: Option<SecretsConfig>,
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

        self.config.provider == ServiceProvider::Docker
            && self
                .config
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
}

fn default_replicas() -> u32 {
    1
}
