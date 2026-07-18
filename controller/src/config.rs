use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fmt;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::path::Path;

use anyhow::{Result, anyhow, bail};
use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::logs::Logger;
use crate::utils::crypto::SecretString;
use crate::utils::secrets::SecretProvider;

const CONFIG_EXTENDS_KEY: &str = "$extends";
const MAX_CONFIG_EXTENDS_DEPTH: usize = 16;
const START_CONFIG_KEY_ALIASES: &[(&str, &str)] = &[
    ("encryptionKey", "encryption-key"),
    ("jwtSecretKey", "jwt-secret-key"),
    ("logBackup", "log-backup"),
    ("disableEtcdCert", "disable-etcd-cert"),
    ("allowCliDeployment", "allow-cli-deployment"),
    ("allowExec", "allow-exec"),
];
const CLUSTER_CONFIG_KEY_ALIASES: &[(&str, &str)] = &[
    ("controlAllowCidrs", "control-allow-cidrs"),
    ("joinSecret", "join-secret"),
];
const TAILSCALE_CONFIG_KEY_ALIASES: &[(&str, &str)] = &[
    ("authKey", "auth-key"),
    ("advertiseRoutes", "advertise-routes"),
];
const DATADOG_CONFIG_KEY_ALIASES: &[(&str, &str)] = &[
    ("apiKey", "api-key"),
    ("includeIngressLogs", "include-ingress-logs"),
    ("includeTailscaleLogs", "include-tailscale-logs"),
    ("includeMetrics", "include-metrics"),
];
const DATADOG_LOGS_CONFIG_KEY_ALIASES: &[(&str, &str)] =
    &[("includeHealthcheck", "include-healthcheck")];
const SLACK_CONFIG_KEY_ALIASES: &[(&str, &str)] = &[("webhookUrl", "webhook-url")];
const LOG_BACKUP_CONFIG_KEY_ALIASES: &[(&str, &str)] = &[
    ("kmsKeyId", "kms-key-id"),
    ("retentionDays", "retention-days"),
];

#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RuntimeType {
    #[default]
    Docker,
    Nerdctl,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum BuilderType {
    #[default]
    Default,
    Depot,
}

impl fmt::Display for RuntimeType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RuntimeType::Docker => write!(f, "docker"),
            RuntimeType::Nerdctl => write!(f, "nerdctl"),
        }
    }
}

impl std::str::FromStr for RuntimeType {
    type Err = String;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "docker" => Ok(RuntimeType::Docker),
            "nerdctl" => Ok(RuntimeType::Nerdctl),
            other => Err(format!(
                "unsupported runtime: {other} (use 'docker' or 'nerdctl')"
            )),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct StartConfig {
    pub cluster: ClusterConfig,
    #[serde(default)]
    pub node: NodeConfig,
    pub ingress: IngressConfig,
    #[serde(default)]
    pub subnet: Option<String>,
    #[serde(default)]
    pub egress: EgressConfig,
    #[serde(alias = "encryptionKey")]
    pub encryption_key: String,
    #[serde(default)]
    pub tailscale: Option<TailscaleConfig>,
    #[serde(default, alias = "jwtSecretKey")]
    pub jwt_secret_key: Option<String>,
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default)]
    pub datadog: Option<DatadogConfig>,
    #[serde(default)]
    pub system: Option<SystemType>,
    #[serde(default)]
    pub runtime: RuntimeType,
    #[serde(default)]
    pub depot: Option<DepotConfig>,
    #[serde(default)]
    pub cloudflare: Option<CloudflareConfig>,
    #[serde(default)]
    pub slack: Option<SlackConfig>,
    #[serde(default)]
    pub github: Option<GithubConfig>,
    #[serde(default)]
    pub homepage: Option<String>,
    #[serde(default, alias = "logBackup")]
    pub log_backup: Option<LogBackupConfig>,
    #[serde(default, alias = "disableEtcdCert")]
    pub disable_etcd_cert: bool,
    #[serde(default, alias = "allowCliDeployment")]
    pub allow_cli_deployment: bool,
    #[serde(default, alias = "allowExec")]
    pub allow_exec: bool,
}

#[derive(Debug, Clone)]
pub struct NodeConfig {
    pub name: Option<String>,
    pub role: crate::cluster::NodeRole,
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            name: None,
            role: crate::cluster::NodeRole::Hybrid,
        }
    }
}

impl Serialize for NodeConfig {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.name.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for NodeConfig {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Ok(Self {
            name: Option::<String>::deserialize(deserializer)?,
            role: crate::cluster::NodeRole::Hybrid,
        })
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct EgressConfig {
    #[serde(default)]
    pub deny: Vec<String>,
    #[serde(default)]
    pub allow: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct DepotConfig {
    #[serde(default)]
    pub token: Option<SecretString>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct CloudflareConfig {
    pub tunnel: CloudflareTunnelConfig,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct CloudflareTunnelConfig {
    pub token: SecretString,
    #[serde(default)]
    pub replicas: Option<u32>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct SlackConfig {
    #[serde(alias = "webhookUrl")]
    pub webhook_url: SecretString,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct GithubConfig {
    pub token: SecretString,
    #[serde(default = "default_preview_domain", alias = "previewDomain")]
    pub preview_domain: String,
    #[serde(
        default = "default_github_poll_interval_secs",
        alias = "pollIntervalSecs"
    )]
    pub poll_interval_secs: u64,
    #[serde(
        default = "default_max_concurrent_previews",
        alias = "maxConcurrentPreviews"
    )]
    pub max_concurrent_previews: usize,
}

fn default_preview_domain() -> String {
    "preview.getbaton.ai".to_string()
}

fn default_github_poll_interval_secs() -> u64 {
    60
}

fn default_max_concurrent_previews() -> usize {
    10
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct LogBackupConfig {
    pub bucket: String,
    #[serde(alias = "kmsKeyId")]
    pub kms_key_id: String,
    #[serde(default)]
    pub region: Option<String>,
    #[serde(default)]
    pub prefix: Option<String>,
    #[serde(default, alias = "retentionDays")]
    pub retention_days: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SystemType {
    Nixos,
}

impl fmt::Display for SystemType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SystemType::Nixos => write!(f, "nixos"),
        }
    }
}

impl std::str::FromStr for SystemType {
    type Err = String;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "nixos" => Ok(SystemType::Nixos),
            other => Err(format!(
                "unsupported system type: {other} (only 'nixos' is supported)"
            )),
        }
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub struct ClusterConfig {
    pub name: String,
    #[serde(default)]
    pub nodes: BTreeMap<String, ClusterNodeConfig>,
    #[serde(skip, default = "default_cluster_api_port")]
    pub api_port: u16,
    #[serde(skip, default = "default_cluster_gateway_port")]
    pub gateway_port: u16,
    #[serde(default, alias = "controlAllowCidrs")]
    pub control_allow_cidrs: Vec<String>,
    #[serde(skip, default = "default_etcd_client_port")]
    pub etcd_client_port: u16,
    #[serde(skip, default = "default_etcd_peer_port")]
    pub etcd_peer_port: u16,
    #[serde(default, alias = "joinSecret")]
    pub join_secret: Option<String>,
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    #[serde(skip)]
    pub(crate) selected_node: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ClusterEndpointConfig {
    Endpoint(SocketAddrV4),
    Address(Ipv4Addr),
}

impl ClusterEndpointConfig {
    pub fn host_ip(self) -> Ipv4Addr {
        match self {
            Self::Endpoint(endpoint) => *endpoint.ip(),
            Self::Address(ip) => ip,
        }
    }

    pub fn explicit_api_port(self) -> Option<u16> {
        match self {
            Self::Endpoint(endpoint) => Some(endpoint.port()),
            Self::Address(_) => None,
        }
    }
}

impl fmt::Display for ClusterEndpointConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Endpoint(endpoint) => endpoint.fmt(formatter),
            Self::Address(ip) => ip.fmt(formatter),
        }
    }
}

impl std::str::FromStr for ClusterEndpointConfig {
    type Err = String;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        if value.contains(':') {
            value
                .parse::<SocketAddrV4>()
                .map(Self::Endpoint)
                .map_err(|error| error.to_string())
        } else {
            value
                .parse::<Ipv4Addr>()
                .map(Self::Address)
                .map_err(|error| error.to_string())
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub struct ClusterNodeConfig {
    pub endpoint: ClusterEndpointConfig,
    pub subnet: String,
    #[serde(default)]
    pub role: crate::cluster::NodeRole,
}

impl ClusterConfig {
    pub fn voter_api_endpoints(&self) -> Vec<SocketAddrV4> {
        let mut voters = self
            .nodes
            .iter()
            .filter(|(_, node)| node.role.is_voter())
            .map(|(_, node)| {
                (
                    node.role,
                    SocketAddrV4::new(
                        node.endpoint.host_ip(),
                        node.endpoint.explicit_api_port().unwrap_or(3000),
                    ),
                )
            })
            .collect::<Vec<_>>();
        voters
            .sort_by_key(|(role, endpoint)| (*role != crate::cluster::NodeRole::Master, *endpoint));
        voters.into_iter().map(|(_, endpoint)| endpoint).collect()
    }

    pub fn local_endpoint(
        &self,
        host_ip: Ipv4Addr,
        role: crate::cluster::NodeRole,
    ) -> Result<crate::cluster::ClusterNodeEndpoint> {
        let (name, node) = self.selected_node()?;
        if node.role != role {
            bail!("selected node `{name}` role changed after configuration was loaded");
        }
        let endpoint = self.resolve_local_node(node);
        if endpoint.host_ip != host_ip {
            bail!(
                "selected node `{name}` endpoint host `{}` does not match resolved local IP `{host_ip}`",
                endpoint.host_ip
            );
        }
        Ok(endpoint)
    }

    pub fn selected_node(&self) -> Result<(&str, &ClusterNodeConfig)> {
        let name = self
            .selected_node
            .as_deref()
            .ok_or_else(|| anyhow!("field `node` is missing: required in cluster mode"))?;
        let node = self.nodes.get(name).ok_or_else(|| {
            anyhow!(
                "field `node` is invalid: selected node `{name}` is absent from `cluster.nodes`"
            )
        })?;
        Ok((name, node))
    }

    pub fn master_node(&self) -> Result<(&str, &ClusterNodeConfig)> {
        let masters = self
            .nodes
            .iter()
            .filter(|(_, node)| node.role == crate::cluster::NodeRole::Master)
            .collect::<Vec<_>>();
        match masters.as_slice() {
            [(name, node)] => Ok((name.as_str(), *node)),
            [] => bail!(
                "field `cluster.nodes` is invalid: must contain exactly one node with role `master`"
            ),
            _ => bail!(
                "field `cluster.nodes` is invalid: contains more than one node with role `master`"
            ),
        }
    }

    pub fn set_selected_node(&mut self, name: String) -> Result<()> {
        let node = self.nodes.get(&name).ok_or_else(|| {
            anyhow!(
                "field `node` is invalid: selected node `{name}` is absent from `cluster.nodes`"
            )
        })?;
        self.api_port = node.endpoint.explicit_api_port().unwrap_or(3000);
        self.selected_node = Some(name);
        Ok(())
    }

    pub fn set_local_ports(&mut self, gateway: u16, etcd_client: u16, etcd_peer: u16) {
        self.gateway_port = gateway;
        self.etcd_client_port = etcd_client;
        self.etcd_peer_port = etcd_peer;
    }

    fn resolve_local_node(&self, node: &ClusterNodeConfig) -> crate::cluster::ClusterNodeEndpoint {
        let host_ip = node.endpoint.host_ip();
        let api_port = node.endpoint.explicit_api_port().unwrap_or(3000);
        crate::cluster::ClusterNodeEndpoint {
            host_ip,
            api_port,
            gateway_port: self.gateway_port,
            etcd_client_port: self.etcd_client_port,
            etcd_peer_port: self.etcd_peer_port,
        }
    }
}

fn default_cluster_api_port() -> u16 {
    3000
}

fn default_cluster_gateway_port() -> u16 {
    3002
}

fn default_etcd_client_port() -> u16 {
    2379
}

fn default_etcd_peer_port() -> u16 {
    2380
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct IngressConfig {
    #[serde(default)]
    pub port: Option<u16>,
    #[serde(default)]
    pub ports: Vec<u16>,
}

impl IngressConfig {
    pub fn resolved_ports(&self) -> Vec<u16> {
        if self.ports.is_empty() {
            self.port.into_iter().collect()
        } else {
            self.ports.clone()
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct TailscaleConfig {
    #[serde(alias = "authKey")]
    pub auth_key: String,
    /// Additional CIDRs (for example, private VPC networks containing RDS or Redis).
    /// Maestro automatically advertises the node's container subnet separately.
    #[serde(default, alias = "advertiseRoutes")]
    pub advertise_routes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct DatadogConfig {
    #[serde(alias = "apiKey")]
    pub api_key: String,
    #[serde(default)]
    pub site: Option<String>,
    #[serde(default = "default_true", alias = "includeIngressLogs")]
    pub include_ingress_logs: bool,
    #[serde(default = "default_true", alias = "includeTailscaleLogs")]
    pub include_tailscale_logs: bool,
    #[serde(default)]
    pub logs: DatadogLogsConfig,
    #[serde(default, alias = "includeMetrics")]
    pub include_metrics: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct DatadogLogsConfig {
    #[serde(default = "default_true", alias = "includeHealthcheck")]
    pub include_healthcheck: bool,
}

impl Default for DatadogLogsConfig {
    fn default() -> Self {
        Self {
            include_healthcheck: true,
        }
    }
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct MaskedConfig {
    pub cluster: ClusterView,
    pub node: NodeView,
    pub ingress: IngressView,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subnet: Option<String>,
    pub egress: EgressView,
    pub encryption_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tailscale: Option<TailscaleView>,
    pub jwt_secret_key: Option<String>,
    pub tags: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub datadog: Option<DatadogView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub system: Option<String>,
    pub runtime: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub depot: Option<DepotView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cloudflare: Option<CloudflareView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub slack: Option<SlackView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub github: Option<GithubView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub homepage: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub log_backup: Option<LogBackupConfig>,
    pub disable_etcd_cert: bool,
    #[serde(default)]
    pub allow_cli_deployment: bool,
    #[serde(default)]
    pub allow_exec: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct ClusterView {
    pub name: String,
    pub nodes: BTreeMap<String, ClusterNodeConfig>,
    pub control_allow_cidrs: Vec<String>,
    pub join_secret: Option<String>,
    pub labels: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct NodeView {
    pub name: Option<String>,
    pub role: crate::cluster::NodeRole,
    pub api_port: u16,
    pub gateway_port: u16,
    pub etcd_client_port: u16,
    pub etcd_peer_port: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct IngressView {
    pub ports: Vec<u16>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct EgressView {
    pub deny: Vec<String>,
    pub allow: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct TailscaleView {
    pub auth_key: Option<String>,
    pub advertise_routes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct DatadogView {
    pub api_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub site: Option<String>,
    pub include_ingress_logs: bool,
    pub include_tailscale_logs: bool,
    pub logs: DatadogLogsView,
    #[serde(default)]
    pub include_metrics: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct DatadogLogsView {
    pub include_healthcheck: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct DepotView {
    pub token: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct CloudflareView {
    pub tunnel: CloudflareTunnelView,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct CloudflareTunnelView {
    pub token: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub replicas: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct SlackView {
    pub webhook_url: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct GithubView {
    pub token: Option<String>,
    pub preview_domain: String,
    pub poll_interval_secs: u64,
    pub max_concurrent_previews: usize,
}

fn mask(value: &str) -> Option<String> {
    if value.is_empty() {
        None
    } else {
        Some("***".to_string())
    }
}

impl StartConfig {
    fn hydrate_node_selection(&mut self) -> Result<()> {
        if self.cluster.nodes.is_empty() {
            if self.node.name.is_some() {
                bail!("field `node` is invalid: only valid when `cluster.nodes` is configured");
            }
            return Ok(());
        }
        if self.subnet.is_some() {
            bail!("field `subnet` is invalid: only valid outside cluster mode");
        }
        let name = self
            .node
            .name
            .clone()
            .ok_or_else(|| anyhow!("field `node` is missing: required in cluster mode"))?;
        self.cluster.set_selected_node(name.clone())?;
        let selected = self
            .cluster
            .nodes
            .get(&name)
            .expect("selected cluster node was validated");
        self.node.role = selected.role;
        self.subnet = Some(selected.subnet.clone());
        Ok(())
    }

    pub fn masked(&self) -> MaskedConfig {
        MaskedConfig {
            cluster: ClusterView {
                name: self.cluster.name.clone(),
                nodes: self.cluster.nodes.clone(),
                control_allow_cidrs: self.cluster.control_allow_cidrs.clone(),
                join_secret: self.cluster.join_secret.as_deref().and_then(mask),
                labels: self.cluster.labels.clone(),
            },
            node: NodeView {
                name: self.node.name.clone(),
                role: self.node.role,
                api_port: self.cluster.api_port,
                gateway_port: self.cluster.gateway_port,
                etcd_client_port: self.cluster.etcd_client_port,
                etcd_peer_port: self.cluster.etcd_peer_port,
            },
            ingress: IngressView {
                ports: self.ingress.resolved_ports(),
            },
            subnet: self.subnet.clone(),
            egress: EgressView {
                deny: self.egress.deny.clone(),
                allow: self.egress.allow.clone(),
            },
            encryption_key: mask(&self.encryption_key),
            tailscale: self.tailscale.as_ref().map(|ts| TailscaleView {
                auth_key: mask(&ts.auth_key),
                advertise_routes: ts.advertise_routes.clone(),
            }),
            jwt_secret_key: self.jwt_secret_key.as_deref().and_then(mask),
            tags: self.tags.clone(),
            datadog: self.datadog.as_ref().map(|dd| DatadogView {
                api_key: mask(&dd.api_key),
                site: dd.site.clone(),
                include_ingress_logs: dd.include_ingress_logs,
                include_tailscale_logs: dd.include_tailscale_logs,
                logs: DatadogLogsView {
                    include_healthcheck: dd.logs.include_healthcheck,
                },
                include_metrics: dd.include_metrics,
            }),
            system: self.system.as_ref().map(|s| s.to_string()),
            runtime: self.runtime.to_string(),
            depot: self.depot.as_ref().map(|depot| DepotView {
                token: depot.token.as_ref().and_then(|t| mask(t.as_str())),
            }),
            cloudflare: self.cloudflare.as_ref().map(|cf| CloudflareView {
                tunnel: CloudflareTunnelView {
                    token: mask(cf.tunnel.token.as_str()),
                    replicas: cf.tunnel.replicas,
                },
            }),
            slack: self.slack.as_ref().map(|sl| SlackView {
                webhook_url: mask(sl.webhook_url.as_str()),
            }),
            github: self.github.as_ref().map(|github| GithubView {
                token: mask(github.token.as_str()),
                preview_domain: github.preview_domain.clone(),
                poll_interval_secs: github.poll_interval_secs,
                max_concurrent_previews: github.max_concurrent_previews,
            }),
            homepage: self.homepage.clone(),
            log_backup: self.log_backup.clone(),
            disable_etcd_cert: self.disable_etcd_cert,
            allow_cli_deployment: self.allow_cli_deployment,
            allow_exec: self.allow_exec,
        }
    }
}

pub async fn load_config(source: &str) -> Result<StartConfig> {
    load_config_with_diagnostics(source)
        .await
        .map(|(config, _)| config)
}

pub async fn load_config_with_diagnostics(source: &str) -> Result<(StartConfig, Vec<String>)> {
    let value = load_config_value(source).await?;
    let (mut config, ignored_fields) = deserialize_config_value::<StartConfig>(value)
        .map_err(|err| anyhow!("failed to parse merged config `{source}`: {err}"))?;
    normalize_integration_config(&mut config)?;
    config.hydrate_node_selection()?;
    Ok((config, ignored_fields))
}

pub(crate) fn deserialize_config_value<T>(value: serde_json::Value) -> Result<(T, Vec<String>)>
where
    T: DeserializeOwned,
{
    let serialized = serde_json::to_string(&value)?;
    let mut deserializer = serde_json::Deserializer::from_str(&serialized);
    let mut ignored_fields = BTreeSet::new();
    let mut track = serde_path_to_error::Track::new();
    let path_deserializer = serde_path_to_error::Deserializer::new(&mut deserializer, &mut track);
    let result: std::result::Result<T, _> = serde_ignored::deserialize(path_deserializer, |path| {
        let path = path
            .to_string()
            .split('.')
            .filter(|segment| *segment != "?")
            .collect::<Vec<_>>()
            .join(".");
        if path != "$schema" {
            ignored_fields.insert(path);
        }
    });

    match result {
        Ok(config) => Ok((config, ignored_fields.into_iter().collect())),
        Err(error) => {
            let path = track.path().to_string();
            bail!(format_deserialization_error(&path, &error.to_string()))
        }
    }
}

fn format_deserialization_error(path: &str, message: &str) -> String {
    let path = path.trim_matches('.');
    let message = message
        .split_once(" at line ")
        .map_or(message, |(message, _)| message);

    for kind in ["missing field", "unknown field"] {
        if let Some(field) = quoted_field(message, kind) {
            let path = append_field_path(path, field);
            if kind == "missing field" {
                return format!("{path}: required field is missing");
            }
            let expected = message
                .split_once(", expected ")
                .map(|(_, expected)| format!("; expected {expected}"))
                .unwrap_or_default();
            return format!("{path}: unknown field{expected}");
        }
    }

    let message = humanize_deserialization_message(message);
    if path.is_empty() {
        message
    } else {
        format!("{path}: {message}")
    }
}

fn humanize_deserialization_message(message: &str) -> String {
    let Some(type_error) = message.strip_prefix("invalid type: ") else {
        return message.to_string();
    };
    let Some((actual, expected)) = type_error.split_once(", expected ") else {
        return message.to_string();
    };
    format!(
        "expected {}, got {}",
        humanize_expected_type(expected),
        humanize_actual_type(actual)
    )
}

fn humanize_expected_type(expected: &str) -> &str {
    match expected {
        "a map" | "map" => "an object",
        "a sequence" | "sequence" => "an array",
        "a string" | "string" => "a string",
        "a boolean" | "boolean" | "bool" => "a boolean",
        "an integer" | "integer" => "an integer",
        "a floating point" | "floating point" => "a number",
        other => other,
    }
}

fn humanize_actual_type(actual: &str) -> &str {
    match actual.split_whitespace().next().unwrap_or(actual) {
        "map" => "an object",
        "sequence" => "an array",
        "string" | "borrowed" => "a string",
        "bool" | "boolean" => "a boolean",
        "integer" | "u64" | "i64" => "an integer",
        "floating" | "f64" => "a number",
        "unit" | "null" => "null",
        _ => "a value of the wrong type",
    }
}

fn quoted_field<'a>(message: &'a str, kind: &str) -> Option<&'a str> {
    let value = message.strip_prefix(kind)?.trim_start();
    let quote = value.chars().next()?;
    if !matches!(quote, '`' | '\'' | '"') {
        return None;
    }
    value[quote.len_utf8()..].split(quote).next()
}

fn append_field_path(path: &str, field: &str) -> String {
    if path.is_empty() {
        field.to_string()
    } else if path == field || path.ends_with(&format!(".{field}")) {
        path.to_string()
    } else {
        format!("{path}.{field}")
    }
}

fn normalize_integration_config(config: &mut StartConfig) -> Result<()> {
    if let Some(homepage) = &mut config.homepage {
        let trimmed = homepage.trim().trim_end_matches('/');
        let parsed = reqwest::Url::parse(trimmed)
            .map_err(|err| anyhow!("homepage must be an absolute HTTP(S) URL: {err}"))?;
        if !matches!(parsed.scheme(), "http" | "https") {
            bail!("homepage must use the http or https scheme");
        }
        if parsed.host_str().is_none() || parsed.query().is_some() || parsed.fragment().is_some() {
            bail!("homepage must have a host and cannot contain a query or fragment");
        }
        *homepage = trimmed.to_string();
    }

    if let Some(github) = &mut config.github {
        if github.token.as_str().trim().is_empty() {
            bail!("github.token cannot be empty");
        }
        github.token = SecretString::new(github.token.as_str().trim().to_string());
        let preview_domain = github
            .preview_domain
            .trim()
            .trim_end_matches('.')
            .to_ascii_lowercase();
        validate_dns_name(&preview_domain, "github.preview-domain")?;
        github.preview_domain = preview_domain;
        if github.poll_interval_secs == 0 {
            bail!("github.poll-interval-secs must be at least 1");
        }
        if github.max_concurrent_previews == 0 {
            bail!("github.max-concurrent-previews must be at least 1");
        }
    }
    Ok(())
}

fn validate_dns_name(value: &str, field: &str) -> Result<()> {
    let valid = !value.is_empty()
        && value.len() <= 253
        && value.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= 63
                && !label.starts_with('-')
                && !label.ends_with('-')
                && label.chars().all(|character| {
                    character.is_ascii_lowercase() || character.is_ascii_digit() || character == '-'
                })
        });
    if !valid {
        bail!("{field} must be a valid DNS name");
    }
    Ok(())
}

async fn load_config_value(source: &str) -> Result<serde_json::Value> {
    let mut current_source = source.to_string();
    let mut seen = HashSet::new();
    let mut layers = Vec::new();

    loop {
        if layers.len() > MAX_CONFIG_EXTENDS_DEPTH {
            bail!(
                "config `{source}` exceeds the maximum $extends depth of {MAX_CONFIG_EXTENDS_DEPTH}"
            );
        }

        let identity = config_source_identity(&current_source);
        if !seen.insert(identity) {
            bail!("config $extends cycle detected at `{current_source}`");
        }

        let raw = read_config_source(&current_source).await?;
        let mut value: serde_json::Value = json5::from_str(&raw)
            .or_else(|_| serde_json::from_str(&raw))
            .map_err(|err| anyhow!("failed to parse config `{current_source}`: {err}"))?;
        if !value.is_object() {
            bail!("config `{current_source}` must contain a JSON object at the top level");
        }
        normalize_start_config_keys(&mut value).map_err(|err| {
            anyhow!("failed to normalize config `{current_source}` field names: {err}")
        })?;
        let object = value.as_object_mut().ok_or_else(|| {
            anyhow!("config `{current_source}` must contain a JSON object at the top level")
        })?;
        let extends = object.remove(CONFIG_EXTENDS_KEY);
        layers.push(value);

        let Some(extends) = extends else {
            break;
        };
        let extends = extends.as_str().ok_or_else(|| {
            anyhow!("`$extends` in config `{current_source}` must be a non-empty string")
        })?;
        let extends = extends.trim();
        if extends.is_empty() {
            bail!("`$extends` in config `{current_source}` must be a non-empty string");
        }
        current_source = resolve_extended_source(&current_source, extends)?;
    }

    let mut layers = layers.into_iter().rev();
    let mut merged = layers
        .next()
        .expect("a config source always contributes one layer");
    for layer in layers {
        merge_config_value(&mut merged, layer);
    }
    Ok(merged)
}

fn normalize_start_config_keys(value: &mut serde_json::Value) -> Result<()> {
    let root = value
        .as_object_mut()
        .expect("the startup config top level was checked before normalization");
    normalize_object_aliases(root, "config", START_CONFIG_KEY_ALIASES)?;

    if let Some(cluster) = child_object_mut(root, "cluster") {
        normalize_object_aliases(cluster, "config.cluster", CLUSTER_CONFIG_KEY_ALIASES)?;
    }
    if let Some(tailscale) = child_object_mut(root, "tailscale") {
        normalize_object_aliases(tailscale, "config.tailscale", TAILSCALE_CONFIG_KEY_ALIASES)?;
    }
    if let Some(datadog) = child_object_mut(root, "datadog") {
        normalize_object_aliases(datadog, "config.datadog", DATADOG_CONFIG_KEY_ALIASES)?;
        if let Some(logs) = child_object_mut(datadog, "logs") {
            normalize_object_aliases(logs, "config.datadog.logs", DATADOG_LOGS_CONFIG_KEY_ALIASES)?;
        }
    }
    if let Some(slack) = child_object_mut(root, "slack") {
        normalize_object_aliases(slack, "config.slack", SLACK_CONFIG_KEY_ALIASES)?;
    }
    if let Some(log_backup) = child_object_mut(root, "log-backup") {
        normalize_object_aliases(
            log_backup,
            "config.log-backup",
            LOG_BACKUP_CONFIG_KEY_ALIASES,
        )?;
    }
    Ok(())
}

fn child_object_mut<'a>(
    parent: &'a mut serde_json::Map<String, serde_json::Value>,
    key: &str,
) -> Option<&'a mut serde_json::Map<String, serde_json::Value>> {
    parent.get_mut(key)?.as_object_mut()
}

fn normalize_object_aliases(
    object: &mut serde_json::Map<String, serde_json::Value>,
    path: &str,
    aliases: &[(&str, &str)],
) -> Result<()> {
    for &(alias, canonical) in aliases {
        let Some(value) = object.remove(alias) else {
            continue;
        };
        if object.contains_key(canonical) {
            bail!("both `{path}.{canonical}` and `{path}.{alias}` are set; use only one spelling");
        }
        object.insert(canonical.to_string(), value);
    }
    Ok(())
}

pub(crate) async fn read_config_source(source: &str) -> Result<String> {
    if let Some(path) = local_config_path(source) {
        std::fs::read_to_string(path)
            .map_err(|err| anyhow!("failed to read config file `{}`: {err}", path.display()))
    } else {
        SecretProvider::new(source, &Logger::noop())?
            .fetch_raw()
            .await
    }
}

fn local_config_path(source: &str) -> Option<&Path> {
    if source.starts_with("aws-secret://") {
        return None;
    }
    if let Some(path) = source.strip_prefix("file://") {
        return Some(Path::new(path));
    }
    (!source.contains("://")).then(|| Path::new(source))
}

fn config_source_identity(source: &str) -> String {
    let Some(path) = local_config_path(source) else {
        return source.to_string();
    };
    let absolute = std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
    format!("file://{}", absolute.display())
}

fn resolve_extended_source(current_source: &str, extends: &str) -> Result<String> {
    if extends.starts_with("aws-secret://") {
        return Ok(extends.to_string());
    }

    let explicit_file = extends.strip_prefix("file://");
    let extends_path = explicit_file.unwrap_or(extends);
    if explicit_file.is_none() && extends.contains("://") {
        return Ok(extends.to_string());
    }
    let extends_path = Path::new(extends_path);
    if extends_path.is_absolute() {
        return Ok(format!("file://{}", extends_path.display()));
    }

    let Some(current_path) = local_config_path(current_source) else {
        bail!(
            "relative `$extends` source `{extends}` cannot be resolved from remote config `{current_source}`; use an aws-secret:// source or an absolute file:// path"
        );
    };
    let parent = current_path.parent().unwrap_or_else(|| Path::new("."));
    Ok(format!("file://{}", parent.join(extends_path).display()))
}

fn merge_config_value(base: &mut serde_json::Value, overlay: serde_json::Value) {
    match (base, overlay) {
        (serde_json::Value::Object(base), serde_json::Value::Object(overlay)) => {
            for (key, value) in overlay {
                if let Some(existing) = base.get_mut(&key) {
                    merge_config_value(existing, value);
                } else {
                    base.insert(key, value);
                }
            }
        }
        (base, overlay) => *base = overlay,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_config_dir(label: &str) -> std::path::PathBuf {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("time")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "maestro-config-{label}-{}-{unique}",
            std::process::id()
        ))
    }

    #[test]
    fn cluster_nodes_are_named_and_endpoint_port_is_optional() {
        let default_port: ClusterConfig = serde_json::from_str(
            r#"{"name":"test","nodes":{"node1":{"endpoint":"10.20.0.11","subnet":"10.1.0.0/24","role":"master"}}}"#,
        )
        .unwrap();
        assert_eq!(
            default_port.nodes["node1"].endpoint.host_ip(),
            "10.20.0.11".parse::<Ipv4Addr>().unwrap()
        );
        assert_eq!(
            default_port.nodes["node1"].endpoint.explicit_api_port(),
            None
        );

        let endpoint: ClusterConfig =
            serde_json::from_str(r#"{"name":"test","nodes":{"node1":{"endpoint":"10.20.0.11:3101","subnet":"10.1.0.0/24","role":"master"}}}"#).unwrap();
        assert_eq!(
            endpoint.nodes["node1"].endpoint.explicit_api_port(),
            Some(3101)
        );
        assert_eq!(
            serde_json::to_value(&endpoint).unwrap()["nodes"]["node1"]["endpoint"],
            "10.20.0.11:3101"
        );

        let default_role: ClusterConfig = serde_json::from_str(
            r#"{"name":"test","nodes":{"node1":{"endpoint":"10.20.0.11","subnet":"10.1.0.0/24"}}}"#,
        )
        .unwrap();
        assert_eq!(
            default_role.nodes["node1"].role,
            crate::cluster::NodeRole::Hybrid
        );
    }

    #[test]
    fn legacy_cluster_node_shapes_are_rejected() {
        assert!(
            serde_json::from_str::<ClusterConfig>(r#"{"name":"test","nodes":["10.20.0.11"]}"#,)
                .is_err()
        );
        assert!(
            json5::from_str::<StartConfig>(
                r#"{
                    cluster: { name: "test", nodes: {} },
                    node: { role: "hybrid" },
                    ingress: { port: 8080 },
                    "encryption-key": "secret"
                }"#,
            )
            .is_err()
        );
    }

    #[test]
    fn named_node_selects_role_subnet_and_default_api_port() {
        let mut config: StartConfig = json5::from_str(
            r#"{
                node: "node2",
                cluster: {
                    name: "prod",
                    nodes: {
                        node1: { endpoint: "10.20.0.11", subnet: "10.1.0.0/24", role: "master" },
                        node2: { endpoint: "10.20.0.12", subnet: "10.2.0.0/24" },
                        node3: { endpoint: "10.20.0.13:3100", subnet: "10.3.0.0/24", role: "voter" }
                    }
                },
                ingress: { port: 8080 },
                "encryption-key": "secret"
            }"#,
        )
        .unwrap();
        config.hydrate_node_selection().unwrap();
        assert_eq!(config.node.name.as_deref(), Some("node2"));
        assert_eq!(config.node.role, crate::cluster::NodeRole::Hybrid);
        assert_eq!(config.subnet.as_deref(), Some("10.2.0.0/24"));
        assert_eq!(config.cluster.api_port, 3000);
    }

    #[test]
    fn camel_case_start_config_fields_deserialize_and_serialize_canonically() {
        let config: StartConfig = json5::from_str(
            r#"{
                cluster: {
                    name: "prod",
                    controlAllowCidrs: ["10.20.0.0/24"],
                    joinSecret: "0123456789abcdef0123456789abcdef"
                },
                ingress: { port: 8080 },
                encryptionKey: "encryption-secret",
                tailscale: {
                    authKey: "tailscale-secret",
                    advertiseRoutes: ["172.22.2.0/24"]
                },
                jwtSecretKey: "jwt-secret",
                datadog: {
                    apiKey: "datadog-secret",
                    includeIngressLogs: false,
                    includeTailscaleLogs: false,
                    logs: { includeHealthcheck: false },
                    includeMetrics: true
                },
                slack: { webhookUrl: "https://hooks.slack.test" },
                logBackup: {
                    bucket: "maestro-logs",
                    kmsKeyId: "kms-key",
                    retentionDays: 30
                },
                disableEtcdCert: true,
                allowCliDeployment: true
            }"#,
        )
        .expect("camelCase aliases should deserialize");

        assert_eq!(config.cluster.api_port, 3000);
        assert_eq!(config.cluster.control_allow_cidrs, vec!["10.20.0.0/24"]);
        assert_eq!(
            config.cluster.join_secret.as_deref(),
            Some("0123456789abcdef0123456789abcdef")
        );
        assert_eq!(
            config
                .tailscale
                .as_ref()
                .expect("tailscale")
                .advertise_routes,
            vec!["172.22.2.0/24"]
        );
        assert!(
            !config
                .datadog
                .as_ref()
                .expect("datadog")
                .include_ingress_logs
        );
        assert!(config.datadog.as_ref().expect("datadog").include_metrics);
        assert_eq!(
            config.log_backup.as_ref().expect("log backup").kms_key_id,
            "kms-key"
        );
        assert!(config.disable_etcd_cert);
        assert!(config.allow_cli_deployment);

        let serialized = serde_json::to_value(config).expect("serialize startup config");
        assert!(serialized.get("encryption-key").is_some());
        assert!(serialized.get("encryptionKey").is_none());
        assert!(serialized["cluster"].get("api-port").is_none());
        assert!(serialized["datadog"].get("include-metrics").is_some());
        assert!(serialized["datadog"].get("includeMetrics").is_none());
        assert!(serialized["log-backup"].get("kms-key-id").is_some());
        assert!(serialized.get("logBackup").is_none());
    }

    #[tokio::test]
    async fn camel_case_overrides_kebab_case_across_extended_configs() {
        let directory = temp_config_dir("camel-extends");
        std::fs::create_dir_all(&directory).unwrap();
        let base = directory.join("base.jsonc");
        let node = directory.join("node.jsonc");

        std::fs::write(
            &base,
            r#"{
                cluster: {
                    name: "prod",
                    labels: { apiPort: "label-must-not-be-normalized" }
                },
                ingress: { port: 8080 },
                "encryption-key": "base-key"
            }"#,
        )
        .unwrap();
        std::fs::write(
            &node,
            r#"{
                "$extends": "base.jsonc",
                encryptionKey: "node-key"
            }"#,
        )
        .unwrap();

        let config = load_config(node.to_str().unwrap()).await.unwrap();
        assert_eq!(config.encryption_key, "node-key");
        assert_eq!(
            config.cluster.labels.get("apiPort").map(String::as_str),
            Some("label-must-not-be-normalized")
        );

        std::fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn duplicate_camel_and_kebab_case_fields_are_rejected() {
        let directory = temp_config_dir("duplicate-case");
        std::fs::create_dir_all(&directory).unwrap();
        let config_path = directory.join("maestro.jsonc");
        std::fs::write(
            &config_path,
            r#"{
                cluster: {
                    name: "prod",
                    "control-allow-cidrs": ["10.20.0.0/24"],
                    controlAllowCidrs: ["10.30.0.0/24"]
                },
                ingress: { port: 8080 },
                encryptionKey: "secret"
            }"#,
        )
        .unwrap();

        let error = load_config(config_path.to_str().unwrap())
            .await
            .unwrap_err();
        assert!(error.to_string().contains(
            "both `config.cluster.control-allow-cidrs` and `config.cluster.controlAllowCidrs` are set"
        ));

        std::fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn extended_configs_merge_objects_and_replace_arrays() {
        let directory = temp_config_dir("extends");
        std::fs::create_dir_all(&directory).unwrap();
        let base = directory.join("base.jsonc");
        let shared = directory.join("shared.jsonc");
        let node = directory.join("node.jsonc");

        std::fs::write(
            &base,
            r#"{
                cluster: {
                    name: "prod",
                    nodes: {
                        node1: { endpoint: "10.20.0.11", subnet: "172.22.1.0/24", role: "master" },
                        node2: { endpoint: "10.20.0.12:3101", subnet: "172.22.2.0/24", role: "worker" }
                    },
                    labels: { environment: "prod", region: "us-west-2" }
                },
                ingress: { port: 8080 },
                "encryption-key": "base-key",
                tags: ["base"],
                tailscale: {
                    "auth-key": "tailscale-key",
                    "advertise-routes": ["172.22.1.0/24"]
                },
                cloudflare: {
                    tunnel: { token: "base-token", replicas: 3 }
                }
            }"#,
        )
        .unwrap();
        std::fs::write(
            &shared,
            r#"{
                "$extends": "base.jsonc",
                cluster: {
                    labels: { environment: "staging", zone: "us-west-2a" }
                },
                tags: ["shared"],
                tailscale: { "advertise-routes": ["172.22.2.0/24"] }
            }"#,
        )
        .unwrap();
        std::fs::write(
            &node,
            r#"{
                "$extends": "shared.jsonc",
                node: "node2",
                cloudflare: { tunnel: { token: "node-token" } }
            }"#,
        )
        .unwrap();

        let config = load_config(node.to_str().unwrap()).await.unwrap();
        assert_eq!(config.cluster.name, "prod");
        assert_eq!(config.cluster.api_port, 3101);
        assert_eq!(config.cluster.nodes.len(), 2);
        assert_eq!(config.cluster.labels["environment"], "staging");
        assert_eq!(config.cluster.labels["region"], "us-west-2");
        assert_eq!(config.cluster.labels["zone"], "us-west-2a");
        assert_eq!(config.node.role, crate::cluster::NodeRole::Worker);
        assert_eq!(config.subnet.as_deref(), Some("172.22.2.0/24"));
        assert_eq!(config.tags, vec!["shared"]);
        assert_eq!(
            config.tailscale.unwrap().advertise_routes,
            vec!["172.22.2.0/24"]
        );
        assert_eq!(
            config.cloudflare.unwrap().tunnel.replicas,
            Some(3),
            "an override must preserve inherited sibling fields"
        );

        std::fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn extended_config_cycles_are_rejected() {
        let directory = temp_config_dir("cycle");
        std::fs::create_dir_all(&directory).unwrap();
        let first = directory.join("first.jsonc");
        let second = directory.join("second.jsonc");
        std::fs::write(&first, r#"{ "$extends": "second.jsonc" }"#).unwrap();
        std::fs::write(&second, r#"{ "$extends": "first.jsonc" }"#).unwrap();

        let error = load_config(first.to_str().unwrap()).await.unwrap_err();
        assert!(error.to_string().contains("$extends cycle"));

        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn remote_configs_require_explicit_extended_sources() {
        assert_eq!(
            resolve_extended_source(
                "aws-secret://maestro/staging/node-1",
                "aws-secret://maestro/staging/common"
            )
            .unwrap(),
            "aws-secret://maestro/staging/common"
        );
        let error =
            resolve_extended_source("aws-secret://maestro/staging/node-1", "../common.jsonc")
                .unwrap_err();
        assert!(error.to_string().contains("aws-secret:// source"));
    }

    #[test]
    fn masked_start_config_never_serializes_plaintext_secrets() {
        let config: StartConfig = json5::from_str(
            r#"{
                cluster: {
                    name: "test",
                    "join-secret": "join-plaintext-sentinel"
                },
                ingress: { port: 8080 },
                subnet: "172.22.0.0/16",
                "encryption-key": "encryption-plaintext-sentinel",
                "jwt-secret-key": "jwt-plaintext-sentinel",
                tailscale: { "auth-key": "tailscale-plaintext-sentinel" },
                datadog: { "api-key": "datadog-plaintext-sentinel" },
                depot: { token: "depot-plaintext-sentinel" },
                cloudflare: {
                    tunnel: { token: "cloudflare-plaintext-sentinel" }
                },
                slack: { "webhook-url": "slack-plaintext-sentinel" },
                github: { token: "github-plaintext-sentinel" },
                homepage: "http://maestro.example.test/"
            }"#,
        )
        .expect("start config");

        let json = serde_json::to_string(&config.masked()).expect("masked config JSON");
        for plaintext in [
            "join-plaintext-sentinel",
            "encryption-plaintext-sentinel",
            "jwt-plaintext-sentinel",
            "tailscale-plaintext-sentinel",
            "datadog-plaintext-sentinel",
            "depot-plaintext-sentinel",
            "cloudflare-plaintext-sentinel",
            "slack-plaintext-sentinel",
            "github-plaintext-sentinel",
        ] {
            assert!(!json.contains(plaintext), "API config leaked `{plaintext}`");
        }
        assert!(json.matches("***").count() >= 9);
        assert!(json.contains("http://maestro.example.test/"));
    }

    #[test]
    fn integration_config_normalizes_homepage_and_github_defaults() {
        let mut config: StartConfig = json5::from_str(
            r#"{
                cluster: { name: "test" },
                ingress: { port: 8080 },
                "encryption-key": "secret",
                homepage: " http://maestro.example.test/root/ ",
                github: { token: "token" }
            }"#,
        )
        .unwrap();
        normalize_integration_config(&mut config).unwrap();
        assert_eq!(
            config.homepage.as_deref(),
            Some("http://maestro.example.test/root")
        );
        let github = config.github.unwrap();
        assert_eq!(github.preview_domain, "preview.getbaton.ai");
        assert_eq!(github.poll_interval_secs, 60);
        assert_eq!(github.max_concurrent_previews, 10);
    }

    #[test]
    fn integration_config_rejects_invalid_homepage_and_preview_domain() {
        let mut config: StartConfig = json5::from_str(
            r#"{
                cluster: { name: "test" },
                ingress: { port: 8080 },
                "encryption-key": "secret",
                homepage: "ftp://maestro.example.test"
            }"#,
        )
        .unwrap();
        assert!(
            normalize_integration_config(&mut config)
                .unwrap_err()
                .to_string()
                .contains("http or https")
        );

        let mut config: StartConfig = json5::from_str(
            r#"{
                cluster: { name: "test" },
                ingress: { port: 8080 },
                "encryption-key": "secret",
                github: { token: "token", "preview-domain": "https://bad.example" }
            }"#,
        )
        .unwrap();
        assert!(
            normalize_integration_config(&mut config)
                .unwrap_err()
                .to_string()
                .contains("valid DNS name")
        );
    }
}
