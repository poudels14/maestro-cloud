use std::collections::BTreeMap;
use std::path::PathBuf;

use cluster::{DEFAULT_CLOUDFLARE_TUNNEL_REPLICAS, DEFAULT_WIREGUARD_PORT};
use kernel_api::NodeRole;
use serde::Deserialize;

const DEFAULT_GATEWAY_PORT: u16 = 3_001;
const DEFAULT_STORE_CLIENT_PORT: u16 = 2_379;
const DEFAULT_STORE_PEER_PORT: u16 = 2_380;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) struct ClusterDocument {
    #[serde(rename = "$schema", default)]
    _schema: Option<String>,
    pub(crate) jwt_secret_key: String,
    pub(crate) encryption_key: String,
    pub(crate) cluster: ClusterInput,
    #[serde(default)]
    pub(crate) node: Option<String>,
    #[serde(default)]
    pub(crate) tailscale: Option<TailscaleInput>,
    #[serde(default)]
    pub(crate) cloudflare: Option<CloudflareInput>,
    #[serde(default)]
    pub(crate) datadog: Option<DatadogInput>,
    #[serde(default)]
    pub(crate) depot: Option<DepotInput>,
    #[serde(default)]
    pub(crate) log_backup: Option<LogBackupInput>,
    #[serde(default)]
    pub(crate) preview: Option<PreviewInput>,
    #[serde(default)]
    pub(crate) nixos_upgrade: Option<NixosUpgradeInput>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) struct DatadogInput {
    pub(crate) api_key: String,
    pub(crate) site: String,
    #[serde(default = "default_true")]
    pub(crate) include_ingress_logs: bool,
    #[serde(default = "default_true")]
    pub(crate) include_tailscale_logs: bool,
    #[serde(default)]
    pub(crate) logs: DatadogLogsInput,
    #[serde(default)]
    pub(crate) metrics: DatadogMetricsInput,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) struct DatadogLogsInput {
    #[serde(default = "default_true")]
    pub(crate) include_healthcheck: bool,
}

impl Default for DatadogLogsInput {
    fn default() -> Self {
        Self {
            include_healthcheck: true,
        }
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) struct DatadogMetricsInput {
    #[serde(default)]
    pub(crate) enabled: bool,
    #[serde(default)]
    pub(crate) tags: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) struct DepotInput {
    pub(crate) token: String,
    #[serde(default = "default_depot_executable")]
    pub(crate) executable: PathBuf,
    #[serde(default = "default_depot_timeout_secs")]
    pub(crate) timeout_secs: u64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) struct LogBackupInput {
    pub(crate) bucket: String,
    pub(crate) kms_key_id: String,
    #[serde(default)]
    pub(crate) region: Option<String>,
    #[serde(default)]
    pub(crate) prefix: Option<String>,
    #[serde(default)]
    pub(crate) retention_days: Option<u64>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) struct PreviewInput {
    pub(crate) domain: String,
    pub(crate) github_token: String,
    pub(crate) max_concurrent_previews: usize,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) struct NixosUpgradeInput {
    pub(crate) flake: PathBuf,
    #[serde(default = "default_nixos_configuration")]
    pub(crate) configuration: String,
    #[serde(default)]
    pub(crate) nix_binary: Option<PathBuf>,
    #[serde(default)]
    pub(crate) nixos_rebuild_binary: Option<PathBuf>,
    #[serde(default)]
    pub(crate) systemctl_binary: Option<PathBuf>,
}

const fn default_true() -> bool {
    true
}

fn default_depot_executable() -> PathBuf {
    PathBuf::from("depot")
}

const fn default_depot_timeout_secs() -> u64 {
    30 * 60
}

fn default_nixos_configuration() -> String {
    "default".to_owned()
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) struct TailscaleInput {
    pub(crate) auth_key: String,
    #[serde(default)]
    pub(crate) advertise_routes: Option<Vec<String>>,
    #[serde(default = "default_tailscale_replicas")]
    pub(crate) replicas: u32,
    #[serde(default = "default_tailscale_tags")]
    pub(crate) tags: Vec<String>,
    #[serde(default)]
    pub(crate) cross_cluster_dns: Vec<CrossClusterDnsInput>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) struct CloudflareInput {
    pub(crate) tunnel: CloudflareTunnelInput,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) struct CloudflareTunnelInput {
    pub(crate) token: String,
    #[serde(default = "default_cloudflare_tunnel_replicas")]
    pub(crate) replicas: u32,
}

const fn default_cloudflare_tunnel_replicas() -> u32 {
    DEFAULT_CLOUDFLARE_TUNNEL_REPLICAS
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub(crate) struct CrossClusterDnsInput {
    pub(crate) cluster_id: String,
    pub(crate) nameservers: Vec<String>,
}

const fn default_tailscale_replicas() -> u32 {
    2
}

fn default_tailscale_tags() -> Vec<String> {
    Vec::new()
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) struct ClusterInput {
    #[serde(default)]
    pub(crate) cluster_id: Option<String>,
    pub(crate) name: String,
    #[serde(default)]
    pub(crate) nodes: BTreeMap<String, NodeInput>,
    #[serde(default)]
    pub(crate) control_allow_cidrs: Vec<String>,
    #[serde(default)]
    pub(crate) ports: PortsInput,
    #[serde(default)]
    pub(crate) join_secret: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) struct NodeInput {
    pub(crate) endpoint: String,
    pub(crate) subnet: String,
    #[serde(default)]
    pub(crate) hostname: Option<String>,
    #[serde(default)]
    pub(crate) role: NodeRoleInput,
}

#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum NodeRoleInput {
    Master,
    #[default]
    Hybrid,
    ControlPlane,
    Worker,
}

impl From<NodeRoleInput> for NodeRole {
    fn from(value: NodeRoleInput) -> Self {
        match value {
            NodeRoleInput::Master => Self::Master,
            NodeRoleInput::Hybrid => Self::Hybrid,
            NodeRoleInput::ControlPlane => Self::ControlPlane,
            NodeRoleInput::Worker => Self::Worker,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) struct PortsInput {
    #[serde(default = "default_gateway")]
    pub(crate) gateway: u16,
    #[serde(default = "default_store_client")]
    pub(crate) store_client: u16,
    #[serde(default = "default_store_peer")]
    pub(crate) store_peer: u16,
    #[serde(default = "default_wireguard")]
    pub(crate) wireguard: u16,
}

impl Default for PortsInput {
    fn default() -> Self {
        Self {
            gateway: default_gateway(),
            store_client: default_store_client(),
            store_peer: default_store_peer(),
            wireguard: default_wireguard(),
        }
    }
}

fn default_gateway() -> u16 {
    DEFAULT_GATEWAY_PORT
}

fn default_store_client() -> u16 {
    DEFAULT_STORE_CLIENT_PORT
}

fn default_store_peer() -> u16 {
    DEFAULT_STORE_PEER_PORT
}

fn default_wireguard() -> u16 {
    DEFAULT_WIREGUARD_PORT
}
