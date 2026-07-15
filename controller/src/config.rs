use std::collections::BTreeMap;
use std::fmt;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::path::Path;

use anyhow::{Result, anyhow, bail};
use serde::{Deserialize, Serialize};

use crate::logs::Logger;
use crate::utils::crypto::SecretString;
use crate::utils::secrets::SecretProvider;

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
    pub encryption_key: String,
    #[serde(default)]
    pub tailscale: Option<TailscaleConfig>,
    #[serde(default)]
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
    pub log_backup: Option<LogBackupConfig>,
    #[serde(default)]
    pub disable_etcd_cert: bool,
    #[serde(default)]
    pub allow_cli_deployment: bool,
}

#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct NodeConfig {
    #[serde(default)]
    pub role: crate::cluster::NodeRole,
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
    pub webhook_url: SecretString,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct LogBackupConfig {
    pub bucket: String,
    pub kms_key_id: String,
    #[serde(default)]
    pub region: Option<String>,
    #[serde(default)]
    pub prefix: Option<String>,
    #[serde(default)]
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
#[serde(rename_all = "kebab-case")]
pub struct ClusterConfig {
    pub name: String,
    #[serde(default)]
    pub nodes: Vec<ClusterNodeConfig>,
    #[serde(default)]
    pub bind_ip: Option<Ipv4Addr>,
    #[serde(default)]
    pub subnets: Vec<String>,
    #[serde(default = "default_cluster_api_port")]
    pub api_port: u16,
    #[serde(default = "default_cluster_gateway_port")]
    pub gateway_port: u16,
    #[serde(default)]
    pub control_allow_cidrs: Vec<String>,
    #[serde(default = "default_etcd_client_port")]
    pub etcd_client_port: u16,
    #[serde(default = "default_etcd_peer_port")]
    pub etcd_peer_port: u16,
    #[serde(default)]
    pub shared_registry: Option<String>,
    #[serde(default)]
    pub join_secret: Option<String>,
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ClusterNodeConfig {
    Endpoint(SocketAddrV4),
    Address(Ipv4Addr),
}

impl ClusterNodeConfig {
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

impl fmt::Display for ClusterNodeConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Endpoint(endpoint) => endpoint.fmt(formatter),
            Self::Address(ip) => ip.fmt(formatter),
        }
    }
}

impl std::str::FromStr for ClusterNodeConfig {
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

impl ClusterConfig {
    pub fn uses_node_ports(&self) -> bool {
        self.nodes
            .first()
            .is_some_and(|node| node.explicit_api_port().is_some())
    }

    pub fn resolved_nodes(&self) -> Result<Vec<crate::cluster::ClusterNodeEndpoint>> {
        self.nodes
            .iter()
            .copied()
            .map(|node| self.resolve_node(node))
            .collect()
    }

    pub fn local_endpoint(
        &self,
        host_ip: Ipv4Addr,
        role: crate::cluster::NodeRole,
    ) -> Result<crate::cluster::ClusterNodeEndpoint> {
        if self.uses_node_ports() {
            if role.is_voter() {
                let matches = self
                    .resolved_nodes()?
                    .into_iter()
                    .filter(|node| node.host_ip == host_ip && node.api_port == self.api_port)
                    .collect::<Vec<_>>();
                return match matches.as_slice() {
                    [node] => Ok(*node),
                    [] => bail!(
                        "this voter's cluster.api-port ({}) does not select a configured node endpoint on {host_ip}",
                        self.api_port
                    ),
                    _ => bail!(
                        "cluster.nodes contains duplicate endpoint `{host_ip}:{}`",
                        self.api_port
                    ),
                };
            }
            return self.resolve_node(ClusterNodeConfig::Endpoint(SocketAddrV4::new(
                host_ip,
                self.api_port,
            )));
        }
        self.resolve_node(ClusterNodeConfig::Address(host_ip))
    }

    fn resolve_node(&self, node: ClusterNodeConfig) -> Result<crate::cluster::ClusterNodeEndpoint> {
        let host_ip = node.host_ip();
        if let Some(api_port) = node.explicit_api_port() {
            let gateway_port = api_port.checked_add(1).ok_or_else(|| {
                anyhow!("cluster node endpoint `{node}` leaves no room for its gateway port")
            })?;
            let etcd_client_port = api_port.checked_add(2).ok_or_else(|| {
                anyhow!("cluster node endpoint `{node}` leaves no room for its etcd client port")
            })?;
            let etcd_peer_port = api_port.checked_add(3).ok_or_else(|| {
                anyhow!("cluster node endpoint `{node}` leaves no room for its etcd peer port")
            })?;
            Ok(crate::cluster::ClusterNodeEndpoint {
                host_ip,
                api_port,
                gateway_port,
                etcd_client_port,
                etcd_peer_port,
                identity_api_port: Some(api_port),
            })
        } else {
            Ok(crate::cluster::ClusterNodeEndpoint {
                host_ip,
                api_port: self.api_port,
                gateway_port: self.gateway_port,
                etcd_client_port: self.etcd_client_port,
                etcd_peer_port: self.etcd_peer_port,
                identity_api_port: None,
            })
        }
    }
}

fn default_cluster_api_port() -> u16 {
    3001
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
    pub auth_key: String,
    #[serde(default)]
    pub advertise_routes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct DatadogConfig {
    pub api_key: String,
    #[serde(default)]
    pub site: Option<String>,
    #[serde(default = "default_true")]
    pub include_ingress_logs: bool,
    #[serde(default = "default_true")]
    pub include_tailscale_logs: bool,
    #[serde(default)]
    pub logs: DatadogLogsConfig,
    #[serde(default)]
    pub include_metrics: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct DatadogLogsConfig {
    #[serde(default = "default_true")]
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
    pub log_backup: Option<LogBackupConfig>,
    pub disable_etcd_cert: bool,
    #[serde(default)]
    pub allow_cli_deployment: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct ClusterView {
    pub name: String,
    pub nodes: Vec<ClusterNodeConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bind_ip: Option<Ipv4Addr>,
    pub subnets: Vec<String>,
    pub api_port: u16,
    pub gateway_port: u16,
    pub control_allow_cidrs: Vec<String>,
    pub etcd_client_port: u16,
    pub etcd_peer_port: u16,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub shared_registry: Option<String>,
    pub join_secret: Option<String>,
    pub labels: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct NodeView {
    pub role: crate::cluster::NodeRole,
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

fn mask(value: &str) -> Option<String> {
    if value.is_empty() {
        None
    } else {
        Some("***".to_string())
    }
}

impl StartConfig {
    pub fn masked(&self) -> MaskedConfig {
        MaskedConfig {
            cluster: ClusterView {
                name: self.cluster.name.clone(),
                nodes: self.cluster.nodes.clone(),
                bind_ip: self.cluster.bind_ip,
                subnets: self.cluster.subnets.clone(),
                api_port: self.cluster.api_port,
                gateway_port: self.cluster.gateway_port,
                control_allow_cidrs: self.cluster.control_allow_cidrs.clone(),
                etcd_client_port: self.cluster.etcd_client_port,
                etcd_peer_port: self.cluster.etcd_peer_port,
                shared_registry: self.cluster.shared_registry.clone(),
                join_secret: self.cluster.join_secret.as_deref().and_then(mask),
                labels: self.cluster.labels.clone(),
            },
            node: NodeView {
                role: self.node.role,
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
            log_backup: self.log_backup.clone(),
            disable_etcd_cert: self.disable_etcd_cert,
            allow_cli_deployment: self.allow_cli_deployment,
        }
    }
}

pub async fn load_config(source: &str) -> Result<StartConfig> {
    let path = source.strip_prefix("file://").unwrap_or(source);
    let raw = if Path::new(path).exists() {
        std::fs::read_to_string(path)
            .map_err(|err| anyhow!("failed to read config file `{path}`: {err}"))?
    } else {
        SecretProvider::new(source, &Logger::noop())?
            .fetch_raw()
            .await?
    };
    let config: StartConfig = json5::from_str(&raw)
        .or_else(|_| serde_json::from_str(&raw))
        .map_err(|err| anyhow!("failed to parse config `{source}`: {err}"))?;
    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cluster_nodes_accept_bare_ips_and_controller_endpoints() {
        let legacy: ClusterConfig =
            serde_json::from_str(r#"{"name":"test","nodes":["10.20.0.11"]}"#).unwrap();
        assert_eq!(
            legacy.nodes[0].host_ip(),
            "10.20.0.11".parse::<Ipv4Addr>().unwrap()
        );
        assert_eq!(legacy.nodes[0].explicit_api_port(), None);

        let endpoint: ClusterConfig =
            serde_json::from_str(r#"{"name":"test","nodes":["10.20.0.11:3101"]}"#).unwrap();
        assert_eq!(
            endpoint.nodes[0].host_ip(),
            "10.20.0.11".parse::<Ipv4Addr>().unwrap()
        );
        assert_eq!(endpoint.nodes[0].explicit_api_port(), Some(3101));
        assert_eq!(
            serde_json::to_value(&endpoint).unwrap()["nodes"][0],
            "10.20.0.11:3101"
        );
    }
}
