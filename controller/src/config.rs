use std::fmt;
use std::path::Path;

use anyhow::{Result, anyhow};
use serde::{Deserialize, Serialize};

use crate::logs::Logger;
use crate::utils::crypto::SecretString;
use crate::utils::secrets::SecretProvider;

#[derive(Debug, Clone, Copy, Default, PartialEq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RuntimeType {
    #[default]
    Docker,
    Nerdctl,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Deserialize)]
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

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct StartConfig {
    pub cluster: ClusterConfig,
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
    pub disable_etcd_cert: bool,
    #[serde(default)]
    pub allow_cli_deployment: bool,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct EgressConfig {
    #[serde(default)]
    pub deny: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct DepotConfig {
    #[serde(default)]
    pub token: Option<SecretString>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct CloudflareConfig {
    pub tunnel: CloudflareTunnelConfig,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct CloudflareTunnelConfig {
    pub token: SecretString,
    #[serde(default)]
    pub replicas: Option<u32>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct SlackConfig {
    pub webhook_url: SecretString,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
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

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct ClusterConfig {
    pub name: String,
}

#[derive(Debug, Deserialize)]
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

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct TailscaleConfig {
    pub auth_key: String,
}

#[derive(Debug, Deserialize)]
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
    pub include_metrics: bool,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct MaskedConfig {
    pub cluster: ClusterView,
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
    pub disable_etcd_cert: bool,
    #[serde(default)]
    pub allow_cli_deployment: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct ClusterView {
    pub name: String,
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct TailscaleView {
    pub auth_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct DatadogView {
    pub api_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub site: Option<String>,
    pub include_ingress_logs: bool,
    pub include_tailscale_logs: bool,
    #[serde(default)]
    pub include_metrics: bool,
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
            },
            ingress: IngressView {
                ports: self.ingress.resolved_ports(),
            },
            subnet: self.subnet.clone(),
            egress: EgressView {
                deny: self.egress.deny.clone(),
            },
            encryption_key: mask(&self.encryption_key),
            tailscale: self.tailscale.as_ref().map(|ts| TailscaleView {
                auth_key: mask(&ts.auth_key),
            }),
            jwt_secret_key: self.jwt_secret_key.as_deref().and_then(mask),
            tags: self.tags.clone(),
            datadog: self.datadog.as_ref().map(|dd| DatadogView {
                api_key: mask(&dd.api_key),
                site: dd.site.clone(),
                include_ingress_logs: dd.include_ingress_logs,
                include_tailscale_logs: dd.include_tailscale_logs,
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
