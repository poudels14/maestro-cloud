use std::fmt;
use std::path::Path;

use anyhow::{Result, anyhow};
use serde::Deserialize;

use crate::utils::secrets::SecretProvider;

#[derive(Debug, Clone, Copy, Default, PartialEq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RuntimeType {
    #[default]
    Docker,
    Nerdctl,
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
    pub encryption_key: String,
    #[serde(default)]
    pub tailscale: Option<TailscaleConfig>,
    #[serde(default)]
    pub jwt_secret: Option<String>,
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default)]
    pub datadog: Option<DatadogConfig>,
    #[serde(default)]
    pub system: Option<SystemType>,
    #[serde(default)]
    pub runtime: RuntimeType,
    #[serde(default)]
    pub disable_etcd_cert: bool,
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
    #[serde(default)]
    pub include_system_logs: bool,
}

pub async fn load_config(source: &str) -> Result<StartConfig> {
    let path = source.strip_prefix("file://").unwrap_or(source);
    let raw = if Path::new(path).exists() {
        std::fs::read_to_string(path)
            .map_err(|err| anyhow!("failed to read config file `{path}`: {err}"))?
    } else {
        SecretProvider::fetch_from_source(source).await?
    };
    let config: StartConfig = json5::from_str(&raw)
        .or_else(|_| serde_json::from_str(&raw))
        .map_err(|err| anyhow!("failed to parse config `{source}`: {err}"))?;
    Ok(config)
}
