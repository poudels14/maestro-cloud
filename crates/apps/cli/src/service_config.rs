use std::collections::BTreeMap;

use kernel_api::{ServiceId, ServiceSpec};
use serde::Deserialize;

use crate::CliError;
use crate::config_source::{ConfigSourceReader, load_merged};
use crate::service_config_convert::convert_service;

#[derive(Debug)]
pub(crate) struct LoadedServices {
    pub(crate) services: BTreeMap<ServiceId, DesiredService>,
    pub(crate) ignored_fields: Vec<String>,
}

#[derive(Debug)]
pub(crate) struct DesiredService {
    pub(crate) spec: ServiceSpec,
    pub(crate) ingress: Option<IngressConfig>,
    pub(crate) egress: Vec<EgressRule>,
}

pub(crate) async fn load_services(
    source: &str,
    reader: &impl ConfigSourceReader,
) -> Result<LoadedServices, CliError> {
    let merged = load_merged(source, reader).await?;
    let encoded = serde_json::to_vec(&merged)
        .map_err(|error| CliError::json("failed to encode merged services config", error))?;
    let mut deserializer = serde_json::Deserializer::from_slice(&encoded);
    let mut ignored_fields = Vec::new();
    let document: ServicesDocument = serde_ignored::deserialize(&mut deserializer, |path| {
        ignored_fields.push(path.to_string());
    })
    .map_err(|error| {
        CliError::invalid_input(format!(
            "failed to parse services config `{source}`: {error}"
        ))
    })?;
    if document.services.is_empty() {
        return Err(CliError::invalid_input(format!(
            "services: no services configured in `{source}`"
        )));
    }
    ignored_fields.sort();
    ignored_fields.dedup();
    let mut services = BTreeMap::new();
    for (raw_id, template) in document.services {
        let id = ServiceId::new(raw_id.clone())
            .map_err(|error| CliError::invalid_input(format!("services.{raw_id}: {error}")))?;
        let desired = convert_service(source, &id, template, reader).await?;
        services.insert(id, desired);
    }
    Ok(LoadedServices {
        services,
        ignored_fields,
    })
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ServicesDocument {
    #[serde(rename = "$schema", default)]
    _schema: Option<String>,
    services: BTreeMap<String, ServiceTemplate>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct ServiceTemplate {
    pub(super) name: String,
    #[serde(default)]
    pub(super) build: Option<BuildConfig>,
    #[serde(default)]
    pub(super) image: Option<String>,
    pub(super) deploy: DeployConfig,
    #[serde(default)]
    pub(super) ingress: Option<IngressConfig>,
    #[serde(default)]
    pub(super) preview: Option<PreviewConfig>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct BuildConfig {
    pub(super) repo: Option<String>,
    pub(super) branch: Option<String>,
    pub(super) dockerfile: String,
    #[serde(default)]
    pub(super) watch: bool,
    #[serde(default)]
    pub(super) env: ValueSource,
    #[serde(default)]
    pub(super) secrets: ValueSource,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct ValueSource {
    pub(super) source: Option<String>,
    #[serde(default)]
    pub(super) items: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct DeployConfig {
    #[serde(default)]
    pub(super) flags: Vec<String>,
    #[serde(default)]
    pub(super) expose_ports: Vec<u16>,
    #[serde(default)]
    pub(super) command: Option<CommandConfig>,
    pub(super) healthcheck_path: Option<String>,
    #[serde(default = "default_healthcheck_interval")]
    pub(super) healthcheck_interval: u32,
    #[serde(default = "default_replicas")]
    pub(super) replicas: u32,
    #[serde(default = "default_true")]
    pub(super) exec: bool,
    pub(super) max_restarts: Option<u32>,
    #[serde(default)]
    pub(super) env: ValueSource,
    pub(super) secrets: Option<SecretConfig>,
    #[serde(default)]
    pub(super) volumes: Vec<VolumeConfig>,
    #[serde(default)]
    pub(super) node_affinity: PlacementConfig,
    #[serde(default)]
    pub(super) egress: EgressConfig,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct CommandConfig {
    pub(super) command: String,
    #[serde(default)]
    pub(super) args: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct SecretConfig {
    pub(super) mount_path: String,
    pub(super) source: Option<String>,
    #[serde(default)]
    pub(super) items: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct VolumeConfig {
    pub(super) host_path: String,
    pub(super) mount_path: String,
    #[serde(default)]
    pub(super) read_only: bool,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(super) struct PlacementConfig {
    pub(super) node_id: Option<String>,
    #[serde(default)]
    pub(super) labels: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct IngressConfig {
    pub(crate) host: Option<String>,
    #[serde(default)]
    pub(crate) hosts: Vec<String>,
    pub(crate) port: Option<u16>,
    pub(crate) session_affinity: Option<SessionAffinityConfig>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SessionAffinityConfig {
    pub(crate) header: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct PreviewConfig {
    #[serde(default)]
    pub(super) enabled: bool,
    #[serde(default = "default_preview_close_grace")]
    pub(super) close_grace_period: String,
    #[serde(default = "default_replicas")]
    pub(super) replicas: u32,
    #[serde(default)]
    pub(super) env: ValueSource,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct EgressConfig {
    #[serde(default)]
    pub(super) allow: Vec<EgressRule>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct EgressRule {
    pub(crate) cidr: String,
    #[serde(default)]
    pub(crate) ports: Vec<u16>,
}

fn default_healthcheck_interval() -> u32 {
    60
}

fn default_replicas() -> u32 {
    1
}

fn default_true() -> bool {
    true
}

fn default_preview_close_grace() -> String {
    "1d".to_string()
}
