use std::collections::BTreeMap;

use kernel_api::{
    FirewallDirection, FirewallPolicySpec, FirewallRule, FirewallSubject, FirewallVerdict,
    IngressRouteSpec, PortRange, ServiceId, ServiceRolloutSpec, ServiceSpec, SessionAffinity,
    TransportProtocol,
};
use serde::Deserialize;

use crate::CliError;
use crate::config_source::{ConfigSourceReader, decode_document, load_merged};
use crate::service_config_convert::BuildSourceSelection;
use crate::service_config_convert::convert_service;

#[derive(Debug)]
pub(crate) struct LoadedServices {
    pub(crate) services: BTreeMap<ServiceId, DesiredService>,
    pub(crate) ignored_fields: Vec<String>,
}

#[derive(Debug)]
pub(crate) struct LoadedUploadedService {
    pub(crate) service_id: ServiceId,
    pub(crate) desired: DesiredService,
    pub(crate) ignored_fields: Vec<String>,
}

#[derive(Debug)]
pub(crate) struct DesiredService {
    pub(crate) spec: ServiceSpec,
    pub(crate) ingress: Option<IngressConfig>,
    pub(crate) egress: Vec<EgressRule>,
}

impl DesiredService {
    pub(crate) fn rollout_spec(
        &self,
        service_id: &ServiceId,
    ) -> Result<ServiceRolloutSpec, CliError> {
        let ingress = self
            .ingress
            .as_ref()
            .map(|ingress| {
                let mut hosts = ingress.hosts.clone();
                if let Some(host) = &ingress.host {
                    hosts.push(host.clone());
                }
                hosts.sort();
                hosts.dedup();
                let target_port = ingress.port.ok_or_else(|| {
                    CliError::invalid_input(format!(
                        "services.{service_id}.ingress.port: is required"
                    ))
                })?;
                Ok(IngressRouteSpec {
                    service_id: service_id.clone(),
                    hosts,
                    path_prefix: None,
                    target_port,
                    session_affinity: ingress.session_affinity.as_ref().map(|affinity| {
                        SessionAffinity {
                            header: affinity.header.clone(),
                        }
                    }),
                })
            })
            .transpose()?;
        let egress = (!self.egress.is_empty()).then(|| FirewallPolicySpec {
            direction: FirewallDirection::Egress,
            subject: FirewallSubject::Service(service_id.clone()),
            rules: self
                .egress
                .iter()
                .map(|rule| FirewallRule {
                    cidr: rule.cidr.clone(),
                    protocol: TransportProtocol::Any,
                    ports: rule
                        .ports
                        .iter()
                        .map(|port| PortRange {
                            start: *port,
                            end: *port,
                        })
                        .collect(),
                    verdict: FirewallVerdict::Allow,
                })
                .collect(),
            default_verdict: FirewallVerdict::Deny,
        });
        Ok(ServiceRolloutSpec {
            service: self.spec.clone(),
            ingress,
            egress,
        })
    }
}

pub(crate) async fn load_services(
    source: &str,
    reader: &impl ConfigSourceReader,
) -> Result<LoadedServices, CliError> {
    let merged = load_merged(source, reader).await?;
    decode_services(source, merged, reader).await
}

pub(crate) async fn decode_services(
    source: &str,
    merged: serde_json::Value,
    reader: &impl ConfigSourceReader,
) -> Result<LoadedServices, CliError> {
    let (document, ignored_fields): (ServicesDocument, _) =
        decode_document(&merged, &format!("services config `{source}`"))?;
    if document.services.is_empty() {
        return Err(CliError::invalid_input(format!(
            "services: no services configured in `{source}`"
        )));
    }
    let mut services = BTreeMap::new();
    for (raw_id, template) in document.services {
        let id = ServiceId::new(raw_id.clone())
            .map_err(|error| CliError::invalid_input(format!("services.{raw_id}: {error}")))?;
        let desired = convert_service(
            source,
            &id,
            template,
            BuildSourceSelection::ConfiguredGit,
            reader,
        )
        .await?;
        services.insert(id, desired);
    }
    Ok(LoadedServices {
        services,
        ignored_fields,
    })
}

pub(crate) async fn load_uploaded_service(
    source: &str,
    service_id: Option<ServiceId>,
    archive_id: kernel_api::ArtifactArchiveId,
    reader: &impl ConfigSourceReader,
) -> Result<LoadedUploadedService, CliError> {
    let merged = load_merged(source, reader).await?;
    decode_uploaded_service(source, merged, service_id, archive_id, reader).await
}

pub(crate) async fn decode_uploaded_service(
    source: &str,
    merged: serde_json::Value,
    service_id: Option<ServiceId>,
    archive_id: kernel_api::ArtifactArchiveId,
    reader: &impl ConfigSourceReader,
) -> Result<LoadedUploadedService, CliError> {
    let (document, ignored_fields): (ServicesDocument, _) =
        decode_document(&merged, &format!("services config `{source}`"))?;
    let mut services = document.services;
    let service_id = match service_id {
        Some(service_id) => service_id,
        None if services.len() == 1 => {
            let raw_id = services
                .first_key_value()
                .map(|(raw_id, _)| raw_id.clone())
                .ok_or_else(|| CliError::invalid_input("services: no services configured"))?;
            ServiceId::new(raw_id.clone())
                .map_err(|error| CliError::invalid_input(format!("services.{raw_id}: {error}")))?
        }
        None if services.is_empty() => {
            return Err(CliError::invalid_input(format!(
                "services: no services configured in `{source}`"
            )));
        }
        None => {
            let configured = services
                .keys()
                .map(String::as_str)
                .collect::<Vec<_>>()
                .join(", ");
            return Err(CliError::invalid_input(format!(
                "`services up` requires SERVICE_ID when `{source}` configures multiple services: \
                 {configured}"
            )));
        }
    };
    let template = services.remove(service_id.as_str()).ok_or_else(|| {
        CliError::invalid_input(format!(
            "service `{service_id}` is not configured in `{source}`"
        ))
    })?;
    if template.build.is_none() || template.image.is_some() {
        return Err(CliError::invalid_input(format!(
            "services.{service_id}: `services up` requires `build` and does not accept `image`"
        )));
    }
    let desired = convert_service(
        source,
        &service_id,
        template,
        BuildSourceSelection::UploadedArchive(archive_id),
        reader,
    )
    .await?;
    Ok(LoadedUploadedService {
        service_id,
        desired,
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
    pub(super) registry: Option<String>,
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
    pub(super) host_path: Option<String>,
    pub(super) managed_volume: Option<String>,
    pub(super) replica_managed_volume: Option<String>,
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
