use std::collections::BTreeMap;
use std::net::Ipv4Addr;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::ids::ServiceId;
use crate::resources::{OomPriority, ResourceSpec};

const INGRESS_HOST_VARIABLE: &str = "MAESTRO_INGRESS_HOST";
const INGRESS_PORT_VARIABLE: &str = "MAESTRO_INGRESS_PORT";
const DEFAULT_HEALTHCHECK_INTERVAL_SECS: u32 = 60;
const MIN_HEALTHCHECK_INTERVAL_SECS: u32 = 5;
const MAX_HEALTHCHECK_INTERVAL_SECS: u32 = 300;
const DEFAULT_INGRESS_PORT: u16 = 80;
const DEFAULT_AFFINITY_HEADER: &str = "X-Session-Affinity";
const DEFAULT_CLOSE_GRACE_PERIOD: &str = "1d";
const PREVIEW_ID_SUFFIX: &str = "-pr-99999";
const PREVIEW_ID_MARKER: &str = "-pr-";
const SEALED_VALUE_PREFIX: &str = "enc:";
const MAX_LABEL_LENGTH: usize = 63;
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServicesFile {
    pub services: BTreeMap<ServiceId, ServiceSpec>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ServiceSpec {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub build: Option<BuildSpec>,
    #[serde(default)]
    pub deploy: DeploySpec,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ingress: Option<IngressSpec>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preview: Option<PreviewSpec>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BuildSpec {
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
    pub depot: Option<DepotSpec>,
    #[serde(default, skip_serializing_if = "EnvSpec::is_empty")]
    pub env: EnvSpec,
    #[serde(default, skip_serializing_if = "EnvSpec::is_empty")]
    pub secrets: EnvSpec,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DepotSpec {
    pub project: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeploySpec {
    #[serde(default = "default_replicas")]
    pub replicas: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command: Option<Command>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub flags: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub expose_ports: Vec<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub healthcheck_path: Option<String>,
    #[serde(default = "default_healthcheck_interval")]
    pub healthcheck_interval: u32,
    #[serde(default = "default_true")]
    pub exec: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_restarts: Option<u32>,
    #[serde(default, skip_serializing_if = "EnvSpec::is_empty")]
    pub env: EnvSpec,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secrets: Option<SecretsSpec>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub volumes: Vec<VolumeMount>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_affinity: Option<NodeAffinity>,
    #[serde(default, skip_serializing_if = "EgressSpec::is_empty")]
    pub egress: EgressSpec,
    #[serde(default, skip_serializing_if = "is_default")]
    pub resources: ResourceSpec,
    #[serde(default, skip_serializing_if = "is_default")]
    pub oom_priority: OomPriority,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Command {
    pub command: String,
    #[serde(default)]
    pub args: Vec<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EnvSpec {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub items: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SecretsSpec {
    pub mount_path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub items: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VolumeMount {
    pub host_path: String,
    pub mount_path: String,
    #[serde(default)]
    pub read_only: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<VolumeOwner>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VolumeOwner {
    pub uid: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gid: Option<u32>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeAffinity {
    #[serde(default, alias = "node-id", skip_serializing_if = "Option::is_none")]
    pub node: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub labels: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EgressSpec {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allow: Vec<EgressRule>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EgressRule {
    pub cidr: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ports: Vec<u16>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IngressSpec {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub hosts: Vec<String>,
    #[serde(default = "default_ingress_port")]
    pub port: u16,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_affinity: Option<SessionAffinity>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionAffinity {
    #[serde(default = "default_affinity_header")]
    pub header: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PreviewSpec {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_close_grace_period")]
    pub close_grace_period: String,
    #[serde(default = "default_replicas")]
    pub replicas: u32,
    #[serde(default, skip_serializing_if = "EnvSpec::is_empty")]
    pub env: EnvSpec,
}

#[derive(Debug, Error)]
pub enum SpecError {
    #[error("failed to parse services file: {0}")]
    Parse(#[from] json5::Error),
    #[error("service {service}: {reason}")]
    Invalid { service: ServiceId, reason: String },
}

impl ServicesFile {
    pub fn parse(text: &str) -> Result<Self, SpecError> {
        let file: Self = json5::from_str(text)?;
        file.services
            .iter()
            .try_for_each(|(service_id, spec)| spec.validate(service_id))?;
        match host_conflict(file.services.iter()) {
            Some(conflict) => Err(conflict),
            None => Ok(file),
        }
    }
}

pub fn host_conflict<'a>(
    services: impl Iterator<Item = (&'a ServiceId, &'a ServiceSpec)>,
) -> Option<SpecError> {
    let mut owners = BTreeMap::<String, &ServiceId>::new();
    services
        .flat_map(|(service_id, spec)| {
            spec.ingress
                .iter()
                .flat_map(|ingress| ingress.all_hosts())
                .map(move |host| (host.to_ascii_lowercase(), service_id))
                .collect::<Vec<_>>()
        })
        .find_map(
            |(host, service_id)| match owners.insert(host.clone(), service_id) {
                Some(owner) if owner != service_id => Some(SpecError::Invalid {
                    service: service_id.clone(),
                    reason: format!("ingress host {host} is already used by service {owner}"),
                }),
                _ => None,
            },
        )
}

impl ServiceSpec {
    pub fn ingress_variables(&self) -> BTreeMap<String, String> {
        let hosts = self
            .ingress
            .iter()
            .flat_map(IngressSpec::all_hosts)
            .map(|host| host.trim().to_lowercase())
            .collect::<std::collections::BTreeSet<_>>();
        let host = match hosts.len() {
            1 => hosts.into_iter().find(|host| !host.starts_with("*.")),
            _ => None,
        };
        host.map(|host| (INGRESS_HOST_VARIABLE.to_string(), host))
            .into_iter()
            .chain(
                self.ingress
                    .as_ref()
                    .map(|ingress| (INGRESS_PORT_VARIABLE.to_string(), ingress.port.to_string())),
            )
            .collect()
    }

    pub fn has_secret_sources(&self) -> bool {
        let build_sources = self
            .build
            .as_ref()
            .is_some_and(|build| build.env.source.is_some() || build.secrets.source.is_some());
        let secrets_source = self
            .deploy
            .secrets
            .as_ref()
            .is_some_and(|secrets| secrets.source.is_some());
        self.deploy.env.source.is_some() || secrets_source || build_sources
    }

    pub fn validate(&self, service_id: &ServiceId) -> Result<(), SpecError> {
        let problem = self.problems(service_id).into_iter().next();
        match problem {
            Some(reason) => Err(SpecError::Invalid {
                service: service_id.clone(),
                reason,
            }),
            None => Ok(()),
        }
    }

    pub fn is_preview_source(&self) -> bool {
        self.preview.as_ref().is_some_and(|preview| preview.enabled)
    }

    fn problems(&self, service_id: &ServiceId) -> Vec<String> {
        let id = service_id.as_str();
        let max_id_length = match self.is_preview_source() {
            true => MAX_LABEL_LENGTH - PREVIEW_ID_SUFFIX.len(),
            false => MAX_LABEL_LENGTH,
        };
        let deploy = &self.deploy;
        let writable_volume = deploy.volumes.iter().any(|volume| !volume.read_only);
        let identity = [
            (!is_dns_label(id) || id.len() > max_id_length).then(|| {
                format!("id must be a lowercase DNS label of at most {max_id_length} characters")
            }),
            id.contains(PREVIEW_ID_MARKER)
                .then(|| format!("id must not contain {PREVIEW_ID_MARKER}")),
            self.name
                .trim()
                .is_empty()
                .then(|| "name is required".to_string()),
        ];
        let source = [
            match (&self.image, &self.build) {
                (Some(_), Some(_)) => Some("set either image or build, not both".to_string()),
                (None, None) => Some("one of image or build is required".to_string()),
                _ => None,
            },
            self.build
                .as_ref()
                .and_then(|build| dockerfile_problem(&build.dockerfile)),
            self.build
                .as_ref()
                .and_then(|build| build.repo.as_deref())
                .and_then(repo_problem),
            self.build
                .as_ref()
                .and_then(|build| build.branch.as_deref())
                .and_then(branch_problem),
            self.build.as_ref().and_then(|build| {
                (build.watch && build.branch.is_none())
                    .then(|| "build.watch requires build.branch".to_string())
            }),
        ];
        let runtime = [
            (writable_volume && deploy.replicas > 1)
                .then(|| "deploy.replicas must be 1 when a volume is writable".to_string()),
            deploy.secrets.as_ref().and_then(|secrets| {
                absolute_path_problem("deploy.secrets.mountPath", &secrets.mount_path)
            }),
        ];
        let volumes = deploy.volumes.iter().flat_map(volume_problems);
        let sealed = sealed_value_problem(self);
        let egress = deploy.egress.allow.iter().filter_map(egress_problem);
        let ingress = self.ingress.iter().flat_map(ingress_problems).chain(sealed);
        let preview = self
            .preview
            .iter()
            .flat_map(|preview| preview_problems(self, preview));
        identity
            .into_iter()
            .chain(source)
            .chain(runtime)
            .flatten()
            .chain(volumes)
            .chain(egress)
            .chain(ingress)
            .chain(preview)
            .collect()
    }
}

impl DeploySpec {
    pub fn effective_healthcheck_interval(&self) -> u32 {
        self.healthcheck_interval
            .clamp(MIN_HEALTHCHECK_INTERVAL_SECS, MAX_HEALTHCHECK_INTERVAL_SECS)
    }
}

impl Default for DeploySpec {
    fn default() -> Self {
        Self {
            replicas: default_replicas(),
            command: None,
            flags: Vec::new(),
            expose_ports: Vec::new(),
            healthcheck_path: None,
            healthcheck_interval: default_healthcheck_interval(),
            exec: true,
            max_restarts: None,
            env: EnvSpec::default(),
            secrets: None,
            volumes: Vec::new(),
            node_affinity: None,
            egress: EgressSpec::default(),
            resources: ResourceSpec::default(),
            oom_priority: OomPriority::default(),
        }
    }
}

impl EnvSpec {
    pub fn is_empty(&self) -> bool {
        self.source.is_none() && self.items.is_empty()
    }
}

impl EgressSpec {
    pub fn is_empty(&self) -> bool {
        self.allow.is_empty()
    }
}

impl IngressSpec {
    pub fn all_hosts(&self) -> impl Iterator<Item = &str> {
        self.host
            .iter()
            .chain(self.hosts.iter())
            .map(String::as_str)
    }
}

fn volume_problems(volume: &VolumeMount) -> Vec<String> {
    let segments = volume.host_path.split('/').skip(1).collect::<Vec<_>>();
    let well_formed = segments.len() >= 2 && segments.iter().all(|segment| !segment.is_empty());
    [
        absolute_path_problem("volume hostPath", &volume.host_path),
        absolute_path_problem("volume mountPath", &volume.mount_path),
        (!well_formed).then(|| {
            format!(
                "volume hostPath {} must be a directory inside a volume root such as /data",
                volume.host_path
            )
        }),
    ]
    .into_iter()
    .flatten()
    .collect()
}

fn egress_problem(rule: &EgressRule) -> Option<String> {
    let canonical = rule.cidr.split_once('/').and_then(|(address, prefix)| {
        let address = address.parse::<Ipv4Addr>().ok()?;
        let prefix = prefix.parse::<u8>().ok().filter(|prefix| *prefix <= 32)?;
        let mask = u32::MAX.checked_shl(32 - u32::from(prefix)).unwrap_or(0);
        (u32::from(address) & mask == u32::from(address)).then_some(())
    });
    let ports_valid = rule.ports.iter().all(|port| *port > 0);
    match (canonical, ports_valid) {
        (None, _) => Some(format!(
            "egress cidr {} must be a canonical IPv4 CIDR",
            rule.cidr
        )),
        (Some(()), false) => Some("egress ports must be between 1 and 65535".to_string()),
        (Some(()), true) => None,
    }
}

fn ingress_problems(ingress: &IngressSpec) -> Vec<String> {
    let hosts = ingress.all_hosts().collect::<Vec<_>>();
    let header_problem = ingress.session_affinity.as_ref().and_then(|affinity| {
        let valid = !affinity.header.is_empty()
            && !affinity.header.eq_ignore_ascii_case("host")
            && affinity
                .header
                .chars()
                .all(|character| character.is_ascii_alphanumeric() || character == '-');
        (!valid).then(|| format!("sessionAffinity header {} is not valid", affinity.header))
    });
    [
        hosts
            .is_empty()
            .then(|| "ingress requires host or hosts".to_string()),
        hosts
            .iter()
            .find(|host| !valid_hostname(host))
            .map(|host| format!("ingress host {host} must be a lowercase DNS name")),
        header_problem,
    ]
    .into_iter()
    .flatten()
    .collect()
}

fn preview_problems(spec: &ServiceSpec, preview: &PreviewSpec) -> Vec<String> {
    let grace_valid = parse_duration_secs(&preview.close_grace_period).is_some();
    let github_repo = spec
        .build
        .as_ref()
        .and_then(|build| build.repo.as_deref())
        .is_some_and(|repo| repo.contains("github.com"));
    match preview.enabled {
        false => (!grace_valid)
            .then(|| "preview.closeGracePeriod must look like 30m, 12h, or 1d".to_string())
            .into_iter()
            .collect(),
        true => [
            (!grace_valid)
                .then(|| "preview.closeGracePeriod must look like 30m, 12h, or 1d".to_string()),
            (preview.replicas != 1).then(|| "preview.replicas must be 1".to_string()),
            (!github_repo).then(|| "previews require a github.com build.repo".to_string()),
            spec.ingress
                .is_none()
                .then(|| "previews require ingress".to_string()),
        ]
        .into_iter()
        .flatten()
        .collect(),
    }
}

fn valid_hostname(host: &str) -> bool {
    host.len() <= 253
        && host.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= MAX_LABEL_LENGTH
                && !label.starts_with('-')
                && !label.ends_with('-')
                && label.chars().all(|character| {
                    character.is_ascii_lowercase() || character.is_ascii_digit() || character == '-'
                })
        })
}

fn sealed_value_problem(spec: &ServiceSpec) -> Option<String> {
    let deploy_secrets = spec
        .deploy
        .secrets
        .iter()
        .flat_map(|secrets| secrets.items.values());
    let build_values = spec
        .build
        .iter()
        .flat_map(|build| build.env.items.values().chain(build.secrets.items.values()));
    let preview_values = spec
        .preview
        .iter()
        .flat_map(|preview| preview.env.items.values());
    spec.deploy
        .env
        .items
        .values()
        .chain(deploy_secrets)
        .chain(build_values)
        .chain(preview_values)
        .any(|value| value.starts_with(SEALED_VALUE_PREFIX))
        .then(|| "env and secret values must be plaintext, not sealed enc: values".to_string())
}

fn dockerfile_problem(dockerfile: &str) -> Option<String> {
    let escapes =
        dockerfile.starts_with('/') || dockerfile.split('/').any(|segment| segment == "..");
    match (dockerfile.trim().is_empty(), escapes) {
        (true, _) => Some("build.dockerfile is required".to_string()),
        (false, true) => {
            Some("build.dockerfile must be a path inside the build context".to_string())
        }
        (false, false) => None,
    }
}

fn repo_problem(repo: &str) -> Option<String> {
    let scp_style = repo
        .strip_prefix("git@")
        .and_then(|rest| rest.split_once(':'))
        .is_some_and(|(host, path)| !host.is_empty() && !path.is_empty());
    let https = repo
        .strip_prefix("https://")
        .is_some_and(|rest| rest.split('/').next().is_some_and(|host| !host.is_empty()));
    let plain = repo
        .chars()
        .all(|character| !character.is_whitespace() && !character.is_control());
    (!(scp_style || https) || !plain)
        .then(|| "build.repo must be an https:// or git@host:path repository URL".to_string())
}

fn branch_problem(branch: &str) -> Option<String> {
    let valid = !branch.is_empty()
        && !branch.starts_with('-')
        && branch
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || "._/-".contains(character));
    (!valid).then(|| "build.branch must be a plain branch name".to_string())
}

fn absolute_path_problem(label: &str, path: &str) -> Option<String> {
    let has_dot_segments = path
        .split('/')
        .any(|segment| segment == "." || segment == "..");
    (!path.starts_with('/') || has_dot_segments)
        .then(|| format!("{label} must be an absolute path without . or .. segments"))
}

pub fn parse_duration_secs(value: &str) -> Option<u64> {
    let trimmed = value.trim();
    let unit = trimmed.chars().last()?;
    let amount = trimmed
        .strip_suffix(unit)?
        .parse::<u64>()
        .ok()
        .filter(|amount| *amount > 0)?;
    let multiplier = match unit {
        's' => Some(1),
        'm' => Some(60),
        'h' => Some(3_600),
        'd' => Some(86_400),
        _ => None,
    }?;
    amount.checked_mul(multiplier)
}

fn default_replicas() -> u32 {
    1
}

fn default_healthcheck_interval() -> u32 {
    DEFAULT_HEALTHCHECK_INTERVAL_SECS
}

fn default_true() -> bool {
    true
}

fn default_ingress_port() -> u16 {
    DEFAULT_INGRESS_PORT
}

fn default_affinity_header() -> String {
    DEFAULT_AFFINITY_HEADER.to_string()
}

fn default_close_grace_period() -> String {
    DEFAULT_CLOSE_GRACE_PERIOD.to_string()
}

fn is_default<T: Default + PartialEq>(value: &T) -> bool {
    *value == T::default()
}

fn is_dns_label(value: &str) -> bool {
    let starts_with_letter = value
        .chars()
        .next()
        .is_some_and(|character| character.is_ascii_lowercase());
    starts_with_letter
        && value.len() <= MAX_LABEL_LENGTH
        && !value.ends_with('-')
        && value.chars().all(|character| {
            character.is_ascii_lowercase() || character.is_ascii_digit() || character == '-'
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exposes_the_single_ingress_host_and_port() {
        let spec = |ingress: &str| {
            serde_json::from_str::<ServiceSpec>(&format!(
                r#"{{ "name": "web", "image": "nginx", "ingress": {ingress} }}"#
            ))
            .unwrap()
            .ingress_variables()
        };
        let single = spec(r#"{ "hosts": ["App.Example.com"], "port": 8080 }"#);
        assert_eq!(single["MAESTRO_INGRESS_HOST"], "app.example.com");
        assert_eq!(single["MAESTRO_INGRESS_PORT"], "8080");
        let several = spec(r#"{ "hosts": ["a.example.com", "b.example.com"], "port": 80 }"#);
        assert!(!several.contains_key("MAESTRO_INGRESS_HOST"));
        let wildcard = spec(r#"{ "hosts": ["*.example.com"], "port": 80 }"#);
        assert!(!wildcard.contains_key("MAESTRO_INGRESS_HOST"));
    }
    use crate::resources::{MemoryLimit, Millicores};

    #[test]
    fn parses_v1_style_services_file() {
        let file = ServicesFile::parse(
            r#"{
              // comments are allowed
              "services": {
                "api": {
                  "name": "API",
                  "image": "ghcr.io/acme/api:1",
                  "ingress": {
                    "host": "api.example.com",
                    "port": 8080,
                    "sessionAffinity": {}
                  },
                  "deploy": {
                    "replicas": 1,
                    "healthcheckPath": "/health",
                    "flags": ["--verbose"],
                    "env": { "items": { "RUST_LOG": "info" } },
                    "secrets": { "mountPath": "/app/.env", "items": { "TOKEN": "x" } },
                    "volumes": [{ "hostPath": "/var/lib/maestro-volumes/api", "mountPath": "/var/lib/api", "owner": { "uid": 1000 } }],
                    "nodeAffinity": { "node-id": "node1", "labels": { "zone": "a" } },
                    "egress": { "allow": [{ "cidr": "10.0.10.0/24", "ports": [5432] }] },
                    "resources": { "cpu": 2, "memory": "8Gi" },
                    "oomPriority": "high",
                  },
                },
              },
            }"#,
        )
        .unwrap();
        let api = &file.services[&ServiceId::from("api")];
        let deploy = &api.deploy;
        assert_eq!(
            deploy.healthcheck_interval,
            DEFAULT_HEALTHCHECK_INTERVAL_SECS
        );
        assert!(deploy.exec);
        assert_eq!(deploy.flags, ["--verbose"]);
        assert_eq!(deploy.secrets.as_ref().unwrap().mount_path, "/app/.env");
        assert_eq!(deploy.volumes[0].owner.unwrap().uid, 1000);
        assert_eq!(
            deploy.node_affinity.as_ref().unwrap().node.as_deref(),
            Some("node1")
        );
        assert_eq!(deploy.egress.allow[0].ports, [5432]);
        assert_eq!(deploy.resources.cpu, Some(Millicores::new(2000)));
        assert!(matches!(
            deploy.resources.memory,
            Some(MemoryLimit::Size(_))
        ));
        assert_eq!(deploy.oom_priority, OomPriority::High);
        let ingress = api.ingress.as_ref().unwrap();
        assert_eq!(ingress.all_hosts().collect::<Vec<_>>(), ["api.example.com"]);
        assert_eq!(
            ingress.session_affinity.as_ref().unwrap().header,
            "X-Session-Affinity"
        );
    }

    #[test]
    fn applies_v1_defaults() {
        let file = ServicesFile::parse(
            r#"{ "services": { "web": { "name": "Web", "image": "nginx", "ingress": { "host": "web.local" } } } }"#,
        )
        .unwrap();
        let web = &file.services[&ServiceId::from("web")];
        assert_eq!(web.ingress.as_ref().unwrap().port, 80);
        assert_eq!(web.deploy.healthcheck_interval, 60);
        let mut quick = web.deploy.clone();
        quick.healthcheck_interval = 1;
        assert_eq!(quick.effective_healthcheck_interval(), 5);
    }

    #[test]
    fn rejects_invalid_services() {
        let cases = [
            r#"{ "services": { "Api": { "name": "x", "image": "nginx" } } }"#,
            r#"{ "services": { "api": { "name": "x" } } }"#,
            r#"{ "services": { "api": { "name": "x", "image": "nginx", "build": { "dockerfile": "Dockerfile" } } } }"#,
            r#"{ "services": { "api": { "name": "x", "image": "nginx", "ingress": { "port": 80 } } } }"#,
            r#"{ "services": { "api-pr-1": { "name": "x", "image": "nginx" } } }"#,
            r#"{ "services": { "api": { "name": " ", "image": "nginx" } } }"#,
            r#"{ "services": { "api": { "name": "x", "image": "nginx", "deploy": { "replicas": 2, "volumes": [{ "hostPath": "/data", "mountPath": "/data" }] } } } }"#,
            r#"{ "services": { "api": { "name": "x", "image": "nginx", "deploy": { "volumes": [{ "hostPath": "/", "mountPath": "/data", "readOnly": true }] } } } }"#,
            r#"{ "services": { "api": { "name": "x", "image": "nginx", "deploy": { "volumes": [{ "hostPath": "/data/../etc", "mountPath": "/data" }] } } } }"#,
            r#"{ "services": { "api": { "name": "x", "image": "nginx", "deploy": { "volumes": [{ "hostPath": "/var/lib/maestro-volumes/../maestro/keys", "mountPath": "/data" }] } } } }"#,
            r#"{ "services": { "api": { "name": "x", "image": "nginx", "deploy": { "volumes": [{ "hostPath": "//var/lib/maestro-volumes/a", "mountPath": "/data" }] } } } }"#,
            r#"{ "services": { "api": { "name": "x", "image": "nginx", "deploy": { "volumes": [{ "hostPath": "/var/lib/maestro-volumes//a", "mountPath": "/data" }] } } } }"#,
            r#"{ "services": { "api": { "name": "x", "image": "nginx", "deploy": { "egress": { "allow": [{ "cidr": "10.0.10.1/24" }] } } } } }"#,
            r#"{ "services": { "api": { "name": "x", "image": "nginx", "ingress": { "host": "a`b" } } } }"#,
            r#"{ "services": { "api": { "name": "x", "build": { "dockerfile": "Dockerfile", "watch": true } } } }"#,
            r#"{ "services": { "api": { "name": "x", "build": { "dockerfile": "../Dockerfile" } } } }"#,
            r#"{ "services": { "api": { "name": "x", "image": "nginx", "ingress": { "host": "a.b) || Host(c" } } } }"#,
            r#"{ "services": { "api": { "name": "x", "image": "nginx", "ingress": { "host": "a.b" } }, "web": { "name": "y", "image": "nginx", "ingress": { "hosts": ["a.b"] } } } }"#,
            r#"{ "services": { "api": { "name": "x", "image": "nginx", "deploy": { "env": { "items": { "A": "enc:v1:abc" } } } } } }"#,
            r#"{ "services": { "api": { "name": "x", "build": { "dockerfile": "/etc/passwd" } } } }"#,
            r#"{ "services": { "api": { "name": "x", "build": { "repo": "--upload-pack=touch /tmp/x", "dockerfile": "Dockerfile" } } } }"#,
            r#"{ "services": { "api": { "name": "x", "build": { "repo": "file:///etc", "dockerfile": "Dockerfile" } } } }"#,
            r#"{ "services": { "api": { "name": "x", "build": { "repo": "ext::sh -c id", "dockerfile": "Dockerfile" } } } }"#,
            r#"{ "services": { "api": { "name": "x", "build": { "repo": "https://github.com/a/b", "branch": "--orphan", "dockerfile": "Dockerfile" } } } }"#,
            r#"{ "services": { "api": { "name": "x", "image": "nginx", "ingress": { "host": "a.b" }, "preview": { "enabled": true } } } }"#,
            r#"{ "services": { "api": { "name": "x", "image": "nginx", "preview": { "closeGracePeriod": "1w" } } } }"#,
        ];
        cases.iter().for_each(|case| {
            assert!(
                matches!(ServicesFile::parse(case), Err(SpecError::Invalid { .. })),
                "expected invalid: {case}"
            );
        });
    }

    #[test]
    fn parses_durations() {
        assert_eq!(parse_duration_secs("30m"), Some(1_800));
        assert_eq!(parse_duration_secs("1d"), Some(86_400));
        assert_eq!(parse_duration_secs("0h"), None);
        assert_eq!(parse_duration_secs("2w"), None);
        assert_eq!(parse_duration_secs("5é"), None);
        assert_eq!(parse_duration_secs("é"), None);
        assert_eq!(parse_duration_secs("99999999999999999999d"), None);
        assert_eq!(parse_duration_secs("18446744073709551615d"), None);
    }

    #[test]
    fn omits_defaults_when_serialized() {
        let spec = ServiceSpec {
            name: "Web".into(),
            image: Some("nginx".into()),
            build: None,
            deploy: DeploySpec::default(),
            ingress: None,
            preview: None,
        };
        let json = serde_json::to_value(&spec).unwrap();
        assert_eq!(
            json["deploy"],
            serde_json::json!({ "replicas": 1, "healthcheckInterval": 60, "exec": true })
        );
    }
}
