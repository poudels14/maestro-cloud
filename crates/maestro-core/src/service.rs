use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::ids::ServiceId;
use crate::resources::{OomPriority, ResourceSpec};

const DEFAULT_HEALTHCHECK_INTERVAL_SECS: u32 = 10;
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
    #[serde(default, skip_serializing_if = "EnvSpec::is_empty")]
    pub env: EnvSpec,
    #[serde(default, skip_serializing_if = "EnvSpec::is_empty")]
    pub secrets: EnvSpec,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeploySpec {
    #[serde(default = "default_replicas")]
    pub replicas: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command: Option<Command>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub expose_ports: Vec<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub healthcheck_path: Option<String>,
    #[serde(default = "default_healthcheck_interval")]
    pub healthcheck_interval: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_restarts: Option<u32>,
    #[serde(default, skip_serializing_if = "EnvSpec::is_empty")]
    pub env: EnvSpec,
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
pub struct IngressSpec {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub hosts: Vec<String>,
    pub port: u16,
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
            .try_for_each(|(service_id, spec)| spec.validate(service_id))
            .map(|_| file)
    }
}

impl ServiceSpec {
    pub fn validate(&self, service_id: &ServiceId) -> Result<(), SpecError> {
        let failure = match (&self.image, &self.build, &self.ingress) {
            _ if !is_dns_label(service_id.as_str()) => Some(format!(
                "id must be a lowercase DNS label of at most {MAX_LABEL_LENGTH} characters"
            )),
            (Some(_), Some(_), _) => Some("set either image or build, not both".to_string()),
            (None, None, _) => Some("one of image or build is required".to_string()),
            (_, _, Some(ingress)) if ingress.host.is_none() && ingress.hosts.is_empty() => {
                Some("ingress requires host or hosts".to_string())
            }
            _ if self.deploy.healthcheck_interval == 0 => {
                Some("deploy.healthcheckInterval must be at least 1 second".to_string())
            }
            _ => None,
        };
        match failure {
            Some(reason) => Err(SpecError::Invalid {
                service: service_id.clone(),
                reason,
            }),
            None => Ok(()),
        }
    }
}

impl Default for DeploySpec {
    fn default() -> Self {
        Self {
            replicas: default_replicas(),
            command: None,
            expose_ports: Vec::new(),
            healthcheck_path: None,
            healthcheck_interval: default_healthcheck_interval(),
            max_restarts: None,
            env: EnvSpec::default(),
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

impl IngressSpec {
    pub fn all_hosts(&self) -> impl Iterator<Item = &str> {
        self.host
            .iter()
            .chain(self.hosts.iter())
            .map(String::as_str)
    }
}

fn default_replicas() -> u32 {
    1
}

fn default_healthcheck_interval() -> u32 {
    DEFAULT_HEALTHCHECK_INTERVAL_SECS
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
                  "ingress": { "host": "api.example.com", "port": 8080 },
                  "deploy": {
                    "replicas": 2,
                    "healthcheckPath": "/health",
                    "env": { "items": { "RUST_LOG": "info" } },
                    "resources": { "cpu": 2, "memory": "8Gi" },
                    "oomPriority": "high",
                  },
                },
              },
            }"#,
        )
        .unwrap();
        let api = &file.services[&ServiceId::from("api")];
        assert_eq!(api.deploy.replicas, 2);
        assert_eq!(
            api.deploy.healthcheck_interval,
            DEFAULT_HEALTHCHECK_INTERVAL_SECS
        );
        assert_eq!(api.deploy.resources.cpu, Some(Millicores::new(2000)));
        assert!(matches!(
            api.deploy.resources.memory,
            Some(MemoryLimit::Size(_))
        ));
        assert_eq!(api.deploy.oom_priority, OomPriority::High);
        assert_eq!(
            api.ingress
                .as_ref()
                .unwrap()
                .all_hosts()
                .collect::<Vec<_>>(),
            ["api.example.com"]
        );
    }

    #[test]
    fn rejects_invalid_services() {
        let cases = [
            r#"{ "services": { "Api": { "name": "x", "image": "nginx" } } }"#,
            r#"{ "services": { "api": { "name": "x" } } }"#,
            r#"{ "services": { "api": { "name": "x", "image": "nginx", "build": { "dockerfile": "Dockerfile" } } } }"#,
            r#"{ "services": { "api": { "name": "x", "image": "nginx", "ingress": { "port": 80 } } } }"#,
        ];
        cases.iter().for_each(|case| {
            assert!(
                matches!(ServicesFile::parse(case), Err(SpecError::Invalid { .. })),
                "expected invalid: {case}"
            );
        });
    }

    #[test]
    fn omits_defaults_when_serialized() {
        let spec = ServiceSpec {
            name: "Web".into(),
            image: Some("nginx".into()),
            build: None,
            deploy: DeploySpec::default(),
            ingress: None,
        };
        let json = serde_json::to_value(&spec).unwrap();
        assert_eq!(
            json["deploy"],
            serde_json::json!({ "replicas": 1, "healthcheckInterval": 10 })
        );
    }
}
