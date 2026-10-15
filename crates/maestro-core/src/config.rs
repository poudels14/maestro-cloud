use std::collections::{BTreeMap, HashSet};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use thiserror::Error;

use crate::resources::{ByteSize, Millicores, ResourceDefaults};

const EXTENDS_KEY: &str = "$extends";
const SCHEMA_KEY: &str = "$schema";
const MAX_EXTENDS_DEPTH: usize = 16;
const FILE_SCHEME: &str = "file://";
const MAX_TUNNEL_REPLICAS: u32 = 25;
const REPLACED_V1_FIELDS: [(&str, &str); 6] = [
    (
        "subnet",
        "each node gets a /24 from cluster.cidr when it joins",
    ),
    ("runtime", "Maestro 3 always uses containerd"),
    ("system", "upgrades use the NixOS module"),
    (
        "disable-etcd-cert",
        "etcd is gone; cluster traffic always uses mTLS",
    ),
    (
        "cluster.nodes",
        "list the voter addresses in cluster.voters; node ids are assigned when nodes join",
    ),
    (
        "cluster.control-allow-cidrs",
        "restrict port 7400 with your firewall or security group",
    ),
];

const CONFIG_MASK: &str = "********";
const DEFAULT_MAX_CONCURRENT_BUILDS: u32 = 5;
const DEFAULT_VOLUME_ROOTS: [&str; 2] = ["/var/lib/maestro-volumes", "/data"];

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ClusterConfig {
    #[serde(default)]
    pub cluster: ClusterSettings,
    #[serde(default)]
    pub node: NodeSettings,
    #[serde(default)]
    pub ingress: IngressSettings,
    #[serde(default)]
    pub defaults: Defaults,
    #[serde(default)]
    pub egress: EgressSettings,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tailscale: Option<TailscaleSettings>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub datadog: Option<DatadogSettings>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub depot: Option<DepotSettings>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cloudflare: Option<CloudflareSettings>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub slack: Option<SlackSettings>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub github: Option<GithubSettings>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub homepage: Option<String>,
    #[serde(default, alias = "log-backup", skip_serializing_if = "Option::is_none")]
    pub log_backup: Option<LogBackupSettings>,
    #[serde(default, alias = "allow-cli-deployment")]
    pub allow_cli_deployment: bool,
    #[serde(default, alias = "allow-exec")]
    pub allow_exec: bool,
    #[serde(default = "default_volume_roots")]
    pub volume_roots: Vec<String>,
    #[serde(
        default,
        alias = "max-concurrent-builds",
        skip_serializing_if = "Option::is_none"
    )]
    pub max_concurrent_builds: Option<u32>,
    #[serde(
        default,
        alias = "encryption-key",
        skip_serializing_if = "Option::is_none"
    )]
    pub encryption_key: Option<String>,
    #[serde(
        default,
        alias = "jwt-secret-key",
        skip_serializing_if = "Option::is_none"
    )]
    pub jwt_secret_key: Option<String>,
}

fn default_volume_roots() -> Vec<String> {
    DEFAULT_VOLUME_ROOTS
        .iter()
        .map(|root| root.to_string())
        .collect()
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TailscaleSettings {
    #[serde(alias = "auth-key")]
    pub auth_key: String,
    #[serde(
        default,
        alias = "advertise-routes",
        skip_serializing_if = "Vec::is_empty"
    )]
    pub advertise_routes: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DatadogSettings {
    #[serde(alias = "api-key")]
    pub api_key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub site: Option<String>,
    #[serde(default = "default_true", alias = "include-ingress-logs")]
    pub include_ingress_logs: bool,
    #[serde(default = "default_true", alias = "include-tailscale-logs")]
    pub include_tailscale_logs: bool,
    #[serde(default)]
    pub logs: DatadogLogSettings,
    #[serde(default, alias = "include-metrics")]
    pub include_metrics: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DatadogLogSettings {
    #[serde(default = "default_true", alias = "include-healthcheck")]
    pub include_healthcheck: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DepotSettings {
    pub token: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CloudflareSettings {
    pub tunnel: CloudflareTunnel,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CloudflareTunnel {
    pub token: String,
    #[serde(default = "default_tunnel_replicas")]
    pub replicas: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SlackSettings {
    #[serde(alias = "webhook-url")]
    pub webhook_url: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GithubSettings {
    pub token: String,
    #[serde(default = "default_preview_domain", alias = "preview-domain")]
    pub preview_domain: String,
    #[serde(default = "default_poll_interval", alias = "poll-interval-secs")]
    pub poll_interval_secs: u64,
    #[serde(default = "default_max_previews", alias = "max-concurrent-previews")]
    pub max_concurrent_previews: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LogBackupSettings {
    pub bucket: String,
    #[serde(alias = "kms-key-id")]
    pub kms_key_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
    #[serde(
        default,
        alias = "retention-days",
        skip_serializing_if = "Option::is_none"
    )]
    pub retention_days: Option<u32>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ClusterSettings {
    #[serde(default = "default_cluster_name")]
    pub name: String,
    #[serde(default = "default_cluster_cidr")]
    pub cidr: String,
    #[serde(default, alias = "join-secret", skip_serializing)]
    pub join_secret: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub voters: Vec<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NodeSettings {
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub labels: BTreeMap<String, String>,
    #[serde(default)]
    pub reserve: ReserveSettings,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ReserveSettings {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu: Option<Millicores>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory: Option<ByteSize>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct IngressSettings {
    #[serde(default = "default_ingress_ports")]
    pub ports: Vec<u16>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Defaults {
    #[serde(default)]
    pub resources: ResourceDefaults,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EgressSettings {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub deny: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allow: Vec<String>,
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("failed to read config {source_name}: {error}")]
    Read { source_name: String, error: String },
    #[error("failed to parse config {source_name}: {error}")]
    Parse { source_name: String, error: String },
    #[error("config {0} must be a JSON object")]
    NotAnObject(String),
    #[error("config {0} extends itself through a cycle")]
    Cycle(String),
    #[error("config {0} exceeds the maximum $extends depth of {MAX_EXTENDS_DEPTH}")]
    TooDeep(String),
    #[error("unsupported config source {0}; use a path or file:// URL")]
    UnsupportedSource(String),
    #[error("invalid config: {0}")]
    Invalid(String),
    #[error("{0} is required in the cluster config")]
    MissingKey(String),
}

impl ClusterConfig {
    pub fn encryption_key(&self) -> Result<&str, ConfigError> {
        required_key(self.encryption_key.as_deref(), "encryptionKey")
    }

    pub fn max_concurrent_builds(&self) -> usize {
        self.max_concurrent_builds
            .unwrap_or(DEFAULT_MAX_CONCURRENT_BUILDS) as usize
    }

    pub fn jwt_secret_key(&self) -> Result<&str, ConfigError> {
        required_key(self.jwt_secret_key.as_deref(), "jwtSecretKey")
    }

    pub fn join_secret(&self) -> Result<&str, ConfigError> {
        required_key(self.cluster.join_secret.as_deref(), "cluster.joinSecret")
    }

    pub fn masked(&self) -> Self {
        let mask = |value: &mut String| {
            if !value.is_empty() {
                *value = CONFIG_MASK.to_string();
            }
        };
        let mut masked = self.clone();
        masked.encryption_key.iter_mut().for_each(mask);
        masked.jwt_secret_key.iter_mut().for_each(mask);
        if let Some(tailscale) = masked.tailscale.as_mut() {
            mask(&mut tailscale.auth_key);
        }
        if let Some(datadog) = masked.datadog.as_mut() {
            mask(&mut datadog.api_key);
        }
        if let Some(depot) = masked.depot.as_mut() {
            mask(&mut depot.token);
        }
        if let Some(cloudflare) = masked.cloudflare.as_mut() {
            mask(&mut cloudflare.tunnel.token);
        }
        if let Some(slack) = masked.slack.as_mut() {
            mask(&mut slack.webhook_url);
        }
        if let Some(github) = masked.github.as_mut() {
            mask(&mut github.token);
        }
        masked
    }

    pub fn load(source: &str) -> Result<Self, ConfigError> {
        Self::load_with(source, &read_file_source)
    }

    pub fn load_with(
        source: &str,
        read_source: &dyn Fn(&str) -> Result<String, ConfigError>,
    ) -> Result<Self, ConfigError> {
        let merged = load_layers(source, read_source, &mut HashSet::new(), 0)?;
        let normalized = normalize_v1_layout(merged)?;
        let config: Self = serde_json::from_value(normalized)
            .map_err(|error| ConfigError::Invalid(error.to_string()))?;
        config.validate()?;
        Ok(config)
    }

    pub fn parse(text: &str) -> Result<Self, ConfigError> {
        Self::load_with("inline", &|_| Ok(text.to_string()))
    }
}

impl Default for DatadogLogSettings {
    fn default() -> Self {
        Self {
            include_healthcheck: true,
        }
    }
}

impl ClusterConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        let problems = [
            self.ingress
                .ports
                .is_empty()
                .then(|| "ingress needs at least one port".to_string()),
            (self.max_concurrent_builds == Some(0))
                .then(|| "maxConcurrentBuilds must be at least 1".to_string()),
            self.cloudflare.as_ref().and_then(|cloudflare| {
                (!(1..=MAX_TUNNEL_REPLICAS).contains(&cloudflare.tunnel.replicas))
                    .then(|| format!("cloudflare.tunnel.replicas must be 1..={MAX_TUNNEL_REPLICAS}"))
            }),
            self.github.as_ref().and_then(|github| {
                (github.token.trim().is_empty()
                    || github.poll_interval_secs == 0
                    || github.max_concurrent_previews == 0)
                    .then(|| "github needs a token, pollIntervalSecs >= 1, and maxConcurrentPreviews >= 1".to_string())
            }),
            self.homepage.as_ref().and_then(|homepage| {
                (!(homepage.starts_with("https://") || homepage.starts_with("http://"))
                    || homepage.contains('?'))
                    .then(|| "homepage must be an http(s) URL without a query".to_string())
            }),
            self.log_backup.as_ref().and_then(|backup| {
                (backup.retention_days == Some(0))
                    .then(|| "logBackup.retentionDays must be at least 1".to_string())
            }),
        ];
        match problems.into_iter().flatten().next() {
            Some(problem) => Err(ConfigError::Invalid(problem)),
            None => Ok(()),
        }
    }
}

impl Default for ClusterSettings {
    fn default() -> Self {
        Self {
            name: default_cluster_name(),
            cidr: default_cluster_cidr(),
            join_secret: None,
            voters: Vec::new(),
        }
    }
}

impl Default for IngressSettings {
    fn default() -> Self {
        Self {
            ports: default_ingress_ports(),
        }
    }
}

fn load_layers(
    source: &str,
    read_source: &dyn Fn(&str) -> Result<String, ConfigError>,
    visited: &mut HashSet<String>,
    depth: usize,
) -> Result<Value, ConfigError> {
    let is_new = visited.insert(source.to_string());
    match (is_new, depth > MAX_EXTENDS_DEPTH) {
        (false, _) => Err(ConfigError::Cycle(source.to_string())),
        (_, true) => Err(ConfigError::TooDeep(source.to_string())),
        (true, false) => {
            let text = read_source(source)?;
            let mut layer = match json5::from_str::<Value>(&text) {
                Ok(Value::Object(map)) => Ok(map),
                Ok(_) => Err(ConfigError::NotAnObject(source.to_string())),
                Err(error) => Err(ConfigError::Parse {
                    source_name: source.to_string(),
                    error: error.to_string(),
                }),
            }?;
            layer.remove(SCHEMA_KEY);
            let base = match layer.remove(EXTENDS_KEY) {
                Some(Value::String(parent)) => {
                    let parent_source = resolve_relative(source, &parent);
                    load_layers(&parent_source, read_source, visited, depth + 1)?
                }
                Some(_) => Err(ConfigError::Invalid(format!(
                    "{EXTENDS_KEY} in {source} must be a string"
                )))?,
                None => Value::Object(Map::new()),
            };
            Ok(merge(base, Value::Object(layer)))
        }
    }
}

fn normalize_v1_layout(config: Value) -> Result<Value, ConfigError> {
    let mut map = match config {
        Value::Object(map) => map,
        _ => Map::new(),
    };
    let replaced = REPLACED_V1_FIELDS
        .iter()
        .find(|(path, _)| lookup(&map, path).is_some());
    match replaced {
        Some((path, replacement)) => Err(ConfigError::Invalid(format!(
            "{path} is no longer used: {replacement}"
        ))),
        None => {
            let cluster_labels = map
                .get_mut("cluster")
                .and_then(Value::as_object_mut)
                .and_then(|cluster| cluster.remove("labels"));
            if let Some(labels) = cluster_labels {
                let node = map
                    .entry("node")
                    .or_insert_with(|| Value::Object(Map::new()));
                if let Some(node) = node.as_object_mut() {
                    node.entry("labels").or_insert(labels);
                }
            }
            let single_port = map
                .get_mut("ingress")
                .and_then(Value::as_object_mut)
                .and_then(|ingress| {
                    let port = ingress.remove("port")?;
                    (!ingress.contains_key("ports")).then_some((ingress, port))
                });
            if let Some((ingress, port)) = single_port {
                ingress.insert("ports".to_string(), Value::Array(vec![port]));
            }
            Ok(Value::Object(map))
        }
    }
}

fn lookup<'a>(map: &'a Map<String, Value>, path: &str) -> Option<&'a Value> {
    let (head, rest) = path.split_once('.').unwrap_or((path, ""));
    let value = map.get(head)?;
    match rest.is_empty() {
        true => Some(value),
        false => value.as_object().and_then(|nested| lookup(nested, rest)),
    }
}

fn merge(base: Value, overlay: Value) -> Value {
    match (base, overlay) {
        (Value::Object(mut base_map), Value::Object(overlay_map)) => {
            overlay_map.into_iter().for_each(|(key, overlay_value)| {
                let merged = match base_map.remove(&key) {
                    Some(base_value) => merge(base_value, overlay_value),
                    None => overlay_value,
                };
                base_map.insert(key, merged);
            });
            Value::Object(base_map)
        }
        (_, overlay) => overlay,
    }
}

fn resolve_relative(current: &str, parent: &str) -> String {
    let parent_path = parent.strip_prefix(FILE_SCHEME).unwrap_or(parent);
    let is_local = !parent.contains("://") || parent.starts_with(FILE_SCHEME);
    let current_dir = Path::new(current.strip_prefix(FILE_SCHEME).unwrap_or(current)).parent();
    match (is_local, Path::new(parent_path).is_relative(), current_dir) {
        (true, true, Some(directory)) => directory.join(parent_path).to_string_lossy().to_string(),
        _ => parent.to_string(),
    }
}

pub fn read_file_source(source: &str) -> Result<String, ConfigError> {
    let path = match source.strip_prefix(FILE_SCHEME) {
        Some(path) => Ok(PathBuf::from(path)),
        None if !source.contains("://") => Ok(PathBuf::from(source)),
        None => Err(ConfigError::UnsupportedSource(source.to_string())),
    }?;
    std::fs::read_to_string(&path).map_err(|error| ConfigError::Read {
        source_name: source.to_string(),
        error: error.to_string(),
    })
}

fn default_true() -> bool {
    true
}

fn default_tunnel_replicas() -> u32 {
    2
}

fn default_preview_domain() -> String {
    "preview.getbaton.ai".to_string()
}

fn default_poll_interval() -> u64 {
    60
}

fn default_max_previews() -> u32 {
    10
}

fn default_cluster_name() -> String {
    "maestro".to_string()
}

fn default_cluster_cidr() -> String {
    "10.200.0.0/16".to_string()
}

fn default_ingress_ports() -> Vec<u16> {
    vec![80, 443]
}

fn required_key<'a>(value: Option<&'a str>, name: &str) -> Result<&'a str, ConfigError> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| ConfigError::MissingKey(name.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resources::MemoryLimit;

    #[test]
    fn ignores_the_schema_key() {
        let config = ClusterConfig::load_with("/c.jsonc", &|_| {
            Ok(
                r#"{ "$schema": "./maestro.schema.json", "cluster": { "name": "sandbox" } }"#
                    .to_string(),
            )
        })
        .unwrap();
        assert_eq!(config.cluster.name, "sandbox");
    }

    #[test]
    fn empty_config_uses_defaults() {
        let config = ClusterConfig::parse("{}").unwrap();
        assert_eq!(config.cluster.name, "maestro");
        assert_eq!(config.cluster.cidr, "10.200.0.0/16");
        assert_eq!(config.ingress.ports, [80, 443]);
        assert_eq!(config.defaults.resources, ResourceDefaults::default());
    }

    #[test]
    fn masks_credentials() {
        let config = ClusterConfig::parse(
            r#"{
              "tailscale": { "authKey": "tskey-secret" },
              "datadog": { "apiKey": "dd-secret", "site": "datadoghq.eu" },
              "github": { "token": "ghp-secret", "previewDomain": "preview.example.com" },
              "slack": { "webhookUrl": "" },
            }"#,
        )
        .unwrap()
        .masked();
        assert_eq!(config.tailscale.unwrap().auth_key, CONFIG_MASK);
        let datadog = config.datadog.unwrap();
        assert_eq!(datadog.api_key, CONFIG_MASK);
        assert_eq!(datadog.site.as_deref(), Some("datadoghq.eu"));
        let github = config.github.unwrap();
        assert_eq!(github.token, CONFIG_MASK);
        assert_eq!(github.preview_domain, "preview.example.com");
        assert_eq!(config.slack.unwrap().webhook_url, "");
    }

    #[test]
    fn merges_extended_layers() {
        let sources = BTreeMap::from([
            (
                "/etc/maestro/common.jsonc",
                r#"{
                  // shared settings
                  "cluster": { "name": "prod", "cidr": "10.50.0.0/16" },
                  "ingress": { "ports": [80, 443] },
                  "defaults": { "resources": { "cpu": 4, "memory": "50%", "request": { "cpu": 0.25, "memory": "512Mi" } } },
                }"#,
            ),
            (
                "/etc/maestro/node.jsonc",
                r#"{
                  "$extends": "common.jsonc",
                  "cluster": { "name": "prod-east" },
                  "ingress": { "ports": [8080] },
                  "node": { "labels": { "zone": "a" }, "reserve": { "memory": "4Gi" } },
                }"#,
            ),
        ]);
        let config = ClusterConfig::load_with("/etc/maestro/node.jsonc", &|source| {
            sources
                .get(source)
                .map(|text| text.to_string())
                .ok_or_else(|| ConfigError::Read {
                    source_name: source.to_string(),
                    error: "missing".to_string(),
                })
        })
        .unwrap();
        assert_eq!(config.cluster.name, "prod-east");
        assert_eq!(config.cluster.cidr, "10.50.0.0/16");
        assert_eq!(config.ingress.ports, [8080]);
        assert_eq!(config.node.labels["zone"], "a");
        assert_eq!(
            config.defaults.resources.memory,
            MemoryLimit::PercentOfNode(50)
        );
    }

    #[test]
    fn detects_extends_cycles() {
        let result = ClusterConfig::load_with("/a.jsonc", &|source| match source {
            "/a.jsonc" => Ok(r#"{ "$extends": "/b.jsonc" }"#.to_string()),
            _ => Ok(r#"{ "$extends": "/a.jsonc" }"#.to_string()),
        });
        assert!(matches!(result, Err(ConfigError::Cycle(_))));
    }

    #[test]
    fn accepts_v1_feature_fields_in_either_case() {
        let config = ClusterConfig::parse(
            r#"{
              "cluster": { "name": "prod", "labels": { "zone": "a" } },
              "ingress": { "port": 8080 },
              "egress": { "deny": ["10.0.0.0/8"], "allow": ["10.0.5.0/24"] },
              "tags": ["team:core"],
              "tailscale": { "auth-key": "tskey", "advertise-routes": ["10.1.0.0/16"] },
              "datadog": { "api-key": "dd", "site": "datadoghq.com", "logs": { "include-healthcheck": false } },
              "depot": { "token": "dp" },
              "cloudflare": { "tunnel": { "token": "cf" } },
              "slack": { "webhook-url": "https://hooks.slack.com/x" },
              "github": { "token": "gh", "maxConcurrentPreviews": 3 },
              "homepage": "https://maestro.example.com",
              "log-backup": { "bucket": "logs", "kms-key-id": "arn:kms", "retention-days": 30 },
              "allow-exec": true,
              "allowCliDeployment": true
            }"#,
        )
        .unwrap();
        assert_eq!(config.ingress.ports, [8080]);
        assert_eq!(config.node.labels["zone"], "a");
        assert_eq!(config.egress.allow, ["10.0.5.0/24"]);
        assert_eq!(config.tailscale.unwrap().advertise_routes, ["10.1.0.0/16"]);
        let datadog = config.datadog.unwrap();
        assert!(datadog.include_ingress_logs);
        assert!(!datadog.logs.include_healthcheck);
        assert_eq!(config.cloudflare.unwrap().tunnel.replicas, 2);
        let github = config.github.unwrap();
        assert_eq!(github.preview_domain, "preview.getbaton.ai");
        assert_eq!(github.max_concurrent_previews, 3);
        assert_eq!(config.log_backup.unwrap().retention_days, Some(30));
        assert!(config.allow_exec && config.allow_cli_deployment);
    }

    #[test]
    fn explains_replaced_v1_fields() {
        let error = ClusterConfig::parse(r#"{ "cluster": { "nodes": {} } }"#).unwrap_err();
        assert!(error.to_string().contains("cluster.voters"));
    }

    #[test]
    fn requires_both_keys_in_either_spelling() {
        let kebab = ClusterConfig::parse(r#"{ "encryption-key": "enc", "jwt-secret-key": "jwt" }"#)
            .unwrap();
        assert_eq!(kebab.encryption_key().unwrap(), "enc");
        assert_eq!(kebab.jwt_secret_key().unwrap(), "jwt");
        let camel =
            ClusterConfig::parse(r#"{ "encryptionKey": " ", "jwtSecretKey": "jwt" }"#).unwrap();
        assert!(matches!(
            camel.encryption_key(),
            Err(ConfigError::MissingKey(_))
        ));
        assert!(ClusterConfig::default().jwt_secret_key().is_err());
    }

    #[test]
    fn reads_the_join_secret_and_voters_but_never_writes_the_secret() {
        let config = ClusterConfig::parse(
            r#"{ "cluster": { "join-secret": "join", "voters": ["10.1.0.11", "10.1.0.12:7400"] } }"#,
        )
        .unwrap();
        assert_eq!(config.join_secret().unwrap(), "join");
        assert_eq!(config.cluster.voters.len(), 2);
        let written = serde_json::to_string(&config.cluster).unwrap();
        assert!(!written.contains("join"));
        assert!(written.contains("10.1.0.12:7400"));
    }

    #[test]
    fn configures_concurrent_builds() {
        assert_eq!(ClusterConfig::default().max_concurrent_builds(), 5);
        let config = ClusterConfig::parse(r#"{ "maxConcurrentBuilds": 12 }"#).unwrap();
        assert_eq!(config.max_concurrent_builds(), 12);
        let kebab = ClusterConfig::parse(r#"{ "max-concurrent-builds": 2 }"#).unwrap();
        assert_eq!(kebab.max_concurrent_builds(), 2);
        assert!(ClusterConfig::parse(r#"{ "maxConcurrentBuilds": 0 }"#).is_err());
    }

    #[test]
    fn rejects_unknown_fields() {
        assert!(matches!(
            ClusterConfig::parse(r#"{ "cluster": { "nmae": "typo" } }"#),
            Err(ConfigError::Invalid(_))
        ));
    }

    #[test]
    fn loads_from_disk_relative_to_child() {
        let directory = std::env::temp_dir().join(format!("maestro-config-{}", std::process::id()));
        std::fs::create_dir_all(&directory).unwrap();
        std::fs::write(
            directory.join("base.jsonc"),
            r#"{ "cluster": { "name": "base" } }"#,
        )
        .unwrap();
        std::fs::write(
            directory.join("node.jsonc"),
            r#"{ "$extends": "file://base.jsonc", "egress": { "deny": ["10.0.0.0/8"] } }"#,
        )
        .unwrap();
        let config = ClusterConfig::load(directory.join("node.jsonc").to_str().unwrap()).unwrap();
        std::fs::remove_dir_all(&directory).unwrap();
        assert_eq!(config.cluster.name, "base");
        assert_eq!(config.egress.deny, ["10.0.0.0/8"]);
    }
}
