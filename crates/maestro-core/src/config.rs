use std::collections::{BTreeMap, HashSet};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use thiserror::Error;

use crate::resources::{ByteSize, Millicores, ResourceDefaults};

const EXTENDS_KEY: &str = "$extends";
const MAX_EXTENDS_DEPTH: usize = 16;
const FILE_SCHEME: &str = "file://";

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
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ClusterSettings {
    #[serde(default = "default_cluster_name")]
    pub name: String,
    #[serde(default = "default_cluster_cidr")]
    pub cidr: String,
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
}

impl ClusterConfig {
    pub fn load(source: &str) -> Result<Self, ConfigError> {
        Self::load_with(source, &read_file_source)
    }

    pub fn load_with(
        source: &str,
        read_source: &dyn Fn(&str) -> Result<String, ConfigError>,
    ) -> Result<Self, ConfigError> {
        let merged = load_layers(source, read_source, &mut HashSet::new(), 0)?;
        serde_json::from_value(merged).map_err(|error| ConfigError::Invalid(error.to_string()))
    }

    pub fn parse(text: &str) -> Result<Self, ConfigError> {
        Self::load_with("inline", &|_| Ok(text.to_string()))
    }
}

impl Default for ClusterSettings {
    fn default() -> Self {
        Self {
            name: default_cluster_name(),
            cidr: default_cluster_cidr(),
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

fn read_file_source(source: &str) -> Result<String, ConfigError> {
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

fn default_cluster_name() -> String {
    "maestro".to_string()
}

fn default_cluster_cidr() -> String {
    "10.200.0.0/16".to_string()
}

fn default_ingress_ports() -> Vec<u16> {
    vec![80, 443]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resources::MemoryLimit;

    #[test]
    fn empty_config_uses_defaults() {
        let config = ClusterConfig::parse("{}").unwrap();
        assert_eq!(config.cluster.name, "maestro");
        assert_eq!(config.cluster.cidr, "10.200.0.0/16");
        assert_eq!(config.ingress.ports, [80, 443]);
        assert_eq!(config.defaults.resources, ResourceDefaults::default());
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
