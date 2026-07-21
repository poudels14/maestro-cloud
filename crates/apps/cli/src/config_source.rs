use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::CliError;

const MAX_EXTENDS_DEPTH: usize = 16;

pub(crate) trait ConfigSourceReader {
    async fn read(&self, source: &str) -> Result<String, CliError>;
}

pub(crate) struct SystemConfigSourceReader;

impl ConfigSourceReader for SystemConfigSourceReader {
    async fn read(&self, source: &str) -> Result<String, CliError> {
        if let Some(path) = local_path(source) {
            return std::fs::read_to_string(path).map_err(|source| {
                CliError::io(
                    format!("failed to read config source `{}`", path.display()),
                    source,
                )
            });
        }
        let reference = source.strip_prefix("aws-secret://").ok_or_else(|| {
            CliError::invalid_input(format!("unsupported config source `{source}`"))
        })?;
        if reference.is_empty() {
            return Err(CliError::invalid_input(
                "aws-secret:// config source must name a secret",
            ));
        }
        let sdk = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
        let response = aws_sdk_secretsmanager::Client::new(&sdk)
            .get_secret_value()
            .secret_id(reference)
            .send()
            .await
            .map_err(|error| {
                CliError::invalid_input(format!(
                    "failed to fetch AWS secret `{reference}`: {error}"
                ))
            })?;
        response
            .secret_string()
            .map(ToString::to_string)
            .ok_or_else(|| {
                CliError::invalid_input(format!(
                    "AWS secret `{reference}` does not contain a string value"
                ))
            })
    }
}

pub(crate) async fn load_merged(
    source: &str,
    reader: &impl ConfigSourceReader,
) -> Result<Value, CliError> {
    let mut current = source.to_string();
    let mut seen = BTreeSet::new();
    let mut layers = Vec::new();

    loop {
        if layers.len() >= MAX_EXTENDS_DEPTH {
            return Err(CliError::invalid_input(format!(
                "config `{source}` exceeds the maximum $extends depth of {MAX_EXTENDS_DEPTH}"
            )));
        }
        let identity = source_identity(&current);
        if !seen.insert(identity) {
            return Err(CliError::invalid_input(format!(
                "config $extends cycle detected at `{current}`"
            )));
        }
        let raw = reader.read(&current).await?;
        let mut value: Value = json5::from_str(&raw).map_err(|error| {
            CliError::invalid_input(format!("failed to parse config `{current}`: {error}"))
        })?;
        let object = value.as_object_mut().ok_or_else(|| {
            CliError::invalid_input(format!(
                "config `{current}` must contain a JSON object at the top level"
            ))
        })?;
        let extends = object.remove("$extends");
        layers.push(value);
        let Some(extends) = extends else {
            break;
        };
        let extends = extends
            .as_str()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                CliError::invalid_input(format!(
                    "`$extends` in config `{current}` must be a non-empty string"
                ))
            })?;
        current = resolve_extended_source(&current, extends)?;
    }

    let mut layers = layers.into_iter().rev();
    let mut merged = layers
        .next()
        .ok_or_else(|| CliError::invalid_input("config source did not produce a document"))?;
    for layer in layers {
        merge(&mut merged, layer);
    }
    Ok(merged)
}

pub(crate) fn decode_document<Document: DeserializeOwned>(
    value: &Value,
    description: &str,
) -> Result<(Document, Vec<String>), CliError> {
    let encoded = serde_json::to_vec(value)
        .map_err(|error| CliError::json(format!("failed to encode {description}"), error))?;
    let mut deserializer = serde_json::Deserializer::from_slice(&encoded);
    let mut tracker = serde_path_to_error::Track::new();
    let tracked = serde_path_to_error::Deserializer::new(&mut deserializer, &mut tracker);
    let mut ignored_fields = Vec::new();
    let document = serde_ignored::deserialize(tracked, |path| {
        ignored_fields.push(path.to_string());
    })
    .map_err(|error| {
        let path = tracker.path().to_string();
        let detail = if path.is_empty() {
            error.to_string()
        } else {
            format!("{path}: {error}")
        };
        CliError::invalid_input(format!("failed to parse {description}: {detail}"))
    })?;
    ignored_fields.sort();
    ignored_fields.dedup();
    Ok((document, ignored_fields))
}

pub(crate) fn resolve_relative_source(
    config_source: &str,
    value: &str,
) -> Result<String, CliError> {
    resolve_extended_source(config_source, value)
}

fn merge(base: &mut Value, overlay: Value) {
    match (base, overlay) {
        (Value::Object(base), Value::Object(overlay)) => {
            for (key, value) in overlay {
                if let Some(existing) = base.get_mut(&key) {
                    merge(existing, value);
                } else {
                    base.insert(key, value);
                }
            }
        }
        (base, overlay) => *base = overlay,
    }
}

fn resolve_extended_source(current: &str, extends: &str) -> Result<String, CliError> {
    if extends.starts_with("aws-secret://") {
        return Ok(extends.to_string());
    }
    let explicit_file = extends.strip_prefix("file://");
    let path = explicit_file.unwrap_or(extends);
    if explicit_file.is_none() && extends.contains("://") {
        return Err(CliError::invalid_input(format!(
            "unsupported config source `{extends}`"
        )));
    }
    let path = Path::new(path);
    if path.is_absolute() {
        return Ok(format!("file://{}", path.display()));
    }
    let current_path = local_path(current).ok_or_else(|| {
        CliError::invalid_input(format!(
            "relative source `{extends}` cannot be resolved from remote config `{current}`"
        ))
    })?;
    let parent = current_path.parent().unwrap_or_else(|| Path::new("."));
    Ok(format!("file://{}", parent.join(path).display()))
}

fn local_path(source: &str) -> Option<&Path> {
    if source.starts_with("aws-secret://") {
        None
    } else if let Some(path) = source.strip_prefix("file://") {
        Some(Path::new(path))
    } else if source.contains("://") {
        None
    } else {
        Some(Path::new(source))
    }
}

fn source_identity(source: &str) -> String {
    let Some(path) = local_path(source) else {
        return source.to_string();
    };
    let path = std::fs::canonicalize(path).unwrap_or_else(|_| PathBuf::from(path));
    format!("file://{}", path.display())
}
