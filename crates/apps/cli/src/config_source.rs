use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use aws_sdk_secretsmanager::error::DisplayErrorContext;
use serde::de::DeserializeOwned;
use serde_json::Value;
use url::Url;

use crate::CliError;

const MAX_EXTENDS_DEPTH: usize = 16;

#[allow(async_fn_in_trait)]
pub trait ConfigSourceReader {
    async fn read(&self, source: &str) -> Result<String, CliError>;
}

pub struct SystemConfigSourceReader;

impl ConfigSourceReader for SystemConfigSourceReader {
    async fn read(&self, source: &str) -> Result<String, CliError> {
        if let Some(path) = local_path(source) {
            return std::fs::read_to_string(&path).map_err(|source| {
                CliError::io(
                    format!("failed to read config source `{}`", path.display()),
                    source,
                )
            });
        }
        let reference = aws_secret_reference(source).ok_or_else(|| {
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
                    "failed to fetch AWS secret `{reference}`: {}",
                    DisplayErrorContext(error)
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
    if aws_secret_reference(extends).is_some() {
        return Ok(extends.to_string());
    }
    let parsed_url = Url::parse(extends).ok();
    let path = match parsed_url.as_ref().map(Url::scheme) {
        Some("file") => local_path(extends).ok_or_else(|| {
            CliError::invalid_input(format!("invalid file config source `{extends}`"))
        })?,
        Some(_) => {
            return Err(CliError::invalid_input(format!(
                "unsupported config source `{extends}`"
            )));
        }
        None if extends.contains("://") => {
            return Err(CliError::invalid_input(format!(
                "unsupported config source `{extends}`"
            )));
        }
        None => PathBuf::from(extends),
    };
    if path.as_os_str().is_empty() {
        return Err(CliError::invalid_input(format!(
            "invalid file config source `{extends}`"
        )));
    }
    if path.is_absolute() {
        return file_source(&path);
    }
    let current_path = local_path(current).ok_or_else(|| {
        CliError::invalid_input(format!(
            "relative source `{extends}` cannot be resolved from remote config `{current}`"
        ))
    })?;
    let parent = current_path.parent().unwrap_or_else(|| Path::new("."));
    file_source(&parent.join(path))
}

fn local_path(source: &str) -> Option<PathBuf> {
    match Url::parse(source) {
        Ok(url) if url.scheme() == "file" => file_url_path(&url),
        Ok(_) => None,
        Err(_) if source.contains("://") => None,
        Err(_) => Some(PathBuf::from(source)),
    }
}

fn file_url_path(url: &Url) -> Option<PathBuf> {
    if !url.username().is_empty()
        || url.password().is_some()
        || url.port().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return None;
    }
    match url.host_str() {
        None | Some("") | Some("localhost") => url.to_file_path().ok(),
        Some(host) if url.path().is_empty() || url.path() == "/" => Some(PathBuf::from(host)),
        Some(host) => {
            // Preserve Maestro's historical `file://relative/path` spelling
            // while letting `url` validate and percent-decode its components.
            let mut path_url = url.clone();
            path_url.set_host(None).ok()?;
            let suffix = path_url.to_file_path().ok()?;
            let suffix = suffix.strip_prefix(Path::new("/")).ok()?;
            Some(PathBuf::from(host).join(suffix))
        }
    }
}

fn aws_secret_reference(source: &str) -> Option<&str> {
    let url = Url::parse(source).ok()?;
    if url.scheme() != "aws-secret" {
        return None;
    }
    let separator = source.find(':')?;
    source
        .get(separator.saturating_add(1)..)?
        .strip_prefix("//")
}

fn file_source(path: &Path) -> Result<String, CliError> {
    if !path.is_absolute() {
        return Ok(path.to_string_lossy().into_owned());
    }
    Url::from_file_path(path)
        .map(|url| url.to_string())
        .map_err(|()| {
            CliError::invalid_input(format!(
                "config file path `{}` cannot be represented as a file URL",
                path.display()
            ))
        })
}

fn source_identity(source: &str) -> String {
    let Some(path) = local_path(source) else {
        return source.to_string();
    };
    let path = std::fs::canonicalize(&path).unwrap_or(path);
    file_source(&path).unwrap_or_else(|_| path.to_string_lossy().into_owned())
}
