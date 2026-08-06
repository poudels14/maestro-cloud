use std::collections::{BTreeMap, BTreeSet};

use kernel_api::{ArtifactTemplate, SecretMountSpec, SecretValue, ServiceDiffChange, ServiceSpec};
use serde::Serialize;

use crate::ApiError;

pub(super) fn changes(
    current: &ServiceSpec,
    desired: &ServiceSpec,
) -> Result<Vec<ServiceDiffChange>, ApiError> {
    let mut changes = Vec::new();
    push_json(&mut changes, "name", &current.name, &desired.name)?;
    push_json(&mut changes, "version", &current.version, &desired.version)?;

    let mut current_artifact = current.artifact.clone();
    let mut desired_artifact = desired.artifact.clone();
    clear_build_values(&mut current_artifact);
    clear_build_values(&mut desired_artifact);
    push_json(
        &mut changes,
        "artifact",
        &current_artifact,
        &desired_artifact,
    )?;
    push_masked_map(
        &mut changes,
        "artifact.environment",
        build_environment(current),
        build_environment(desired),
    );
    push_secret_map(
        &mut changes,
        "artifact.secrets",
        build_secrets(current),
        build_secrets(desired),
    );

    let mut current_preview = current.preview.clone();
    let mut desired_preview = desired.preview.clone();
    clear_preview_values(&mut current_preview);
    clear_preview_values(&mut desired_preview);
    push_json(&mut changes, "preview", &current_preview, &desired_preview)?;
    push_masked_map(
        &mut changes,
        "preview.environment",
        preview_environment(current),
        preview_environment(desired),
    );
    push_json(&mut changes, "command", &current.command, &desired.command)?;
    push_json(
        &mut changes,
        "replicas",
        &current.replicas,
        &desired.replicas,
    )?;
    push_json(
        &mut changes,
        "exposedPorts",
        &current.exposed_ports,
        &desired.exposed_ports,
    )?;
    push_json(
        &mut changes,
        "healthCheck",
        &current.health_check,
        &desired.health_check,
    )?;
    push_json(
        &mut changes,
        "maxRestartAttempts",
        &current.max_restart_attempts,
        &desired.max_restart_attempts,
    )?;
    push_masked_map(
        &mut changes,
        "environment",
        Some(&current.environment),
        Some(&desired.environment),
    );
    push_json(
        &mut changes,
        "environmentSources",
        &current.environment_sources,
        &desired.environment_sources,
    )?;
    push_json(&mut changes, "user", &current.user, &desired.user)?;
    push_json(
        &mut changes,
        "nodeApi",
        &current.node_api,
        &desired.node_api,
    )?;
    push_json(
        &mut changes,
        "secrets.format",
        &current.secrets.as_ref().map(secret_format),
        &desired.secrets.as_ref().map(secret_format),
    )?;
    push_json(
        &mut changes,
        "secrets.mountPath",
        &current.secrets.as_ref().map(SecretMountSpec::mount_path),
        &desired.secrets.as_ref().map(SecretMountSpec::mount_path),
    )?;
    push_json(
        &mut changes,
        "secrets.source",
        &current.secrets.as_ref().and_then(dotenv_source),
        &desired.secrets.as_ref().and_then(dotenv_source),
    )?;
    push_secret_map(
        &mut changes,
        "secrets.items",
        current.secrets.as_ref().and_then(dotenv_items),
        desired.secrets.as_ref().and_then(dotenv_items),
    );
    push_secret_map(
        &mut changes,
        "secrets.files",
        current.secrets.as_ref().and_then(secret_files),
        desired.secrets.as_ref().and_then(secret_files),
    );
    push_json(&mut changes, "volumes", &current.volumes, &desired.volumes)?;
    push_json(
        &mut changes,
        "placement",
        &current.placement,
        &desired.placement,
    )?;
    push_json(&mut changes, "exec", &current.exec, &desired.exec)?;
    Ok(changes)
}

fn secret_format(secrets: &SecretMountSpec) -> &'static str {
    match secrets {
        SecretMountSpec::Dotenv { .. } => "dotenv",
        SecretMountSpec::Files { .. } => "files",
    }
}

fn dotenv_items(secrets: &SecretMountSpec) -> Option<&BTreeMap<String, SecretValue>> {
    match secrets {
        SecretMountSpec::Dotenv { items, .. } => Some(items),
        SecretMountSpec::Files { .. } => None,
    }
}

fn dotenv_source(secrets: &SecretMountSpec) -> Option<&String> {
    match secrets {
        SecretMountSpec::Dotenv { source, .. } => source.as_ref(),
        SecretMountSpec::Files { .. } => None,
    }
}

fn secret_files(secrets: &SecretMountSpec) -> Option<&BTreeMap<String, SecretValue>> {
    match secrets {
        SecretMountSpec::Files { files, .. } => Some(files),
        SecretMountSpec::Dotenv { .. } => None,
    }
}

fn push_json<Value>(
    changes: &mut Vec<ServiceDiffChange>,
    field: &str,
    current: &Value,
    desired: &Value,
) -> Result<(), ApiError>
where
    Value: PartialEq + Serialize,
{
    if current != desired {
        changes.push(ServiceDiffChange {
            field: field.to_string(),
            from: display_json(current)?,
            to: display_json(desired)?,
        });
    }
    Ok(())
}

fn display_json<Value: Serialize>(value: &Value) -> Result<Option<String>, ApiError> {
    let value = serde_json::to_value(value)
        .map_err(|error| ApiError::internal(format!("failed to encode service diff: {error}")))?;
    if value.is_null() {
        Ok(None)
    } else if let Some(value) = value.as_str() {
        Ok(Some(value.to_string()))
    } else {
        Ok(Some(value.to_string()))
    }
}

fn push_masked_map(
    changes: &mut Vec<ServiceDiffChange>,
    prefix: &str,
    current: Option<&BTreeMap<String, String>>,
    desired: Option<&BTreeMap<String, String>>,
) {
    let empty = BTreeMap::new();
    let current = current.unwrap_or(&empty);
    let desired = desired.unwrap_or(&empty);
    for key in keys(current, desired) {
        if current.get(key) != desired.get(key) {
            changes.push(ServiceDiffChange {
                field: format!("{prefix}.{key}"),
                from: current.get(key).map(|value| masked(value)),
                to: desired.get(key).map(|value| masked(value)),
            });
        }
    }
}

fn push_secret_map(
    changes: &mut Vec<ServiceDiffChange>,
    prefix: &str,
    current: Option<&BTreeMap<String, SecretValue>>,
    desired: Option<&BTreeMap<String, SecretValue>>,
) {
    let empty = BTreeMap::new();
    let current = current.unwrap_or(&empty);
    let desired = desired.unwrap_or(&empty);
    for key in keys(current, desired) {
        if current.get(key) != desired.get(key) {
            changes.push(ServiceDiffChange {
                field: format!("{prefix}.{key}"),
                from: current
                    .get(key)
                    .map(|value| value.masked().as_str().to_string()),
                to: desired
                    .get(key)
                    .map(|value| value.masked().as_str().to_string()),
            });
        }
    }
}

fn keys<'a, Value>(
    current: &'a BTreeMap<String, Value>,
    desired: &'a BTreeMap<String, Value>,
) -> BTreeSet<&'a String> {
    current.keys().chain(desired.keys()).collect()
}

fn masked(value: &str) -> String {
    SecretValue::new(value).masked().as_str().to_string()
}

fn clear_build_values(artifact: &mut ArtifactTemplate) {
    if let ArtifactTemplate::Build { template } = artifact {
        template.environment.clear();
        template.secrets.clear();
    }
}

fn build_environment(spec: &ServiceSpec) -> Option<&BTreeMap<String, String>> {
    match &spec.artifact {
        ArtifactTemplate::Image { .. } => None,
        ArtifactTemplate::Build { template } => Some(&template.environment),
    }
}

fn build_secrets(spec: &ServiceSpec) -> Option<&BTreeMap<String, SecretValue>> {
    match &spec.artifact {
        ArtifactTemplate::Image { .. } => None,
        ArtifactTemplate::Build { template } => Some(&template.secrets),
    }
}

fn clear_preview_values(preview: &mut Option<kernel_api::PreviewPolicy>) {
    if let Some(preview) = preview {
        preview.environment.clear();
    }
}

fn preview_environment(spec: &ServiceSpec) -> Option<&BTreeMap<String, String>> {
    spec.preview.as_ref().map(|value| &value.environment)
}
