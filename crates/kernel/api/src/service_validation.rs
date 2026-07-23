use std::collections::BTreeSet;
use std::path::{Component, Path};

use crate::{ArtifactTemplate, BuildSource, HealthProbe, NodeApiAccess, ServiceSpec, VolumeSource};

impl ServiceSpec {
    /// Validates semantic invariants before desired state enters the store.
    pub fn validate(&self) -> Result<(), ServiceSpecError> {
        nonempty_text("name", &self.name)?;
        nonempty_text("version", &self.version)?;
        validate_artifact(&self.artifact)?;
        if let Some(command) = &self.command {
            nonempty_text("command.executable", &command.executable)?;
            for argument in &command.arguments {
                no_nul("command.arguments", argument)?;
            }
        }
        validate_public_environment("environment", &self.environment)?;
        let mut ports = BTreeSet::new();
        for port in &self.exposed_ports {
            if *port == 0 {
                return invalid("exposedPorts", "ports must be non-zero");
            }
            if !ports.insert(*port) {
                return invalid("exposedPorts", "ports must be unique");
            }
        }
        if let Some(health) = &self.health_check {
            if health.interval_secs == 0 || health.unhealthy_threshold == 0 {
                return invalid(
                    "healthCheck",
                    "interval and unhealthy threshold must be non-zero",
                );
            }
            match &health.probe {
                HealthProbe::Http { port, path } => {
                    validate_probe_port(*port)?;
                    if !path.starts_with('/')
                        || path.chars().any(char::is_control)
                        || path.chars().any(char::is_whitespace)
                    {
                        return invalid(
                            "healthCheck.probe.path",
                            "HTTP probe path must be an absolute path without whitespace",
                        );
                    }
                }
                HealthProbe::Tcp { port } => validate_probe_port(*port)?,
            }
        }
        if self.node_api != NodeApiAccess::Disabled && self.user.is_none() {
            return invalid(
                "user",
                "node API access requires an explicit user and group",
            );
        }
        if let Some(secrets) = &self.secrets {
            absolute_clean_path("secrets.mountPath", &secrets.mount_path)?;
            validate_secret_environment("secrets.items", &secrets.items)?;
        }
        let mut targets = BTreeSet::new();
        for volume in &self.volumes {
            absolute_clean_path("volumes.target", &volume.target)?;
            if !targets.insert(volume.target.as_str()) {
                return invalid("volumes.target", "mount targets must be unique");
            }
            match &volume.source {
                VolumeSource::HostPath { path, .. } => {
                    absolute_clean_path("volumes.source.path", path)?;
                }
                VolumeSource::Managed { name } | VolumeSource::ReplicaManaged { name } => {
                    nonempty_text("volumes.source.name", name)?;
                }
            }
        }
        if let Some(preview) = &self.preview {
            if preview.lifetime_secs == 0 || preview.replicas == 0 {
                return invalid("preview", "lifetime and replica count must be non-zero");
            }
            validate_public_environment("preview.environment", &preview.environment)?;
        }
        Ok(())
    }
}

fn validate_artifact(artifact: &ArtifactTemplate) -> Result<(), ServiceSpecError> {
    match artifact {
        ArtifactTemplate::Image { reference } => {
            nonempty_text("artifact.reference", reference)?;
            if reference.chars().any(char::is_whitespace) {
                return invalid(
                    "artifact.reference",
                    "image reference must not contain whitespace",
                );
            }
        }
        ArtifactTemplate::Build { template } => {
            relative_clean_path("artifact.dockerfile", &template.dockerfile)?;
            validate_public_environment("artifact.environment", &template.environment)?;
            validate_secret_environment("artifact.secrets", &template.secrets)?;
            match &template.source {
                BuildSource::Git {
                    repository,
                    revision,
                } => {
                    nonempty_text("artifact.source.repository", repository)?;
                    nonempty_text("artifact.source.revision", revision)?;
                }
                BuildSource::Tarball { .. } => {}
            }
        }
    }
    Ok(())
}

fn validate_public_environment(
    field: &str,
    values: &std::collections::BTreeMap<String, String>,
) -> Result<(), ServiceSpecError> {
    for (key, value) in values {
        validate_environment_key(field, key)?;
        no_nul(field, value)?;
    }
    Ok(())
}

fn validate_secret_environment(
    field: &str,
    values: &std::collections::BTreeMap<String, crate::SecretValue>,
) -> Result<(), ServiceSpecError> {
    for (key, value) in values {
        validate_environment_key(field, key)?;
        no_nul(field, value.expose())?;
    }
    Ok(())
}

fn validate_environment_key(field: &str, key: &str) -> Result<(), ServiceSpecError> {
    let mut characters = key.chars();
    if !characters
        .next()
        .is_some_and(|character| character == '_' || character.is_ascii_alphabetic())
        || !characters.all(|character| character == '_' || character.is_ascii_alphanumeric())
    {
        invalid(field, "environment keys must match [A-Za-z_][A-Za-z0-9_]*")
    } else {
        Ok(())
    }
}

fn validate_probe_port(port: u16) -> Result<(), ServiceSpecError> {
    if port == 0 {
        invalid("healthCheck.probe.port", "probe port must be non-zero")
    } else {
        Ok(())
    }
}

fn absolute_clean_path(field: &str, value: &str) -> Result<(), ServiceSpecError> {
    let path = Path::new(value);
    if value == "/" || !path.is_absolute() || !clean_components(path) {
        invalid(
            field,
            "path must be absolute, non-root, and contain no parent traversal",
        )
    } else {
        no_nul(field, value)
    }
}

fn relative_clean_path(field: &str, value: &str) -> Result<(), ServiceSpecError> {
    let path = Path::new(value);
    if value.is_empty() || path.is_absolute() || !clean_components(path) {
        invalid(
            field,
            "path must be relative and contain no parent traversal",
        )
    } else {
        no_nul(field, value)
    }
}

fn clean_components(path: &Path) -> bool {
    path.components()
        .all(|component| matches!(component, Component::RootDir | Component::Normal(_)))
}

fn nonempty_text(field: &str, value: &str) -> Result<(), ServiceSpecError> {
    if value.trim().is_empty() || value.chars().any(char::is_control) {
        invalid(
            field,
            "value must be non-empty and contain no control characters",
        )
    } else {
        Ok(())
    }
}

fn no_nul(field: &str, value: &str) -> Result<(), ServiceSpecError> {
    if value.contains('\0') {
        invalid(field, "value must not contain NUL")
    } else {
        Ok(())
    }
}

fn invalid<T>(field: &str, message: &str) -> Result<T, ServiceSpecError> {
    Err(ServiceSpecError {
        field: field.to_string(),
        message: message.to_string(),
    })
}

/// Semantic admission failure for a service desired-state document.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid service field `{field}`: {message}")]
pub struct ServiceSpecError {
    /// Stable dotted field path suitable for API diagnostics.
    pub field: String,
    /// Human-readable invariant that was violated.
    pub message: String,
}
