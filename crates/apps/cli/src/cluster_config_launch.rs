use std::path::{Component, Path};

use cluster::{
    ClusterLaunchPolicy, DatadogLaunchConfig, DatadogLogsLaunchConfig, DatadogMetricsLaunchConfig,
    DepotLaunchConfig, LogBackupLaunchConfig, NixosUpgradeLaunchConfig, PreviewLaunchConfig,
};
use kernel_api::SecretValue;

use crate::CliError;
use crate::cluster_config::{invalid, required};
use crate::cluster_config_document::{
    ClusterDocument, DepotInput, LogBackupInput, NixosUpgradeInput, PreviewInput,
};
use crate::config_source::{ConfigSourceReader, resolve_relative_source};

pub(crate) async fn convert_launch_policy(
    config_source: &str,
    document: &ClusterDocument,
    reader: &impl ConfigSourceReader,
) -> Result<ClusterLaunchPolicy, CliError> {
    let datadog = match document.datadog.as_ref() {
        Some(input) => {
            let config = DatadogLaunchConfig {
                api_key: SecretValue::new(
                    resolve_secret(config_source, "datadog.api-key", &input.api_key, reader)
                        .await?,
                ),
                site: required("datadog.site", input.site.clone())?,
                include_ingress_logs: input.include_ingress_logs,
                include_tailscale_logs: input.include_tailscale_logs,
                logs: DatadogLogsLaunchConfig {
                    include_healthcheck: input.logs.include_healthcheck,
                },
                metrics: DatadogMetricsLaunchConfig {
                    enabled: input.metrics.enabled,
                    tags: input.metrics.tags.clone(),
                },
            };
            validate_datadog_input(&config)?;
            Some(config)
        }
        None => None,
    };
    let depot = match document.depot.as_ref() {
        Some(input) => {
            validate_depot_input(input)?;
            let config = DepotLaunchConfig {
                token: SecretValue::new(
                    resolve_secret(config_source, "depot.token", &input.token, reader).await?,
                ),
                executable: input.executable.clone(),
                timeout_secs: input.timeout_secs,
            };
            if config.token.expose().contains('\0') {
                return Err(invalid("depot.token", "must not contain a null byte"));
            }
            Some(config)
        }
        None => None,
    };
    let log_backup = document
        .log_backup
        .as_ref()
        .map(|input| {
            validate_log_backup_input(input)?;
            Ok::<_, CliError>(LogBackupLaunchConfig {
                bucket: required("log-backup.bucket", input.bucket.clone())?,
                kms_key_id: required("log-backup.kms-key-id", input.kms_key_id.clone())?,
                region: input
                    .region
                    .clone()
                    .map(|region| required("log-backup.region", region))
                    .transpose()?,
                prefix: input.prefix.clone(),
                retention_days: input.retention_days,
            })
        })
        .transpose()?;
    let preview = match document.preview.as_ref() {
        Some(input) => {
            validate_preview_input(input)?;
            let config = PreviewLaunchConfig {
                domain: required("preview.domain", input.domain.clone())?,
                github_token: SecretValue::new(
                    resolve_secret(
                        config_source,
                        "preview.github-token",
                        &input.github_token,
                        reader,
                    )
                    .await?,
                ),
                max_concurrent_previews: input.max_concurrent_previews,
            };
            if config.github_token.expose().chars().any(char::is_control) {
                return Err(invalid(
                    "preview.github-token",
                    "must not contain control characters",
                ));
            }
            Some(config)
        }
        None => None,
    };
    let nixos_upgrade = document
        .nixos_upgrade
        .as_ref()
        .map(|input| {
            validate_nixos_upgrade_input(input)?;
            Ok::<_, CliError>(NixosUpgradeLaunchConfig {
                flake: input.flake.clone(),
                configuration: input.configuration.clone(),
                manifest_relative_path: input.manifest_relative_path.clone(),
                nix_binary: input.nix_binary.clone(),
                nixos_rebuild_binary: input.nixos_rebuild_binary.clone(),
                systemctl_binary: input.systemctl_binary.clone(),
            })
        })
        .transpose()?;
    Ok(ClusterLaunchPolicy {
        datadog,
        depot,
        log_backup,
        preview,
        nixos_upgrade,
    })
}

fn validate_datadog_input(config: &DatadogLaunchConfig) -> Result<(), CliError> {
    logs::DatadogLogSinkSettings::new(config.api_key.expose(), &config.site)
        .map_err(|error| invalid("datadog", error))?;
    if config.metrics.tags.len() > 256
        || config
            .metrics
            .tags
            .iter()
            .any(|tag| tag.is_empty() || tag.len() > 200 || tag.chars().any(char::is_control))
    {
        return Err(invalid(
            "datadog.metrics.tags",
            "must contain at most 256 non-empty, bounded values without control characters",
        ));
    }
    Ok(())
}

fn validate_depot_input(input: &DepotInput) -> Result<(), CliError> {
    if input.executable.as_os_str().is_empty()
        || input.executable.as_os_str().as_encoded_bytes().contains(&0)
    {
        return Err(invalid(
            "depot.executable",
            "must not be empty or contain a null byte",
        ));
    }
    if input.timeout_secs == 0 {
        return Err(invalid("depot.timeout-secs", "must be greater than zero"));
    }
    Ok(())
}

fn validate_log_backup_input(input: &LogBackupInput) -> Result<(), CliError> {
    validate_bounded_token("log-backup.bucket", &input.bucket, 255)?;
    validate_bounded_token("log-backup.kms-key-id", &input.kms_key_id, 4_096)?;
    if let Some(region) = &input.region {
        validate_bounded_token("log-backup.region", region, 64)?;
        if !region
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        {
            return Err(invalid(
                "log-backup.region",
                "contains unsupported characters",
            ));
        }
    }
    if input.retention_days == Some(0) {
        return Err(invalid("log-backup.retention-days", "must be at least one"));
    }
    if let Some(prefix) = input
        .prefix
        .as_deref()
        .map(str::trim)
        .filter(|prefix| !prefix.is_empty())
    {
        let prefix = prefix.trim_matches('/');
        if prefix.is_empty()
            || prefix.len() > 4_096
            || prefix.split('/').any(|part| {
                part.is_empty()
                    || matches!(part, "." | "..")
                    || !part.bytes().all(|byte| {
                        byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.')
                    })
            })
        {
            return Err(invalid(
                "log-backup.prefix",
                "must be a safe object-key path",
            ));
        }
    }
    Ok(())
}

fn validate_preview_input(input: &PreviewInput) -> Result<(), CliError> {
    let domain = input.domain.trim().trim_end_matches('.');
    if domain.is_empty()
        || domain.split('.').any(|label| {
            label.is_empty()
                || label.starts_with('-')
                || label.ends_with('-')
                || !label
                    .chars()
                    .all(|character| character.is_ascii_alphanumeric() || character == '-')
        })
    {
        return Err(invalid("preview.domain", "must be a valid DNS suffix"));
    }
    if input.max_concurrent_previews == 0 {
        return Err(invalid(
            "preview.max-concurrent-previews",
            "must be greater than zero",
        ));
    }
    Ok(())
}

fn validate_nixos_upgrade_input(input: &NixosUpgradeInput) -> Result<(), CliError> {
    if !input.flake.is_absolute() {
        return Err(invalid("nixos-upgrade.flake", "must be an absolute path"));
    }
    if input.configuration.is_empty()
        || !input
            .configuration
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || matches!(character, '-' | '_'))
    {
        return Err(invalid(
            "nixos-upgrade.configuration",
            "must contain only ASCII letters, digits, '-' or '_'",
        ));
    }
    if input.manifest_relative_path.as_os_str().is_empty()
        || !input
            .manifest_relative_path
            .components()
            .all(|component| matches!(component, Component::Normal(_)))
    {
        return Err(invalid(
            "nixos-upgrade.manifest-relative-path",
            "must be a non-empty relative path without traversal",
        ));
    }
    if input.nix_binary.is_some() != input.nixos_rebuild_binary.is_some() {
        return Err(invalid(
            "nixos-upgrade",
            "nix-binary and nixos-rebuild-binary must both be configured",
        ));
    }
    validate_absolute_override("nixos-upgrade.nix-binary", input.nix_binary.as_deref())?;
    validate_absolute_override(
        "nixos-upgrade.nixos-rebuild-binary",
        input.nixos_rebuild_binary.as_deref(),
    )?;
    validate_absolute_override(
        "nixos-upgrade.systemctl-binary",
        input.systemctl_binary.as_deref(),
    )
}

fn validate_absolute_override(path: &str, value: Option<&Path>) -> Result<(), CliError> {
    if value.is_some_and(|value| !value.is_absolute()) {
        Err(invalid(path, "must be an absolute path"))
    } else {
        Ok(())
    }
}

fn validate_bounded_token(path: &str, value: &str, maximum: usize) -> Result<(), CliError> {
    if value.is_empty()
        || value.len() > maximum
        || value.trim() != value
        || value.chars().any(char::is_whitespace)
        || value.chars().any(char::is_control)
    {
        Err(invalid(path, "is invalid"))
    } else {
        Ok(())
    }
}

async fn resolve_secret(
    config_source: &str,
    path: &str,
    value: &str,
    reader: &impl ConfigSourceReader,
) -> Result<String, CliError> {
    let value = if value.starts_with("aws-secret://") || value.starts_with("file://") {
        let source = resolve_relative_source(config_source, value)?;
        reader.read(&source).await?
    } else {
        value.to_owned()
    };
    required(path, value)
}
