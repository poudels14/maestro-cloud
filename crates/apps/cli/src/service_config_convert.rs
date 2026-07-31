use std::collections::BTreeMap;

use kernel_api::{
    ArtifactTemplate, BuildSource, BuildTemplate, CommandSpec, DepotBuildConfig, ExecPolicy,
    HealthCheckSpec, HealthProbe, NodeApiAccess, NodeId, PlacementConstraint, PreviewPolicy,
    SecretMountSpec, SecretValue, ServiceId, ServiceSpec, VolumeAccess, VolumeMountSpec,
    VolumeSource,
};
use sha2::{Digest, Sha256};

use crate::CliError;
use crate::config_source::{ConfigSourceReader, resolve_relative_source};
use crate::service_config::{
    BuildConfig, DesiredService, PreviewConfig, ServiceTemplate, ValueSource, VolumeConfig,
};

pub(super) enum BuildSourceSelection {
    ConfiguredGit,
    UploadedArchive(kernel_api::ArtifactArchiveId),
}

pub(super) async fn convert_service(
    config_source: &str,
    service_id: &ServiceId,
    template: ServiceTemplate,
    build_source: BuildSourceSelection,
    reader: &impl ConfigSourceReader,
) -> Result<DesiredService, CliError> {
    let path = format!("services.{service_id}");
    let name = required_text(&format!("{path}.name"), &template.name)?;
    if template.build.is_some() == template.image.is_some() {
        return Err(invalid(&path, "set exactly one of `build` or `image`"));
    }
    if !template.deploy.flags.is_empty() {
        return Err(invalid(
            &format!("{path}.deploy.flags"),
            "runtime flags are not supported by the native runtime contract",
        ));
    }

    let artifact = match (template.build, template.image) {
        (Some(build), None) => ArtifactTemplate::Build {
            template: convert_build(config_source, &path, build, build_source, reader).await?,
        },
        (None, Some(image)) => ArtifactTemplate::Image {
            reference: required_text(&format!("{path}.image"), &image)?,
        },
        _ => return Err(invalid(&path, "set exactly one of `build` or `image`")),
    };
    if matches!(
        &artifact,
        ArtifactTemplate::Build {
            template: BuildTemplate {
                source: BuildSource::Tarball { .. },
                ..
            }
        }
    ) && template
        .preview
        .as_ref()
        .is_some_and(|preview| preview.enabled)
    {
        return Err(invalid(
            &format!("{path}.preview.enabled"),
            "requires a Git build source",
        ));
    }

    let environment = resolve_values(
        config_source,
        &format!("{path}.deploy.env"),
        template.deploy.env,
        reader,
        MaestroTemplatePolicy::Reject,
    )
    .await?;
    let secrets = match template.deploy.secrets {
        None => None,
        Some(secrets) => {
            let mount_path = required_text(
                &format!("{path}.deploy.secrets.mountPath"),
                &secrets.mount_path,
            )?;
            let items = resolve_values(
                config_source,
                &format!("{path}.deploy.secrets"),
                ValueSource {
                    source: secrets.source,
                    items: secrets.items,
                },
                reader,
                MaestroTemplatePolicy::Reject,
            )
            .await?
            .into_iter()
            .map(|(key, value)| (key, SecretValue::new(value)))
            .collect();
            Some(SecretMountSpec::Dotenv { mount_path, items })
        }
    };

    let node_id = template
        .deploy
        .node_affinity
        .node_id
        .map(NodeId::new)
        .transpose()
        .map_err(|error| invalid(&format!("{path}.deploy.nodeAffinity.node-id"), error))?;
    if template
        .deploy
        .volumes
        .iter()
        .any(|volume| volume.host_path.is_some())
        && node_id.is_none()
    {
        return Err(invalid(
            &format!("{path}.deploy.nodeAffinity.node-id"),
            "is required when host volumes are configured",
        ));
    }
    let volumes = template
        .deploy
        .volumes
        .into_iter()
        .enumerate()
        .map(|(index, volume)| convert_volume(&path, index, volume, node_id.as_ref()))
        .collect::<Result<Vec<_>, CliError>>()?;

    let health_check = template
        .deploy
        .healthcheck_path
        .map(|probe_path| {
            let port = template
                .ingress
                .as_ref()
                .and_then(|ingress| ingress.port)
                .or_else(|| template.deploy.expose_ports.first().copied())
                .ok_or_else(|| {
                    invalid(
                        &format!("{path}.deploy.healthcheckPath"),
                        "requires ingress.port or a deploy.exposePorts entry",
                    )
                })?;
            Ok(HealthCheckSpec {
                probe: HealthProbe::Http {
                    port,
                    path: probe_path,
                },
                interval_secs: template.deploy.healthcheck_interval.clamp(5, 300),
                unhealthy_threshold: 10,
            })
        })
        .transpose()?;
    let preview = convert_preview(config_source, &path, template.preview, reader).await?;
    let mut exposed_ports = template.deploy.expose_ports;
    if let Some(port) = template.ingress.as_ref().and_then(|ingress| ingress.port) {
        exposed_ports.push(port);
    }
    exposed_ports.sort_unstable();
    exposed_ports.dedup();
    let mut spec = ServiceSpec {
        name,
        version: "pending".to_string(),
        artifact,
        preview,
        command: template.deploy.command.map(|command| CommandSpec {
            executable: command.command,
            arguments: command.args,
        }),
        replicas: template.deploy.replicas,
        exposed_ports,
        health_check,
        max_restarts: template.deploy.max_restarts,
        environment,
        user: None,
        node_api: NodeApiAccess::Disabled,
        secrets,
        volumes,
        placement: PlacementConstraint {
            node_id,
            labels: template.deploy.node_affinity.labels,
            replica_spread: template.deploy.replica_spread,
        },
        exec: if template.deploy.exec {
            ExecPolicy::Allowed
        } else {
            ExecPolicy::Denied
        },
    };
    let encoded = serde_json::to_vec(&spec)
        .map_err(|error| CliError::json("failed to version service config", error))?;
    let digest = format!("{:x}", Sha256::digest(encoded));
    spec.version = format!("cfg-{}", digest.chars().take(24).collect::<String>());
    spec.validate()
        .map_err(|error| invalid(&path, error.to_string()))?;

    validate_ingress(&path, template.ingress.as_ref())?;
    let mut egress = template.deploy.egress.allow;
    for (index, rule) in egress.iter_mut().enumerate() {
        rule.cidr = rule
            .cidr
            .parse::<ipnet::IpNet>()
            .map_err(|error| invalid(&format!("{path}.deploy.egress.allow[{index}].cidr"), error))?
            .to_string();
        if rule.ports.contains(&0) {
            return Err(invalid(
                &format!("{path}.deploy.egress.allow[{index}].ports"),
                "ports must be non-zero",
            ));
        }
        rule.ports.sort_unstable();
        rule.ports.dedup();
    }
    Ok(DesiredService {
        spec,
        ingress: template.ingress,
        egress,
    })
}

fn convert_volume(
    service_path: &str,
    index: usize,
    volume: VolumeConfig,
    node_id: Option<&NodeId>,
) -> Result<VolumeMountSpec, CliError> {
    let path = format!("{service_path}.deploy.volumes[{index}]");
    let source = match (
        volume.host_path,
        volume.managed_volume,
        volume.replica_managed_volume,
    ) {
        (Some(host_path), None, None) => VolumeSource::HostPath {
            path: required_text(&format!("{path}.hostPath"), &host_path)?,
            node_id: node_id.cloned().ok_or_else(|| {
                invalid(
                    &format!("{service_path}.deploy.nodeAffinity.node-id"),
                    "is required when host volumes are configured",
                )
            })?,
        },
        (None, Some(name), None) => VolumeSource::Managed {
            name: required_text(&format!("{path}.managedVolume"), &name)?,
        },
        (None, None, Some(name)) => VolumeSource::ReplicaManaged {
            name: required_text(&format!("{path}.replicaManagedVolume"), &name)?,
        },
        _ => {
            return Err(invalid(
                &path,
                "set exactly one of `hostPath`, `managedVolume`, or `replicaManagedVolume`",
            ));
        }
    };
    Ok(VolumeMountSpec {
        source,
        target: required_text(&format!("{path}.mountPath"), &volume.mount_path)?,
        access: if volume.read_only {
            VolumeAccess::ReadOnly
        } else {
            VolumeAccess::ReadWrite
        },
    })
}

async fn convert_build(
    config_source: &str,
    service_path: &str,
    build: BuildConfig,
    source: BuildSourceSelection,
    reader: &impl ConfigSourceReader,
) -> Result<BuildTemplate, CliError> {
    let environment = resolve_values(
        config_source,
        &format!("{service_path}.build.env"),
        build.env,
        reader,
        MaestroTemplatePolicy::Reject,
    )
    .await?;
    let secrets = resolve_values(
        config_source,
        &format!("{service_path}.build.secrets"),
        build.secrets,
        reader,
        MaestroTemplatePolicy::Reject,
    )
    .await?
    .into_iter()
    .map(|(key, value)| (key, SecretValue::new(value)))
    .collect();
    let source = match source {
        BuildSourceSelection::ConfiguredGit => {
            let repository = build.repo.ok_or_else(|| {
                invalid(
                    &format!("{service_path}.build.repo"),
                    "is required for declarative rollout; use `services up` for a local context",
                )
            })?;
            BuildSource::Git {
                repository: required_text(&format!("{service_path}.build.repo"), &repository)?,
                revision: build
                    .branch
                    .as_deref()
                    .map(|branch| required_text(&format!("{service_path}.build.branch"), branch))
                    .transpose()?
                    .unwrap_or_else(|| "HEAD".to_string()),
            }
        }
        BuildSourceSelection::UploadedArchive(archive_id) => {
            if build.watch {
                return Err(invalid(
                    &format!("{service_path}.build.watch"),
                    "cannot watch an uploaded context",
                ));
            }
            BuildSource::Tarball { archive_id }
        }
    };
    let registry = match build.registry.as_deref() {
        Some(registry) => {
            let registry = required_text(&format!("{service_path}.build.registry"), registry)?;
            let registry = registry.trim_end_matches('/');
            if registry.is_empty() {
                return Err(invalid(
                    &format!("{service_path}.build.registry"),
                    "cannot contain only `/`",
                ));
            }
            Some(registry.to_owned())
        }
        None => None,
    };
    let depot = build
        .depot
        .map(|depot| {
            required_text(
                &format!("{service_path}.build.depot.project"),
                &depot.project,
            )
            .map(|project| DepotBuildConfig { project })
        })
        .transpose()?;
    Ok(BuildTemplate {
        source,
        dockerfile: required_text(
            &format!("{service_path}.build.dockerfile"),
            &build.dockerfile,
        )?,
        watch: build.watch,
        registry,
        depot,
        environment,
        secrets,
    })
}

async fn convert_preview(
    config_source: &str,
    service_path: &str,
    preview: Option<PreviewConfig>,
    reader: &impl ConfigSourceReader,
) -> Result<Option<PreviewPolicy>, CliError> {
    let Some(preview) = preview.filter(|preview| preview.enabled) else {
        return Ok(None);
    };
    let close_grace_period_secs = parse_duration(
        &format!("{service_path}.preview.closeGracePeriod"),
        &preview.close_grace_period,
    )?;
    let environment = resolve_values(
        config_source,
        &format!("{service_path}.preview.env"),
        preview.env,
        reader,
        MaestroTemplatePolicy::Preserve,
    )
    .await?;
    Ok(Some(PreviewPolicy {
        close_grace_period_secs,
        lifetime_secs: 30 * 24 * 60 * 60,
        replicas: preview.replicas,
        environment,
    }))
}

async fn resolve_values(
    config_source: &str,
    field: &str,
    values: ValueSource,
    reader: &impl ConfigSourceReader,
    maestro_templates: MaestroTemplatePolicy,
) -> Result<BTreeMap<String, String>, CliError> {
    if values.source.is_some() && !values.items.is_empty() {
        return Err(invalid(field, "set either `source` or `items`, not both"));
    }
    let mut items = if let Some(source) = values.source {
        let source = resolve_relative_source(config_source, &source)?;
        parse_key_values(field, &reader.read(&source).await?)?
    } else {
        values.items
    };
    for (key, value) in &mut items {
        *value = expand_local_environment(value, maestro_templates)
            .map_err(|error| invalid(&format!("{field}.items.{key}"), error))?;
    }
    Ok(items)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MaestroTemplatePolicy {
    Preserve,
    Reject,
}

fn expand_local_environment(
    value: &str,
    maestro_templates: MaestroTemplatePolicy,
) -> Result<String, String> {
    let mut remaining = value;
    let mut output = String::with_capacity(value.len());
    while let Some(open) = remaining.find("${{") {
        if maestro_templates == MaestroTemplatePolicy::Reject {
            return Err("Maestro templates are supported only in preview.env".to_owned());
        }
        output.push_str(
            shellexpand::env(&remaining[..open])
                .map_err(|error| error.to_string())?
                .as_ref(),
        );
        let expression = &remaining[open + 3..];
        let Some(close) = expression.find("}}") else {
            output.push_str(&remaining[open..]);
            return Ok(output);
        };
        output.push_str(&remaining[open..open + 3 + close + 2]);
        remaining = &expression[close + 2..];
    }
    output.push_str(
        shellexpand::env(remaining)
            .map_err(|error| error.to_string())?
            .as_ref(),
    );
    Ok(output)
}

fn parse_key_values(field: &str, raw: &str) -> Result<BTreeMap<String, String>, CliError> {
    if let Ok(object) = serde_json::from_str::<BTreeMap<String, serde_json::Value>>(raw) {
        return Ok(object
            .into_iter()
            .map(|(key, value)| {
                let value = value
                    .as_str()
                    .map(ToString::to_string)
                    .unwrap_or_else(|| value.to_string());
                (key, value)
            })
            .collect());
    }
    let mut values = BTreeMap::new();
    for (index, line) in raw.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let line = line.strip_prefix("export ").unwrap_or(line);
        let (key, value) = line
            .split_once('=')
            .ok_or_else(|| invalid(field, format!("source line {} is not KEY=VALUE", index + 1)))?;
        values.insert(key.trim().to_string(), value.trim().to_string());
    }
    Ok(values)
}

fn validate_ingress(
    service_path: &str,
    ingress: Option<&crate::service_config::IngressConfig>,
) -> Result<(), CliError> {
    let Some(ingress) = ingress else {
        return Ok(());
    };
    if ingress.port == Some(0) {
        return Err(invalid(
            &format!("{service_path}.ingress.port"),
            "port must be non-zero",
        ));
    }
    if ingress.port.is_none() {
        return Err(invalid(
            &format!("{service_path}.ingress.port"),
            "is required",
        ));
    }
    let host_count = usize::from(ingress.host.is_some()) + ingress.hosts.len();
    if host_count == 0 {
        return Err(invalid(
            &format!("{service_path}.ingress"),
            "at least one host is required",
        ));
    }
    if let Some(affinity) = &ingress.session_affinity {
        required_text(
            &format!("{service_path}.ingress.sessionAffinity.header"),
            &affinity.header,
        )?;
    }
    Ok(())
}

fn parse_duration(field: &str, value: &str) -> Result<u64, CliError> {
    let split = value
        .find(|character: char| !character.is_ascii_digit())
        .unwrap_or(value.len());
    let (amount, unit) = value.split_at(split);
    let amount = amount.parse::<u64>().map_err(|_| {
        invalid(
            field,
            "expected a duration such as `1d`, `12h`, `30m`, or `60s`",
        )
    })?;
    let multiplier = match unit {
        "s" => 1,
        "m" => 60,
        "h" => 60 * 60,
        "d" => 24 * 60 * 60,
        _ => return Err(invalid(field, "duration unit must be s, m, h, or d")),
    };
    amount
        .checked_mul(multiplier)
        .filter(|seconds| *seconds > 0)
        .ok_or_else(|| invalid(field, "duration must be greater than zero"))
}

fn required_text(field: &str, value: &str) -> Result<String, CliError> {
    let value = value.trim();
    if value.is_empty() {
        Err(invalid(field, "cannot be empty"))
    } else {
        Ok(value.to_string())
    }
}

fn invalid(field: &str, message: impl std::fmt::Display) -> CliError {
    CliError::invalid_input(format!("{field}: {message}"))
}
