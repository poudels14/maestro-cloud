use std::collections::{BTreeMap, BTreeSet};

use kernel_api::{
    ArtifactTemplate, BuildSource, BuildTemplate, CommandSpec, ExecPolicy, FirewallDirection,
    FirewallPolicySpec, FirewallRule, FirewallSubject, FirewallVerdict, HealthCheckSpec,
    HealthProbe, IngressRouteSpec, NodeApiAccess, NodeId, PlacementConstraint, PortRange,
    PreviewPolicy, SecretMountSpec, SecretValue, ServiceId, ServiceSpec, SessionAffinity,
    TransportProtocol, VolumeAccess, VolumeMountSpec, VolumeSource,
};
use sha2::{Digest, Sha256};

use crate::legacy_convert::LegacyPlanError;
use crate::legacy_schema::{
    LegacyBuildConfig, LegacyEnvConfig, LegacySecretKeyMeta, LegacyServiceConfig,
};
use crate::legacy_services::LegacyDeploymentData;

const PREVIEW_LIFETIME_SECS: u64 = 30 * 24 * 60 * 60;
const UNHEALTHY_THRESHOLD: u32 = 10;

pub(crate) struct ConvertedServiceConfig {
    pub(crate) spec: ServiceSpec,
    pub(crate) ingress: Option<IngressRouteSpec>,
    pub(crate) egress: Option<FirewallPolicySpec>,
}

pub(crate) fn convert_service_config(
    config: &LegacyServiceConfig,
    data: &LegacyDeploymentData,
    pinned_revision: Option<&str>,
    artifact_override: Option<ArtifactTemplate>,
) -> Result<ConvertedServiceConfig, LegacyPlanError> {
    let service_id = service_id(&config.id, "service config id")?;
    if !config.deploy.flags.is_empty() {
        return Err(unsupported(
            &config.id,
            "deploy.flags",
            "native workloads cannot preserve runtime-specific flags",
        ));
    }
    let artifact = match artifact_override {
        Some(artifact) => artifact,
        None => convert_artifact(config, data, pinned_revision)?,
    };
    let environment = resolved_values(
        &config.id,
        "deploy.env",
        &config.deploy.env,
        &data.deploy_environment,
    )?;
    let secrets = convert_secrets(config, data)?;
    let placement = convert_placement(config)?;
    let volumes = convert_volumes(config, &placement)?;
    let ingress = convert_ingress(config, &service_id)?;
    let egress = convert_egress(config, &service_id)?;
    let mut exposed_ports = config.deploy.expose_ports.clone();
    if let Some(ingress) = &config.ingress {
        exposed_ports.push(ingress.port.unwrap_or(80));
    }
    exposed_ports.sort_unstable();
    exposed_ports.dedup();
    let health_check = config
        .deploy
        .healthcheck_path
        .as_deref()
        .filter(|path| !path.trim().is_empty())
        .and_then(|path| {
            config.ingress.as_ref().map(|ingress| HealthCheckSpec {
                probe: HealthProbe::Http {
                    port: ingress.port.unwrap_or(80),
                    path: path.to_owned(),
                },
                interval_secs: config.deploy.healthcheck_interval.clamp(5, 300),
                unhealthy_threshold: UNHEALTHY_THRESHOLD,
            })
        });
    let preview = convert_preview(config, data)?;
    let spec = ServiceSpec {
        name: config.name.clone(),
        version: config.version.clone(),
        artifact,
        preview,
        command: config.deploy.command.as_ref().map(|command| CommandSpec {
            executable: command.command.clone(),
            arguments: command.args.clone(),
        }),
        replicas: config.deploy.replicas,
        exposed_ports,
        health_check,
        max_restarts: config.deploy.max_restarts,
        environment,
        user: None,
        node_api: NodeApiAccess::Disabled,
        secrets,
        volumes,
        placement,
        exec: if config.deploy.exec {
            ExecPolicy::Allowed
        } else {
            ExecPolicy::Denied
        },
    };
    spec.validate()
        .map_err(|error| LegacyPlanError::InvalidServiceConfig {
            service_id: config.id.clone(),
            message: error.to_string(),
        })?;
    Ok(ConvertedServiceConfig {
        spec,
        ingress,
        egress,
    })
}

fn convert_artifact(
    config: &LegacyServiceConfig,
    data: &LegacyDeploymentData,
    pinned_revision: Option<&str>,
) -> Result<ArtifactTemplate, LegacyPlanError> {
    match (&config.build, &config.image) {
        (Some(build), None) => convert_build(&config.id, build, data, pinned_revision),
        (None, Some(image)) if !image.trim().is_empty() => {
            if !data.build_environment.is_empty() || !data.build_secrets.is_empty() {
                return Err(invalid(
                    &config.id,
                    "encrypted build sidecars exist but service has no build config",
                ));
            }
            Ok(ArtifactTemplate::Image {
                reference: image.trim().to_owned(),
            })
        }
        (Some(_), Some(_)) => Err(invalid(&config.id, "set either build or image, not both")),
        _ => Err(invalid(
            &config.id,
            "service must contain a non-empty build or image",
        )),
    }
}

fn convert_build(
    service_id: &str,
    build: &LegacyBuildConfig,
    data: &LegacyDeploymentData,
    pinned_revision: Option<&str>,
) -> Result<ArtifactTemplate, LegacyPlanError> {
    if let Some(depot) = &build.depot {
        return Err(unsupported(
            service_id,
            "build.depot",
            format!(
                "Depot project `{}` has no native resource yet",
                depot.project
            ),
        ));
    }
    let repository = build
        .repo
        .as_deref()
        .map(str::trim)
        .filter(|repo| !repo.is_empty());
    let Some(repository) = repository else {
        return Err(unsupported(
            service_id,
            "build.repo",
            "repository-less build is not associated with a resolved upload",
        ));
    };
    let environment =
        resolved_values(service_id, "build.env", &build.env, &data.build_environment)?;
    let secrets = resolved_values(
        service_id,
        "build.secrets",
        &build.secrets,
        &data.build_secrets,
    )?
    .into_iter()
    .map(|(key, value)| (key, SecretValue::new(value)))
    .collect();
    let registry = build
        .registry
        .as_deref()
        .map(str::trim)
        .map(|registry| registry.trim_end_matches('/'))
        .filter(|registry| !registry.is_empty())
        .map(str::to_owned);
    if build.registry.is_some() && registry.is_none() {
        return Err(invalid(service_id, "build.registry cannot be empty"));
    }
    Ok(ArtifactTemplate::Build {
        template: BuildTemplate {
            source: BuildSource::Git {
                repository: repository.to_owned(),
                revision: pinned_revision
                    .map(str::to_owned)
                    .or_else(|| build.branch.clone())
                    .unwrap_or_else(|| "HEAD".to_owned()),
            },
            dockerfile: build.dockerfile.clone(),
            watch: build.watch,
            registry,
            environment,
            secrets,
        },
    })
}

fn convert_secrets(
    config: &LegacyServiceConfig,
    data: &LegacyDeploymentData,
) -> Result<Option<SecretMountSpec>, LegacyPlanError> {
    let Some(secrets) = &config.deploy.secrets else {
        if data.deploy_secrets.is_empty() {
            return Ok(None);
        }
        return Err(invalid(
            &config.id,
            "deployment sidecar contains secrets but config has no secret mount",
        ));
    };
    let values = resolved_secret_values(&config.id, secrets, &data.deploy_secrets)?;
    Ok(Some(SecretMountSpec::Dotenv {
        mount_path: secrets.mount_path.clone(),
        items: values
            .into_iter()
            .map(|(key, value)| (key, SecretValue::new(value)))
            .collect(),
    }))
}

fn resolved_secret_values(
    service_id: &str,
    secrets: &crate::legacy_schema::LegacySecretsConfig,
    sidecar: &BTreeMap<String, String>,
) -> Result<BTreeMap<String, String>, LegacyPlanError> {
    let values = resolved_map(service_id, "deploy.secrets", &secrets.items, sidecar)?;
    if secrets.source.is_some() && values.is_empty() {
        return Err(unresolved(service_id, "deploy.secrets"));
    }
    if !secrets.keys.is_empty() {
        let actual = values.keys().cloned().collect::<BTreeSet<_>>();
        let expected = secrets.keys.keys().cloned().collect::<BTreeSet<_>>();
        if actual != expected {
            return Err(invalid(
                service_id,
                "deploy.secrets metadata does not match its encrypted values",
            ));
        }
    }
    for (key, metadata) in &secrets.keys {
        validate_secret_metadata(service_id, key, metadata, &values)?;
    }
    Ok(values)
}

fn validate_secret_metadata(
    service_id: &str,
    key: &str,
    metadata: &LegacySecretKeyMeta,
    values: &BTreeMap<String, String>,
) -> Result<(), LegacyPlanError> {
    if metadata.changed && metadata.hash.is_empty() {
        return Err(invalid(
            service_id,
            format!("deploy.secrets metadata for `{key}` is changed but has no hash"),
        ));
    }
    if metadata.hash.is_empty() {
        return Ok(());
    }
    let Some(value) = values.get(key) else {
        return Err(invalid(
            service_id,
            format!("deploy.secrets metadata names missing key `{key}`"),
        ));
    };
    let actual = format!("{:x}", Sha256::digest(value.as_bytes()));
    if actual == metadata.hash {
        Ok(())
    } else {
        Err(invalid(
            service_id,
            format!("deploy.secrets value for `{key}` does not match its stored hash"),
        ))
    }
}

fn convert_placement(config: &LegacyServiceConfig) -> Result<PlacementConstraint, LegacyPlanError> {
    let Some(affinity) = &config.deploy.node_affinity else {
        return Ok(PlacementConstraint::default());
    };
    Ok(PlacementConstraint {
        node_id: affinity
            .node_id
            .as_ref()
            .map(|node_id| parse_node_id(&config.id, node_id))
            .transpose()?,
        labels: affinity.labels.clone(),
    })
}

fn convert_volumes(
    config: &LegacyServiceConfig,
    placement: &PlacementConstraint,
) -> Result<Vec<VolumeMountSpec>, LegacyPlanError> {
    config
        .deploy
        .volumes
        .iter()
        .enumerate()
        .map(|(index, volume)| {
            if let Some(owner) = &volume.owner {
                return Err(unsupported(
                    &config.id,
                    format!("deploy.volumes[{index}].owner"),
                    format!(
                        "host-path ownership {}:{} has no native volume contract yet",
                        owner.uid,
                        owner.gid.unwrap_or(owner.uid)
                    ),
                ));
            }
            let node_id = placement.node_id.clone().ok_or_else(|| {
                unsupported(
                    &config.id,
                    format!("deploy.volumes[{index}]"),
                    "host paths require deploy.nodeAffinity.node-id at cutover",
                )
            })?;
            Ok(VolumeMountSpec {
                source: VolumeSource::HostPath {
                    path: volume.host_path.clone(),
                    node_id,
                },
                target: volume.mount_path.clone(),
                access: if volume.read_only {
                    VolumeAccess::ReadOnly
                } else {
                    VolumeAccess::ReadWrite
                },
            })
        })
        .collect()
}

fn convert_preview(
    config: &LegacyServiceConfig,
    data: &LegacyDeploymentData,
) -> Result<Option<PreviewPolicy>, LegacyPlanError> {
    let Some(preview) = config.preview.as_ref().filter(|preview| preview.enabled) else {
        if !data.preview_environment.is_empty() {
            return Err(invalid(
                &config.id,
                "encrypted preview sidecar exists but previews are disabled",
            ));
        }
        return Ok(None);
    };
    Ok(Some(PreviewPolicy {
        close_grace_period_secs: parse_duration(&config.id, &preview.close_grace_period)?,
        lifetime_secs: PREVIEW_LIFETIME_SECS,
        replicas: preview.replicas,
        environment: resolved_values(
            &config.id,
            "preview.env",
            &preview.env,
            &data.preview_environment,
        )?,
    }))
}

fn convert_ingress(
    config: &LegacyServiceConfig,
    service_id: &ServiceId,
) -> Result<Option<IngressRouteSpec>, LegacyPlanError> {
    let Some(ingress) = &config.ingress else {
        return Ok(None);
    };
    let mut hosts = ingress.hosts.clone();
    hosts.extend(ingress.host.clone());
    hosts.sort();
    hosts.dedup();
    if hosts.is_empty() {
        return Ok(None);
    }
    for host in &hosts {
        if !valid_host(host) {
            return Err(invalid(
                &config.id,
                format!("ingress host `{host}` is not a canonical DNS name"),
            ));
        }
    }
    if let Some(affinity) = &ingress.session_affinity
        && !valid_header_name(&affinity.header)
    {
        return Err(invalid(
            &config.id,
            format!(
                "ingress session-affinity header `{}` is invalid",
                affinity.header
            ),
        ));
    }
    Ok(Some(IngressRouteSpec {
        service_id: service_id.clone(),
        hosts,
        path_prefix: None,
        target_port: ingress.port.unwrap_or(80),
        session_affinity: ingress
            .session_affinity
            .as_ref()
            .map(|affinity| SessionAffinity {
                header: affinity.header.clone(),
            }),
    }))
}

fn convert_egress(
    config: &LegacyServiceConfig,
    service_id: &ServiceId,
) -> Result<Option<FirewallPolicySpec>, LegacyPlanError> {
    if config.deploy.egress.allow.is_empty() {
        return Ok(None);
    }
    let mut rules = Vec::with_capacity(config.deploy.egress.allow.len());
    for rule in &config.deploy.egress.allow {
        let cidr = rule
            .cidr
            .parse::<ipnet::IpNet>()
            .map_err(|error| invalid(&config.id, format!("invalid egress CIDR: {error}")))?;
        if rule.ports.contains(&0) {
            return Err(invalid(&config.id, "egress rules cannot contain port zero"));
        }
        let mut ports = rule.ports.clone();
        ports.sort_unstable();
        ports.dedup();
        rules.push(FirewallRule {
            cidr: cidr.to_string(),
            protocol: TransportProtocol::Any,
            ports: ports
                .into_iter()
                .map(|port| PortRange {
                    start: port,
                    end: port,
                })
                .collect(),
            verdict: FirewallVerdict::Allow,
        });
    }
    Ok(Some(FirewallPolicySpec {
        direction: FirewallDirection::Egress,
        subject: FirewallSubject::Service(service_id.clone()),
        rules,
        default_verdict: FirewallVerdict::Deny,
    }))
}

fn valid_host(host: &str) -> bool {
    if host.is_empty()
        || host.len() > 253
        || host.ends_with('.')
        || host.bytes().any(|byte| byte.is_ascii_uppercase())
    {
        return false;
    }
    let host = host.strip_prefix("*.").unwrap_or(host);
    !host.is_empty()
        && host.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= 63
                && !label.starts_with('-')
                && !label.ends_with('-')
                && label
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        })
}

fn valid_header_name(header: &str) -> bool {
    const PUNCTUATION: &[u8] = b"!#$%&'*+-.^_`|~";
    !header.is_empty()
        && !header.eq_ignore_ascii_case("host")
        && header
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || PUNCTUATION.contains(&byte))
}

fn resolved_values(
    service_id: &str,
    field: &'static str,
    config: &LegacyEnvConfig,
    sidecar: &BTreeMap<String, String>,
) -> Result<BTreeMap<String, String>, LegacyPlanError> {
    let values = resolved_map(service_id, field, &config.items, sidecar)?;
    if config.source.is_some() && values.is_empty() {
        Err(unresolved(service_id, field))
    } else {
        Ok(values)
    }
}

fn resolved_map(
    service_id: &str,
    field: &'static str,
    inline: &BTreeMap<String, String>,
    sidecar: &BTreeMap<String, String>,
) -> Result<BTreeMap<String, String>, LegacyPlanError> {
    if !inline.is_empty() && !sidecar.is_empty() && inline != sidecar {
        return Err(invalid(
            service_id,
            format!("{field} differs between its record and encrypted sidecar"),
        ));
    }
    Ok(if sidecar.is_empty() {
        inline.clone()
    } else {
        sidecar.clone()
    })
}

fn parse_duration(service_id: &str, value: &str) -> Result<u64, LegacyPlanError> {
    let value = value.trim();
    let split = value
        .find(|character: char| !character.is_ascii_digit())
        .unwrap_or(value.len());
    let (amount, unit) = value.split_at(split);
    let amount = amount.parse::<u64>().ok().filter(|amount| *amount > 0);
    let seconds = match (amount, unit) {
        (Some(amount), "s") => Some(amount),
        (Some(amount), "m") => amount.checked_mul(60),
        (Some(amount), "h") => amount.checked_mul(60 * 60),
        (Some(amount), "d") => amount.checked_mul(24 * 60 * 60),
        _ => None,
    };
    seconds.ok_or_else(|| invalid(service_id, format!("invalid preview duration `{value}`")))
}

fn service_id(value: &str, field: &'static str) -> Result<ServiceId, LegacyPlanError> {
    ServiceId::new(value).map_err(|error| LegacyPlanError::InvalidIdentifier {
        field,
        value: value.to_owned(),
        message: error.to_string(),
    })
}

fn parse_node_id(service_id: &str, value: &str) -> Result<NodeId, LegacyPlanError> {
    NodeId::new(value).map_err(|error| {
        invalid(
            service_id,
            format!("invalid deploy.nodeAffinity.node-id `{value}`: {error}"),
        )
    })
}

fn invalid(service_id: &str, message: impl Into<String>) -> LegacyPlanError {
    LegacyPlanError::InvalidServiceConfig {
        service_id: service_id.to_owned(),
        message: message.into(),
    }
}

fn unresolved(service_id: &str, field: &'static str) -> LegacyPlanError {
    invalid(
        service_id,
        format!("{field} names an external source but has no captured values"),
    )
}

fn unsupported(
    service_id: &str,
    field: impl Into<String>,
    message: impl Into<String>,
) -> LegacyPlanError {
    LegacyPlanError::UnsupportedServiceField {
        service_id: service_id.to_owned(),
        field: field.into(),
        message: message.into(),
    }
}
