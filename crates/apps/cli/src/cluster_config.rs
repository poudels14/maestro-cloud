use std::collections::BTreeMap;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::path::{Component, Path, PathBuf};

use cluster::{
    CloudflareTunnelConfig, CloudflareTunnelConfigError, ClusterConfig, ClusterLaunchPolicy,
    ClusterPorts, ClusterPreflightError, CrossClusterDnsRoute, DEFAULT_CLOUDFLARE_TUNNEL_REPLICAS,
    DEFAULT_WIREGUARD_PORT, DatadogLaunchConfig, DatadogLogsLaunchConfig,
    DatadogMetricsLaunchConfig, DepotLaunchConfig, Ipv4Cidr, LogBackupLaunchConfig,
    NixosUpgradeLaunchConfig, NodeDefinition, NodeEndpoint, PreviewLaunchConfig,
    TailscaleConfigError, TailscaleGatewayConfig,
};
use kernel_api::{ClusterId, NodeId, NodeRole, SecretValue};
use serde::Deserialize;
use serde_json::Value;

use crate::CliError;
use crate::config_source::{ConfigSourceReader, decode_document, resolve_relative_source};

const DEFAULT_API_PORT: u16 = 3_000;
const DEFAULT_GATEWAY_PORT: u16 = 3_001;
const DEFAULT_STORE_CLIENT_PORT: u16 = 2_379;
const DEFAULT_STORE_PEER_PORT: u16 = 2_380;

#[derive(Debug)]
pub(crate) struct LoadedClusterConfig {
    pub(crate) cluster: ClusterConfig,
    pub(crate) launch_policy: ClusterLaunchPolicy,
    pub(crate) node_id: NodeId,
    pub(crate) ignored_fields: Vec<String>,
}

pub(crate) async fn decode_cluster(
    source: &str,
    value: Value,
    reader: &impl ConfigSourceReader,
) -> Result<LoadedClusterConfig, CliError> {
    let (document, ignored_fields): (ClusterDocument, _) =
        decode_document(&value, &format!("cluster config `{source}`"))?;
    let launch_policy = convert_launch_policy(source, &document, reader).await?;
    let tailscale = convert_tailscale(source, document.tailscale, reader).await?;
    let cloudflare = convert_cloudflare(source, document.cloudflare, reader).await?;
    let cluster = convert_cluster(document.cluster, tailscale, cloudflare)?;
    let node_id = select_node(document.node, &cluster.nodes)?;
    cluster.preflight().map_err(preflight_error)?;
    Ok(LoadedClusterConfig {
        cluster,
        launch_policy,
        node_id,
        ignored_fields,
    })
}

async fn convert_launch_policy(
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

fn convert_cluster(
    input: ClusterInput,
    tailscale: Option<TailscaleGatewayConfig>,
    cloudflare: Option<CloudflareTunnelConfig>,
) -> Result<ClusterConfig, CliError> {
    let name = required("cluster.name", input.name)?;
    let cluster_id = input.cluster_id.unwrap_or_else(|| name.clone());
    let cluster_id = ClusterId::new(cluster_id)
        .map_err(|error| invalid("cluster.cluster-id", error.to_string()))?;
    let cluster_cidr = input
        .cluster_cidr
        .parse::<Ipv4Cidr>()
        .map_err(|error| invalid("cluster.cluster-cidr", error.to_string()))?;
    let nodes = input
        .nodes
        .into_iter()
        .map(|(raw_id, node)| convert_node(raw_id, node))
        .collect::<Result<BTreeMap<_, _>, CliError>>()?;
    let control_allow_cidrs = input
        .control_allow_cidrs
        .into_iter()
        .enumerate()
        .map(|(index, cidr)| {
            cidr.parse::<Ipv4Cidr>().map_err(|error| {
                invalid(
                    &format!("cluster.control-allow-cidrs[{index}]"),
                    error.to_string(),
                )
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let ports = ClusterPorts::new(
        input.ports.gateway,
        input.ports.store_client,
        input.ports.store_peer,
        input.ports.wireguard,
    )
    .map_err(|error| invalid("cluster.ports", error.to_string()))?;
    let join_secret = input
        .join_secret
        .map(|secret| required("cluster.join-secret", secret))
        .transpose()?
        .ok_or_else(|| invalid("cluster.join-secret", "is required"))?;
    Ok(ClusterConfig {
        cluster_id,
        name,
        cluster_cidr,
        node_limit: input.node_limit,
        node_prefix: input.node_prefix,
        nodes,
        control_allow_cidrs,
        ports,
        join_secret: SecretValue::new(join_secret),
        tailscale,
        cloudflare,
    })
}

async fn convert_tailscale(
    config_source: &str,
    input: Option<TailscaleInput>,
    reader: &impl ConfigSourceReader,
) -> Result<Option<TailscaleGatewayConfig>, CliError> {
    let Some(input) = input else {
        return Ok(None);
    };
    let auth_key =
        if input.auth_key.starts_with("aws-secret://") || input.auth_key.starts_with("file://") {
            let source = resolve_relative_source(config_source, &input.auth_key)?;
            reader.read(&source).await?
        } else {
            input.auth_key
        };
    let auth_key = required("tailscale.auth-key", auth_key)?;
    let advertise_routes = input
        .advertise_routes
        .map(|routes| {
            routes
                .into_iter()
                .enumerate()
                .map(|(index, route)| {
                    route.parse::<Ipv4Cidr>().map_err(|error| {
                        invalid(
                            &format!("tailscale.advertise-routes[{index}]"),
                            error.to_string(),
                        )
                    })
                })
                .collect::<Result<Vec<_>, _>>()
        })
        .transpose()?;
    let cross_cluster_dns = input
        .cross_cluster_dns
        .into_iter()
        .enumerate()
        .map(|(route_index, route)| {
            let cluster_path = format!("tailscale.cross-cluster-dns[{route_index}].cluster-id");
            let cluster_id = ClusterId::new(required(&cluster_path, route.cluster_id)?)
                .map_err(|error| invalid(&cluster_path, error))?;
            let nameservers = route
                .nameservers
                .into_iter()
                .enumerate()
                .map(|(nameserver_index, nameserver)| {
                    nameserver.parse::<Ipv4Addr>().map_err(|error| {
                        invalid(
                            &format!(
                                "tailscale.cross-cluster-dns[{route_index}].nameservers[{nameserver_index}]"
                            ),
                            error,
                        )
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(CrossClusterDnsRoute {
                cluster_id,
                nameservers,
            })
        })
        .collect::<Result<Vec<_>, CliError>>()?;
    Ok(Some(TailscaleGatewayConfig {
        auth_key: SecretValue::new(auth_key),
        advertise_routes,
        replicas: input.replicas,
        tags: input.tags,
        cross_cluster_dns,
    }))
}

async fn convert_cloudflare(
    config_source: &str,
    input: Option<CloudflareInput>,
    reader: &impl ConfigSourceReader,
) -> Result<Option<CloudflareTunnelConfig>, CliError> {
    let Some(input) = input else {
        return Ok(None);
    };
    let token = if input.tunnel.token.starts_with("aws-secret://")
        || input.tunnel.token.starts_with("file://")
    {
        let source = resolve_relative_source(config_source, &input.tunnel.token)?;
        reader.read(&source).await?
    } else {
        input.tunnel.token
    };
    Ok(Some(CloudflareTunnelConfig {
        token: SecretValue::new(required("cloudflare.tunnel.token", token)?),
        replicas: input.tunnel.replicas,
    }))
}

fn convert_node(raw_id: String, input: NodeInput) -> Result<(NodeId, NodeDefinition), CliError> {
    let path = format!("cluster.nodes.{raw_id}");
    let node_id =
        NodeId::new(raw_id).map_err(|error| invalid(&path, format!("invalid node ID: {error}")))?;
    let (host_address, api_port) = parse_endpoint(&format!("{path}.endpoint"), &input.endpoint)?;
    let workload_subnet = input
        .subnet
        .parse::<Ipv4Cidr>()
        .map_err(|error| invalid(&format!("{path}.subnet"), error.to_string()))?;
    let hostname = input
        .hostname
        .map(|hostname| required(&format!("{path}.hostname"), hostname))
        .transpose()?
        .unwrap_or_else(|| node_id.as_str().to_string());
    Ok((
        node_id,
        NodeDefinition {
            hostname,
            endpoint: NodeEndpoint {
                host_address,
                api_port,
            },
            workload_subnet,
            role: input.role.into(),
        },
    ))
}

fn parse_endpoint(path: &str, value: &str) -> Result<(Ipv4Addr, u16), CliError> {
    if value.contains(':') {
        return value
            .parse::<SocketAddrV4>()
            .map(|endpoint| (*endpoint.ip(), endpoint.port()))
            .map_err(|error| invalid(path, error.to_string()));
    }
    value
        .parse::<Ipv4Addr>()
        .map(|address| (address, DEFAULT_API_PORT))
        .map_err(|error| invalid(path, error.to_string()))
}

fn select_node(
    selected: Option<String>,
    nodes: &BTreeMap<NodeId, NodeDefinition>,
) -> Result<NodeId, CliError> {
    let selected = match selected {
        Some(selected) => NodeId::new(selected)
            .map_err(|error| invalid("node", format!("invalid node ID: {error}")))?,
        None if nodes.len() == 1 => nodes
            .keys()
            .next()
            .cloned()
            .ok_or_else(|| invalid("node", "could not select the only configured node"))?,
        None => {
            return Err(invalid(
                "node",
                "is required when multiple nodes are configured",
            ));
        }
    };
    if !nodes.contains_key(&selected) {
        return Err(invalid(
            "node",
            format!("`{selected}` is absent from cluster.nodes"),
        ));
    }
    Ok(selected)
}

fn preflight_error(error: ClusterPreflightError) -> CliError {
    let detail = error.to_string();
    let path = match &error {
        ClusterPreflightError::InvalidDnsLabel { .. } => "cluster.name".to_string(),
        ClusterPreflightError::InvalidClusterCidr { .. }
        | ClusterPreflightError::InsufficientClusterCapacity { .. } => {
            "cluster.cluster-cidr".to_string()
        }
        ClusterPreflightError::ZeroNodeLimit | ClusterPreflightError::NodeLimitExceeded { .. } => {
            "cluster.node-limit".to_string()
        }
        ClusterPreflightError::InvalidNodePrefix { .. } => "cluster.node-prefix".to_string(),
        ClusterPreflightError::InvalidNodeName { node_id } => {
            format!("cluster.nodes.{node_id}")
        }
        ClusterPreflightError::InvalidHostname { node_id, .. } => {
            format!("cluster.nodes.{node_id}.hostname")
        }
        ClusterPreflightError::NoNodes
        | ClusterPreflightError::MissingMaster
        | ClusterPreflightError::MultipleMasters
        | ClusterPreflightError::InvalidControlPlaneCount { .. }
        | ClusterPreflightError::DuplicateEndpoint { .. }
        | ClusterPreflightError::OverlappingWorkloadSubnets { .. }
        | ClusterPreflightError::EndpointInsideWorkloadSubnet { .. }
        | ClusterPreflightError::EndpointInsideClusterCidr { .. } => "cluster.nodes".to_string(),
        ClusterPreflightError::InvalidEndpointAddress { node_id, .. }
        | ClusterPreflightError::ZeroApiPort { node_id } => {
            format!("cluster.nodes.{node_id}.endpoint")
        }
        ClusterPreflightError::InvalidWorkloadSubnet { node_id, .. }
        | ClusterPreflightError::WorkloadSubnetOutsideCluster { node_id, .. }
        | ClusterPreflightError::WorkloadSubnetInsideTunnelRegion { node_id, .. } => {
            format!("cluster.nodes.{node_id}.subnet")
        }
        ClusterPreflightError::NonPrivateControlNetwork { index, .. }
        | ClusterPreflightError::ControlNetworkOverlapsCluster { index, .. } => {
            format!("cluster.control-allow-cidrs[{index}]")
        }
        ClusterPreflightError::EndpointOutsideControlNetworks { .. } => {
            "cluster.control-allow-cidrs".to_string()
        }
        ClusterPreflightError::WeakJoinSecret => "cluster.join-secret".to_string(),
        ClusterPreflightError::InvalidPorts(_) => "cluster.ports".to_string(),
        ClusterPreflightError::InvalidTailscale(error) => match error {
            TailscaleConfigError::WeakAuthKey | TailscaleConfigError::AuthKeyTooLong => {
                "tailscale.auth-key".to_string()
            }
            TailscaleConfigError::ZeroReplicas
            | TailscaleConfigError::InsufficientWorkloadNodes { .. } => {
                "tailscale.replicas".to_string()
            }
            TailscaleConfigError::EmptyAdvertiseRoutes
            | TailscaleConfigError::NoReachableDnsResolver => {
                "tailscale.advertise-routes".to_string()
            }
            TailscaleConfigError::RouteOutsideCluster { index, .. }
            | TailscaleConfigError::DuplicateRoute { index, .. } => {
                format!("tailscale.advertise-routes[{index}]")
            }
            TailscaleConfigError::NoTags => "tailscale.tags".to_string(),
            TailscaleConfigError::InvalidTag { index, .. }
            | TailscaleConfigError::DuplicateTag { index, .. } => {
                format!("tailscale.tags[{index}]")
            }
            TailscaleConfigError::LocalDnsRoute { route_index }
            | TailscaleConfigError::DuplicateDnsRoute { route_index, .. }
            | TailscaleConfigError::EmptyDnsNameservers { route_index } => {
                format!("tailscale.cross-cluster-dns[{route_index}]")
            }
            TailscaleConfigError::UnsafeDnsNameserver {
                route_index,
                nameserver_index,
                ..
            }
            | TailscaleConfigError::DuplicateDnsNameserver {
                route_index,
                nameserver_index,
                ..
            } => format!(
                "tailscale.cross-cluster-dns[{route_index}].nameservers[{nameserver_index}]"
            ),
        },
        ClusterPreflightError::InvalidCloudflare(error) => match error {
            CloudflareTunnelConfigError::InvalidToken
            | CloudflareTunnelConfigError::TokenTooLong => "cloudflare.tunnel.token".to_string(),
            CloudflareTunnelConfigError::ZeroReplicas
            | CloudflareTunnelConfigError::TooManyReplicas { .. } => {
                "cloudflare.tunnel.replicas".to_string()
            }
        },
    };
    invalid(&path, detail)
}

fn required(path: &str, value: String) -> Result<String, CliError> {
    let value = value.trim();
    if value.is_empty() {
        return Err(invalid(path, "must not be empty"));
    }
    Ok(value.to_string())
}

fn invalid(path: &str, detail: impl std::fmt::Display) -> CliError {
    CliError::invalid_input(format!("{path}: {detail}"))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct ClusterDocument {
    #[serde(rename = "$schema", default)]
    _schema: Option<String>,
    cluster: ClusterInput,
    #[serde(default)]
    node: Option<String>,
    #[serde(default)]
    tailscale: Option<TailscaleInput>,
    #[serde(default)]
    cloudflare: Option<CloudflareInput>,
    #[serde(default)]
    datadog: Option<DatadogInput>,
    #[serde(default)]
    depot: Option<DepotInput>,
    #[serde(default)]
    log_backup: Option<LogBackupInput>,
    #[serde(default)]
    preview: Option<PreviewInput>,
    #[serde(default)]
    nixos_upgrade: Option<NixosUpgradeInput>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct DatadogInput {
    api_key: String,
    site: String,
    #[serde(default = "default_true")]
    include_ingress_logs: bool,
    #[serde(default = "default_true")]
    include_tailscale_logs: bool,
    #[serde(default)]
    logs: DatadogLogsInput,
    #[serde(default)]
    metrics: DatadogMetricsInput,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct DatadogLogsInput {
    #[serde(default = "default_true")]
    include_healthcheck: bool,
}

impl Default for DatadogLogsInput {
    fn default() -> Self {
        Self {
            include_healthcheck: true,
        }
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct DatadogMetricsInput {
    #[serde(default)]
    enabled: bool,
    #[serde(default)]
    tags: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct DepotInput {
    token: String,
    #[serde(default = "default_depot_executable")]
    executable: PathBuf,
    #[serde(default = "default_depot_timeout_secs")]
    timeout_secs: u64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct LogBackupInput {
    bucket: String,
    kms_key_id: String,
    #[serde(default)]
    region: Option<String>,
    #[serde(default)]
    prefix: Option<String>,
    #[serde(default)]
    retention_days: Option<u64>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct PreviewInput {
    domain: String,
    github_token: String,
    max_concurrent_previews: usize,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct NixosUpgradeInput {
    flake: PathBuf,
    #[serde(default = "default_nixos_configuration")]
    configuration: String,
    #[serde(default = "default_manifest_relative_path")]
    manifest_relative_path: PathBuf,
    #[serde(default)]
    nix_binary: Option<PathBuf>,
    #[serde(default)]
    nixos_rebuild_binary: Option<PathBuf>,
    #[serde(default)]
    systemctl_binary: Option<PathBuf>,
}

const fn default_true() -> bool {
    true
}

fn default_depot_executable() -> PathBuf {
    PathBuf::from("depot")
}

const fn default_depot_timeout_secs() -> u64 {
    30 * 60
}

fn default_nixos_configuration() -> String {
    "default".to_owned()
}

fn default_manifest_relative_path() -> PathBuf {
    PathBuf::from("crates/apps/daemon/Cargo.toml")
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct TailscaleInput {
    auth_key: String,
    #[serde(default)]
    advertise_routes: Option<Vec<String>>,
    #[serde(default = "default_tailscale_replicas")]
    replicas: u32,
    #[serde(default = "default_tailscale_tags")]
    tags: Vec<String>,
    #[serde(default)]
    cross_cluster_dns: Vec<CrossClusterDnsInput>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct CloudflareInput {
    tunnel: CloudflareTunnelInput,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct CloudflareTunnelInput {
    token: String,
    #[serde(default = "default_cloudflare_tunnel_replicas")]
    replicas: u32,
}

const fn default_cloudflare_tunnel_replicas() -> u32 {
    DEFAULT_CLOUDFLARE_TUNNEL_REPLICAS
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
struct CrossClusterDnsInput {
    cluster_id: String,
    nameservers: Vec<String>,
}

const fn default_tailscale_replicas() -> u32 {
    2
}

fn default_tailscale_tags() -> Vec<String> {
    vec!["tag:maestro-gateway".to_owned()]
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct ClusterInput {
    #[serde(default)]
    cluster_id: Option<String>,
    name: String,
    cluster_cidr: String,
    #[serde(default = "default_node_limit")]
    node_limit: u32,
    #[serde(default = "default_node_prefix")]
    node_prefix: u8,
    #[serde(default)]
    nodes: BTreeMap<String, NodeInput>,
    #[serde(default)]
    control_allow_cidrs: Vec<String>,
    #[serde(default)]
    ports: PortsInput,
    #[serde(default)]
    join_secret: Option<String>,
}

const fn default_node_limit() -> u32 {
    254
}

const fn default_node_prefix() -> u8 {
    24
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct NodeInput {
    endpoint: String,
    subnet: String,
    #[serde(default)]
    hostname: Option<String>,
    #[serde(default)]
    role: NodeRoleInput,
}

#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum NodeRoleInput {
    Master,
    #[default]
    Hybrid,
    ControlPlane,
    Worker,
}

impl From<NodeRoleInput> for NodeRole {
    fn from(value: NodeRoleInput) -> Self {
        match value {
            NodeRoleInput::Master => Self::Master,
            NodeRoleInput::Hybrid => Self::Hybrid,
            NodeRoleInput::ControlPlane => Self::ControlPlane,
            NodeRoleInput::Worker => Self::Worker,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct PortsInput {
    #[serde(default = "default_gateway")]
    gateway: u16,
    #[serde(default = "default_store_client")]
    store_client: u16,
    #[serde(default = "default_store_peer")]
    store_peer: u16,
    #[serde(default = "default_wireguard")]
    wireguard: u16,
}

impl Default for PortsInput {
    fn default() -> Self {
        Self {
            gateway: default_gateway(),
            store_client: default_store_client(),
            store_peer: default_store_peer(),
            wireguard: default_wireguard(),
        }
    }
}

fn default_gateway() -> u16 {
    DEFAULT_GATEWAY_PORT
}

fn default_store_client() -> u16 {
    DEFAULT_STORE_CLIENT_PORT
}

fn default_store_peer() -> u16 {
    DEFAULT_STORE_PEER_PORT
}

fn default_wireguard() -> u16 {
    DEFAULT_WIREGUARD_PORT
}
