use std::collections::BTreeMap;
use std::net::{Ipv4Addr, SocketAddrV4};

use cluster::{
    CloudflareTunnelConfig, CloudflareTunnelConfigError, ClusterConfig, ClusterLaunchPolicy,
    ClusterPorts, ClusterPreflightError, CrossClusterDnsRoute, Ipv4Cidr, NodeDefinition,
    NodeEndpoint, TailscaleConfigError, TailscaleGatewayConfig,
};
use kernel_api::{ClusterId, NodeId, SecretValue};
use serde_json::Value;

use crate::CliError;
use crate::cluster_config_document::{
    CloudflareInput, ClusterDocument, ClusterInput, NodeInput, TailscaleInput,
};
use crate::cluster_config_launch::convert_launch_policy;
use crate::config_source::{ConfigSourceReader, decode_document, resolve_relative_source};

const DEFAULT_API_PORT: u16 = 3_000;

#[derive(Debug)]
pub(crate) struct LoadedClusterConfig {
    pub(crate) cluster: ClusterConfig,
    pub(crate) launch_policy: ClusterLaunchPolicy,
    pub(crate) node_id: NodeId,
    pub(crate) jwt_secret_key: SecretValue,
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
    let jwt_secret_key = convert_jwt_secret_key(document.jwt_secret_key)?;
    let tailscale = convert_tailscale(source, document.tailscale, reader).await?;
    let cloudflare = convert_cloudflare(source, document.cloudflare, reader).await?;
    let cluster = convert_cluster(document.cluster, tailscale, cloudflare)?;
    let node_id = select_node(document.node, &cluster.nodes)?;
    cluster.preflight().map_err(preflight_error)?;
    Ok(LoadedClusterConfig {
        cluster,
        launch_policy,
        node_id,
        jwt_secret_key,
        ignored_fields,
    })
}

fn convert_jwt_secret_key(value: String) -> Result<SecretValue, CliError> {
    let value = required("jwt-secret-key", value)?;
    if value.len() < 32 || value.contains('\0') {
        return Err(invalid(
            "jwt-secret-key",
            "must contain at least 32 bytes and no NUL bytes",
        ));
    }
    Ok(SecretValue::new(value))
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

pub(crate) fn required(path: &str, value: String) -> Result<String, CliError> {
    let value = value.trim();
    if value.is_empty() {
        return Err(invalid(path, "must not be empty"));
    }
    Ok(value.to_string())
}

pub(crate) fn invalid(path: &str, detail: impl std::fmt::Display) -> CliError {
    CliError::invalid_input(format!("{path}: {detail}"))
}
