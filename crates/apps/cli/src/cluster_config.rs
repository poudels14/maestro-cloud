use std::collections::BTreeMap;
use std::net::{Ipv4Addr, SocketAddrV4};

use cluster::{
    ClusterConfig, ClusterPorts, ClusterPreflightError, DEFAULT_WIREGUARD_PORT, Ipv4Cidr,
    NodeDefinition, NodeEndpoint,
};
use kernel_api::{ClusterId, NodeId, NodeRole, SecretValue};
use serde::Deserialize;
use serde_json::Value;

use crate::CliError;
use crate::config_source::decode_document;

const DEFAULT_API_PORT: u16 = 3_000;
const DEFAULT_GATEWAY_PORT: u16 = 3_001;
const DEFAULT_STORE_CLIENT_PORT: u16 = 2_379;
const DEFAULT_STORE_PEER_PORT: u16 = 2_380;

#[derive(Debug)]
pub(crate) struct LoadedClusterConfig {
    pub(crate) cluster: ClusterConfig,
    pub(crate) node_id: NodeId,
    pub(crate) ignored_fields: Vec<String>,
}

pub(crate) fn decode_cluster(source: &str, value: Value) -> Result<LoadedClusterConfig, CliError> {
    let (document, ignored_fields): (ClusterDocument, _) =
        decode_document(&value, &format!("cluster config `{source}`"))?;
    let cluster = convert_cluster(document.cluster)?;
    let node_id = select_node(document.node, &cluster.nodes)?;
    cluster.preflight().map_err(preflight_error)?;
    Ok(LoadedClusterConfig {
        cluster,
        node_id,
        ignored_fields,
    })
}

fn convert_cluster(input: ClusterInput) -> Result<ClusterConfig, CliError> {
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
    })
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
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct ClusterInput {
    #[serde(default, alias = "clusterId")]
    cluster_id: Option<String>,
    name: String,
    #[serde(alias = "clusterCidr")]
    cluster_cidr: String,
    #[serde(default = "default_node_limit", alias = "nodeLimit")]
    node_limit: u32,
    #[serde(default = "default_node_prefix", alias = "nodePrefix")]
    node_prefix: u8,
    #[serde(default)]
    nodes: BTreeMap<String, NodeInput>,
    #[serde(default, alias = "controlAllowCidrs")]
    control_allow_cidrs: Vec<String>,
    #[serde(default)]
    ports: PortsInput,
    #[serde(default, alias = "joinSecret")]
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
    #[serde(alias = "workloadSubnet")]
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
    #[serde(alias = "controlPlane", alias = "voter")]
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
    #[serde(default = "default_store_client", alias = "storeClient")]
    store_client: u16,
    #[serde(default = "default_store_peer", alias = "storePeer")]
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
