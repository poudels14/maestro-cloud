use std::collections::{BTreeMap, BTreeSet};
use std::net::Ipv4Addr;

use kernel_api::{ClusterId, NodeId, NodeRole, SecretValue};
use serde::{Deserialize, Serialize};

use crate::{ClusterPorts, ClusterPortsError, Ipv4Cidr};

/// WireGuard MTU applied consistently to the mesh and workload interfaces.
pub const WIREGUARD_MTU_BYTES: u16 = 1_420;

/// A node address reachable by the other cluster members.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NodeEndpoint {
    /// Stable private host address used for cluster control traffic.
    pub host_address: Ipv4Addr,
    /// Port of the node's public control API.
    pub api_port: u16,
}

/// Operator-facing definition of one cluster member.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NodeDefinition {
    /// Hostname configured on the node.
    pub hostname: String,
    /// Control-plane address advertised to cluster peers.
    pub endpoint: NodeEndpoint,
    /// Private `/24` allocated exclusively to workloads on this node.
    pub workload_subnet: Ipv4Cidr,
    /// Public scheduling and control-plane capability.
    pub role: NodeRole,
}

/// Persisted input required to form or join a cluster.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ClusterConfig {
    /// Stable cluster identity.
    pub cluster_id: ClusterId,
    /// Lowercase DNS label used in certificates and discovery.
    pub name: String,
    /// Desired members keyed by stable node identity.
    pub nodes: BTreeMap<NodeId, NodeDefinition>,
    /// Optional private networks allowed to initiate control traffic.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub control_allow_cidrs: Vec<Ipv4Cidr>,
    /// Cluster-wide service ports fixed during initialization.
    pub ports: ClusterPorts,
    /// Shared bootstrap credential; replaced by node certificates after join.
    pub join_secret: SecretValue,
}

impl ClusterConfig {
    /// Performs deterministic validation before any local state is changed.
    pub fn preflight(&self) -> Result<ValidatedTopology, ClusterPreflightError> {
        validate_dns_label("cluster name", &self.name)?;
        self.ports.validate()?;
        validate_join_secret(&self.join_secret)?;

        if self.nodes.is_empty() {
            return Err(ClusterPreflightError::NoNodes);
        }

        let mut master = None;
        let mut control_plane_nodes = Vec::new();
        let mut endpoints = BTreeSet::new();
        let mut subnets: Vec<(&NodeId, Ipv4Cidr)> = Vec::new();

        for (node_id, node) in &self.nodes {
            validate_dns_label("node ID", node_id.as_str()).map_err(|_| {
                ClusterPreflightError::InvalidNodeName {
                    node_id: node_id.clone(),
                }
            })?;
            validate_hostname(node_id, &node.hostname)?;

            if node.role == NodeRole::Master && master.replace(node_id.clone()).is_some() {
                return Err(ClusterPreflightError::MultipleMasters);
            }
            if node.role.is_control_plane() {
                control_plane_nodes.push(node_id.clone());
            }

            validate_endpoint(node_id, node.endpoint)?;
            self.ports.validate_api_port(node.endpoint.api_port)?;
            if !endpoints.insert((node.endpoint.host_address, node.endpoint.api_port)) {
                return Err(ClusterPreflightError::DuplicateEndpoint {
                    address: node.endpoint.host_address,
                    port: node.endpoint.api_port,
                });
            }

            validate_workload_subnet(node_id, node.workload_subnet)?;
            if let Some((other_node, _)) = subnets
                .iter()
                .find(|(_, subnet)| subnet.overlaps(node.workload_subnet))
            {
                return Err(ClusterPreflightError::OverlappingWorkloadSubnets {
                    first: (*other_node).clone(),
                    second: node_id.clone(),
                });
            }
            subnets.push((node_id, node.workload_subnet));
        }

        let master = master.ok_or(ClusterPreflightError::MissingMaster)?;
        if !matches!(control_plane_nodes.len(), 1 | 3) {
            return Err(ClusterPreflightError::InvalidControlPlaneCount {
                count: control_plane_nodes.len(),
            });
        }

        for (subnet_node, subnet) in &subnets {
            for (endpoint_node, endpoint) in &self.nodes {
                if subnet.contains(endpoint.endpoint.host_address) {
                    return Err(ClusterPreflightError::EndpointInsideWorkloadSubnet {
                        subnet_node: (*subnet_node).clone(),
                        endpoint_node: endpoint_node.clone(),
                        address: endpoint.endpoint.host_address,
                    });
                }
            }
        }

        self.validate_control_allowlist(&subnets)?;
        control_plane_nodes.sort();

        Ok(ValidatedTopology {
            master,
            control_plane_nodes,
        })
    }

    fn validate_control_allowlist(
        &self,
        subnets: &[(&NodeId, Ipv4Cidr)],
    ) -> Result<(), ClusterPreflightError> {
        for (index, control) in self.control_allow_cidrs.iter().copied().enumerate() {
            if !control.is_private() {
                return Err(ClusterPreflightError::NonPrivateControlNetwork {
                    index,
                    network: control,
                });
            }
            if let Some((node_id, _)) = subnets.iter().find(|(_, subnet)| subnet.overlaps(control))
            {
                return Err(ClusterPreflightError::ControlNetworkOverlapsWorkload {
                    index,
                    node_id: (*node_id).clone(),
                });
            }
        }

        if self.control_allow_cidrs.is_empty() {
            return Ok(());
        }

        for (node_id, node) in &self.nodes {
            if !self
                .control_allow_cidrs
                .iter()
                .any(|network| network.contains(node.endpoint.host_address))
            {
                return Err(ClusterPreflightError::EndpointOutsideControlNetworks {
                    node_id: node_id.clone(),
                    address: node.endpoint.host_address,
                });
            }
        }
        Ok(())
    }
}

/// Stable facts derived by successful cluster preflight.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatedTopology {
    master: NodeId,
    control_plane_nodes: Vec<NodeId>,
}

impl ValidatedTopology {
    /// Returns the one node responsible for initial cluster formation.
    pub fn master(&self) -> &NodeId {
        &self.master
    }

    /// Returns the one or three nodes eligible to run cluster controllers.
    pub fn control_plane_nodes(&self) -> &[NodeId] {
        &self.control_plane_nodes
    }
}

/// Why a topology cannot safely form a cluster.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ClusterPreflightError {
    /// Cluster names must be safe in DNS and certificates.
    #[error("{field} `{value}` must be a lowercase DNS label")]
    InvalidDnsLabel {
        /// Operator-facing field name.
        field: &'static str,
        /// Rejected value.
        value: String,
    },
    /// Node identifiers have a narrower topology constraint than resource IDs.
    #[error("node ID `{node_id}` must be a lowercase DNS label")]
    InvalidNodeName { node_id: NodeId },
    /// Hostnames may contain several DNS labels.
    #[error("hostname `{hostname}` for node `{node_id}` is not a lowercase DNS hostname")]
    InvalidHostname { node_id: NodeId, hostname: String },
    /// At least the initializing node must be declared.
    #[error("cluster topology must contain at least one node")]
    NoNodes,
    /// One master initializes the cluster trust root.
    #[error("cluster topology must contain one master node")]
    MissingMaster,
    /// More than one trust initializer is ambiguous.
    #[error("cluster topology cannot contain more than one master node")]
    MultipleMasters,
    /// Control-plane quorum is intentionally limited to supported shapes.
    #[error(
        "cluster topology must contain exactly one or three control-plane nodes, found {count}"
    )]
    InvalidControlPlaneCount { count: usize },
    /// Control endpoints must use private, routable IPv4 addresses.
    #[error("node `{node_id}` endpoint `{address}` must be private and non-loopback")]
    InvalidEndpointAddress { node_id: NodeId, address: Ipv4Addr },
    /// Port zero would select a different ephemeral port on each start.
    #[error("node `{node_id}` API port must be non-zero")]
    ZeroApiPort { node_id: NodeId },
    /// An API endpoint must unambiguously identify one node.
    #[error("endpoint `{address}:{port}` is assigned to more than one node")]
    DuplicateEndpoint { address: Ipv4Addr, port: u16 },
    /// Node workload allocations use a fixed prefix for deterministic IPAM.
    #[error("node `{node_id}` workload network `{network}` must be a private IPv4 /24")]
    InvalidWorkloadSubnet { node_id: NodeId, network: Ipv4Cidr },
    /// Per-node workload address spaces cannot collide.
    #[error("workload networks for nodes `{first}` and `{second}` overlap")]
    OverlappingWorkloadSubnets { first: NodeId, second: NodeId },
    /// Host control traffic cannot traverse a workload address space.
    #[error(
        "node `{endpoint_node}` endpoint `{address}` is inside node `{subnet_node}` workload network"
    )]
    EndpointInsideWorkloadSubnet {
        subnet_node: NodeId,
        endpoint_node: NodeId,
        address: Ipv4Addr,
    },
    /// Control allowlists are restricted to private address space.
    #[error("control network {index} `{network}` must be private IPv4 space")]
    NonPrivateControlNetwork { index: usize, network: Ipv4Cidr },
    /// Control and workload traffic use disjoint address spaces.
    #[error("control network {index} overlaps the workload network for node `{node_id}`")]
    ControlNetworkOverlapsWorkload { index: usize, node_id: NodeId },
    /// A non-empty allowlist must admit all declared members.
    #[error("node `{node_id}` endpoint `{address}` is absent from the control allowlist")]
    EndpointOutsideControlNetworks { node_id: NodeId, address: Ipv4Addr },
    /// Bootstrap credentials need sufficient entropy before certificate issue.
    #[error("cluster join secret must contain at least 32 characters")]
    WeakJoinSecret,
    /// Persisted port allocation is invalid.
    #[error(transparent)]
    InvalidPorts(#[from] ClusterPortsError),
}

fn validate_join_secret(secret: &SecretValue) -> Result<(), ClusterPreflightError> {
    if secret.expose().chars().count() < 32 {
        return Err(ClusterPreflightError::WeakJoinSecret);
    }
    Ok(())
}

fn validate_endpoint(
    node_id: &NodeId,
    endpoint: NodeEndpoint,
) -> Result<(), ClusterPreflightError> {
    let address = endpoint.host_address;
    if !address.is_private() || address.is_loopback() || address.is_unspecified() {
        return Err(ClusterPreflightError::InvalidEndpointAddress {
            node_id: node_id.clone(),
            address,
        });
    }
    if endpoint.api_port == 0 {
        return Err(ClusterPreflightError::ZeroApiPort {
            node_id: node_id.clone(),
        });
    }
    Ok(())
}

fn validate_workload_subnet(
    node_id: &NodeId,
    network: Ipv4Cidr,
) -> Result<(), ClusterPreflightError> {
    if network.prefix() != 24 || !network.is_private() {
        return Err(ClusterPreflightError::InvalidWorkloadSubnet {
            node_id: node_id.clone(),
            network,
        });
    }
    Ok(())
}

fn validate_hostname(node_id: &NodeId, hostname: &str) -> Result<(), ClusterPreflightError> {
    let valid =
        !hostname.is_empty() && hostname.len() <= 253 && hostname.split('.').all(is_dns_label);
    if !valid {
        return Err(ClusterPreflightError::InvalidHostname {
            node_id: node_id.clone(),
            hostname: hostname.to_owned(),
        });
    }
    Ok(())
}

fn validate_dns_label(field: &'static str, value: &str) -> Result<(), ClusterPreflightError> {
    if !is_dns_label(value) {
        return Err(ClusterPreflightError::InvalidDnsLabel {
            field,
            value: value.to_owned(),
        });
    }
    Ok(())
}

fn is_dns_label(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 63
        && value.chars().all(|character| {
            character.is_ascii_lowercase() || character.is_ascii_digit() || character == '-'
        })
        && value
            .chars()
            .next()
            .is_some_and(|character| character.is_ascii_lowercase() || character.is_ascii_digit())
        && value
            .chars()
            .next_back()
            .is_some_and(|character| character.is_ascii_lowercase() || character.is_ascii_digit())
}
