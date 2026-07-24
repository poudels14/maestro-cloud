use std::collections::BTreeSet;

use kernel_api::{NodeId, NodeRole, SecretValue};

use crate::Ipv4Cidr;

use super::{ClusterConfig, ClusterPreflightError, NodeEndpoint, ValidatedTopology};

impl ClusterConfig {
    /// Performs deterministic validation before any local state is changed.
    pub fn preflight(&self) -> Result<ValidatedTopology, ClusterPreflightError> {
        validate_dns_label("cluster name", &self.name)?;
        self.ports.validate()?;
        validate_join_secret(&self.join_secret)?;
        let address_pool = validate_address_pool(self)?;

        if self.nodes.is_empty() {
            return Err(ClusterPreflightError::NoNodes);
        }
        if self.nodes.len() > self.node_limit as usize {
            return Err(ClusterPreflightError::NodeLimitExceeded {
                count: self.nodes.len(),
                limit: self.node_limit,
            });
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

            validate_workload_subnet(
                node_id,
                node.workload_subnet,
                self.node_prefix,
                self.cluster_cidr,
                address_pool.container_start,
            )?;
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
        for (node_id, node) in &self.nodes {
            if self.cluster_cidr.contains(node.endpoint.host_address) {
                return Err(ClusterPreflightError::EndpointInsideClusterCidr {
                    node_id: node_id.clone(),
                    address: node.endpoint.host_address,
                    network: self.cluster_cidr,
                });
            }
        }

        self.validate_control_allowlist()?;
        if let Some(tailscale) = &self.tailscale {
            let workload_subnets = self
                .nodes
                .values()
                .filter(|node| node.role.runs_workloads())
                .map(|node| node.workload_subnet)
                .collect::<Vec<_>>();
            tailscale.validate(&self.cluster_id, self.cluster_cidr, &workload_subnets)?;
        }
        if let Some(cloudflare) = &self.cloudflare {
            cloudflare.validate()?;
        }
        control_plane_nodes.sort();

        Ok(ValidatedTopology {
            master,
            control_plane_nodes,
        })
    }

    fn validate_control_allowlist(&self) -> Result<(), ClusterPreflightError> {
        for (index, control) in self.control_allow_cidrs.iter().copied().enumerate() {
            if !control.is_private() {
                return Err(ClusterPreflightError::NonPrivateControlNetwork {
                    index,
                    network: control,
                });
            }
            if self.cluster_cidr.overlaps(control) {
                return Err(ClusterPreflightError::ControlNetworkOverlapsCluster {
                    index,
                    network: control,
                    cluster_cidr: self.cluster_cidr,
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
    expected_prefix: u8,
    cluster_cidr: Ipv4Cidr,
    container_start: u64,
) -> Result<(), ClusterPreflightError> {
    if network.prefix() != expected_prefix || !network.is_private() {
        return Err(ClusterPreflightError::InvalidWorkloadSubnet {
            node_id: node_id.clone(),
            network,
            expected_prefix,
        });
    }
    if !cluster_cidr.contains_network(network) {
        return Err(ClusterPreflightError::WorkloadSubnetOutsideCluster {
            node_id: node_id.clone(),
            network,
            cluster_cidr,
        });
    }
    if u64::from(u32::from(network.network_address())) < container_start {
        return Err(ClusterPreflightError::WorkloadSubnetInsideTunnelRegion {
            node_id: node_id.clone(),
            network,
        });
    }
    Ok(())
}

#[derive(Debug, Clone, Copy)]
struct AddressPool {
    container_start: u64,
}

fn validate_address_pool(config: &ClusterConfig) -> Result<AddressPool, ClusterPreflightError> {
    if !config.cluster_cidr.is_private() {
        return Err(ClusterPreflightError::InvalidClusterCidr {
            network: config.cluster_cidr,
        });
    }
    if config.node_limit == 0 {
        return Err(ClusterPreflightError::ZeroNodeLimit);
    }
    if config.node_prefix <= config.cluster_cidr.prefix() || config.node_prefix > 24 {
        return Err(ClusterPreflightError::InvalidNodePrefix {
            node_prefix: config.node_prefix,
            cluster_cidr: config.cluster_cidr,
        });
    }

    let subnet_size = 1_u64 << (32 - config.node_prefix);
    let tunnel_blocks = u64::from(config.node_limit).div_ceil(254);
    let tunnel_size = tunnel_blocks.saturating_mul(256);
    let container_offset = tunnel_size
        .div_ceil(subnet_size)
        .saturating_mul(subnet_size);
    let required =
        container_offset.saturating_add(u64::from(config.node_limit).saturating_mul(subnet_size));
    if required > config.cluster_cidr.address_count() {
        return Err(ClusterPreflightError::InsufficientClusterCapacity {
            network: config.cluster_cidr,
            node_limit: config.node_limit,
            node_prefix: config.node_prefix,
        });
    }
    Ok(AddressPool {
        container_start: u64::from(u32::from(config.cluster_cidr.network_address()))
            + container_offset,
    })
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
