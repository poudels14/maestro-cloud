use std::collections::BTreeSet;

use kernel_api::{DnsLabel, DnsName, NodeId, NodeRole, SecretValue};

use crate::Ipv4Cidr;

use super::{ClusterConfig, ClusterPreflightError, NodeEndpoint, ValidatedTopology};

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

            validate_workload_subnet(node_id, node.workload_subnet, self.nodes.len() == 1)?;
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
        if let Some(tailscale) = &self.tailscale {
            let workload_subnets = self
                .nodes
                .values()
                .filter(|node| node.role.runs_workloads())
                .map(|node| node.workload_subnet)
                .collect::<Vec<_>>();
            tailscale.validate(&self.cluster_id, &workload_subnets)?;
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
    one_node_topology: bool,
) -> Result<(), ClusterPreflightError> {
    let valid_prefix =
        network.prefix() == 24 || one_node_topology && (16..24).contains(&network.prefix());
    if !valid_prefix || !network.is_private() {
        return Err(ClusterPreflightError::InvalidWorkloadSubnet {
            node_id: node_id.clone(),
            network,
        });
    }
    Ok(())
}

fn validate_hostname(node_id: &NodeId, hostname: &str) -> Result<(), ClusterPreflightError> {
    if DnsName::parse(hostname).is_err() {
        return Err(ClusterPreflightError::InvalidHostname {
            node_id: node_id.clone(),
            hostname: hostname.to_owned(),
        });
    }
    Ok(())
}

fn validate_dns_label(field: &'static str, value: &str) -> Result<(), ClusterPreflightError> {
    if DnsLabel::parse(value).is_err() {
        return Err(ClusterPreflightError::InvalidDnsLabel {
            field,
            value: value.to_owned(),
        });
    }
    Ok(())
}
