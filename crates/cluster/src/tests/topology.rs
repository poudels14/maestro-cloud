use std::collections::BTreeMap;
use std::net::Ipv4Addr;

use kernel_api::{ClusterId, NodeId, NodeRole, SecretValue};

use crate::{
    ClusterConfig, ClusterPorts, ClusterPreflightError, DEFAULT_WIREGUARD_PORT, Ipv4Cidr,
    NodeDefinition, NodeEndpoint,
};

#[test]
fn validates_a_three_node_control_plane() -> Result<(), Box<dyn std::error::Error>> {
    let config = valid_config()?;
    let topology = config.preflight()?;

    assert_eq!(topology.master().as_str(), "node-1");
    assert_eq!(topology.control_plane_nodes().len(), 3);
    Ok(())
}

#[test]
fn rejects_unsupported_control_plane_shapes() -> Result<(), Box<dyn std::error::Error>> {
    let mut config = valid_config()?;
    let node = config.nodes.get_mut(&NodeId::new("node-3")?);
    if let Some(node) = node {
        node.role = NodeRole::Worker;
    }

    assert_eq!(
        config.preflight(),
        Err(ClusterPreflightError::InvalidControlPlaneCount { count: 2 })
    );
    Ok(())
}

#[test]
fn rejects_overlapping_workload_and_control_networks() -> Result<(), Box<dyn std::error::Error>> {
    let mut workloads = valid_config()?;
    let node = workloads.nodes.get_mut(&NodeId::new("node-2")?);
    if let Some(node) = node {
        node.workload_subnet = "172.22.1.0/24".parse()?;
    }
    assert!(matches!(
        workloads.preflight(),
        Err(ClusterPreflightError::OverlappingWorkloadSubnets { .. })
    ));

    let mut controls = valid_config()?;
    controls.control_allow_cidrs = vec!["172.22.0.0/16".parse()?];
    assert!(matches!(
        controls.preflight(),
        Err(ClusterPreflightError::ControlNetworkOverlapsWorkload { .. })
    ));
    Ok(())
}

#[test]
fn a_control_allowlist_must_include_every_endpoint() -> Result<(), Box<dyn std::error::Error>> {
    let mut config = valid_config()?;
    config.control_allow_cidrs = vec!["10.20.0.11/32".parse()?];

    assert!(matches!(
        config.preflight(),
        Err(ClusterPreflightError::EndpointOutsideControlNetworks { .. })
    ));
    Ok(())
}

fn valid_config() -> Result<ClusterConfig, Box<dyn std::error::Error>> {
    let nodes = [
        (
            "node-1",
            Ipv4Addr::new(10, 20, 0, 11),
            "172.22.1.0/24",
            NodeRole::Master,
        ),
        (
            "node-2",
            Ipv4Addr::new(10, 20, 0, 12),
            "172.22.2.0/24",
            NodeRole::Hybrid,
        ),
        (
            "node-3",
            Ipv4Addr::new(10, 20, 0, 13),
            "172.22.3.0/24",
            NodeRole::ControlPlane,
        ),
    ]
    .into_iter()
    .map(|(id, address, subnet, role)| {
        Ok((
            NodeId::new(id)?,
            NodeDefinition {
                hostname: format!("{id}.internal"),
                endpoint: NodeEndpoint {
                    host_address: address,
                    api_port: 3_000,
                },
                workload_subnet: subnet.parse::<Ipv4Cidr>()?,
                role,
            },
        ))
    })
    .collect::<Result<BTreeMap<_, _>, Box<dyn std::error::Error>>>()?;

    Ok(ClusterConfig {
        cluster_id: ClusterId::new("test-cluster")?,
        name: "test-cluster".to_owned(),
        nodes,
        control_allow_cidrs: vec!["10.20.0.0/24".parse()?],
        ports: ClusterPorts::new(3_001, 23_79, 23_80, DEFAULT_WIREGUARD_PORT)?,
        join_secret: SecretValue::new("a-test-join-secret-with-at-least-32-characters"),
    })
}
