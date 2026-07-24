use std::collections::BTreeMap;
use std::net::Ipv4Addr;

use kernel_api::{ClusterId, NodeId, NodeRole, SecretValue};

use crate::{
    CloudflareTunnelConfig, CloudflareTunnelConfigError, ClusterConfig, ClusterPorts,
    ClusterPreflightError, CrossClusterDnsRoute, DEFAULT_WIREGUARD_PORT, Ipv4Cidr, NodeDefinition,
    NodeEndpoint, TailscaleAuthKeyRecord, TailscaleConfigError, TailscaleGatewayConfig,
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
        Err(ClusterPreflightError::ControlNetworkOverlapsCluster { .. })
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

#[test]
fn rejects_workload_pins_outside_the_container_pool() -> Result<(), Box<dyn std::error::Error>> {
    let mut tunnel = valid_config()?;
    tunnel
        .nodes
        .get_mut(&NodeId::new("node-1")?)
        .ok_or("missing node")?
        .workload_subnet = "172.22.0.0/24".parse()?;
    assert!(matches!(
        tunnel.preflight(),
        Err(ClusterPreflightError::WorkloadSubnetInsideTunnelRegion { .. })
    ));

    let mut outside = valid_config()?;
    outside
        .nodes
        .get_mut(&NodeId::new("node-1")?)
        .ok_or("missing node")?
        .workload_subnet = "172.23.1.0/24".parse()?;
    assert!(matches!(
        outside.preflight(),
        Err(ClusterPreflightError::WorkloadSubnetOutsideCluster { .. })
    ));
    Ok(())
}

#[test]
fn validates_a_wide_node_pool_and_rejects_insufficient_capacity()
-> Result<(), Box<dyn std::error::Error>> {
    let mut config = valid_config()?;
    config.cluster_cidr = "10.0.0.0/12".parse()?;
    config.node_limit = 1_000;
    config.node_prefix = 22;
    config.control_allow_cidrs = vec!["192.168.50.0/24".parse()?];
    for (index, node) in config.nodes.values_mut().enumerate() {
        let block = u8::try_from((index + 1) * 4)?;
        node.endpoint.host_address = Ipv4Addr::new(192, 168, 50, 11 + u8::try_from(index)?);
        node.workload_subnet = format!("10.0.{block}.0/22").parse()?;
    }
    config.preflight()?;

    config.cluster_cidr = "10.0.0.0/16".parse()?;
    assert_eq!(
        config.preflight(),
        Err(ClusterPreflightError::InsufficientClusterCapacity {
            network: "10.0.0.0/16".parse()?,
            node_limit: 1_000,
            node_prefix: 22,
        })
    );
    Ok(())
}

#[test]
fn validates_tailscale_routes_replicas_tags_and_secret_strength()
-> Result<(), Box<dyn std::error::Error>> {
    let mut config = valid_config()?;
    config.tailscale = Some(TailscaleGatewayConfig {
        auth_key: SecretValue::new("tskey-auth-reusable-test-secret"),
        advertise_routes: None,
        replicas: 2,
        tags: vec!["tag:maestro-gateway".to_owned()],
        cross_cluster_dns: Vec::new(),
    });
    config.preflight()?;

    let record = TailscaleAuthKeyRecord::new(SecretValue::new(
        "  tskey-auth-normalized-reusable-secret  ",
    ))?;
    assert_eq!(
        record.auth_key.expose(),
        "tskey-auth-normalized-reusable-secret"
    );
    assert_eq!(
        TailscaleAuthKeyRecord::new(SecretValue::new("x".repeat(513))),
        Err(TailscaleConfigError::AuthKeyTooLong)
    );

    let tailscale = config.tailscale.as_mut().ok_or("tailscale missing")?;
    tailscale.advertise_routes = Some(vec!["192.168.50.0/24".parse()?]);
    assert!(matches!(
        config.preflight(),
        Err(ClusterPreflightError::InvalidTailscale(
            TailscaleConfigError::RouteOutsideCluster { index: 0, .. }
        ))
    ));

    let tailscale = config.tailscale.as_mut().ok_or("tailscale missing")?;
    tailscale.advertise_routes = Some(vec!["172.22.250.0/24".parse()?]);
    assert_eq!(
        config.preflight(),
        Err(ClusterPreflightError::InvalidTailscale(
            TailscaleConfigError::NoReachableDnsResolver
        ))
    );

    let tailscale = config.tailscale.as_mut().ok_or("tailscale missing")?;
    tailscale.advertise_routes = None;
    tailscale.replicas = 3;
    assert_eq!(
        config.preflight(),
        Err(ClusterPreflightError::InvalidTailscale(
            TailscaleConfigError::InsufficientWorkloadNodes {
                replicas: 3,
                workload_nodes: 2,
            }
        ))
    );

    let tailscale = config.tailscale.as_mut().ok_or("tailscale missing")?;
    tailscale.replicas = 2;
    tailscale.tags = vec!["maestro-gateway".to_owned()];
    assert!(matches!(
        config.preflight(),
        Err(ClusterPreflightError::InvalidTailscale(
            TailscaleConfigError::InvalidTag { index: 0, .. }
        ))
    ));

    let tailscale = config.tailscale.as_mut().ok_or("tailscale missing")?;
    tailscale.tags = vec!["tag:maestro-gateway".to_owned()];
    tailscale.cross_cluster_dns = vec![CrossClusterDnsRoute {
        cluster_id: ClusterId::new("remote")?,
        nameservers: vec![Ipv4Addr::new(172, 23, 1, 1)],
    }];
    config.preflight()?;

    let tailscale = config.tailscale.as_mut().ok_or("tailscale missing")?;
    tailscale
        .cross_cluster_dns
        .first_mut()
        .ok_or("cross-cluster DNS route missing")?
        .nameservers = vec![Ipv4Addr::new(172, 22, 1, 1)];
    assert!(matches!(
        config.preflight(),
        Err(ClusterPreflightError::InvalidTailscale(
            TailscaleConfigError::UnsafeDnsNameserver {
                route_index: 0,
                nameserver_index: 0,
                ..
            }
        ))
    ));
    Ok(())
}

#[test]
fn validates_cloudflare_tunnel_token_and_replica_bounds() -> Result<(), Box<dyn std::error::Error>>
{
    let mut config = valid_config()?;
    config.cloudflare = Some(CloudflareTunnelConfig {
        token: SecretValue::new("test-cloudflare-tunnel-token"),
        replicas: 2,
    });
    config.preflight()?;

    config
        .cloudflare
        .as_mut()
        .ok_or("Cloudflare config missing")?
        .replicas = 0;
    assert_eq!(
        config.preflight(),
        Err(ClusterPreflightError::InvalidCloudflare(
            CloudflareTunnelConfigError::ZeroReplicas
        ))
    );

    let cloudflare = config
        .cloudflare
        .as_mut()
        .ok_or("Cloudflare config missing")?;
    cloudflare.replicas = 2;
    cloudflare.token = SecretValue::new(" ");
    assert_eq!(
        config.preflight(),
        Err(ClusterPreflightError::InvalidCloudflare(
            CloudflareTunnelConfigError::InvalidToken
        ))
    );
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
        cluster_cidr: "172.22.0.0/16".parse()?,
        node_limit: 254,
        node_prefix: 24,
        nodes,
        control_allow_cidrs: vec!["10.20.0.0/24".parse()?],
        ports: ClusterPorts::new(3_001, 23_79, 23_80, DEFAULT_WIREGUARD_PORT)?,
        join_secret: SecretValue::new("a-test-join-secret-with-at-least-32-characters"),
        tailscale: None,
        cloudflare: None,
    })
}
