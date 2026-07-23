use std::collections::BTreeMap;
use std::net::Ipv4Addr;

use cluster::{
    ClusterConfig, ClusterPorts, Ipv4Cidr, NodeDefinition, NodeEndpoint, TailscaleGatewayConfig,
};
use kernel_api::{ClusterId, NodeId, NodeRole, SecretValue};

use crate::config_view::masked_cluster_config;

#[test]
fn operator_view_cannot_serialize_the_join_secret() -> Result<(), Box<dyn std::error::Error>> {
    let node_id = NodeId::new("node-a")?;
    let secret = "join-secret-that-must-never-cross-the-api";
    let tailscale_secret = "tskey-auth-secret-that-must-never-cross-the-api";
    let cluster = ClusterConfig {
        cluster_id: ClusterId::new("config-view-test")?,
        name: "config-view-test".to_string(),
        cluster_cidr: "172.22.0.0/16".parse()?,
        node_limit: 254,
        node_prefix: 24,
        nodes: BTreeMap::from([(
            node_id.clone(),
            NodeDefinition {
                hostname: "node-a.internal".to_string(),
                endpoint: NodeEndpoint {
                    host_address: Ipv4Addr::new(10, 20, 0, 11),
                    api_port: 3_011,
                },
                workload_subnet: "172.22.1.0/24".parse::<Ipv4Cidr>()?,
                role: NodeRole::Master,
            },
        )]),
        control_allow_cidrs: vec!["10.20.0.0/24".parse()?],
        ports: ClusterPorts::new(3_001, 2_379, 2_380, 51_820)?,
        join_secret: SecretValue::new(secret),
        tailscale: Some(TailscaleGatewayConfig {
            auth_key: SecretValue::new(tailscale_secret),
            advertise_routes: None,
            replicas: 1,
            tags: vec!["tag:maestro-gateway".to_owned()],
        }),
    };

    let encoded = serde_json::to_string(&masked_cluster_config(&cluster, &node_id))?;
    assert!(!encoded.contains(secret));
    assert!(!encoded.contains(tailscale_secret));
    assert!(encoded.contains("node-a.internal"));
    assert!(encoded.contains("\"advertiseRoutes\":[\"172.22.0.0/16\"]"));
    assert!(encoded.contains("\"dnsNameservers\":[\"172.22.1.1\"]"));
    Ok(())
}
