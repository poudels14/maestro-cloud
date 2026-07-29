use std::collections::BTreeMap;

use cluster::{ClusterConfig, ClusterPorts, NodeDefinition, NodeEndpoint};
use kernel_api::{ClusterId, NodeId, NodeRole, SecretValue};

use crate::cluster_preview_config::admin_targets;

#[test]
fn preview_sync_targets_each_predictable_admin_address() -> Result<(), Box<dyn std::error::Error>> {
    let nodes = [
        ("node-1", NodeRole::Master),
        ("node-2", NodeRole::ControlPlane),
        ("node-3", NodeRole::ControlPlane),
    ]
    .into_iter()
    .enumerate()
    .map(|(index, (node_id, role))| {
        Ok((
            NodeId::new(node_id)?,
            NodeDefinition {
                hostname: format!("{node_id}.internal"),
                endpoint: NodeEndpoint {
                    host_address: format!("10.20.0.{}", index + 11).parse()?,
                    api_port: 3_000,
                },
                workload_subnet: format!("172.22.{index}.0/24").parse()?,
                role,
            },
        ))
    })
    .collect::<Result<BTreeMap<_, _>, Box<dyn std::error::Error>>>()?;
    let cluster = ClusterConfig {
        cluster_id: ClusterId::new("preview-sync-test")?,
        name: "preview-sync-test".to_string(),
        nodes,
        control_allow_cidrs: vec!["10.20.0.0/24".parse()?],
        ports: ClusterPorts::new(3_001, 2_379, 2_380, 51_820)?,
        join_secret: SecretValue::new("preview-sync-test-join-secret"),
        tailscale: None,
        cloudflare: None,
    };

    assert_eq!(
        admin_targets(&cluster)?,
        vec![
            (
                kernel_api::NodeId::new("node-1")?,
                "http://172.22.0.250".to_string()
            ),
            (
                kernel_api::NodeId::new("node-2")?,
                "http://172.22.1.250".to_string()
            ),
            (
                kernel_api::NodeId::new("node-3")?,
                "http://172.22.2.250".to_string()
            ),
        ]
    );
    Ok(())
}
