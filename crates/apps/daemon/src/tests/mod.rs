mod build_backend;
mod control_plane;
mod control_plane_resources;
mod control_plane_store;
mod launch;
mod leadership;
mod log_maintenance;
mod operators;
mod orchestration;
mod orchestration_acceptance;
mod orchestration_affinity;
mod orchestration_build_acceptance;
mod orchestration_commands;
mod orchestration_delete;
mod orchestration_deployment;
mod orchestration_drain;
mod orchestration_egress;
mod orchestration_egress_acceptance;
mod orchestration_fixture;
mod orchestration_freeze;
mod orchestration_node;
mod orchestration_preview_acceptance;
mod orchestration_scale;
mod orchestration_service;
mod orchestration_upgrade_acceptance;
mod orchestration_upgrade_backend;
mod plan;
mod runtime;
mod s3_backup;

use std::collections::BTreeMap;
use std::net::Ipv4Addr;

use cluster::{ClusterConfig, ClusterPorts, Ipv4Cidr, NodeDefinition, NodeEndpoint};
use kernel_api::{ClusterId, NodeId, NodeRole, SecretValue};

fn cluster_with_nodes(
    definitions: &[(&str, NodeRole)],
) -> Result<ClusterConfig, Box<dyn std::error::Error>> {
    let nodes = definitions
        .iter()
        .enumerate()
        .map(|(index, (name, role))| {
            let suffix = u8::try_from(index)?.saturating_add(11);
            Ok((
                NodeId::new(*name)?,
                NodeDefinition {
                    hostname: format!("{name}.internal"),
                    endpoint: NodeEndpoint {
                        host_address: Ipv4Addr::new(10, 20, 0, suffix),
                        api_port: 3_000_u16.saturating_add(u16::from(suffix)),
                    },
                    workload_subnet: format!("172.22.{index}.0/24").parse::<Ipv4Cidr>()?,
                    role: *role,
                },
            ))
        })
        .collect::<Result<BTreeMap<_, _>, Box<dyn std::error::Error>>>()?;
    Ok(ClusterConfig {
        cluster_id: ClusterId::new("daemon-test")?,
        name: "daemon-test".to_owned(),
        nodes,
        control_allow_cidrs: vec!["10.20.0.0/24".parse()?],
        ports: ClusterPorts::new(3_001, 2_379, 2_380, 51_820)?,
        join_secret: SecretValue::new("daemon-test-join-secret-with-32-characters"),
    })
}
