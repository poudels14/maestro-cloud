use std::collections::BTreeMap;
use std::net::Ipv4Addr;

use kernel_api::{ClusterId, NodeId, NodeRole, SecretValue};
use time::{Duration, OffsetDateTime};

use crate::{
    CertificateValidity, ClusterConfig, ClusterPorts, DEFAULT_WIREGUARD_PORT, Ipv4Cidr,
    NodeDefinition, NodeEndpoint,
};

pub(crate) fn valid_config() -> Result<ClusterConfig, Box<dyn std::error::Error>> {
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

pub(crate) fn validity() -> Result<CertificateValidity, Box<dyn std::error::Error>> {
    let start = OffsetDateTime::UNIX_EPOCH + Duration::days(20_000);
    Ok(CertificateValidity::new(
        start,
        start + Duration::days(365),
    )?)
}
