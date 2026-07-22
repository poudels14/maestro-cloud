use cluster::ClusterConfig;
use kernel_api::{MaskedClusterConfig, MaskedClusterConfigNode, MaskedClusterConfigPorts, NodeId};

pub(crate) fn masked_cluster_config(
    cluster: &ClusterConfig,
    local_node_id: &NodeId,
) -> MaskedClusterConfig {
    MaskedClusterConfig {
        cluster_id: cluster.cluster_id.clone(),
        name: cluster.name.clone(),
        local_node_id: local_node_id.clone(),
        nodes: cluster
            .nodes
            .iter()
            .map(|(node_id, node)| MaskedClusterConfigNode {
                node_id: node_id.clone(),
                hostname: node.hostname.clone(),
                role: node.role,
                host_address: node.endpoint.host_address.to_string(),
                api_port: node.endpoint.api_port,
                workload_subnet: node.workload_subnet.to_string(),
            })
            .collect(),
        control_allow_cidrs: cluster
            .control_allow_cidrs
            .iter()
            .map(ToString::to_string)
            .collect(),
        ports: MaskedClusterConfigPorts {
            gateway: cluster.ports.gateway,
            store_client: cluster.ports.store_client,
            store_peer: cluster.ports.store_peer,
            wireguard: cluster.ports.wireguard,
        },
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::net::Ipv4Addr;

    use cluster::{ClusterPorts, Ipv4Cidr, NodeDefinition, NodeEndpoint};
    use kernel_api::{ClusterId, NodeRole, SecretValue};

    use super::*;

    #[test]
    fn operator_view_cannot_serialize_the_join_secret() -> Result<(), Box<dyn std::error::Error>> {
        let node_id = NodeId::new("node-a")?;
        let secret = "join-secret-that-must-never-cross-the-api";
        let cluster = ClusterConfig {
            cluster_id: ClusterId::new("config-view-test")?,
            name: "config-view-test".to_string(),
            nodes: BTreeMap::from([(
                node_id.clone(),
                NodeDefinition {
                    hostname: "node-a.internal".to_string(),
                    endpoint: NodeEndpoint {
                        host_address: Ipv4Addr::new(10, 20, 0, 11),
                        api_port: 3_011,
                    },
                    workload_subnet: "172.22.0.0/24".parse::<Ipv4Cidr>()?,
                    role: NodeRole::Master,
                },
            )]),
            control_allow_cidrs: vec!["10.20.0.0/24".parse()?],
            ports: ClusterPorts::new(3_001, 2_379, 2_380, 51_820)?,
            join_secret: SecretValue::new(secret),
        };

        let encoded = serde_json::to_string(&masked_cluster_config(&cluster, &node_id))?;
        assert!(!encoded.contains(secret));
        assert!(encoded.contains("node-a.internal"));
        Ok(())
    }
}
