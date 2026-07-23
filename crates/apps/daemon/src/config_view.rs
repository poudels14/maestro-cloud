use cluster::ClusterConfig;
use kernel_api::{MaskedClusterConfig, MaskedClusterConfigNode, MaskedClusterConfigPorts, NodeId};

pub(crate) fn masked_cluster_config(
    cluster: &ClusterConfig,
    local_node_id: &NodeId,
) -> MaskedClusterConfig {
    MaskedClusterConfig {
        cluster_id: cluster.cluster_id.clone(),
        name: cluster.name.clone(),
        cluster_cidr: cluster.cluster_cidr.to_string(),
        node_limit: cluster.node_limit,
        node_prefix: cluster.node_prefix,
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
