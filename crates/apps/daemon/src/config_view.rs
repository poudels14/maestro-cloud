use cluster::ClusterConfig;
use kernel_api::{
    MaskedCloudflareConfig, MaskedCloudflareTunnelConfig, MaskedClusterConfig,
    MaskedClusterConfigNode, MaskedClusterConfigPorts, MaskedCrossClusterDnsRoute,
    MaskedTailscaleConfig, NodeId,
};

pub(crate) fn masked_cluster_config(
    cluster: &ClusterConfig,
    local_node_id: &NodeId,
) -> MaskedClusterConfig {
    let workload_subnets = cluster
        .nodes
        .values()
        .filter(|node| node.role.runs_workloads())
        .map(|node| node.workload_subnet)
        .collect::<Vec<_>>();
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
        tailscale: cluster.tailscale.as_ref().map(|tailscale| {
            let routes = tailscale.advertised_routes(&workload_subnets);
            MaskedTailscaleConfig {
                advertise_routes: routes.iter().map(ToString::to_string).collect(),
                dns_nameservers: cluster
                    .nodes
                    .values()
                    .filter(|node| node.role.runs_workloads())
                    .filter_map(|node| node.workload_subnet.gateway_address())
                    .filter(|address| routes.iter().any(|route| route.contains(*address)))
                    .map(|address| address.to_string())
                    .collect(),
                tags: tailscale.tags.clone(),
                cross_cluster_dns: tailscale
                    .cross_cluster_dns
                    .iter()
                    .map(|route| MaskedCrossClusterDnsRoute {
                        cluster_id: route.cluster_id.clone(),
                        nameservers: route.nameservers.iter().map(ToString::to_string).collect(),
                    })
                    .collect(),
            }
        }),
        cloudflare: cluster
            .cloudflare
            .as_ref()
            .map(|cloudflare| MaskedCloudflareConfig {
                tunnel: MaskedCloudflareTunnelConfig {
                    replicas: cloudflare.replicas,
                },
            }),
    }
}
