use axum::http::StatusCode;
use kernel_api::{
    MaskedClusterConfig, MaskedClusterConfigNode, MaskedClusterConfigPorts, NodeId, NodeRole,
};

use crate::{ApiServer, ServerSettings};

use super::{decode, request, seeded_store};

#[tokio::test]
async fn config_view_is_explicitly_configured_and_secret_free()
-> Result<(), Box<dyn std::error::Error>> {
    let (store, cluster_id) = seeded_store().await?;
    let unconfigured = ApiServer::new(
        store.clone(),
        cluster_id.clone(),
        ServerSettings::new("127.0.0.1:3000".parse()?, None),
    )?;
    assert_eq!(
        request(&unconfigured, "/api/config", None).await?.status(),
        StatusCode::SERVICE_UNAVAILABLE
    );

    let expected = MaskedClusterConfig {
        cluster_id: cluster_id.clone(),
        name: "server-test".to_string(),
        cluster_cidr: "10.42.0.0/16".to_string(),
        node_limit: 254,
        node_prefix: 24,
        local_node_id: NodeId::new("node-1")?,
        nodes: vec![MaskedClusterConfigNode {
            node_id: NodeId::new("node-1")?,
            hostname: "node-1.internal".to_string(),
            role: NodeRole::Master,
            host_address: "10.20.0.1".to_string(),
            api_port: 8443,
            workload_subnet: "10.42.1.0/24".to_string(),
        }],
        control_allow_cidrs: vec!["10.20.0.0/24".to_string()],
        ports: MaskedClusterConfigPorts {
            gateway: 443,
            store_client: 2379,
            store_peer: 2380,
            wireguard: 51_820,
        },
        tailscale: None,
        cloudflare: None,
    };
    let server = ApiServer::new(
        store,
        cluster_id,
        ServerSettings::new("127.0.0.1:3000".parse()?, None),
    )?
    .with_cluster_config(expected.clone());

    let response = request(&server, "/api/config", None).await?;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(decode::<MaskedClusterConfig>(response).await?, expected);
    Ok(())
}
