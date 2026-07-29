use std::sync::{Arc, Mutex};

use axum::body::Body;
use axum::http::{Request, StatusCode};
use cluster::PreviewLaunchConfig;
use kernel_api::{
    MaskedClusterConfig, MaskedClusterConfigNode, MaskedClusterConfigPorts, NodeId, NodeRole,
    PreviewLaunchConfigUpdateRequest, PreviewLaunchConfigUpdateResponse, SecretValue,
};
use tower::ServiceExt;

use crate::{ApiServer, LaunchConfigAdmin, LaunchConfigAdminError, ServerSettings};

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

#[tokio::test]
async fn preview_launch_config_update_targets_one_exact_admin_endpoint()
-> Result<(), Box<dyn std::error::Error>> {
    let (store, cluster_id) = seeded_store().await?;
    let node_id = NodeId::new("node-1")?;
    let admin = Arc::new(RecordingLaunchConfigAdmin {
        cluster_id: cluster_id.clone(),
        node_id: node_id.clone(),
        previews: Mutex::new(Vec::new()),
    });
    let server = ApiServer::new(
        store,
        cluster_id.clone(),
        ServerSettings::new("127.0.0.1:3000".parse()?, None),
    )?
    .with_launch_config_admin(admin.clone());
    let payload = PreviewLaunchConfigUpdateRequest {
        cluster_id: cluster_id.clone(),
        node_id: node_id.clone(),
        domain: "preview.example.test".to_string(),
        github_token: SecretValue::new("github-super-secret"),
        max_concurrent_previews: 5,
    };
    assert_eq!(
        update_request(server.router(), &payload).await?.status(),
        StatusCode::NOT_FOUND
    );
    let listener = server
        .bind_additional(ServerSettings::new("127.0.0.1:0".parse()?, None))
        .await?;
    let address = listener.local_address();
    let (shutdown, receiver) = tokio::sync::watch::channel(false);
    let task = tokio::spawn(listener.serve(receiver));
    let client = reqwest::Client::new();
    let endpoint = format!("http://{address}/api/config/preview");
    let response = client
        .put(&endpoint)
        .header("content-type", "application/json")
        .body(serde_json::to_vec(&payload)?)
        .send()
        .await?;

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        serde_json::from_slice::<PreviewLaunchConfigUpdateResponse>(&response.bytes().await?)?,
        PreviewLaunchConfigUpdateResponse {
            node_id,
            changed: true,
        }
    );
    let github_token = {
        let previews = admin.previews.lock().map_err(|_| "preview lock poisoned")?;
        assert_eq!(previews.len(), 1);
        previews
            .first()
            .ok_or("recorded preview is missing")?
            .github_token
            .expose()
            .to_string()
    };
    assert_eq!(github_token, "github-super-secret");

    let mismatched = PreviewLaunchConfigUpdateRequest {
        node_id: NodeId::new("other")?,
        ..payload
    };
    assert_eq!(
        client
            .put(endpoint)
            .header("content-type", "application/json")
            .body(serde_json::to_vec(&mismatched)?)
            .send()
            .await?
            .status(),
        StatusCode::CONFLICT
    );
    assert_eq!(
        admin
            .previews
            .lock()
            .map_err(|_| "preview lock poisoned")?
            .len(),
        1
    );
    shutdown.send(true)?;
    task.await??;
    Ok(())
}

async fn update_request(
    router: axum::Router,
    payload: &PreviewLaunchConfigUpdateRequest,
) -> Result<axum::response::Response, Box<dyn std::error::Error>> {
    Ok(router
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/api/config/preview")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(payload)?))?,
        )
        .await?)
}

struct RecordingLaunchConfigAdmin {
    cluster_id: kernel_api::ClusterId,
    node_id: NodeId,
    previews: Mutex<Vec<PreviewLaunchConfig>>,
}

#[async_trait::async_trait]
impl LaunchConfigAdmin for RecordingLaunchConfigAdmin {
    fn cluster_id(&self) -> &kernel_api::ClusterId {
        &self.cluster_id
    }

    fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    async fn replace_preview(
        &self,
        preview: PreviewLaunchConfig,
    ) -> Result<bool, LaunchConfigAdminError> {
        self.previews
            .lock()
            .map_err(|_| LaunchConfigAdminError::new("preview lock poisoned"))?
            .push(preview);
        Ok(true)
    }
}
