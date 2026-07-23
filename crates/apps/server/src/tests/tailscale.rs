use axum::body::Body;
use axum::http::{Method, Request, StatusCode, header};
use cluster::TailscaleAuthKeyRecord;
use kernel_api::{
    MaskedClusterConfig, MaskedClusterConfigNode, MaskedClusterConfigPorts, MaskedTailscaleConfig,
    NodeId, NodeRole, TailscaleAuthKeyRotationRequest, TailscaleAuthKeyRotationResponse,
    TailscaleAuthKeyStatus,
};
use kernel_store::{Keyspace, Store};
use tower::ServiceExt;

use crate::{ApiServer, ServerSettings};

use super::{decode, request, seeded_store};

#[tokio::test]
async fn auth_key_rotation_is_optimistic_replayable_and_secret_safe()
-> Result<(), Box<dyn std::error::Error>> {
    let (store, cluster_id) = seeded_store().await?;
    let server = ApiServer::new(
        store.clone(),
        cluster_id.clone(),
        ServerSettings::new("127.0.0.1:3000".parse()?, None),
    )?
    .with_cluster_config(config(cluster_id.clone(), true)?);

    let initial = request(&server, "/api/cluster/tailscale/auth-key", None).await?;
    assert_eq!(initial.status(), StatusCode::OK);
    assert_eq!(
        decode::<TailscaleAuthKeyStatus>(initial)
            .await?
            .override_revision,
        None
    );

    let first = TailscaleAuthKeyRotationRequest {
        expected_revision: None,
        auth_key: kernel_api::SecretValue::new("tskey-auth-first-reusable-secret"),
    };
    let accepted = rotate(&server, "tailscale-key-1", &first).await?;
    assert_eq!(accepted.status(), StatusCode::ACCEPTED);
    let response = decode::<TailscaleAuthKeyRotationResponse>(accepted).await?;
    assert_eq!(response.request_id.as_str(), "tailscale-key-1");
    assert_eq!(
        rotate(&server, "tailscale-key-1", &first).await?.status(),
        StatusCode::ACCEPTED
    );

    let stored = store
        .get(&Keyspace::new(&cluster_id).tailscale_auth_key())
        .await?
        .ok_or("Tailscale auth-key override was not stored")?;
    let record: TailscaleAuthKeyRecord = serde_json::from_slice(&stored.value)?;
    assert_eq!(record.auth_key.expose(), "tskey-auth-first-reusable-secret");
    let status_response = request(&server, "/api/cluster/tailscale/auth-key", None).await?;
    let body = http_body_util::BodyExt::collect(status_response.into_body())
        .await?
        .to_bytes();
    assert!(
        !body
            .windows(b"tskey-auth".len())
            .any(|part| part == b"tskey-auth")
    );
    let status: TailscaleAuthKeyStatus = serde_json::from_slice(&body)?;
    let revision = status
        .override_revision
        .ok_or("override status omitted its revision")?;
    assert_eq!(revision, stored.version.resource_revision());
    assert_eq!(
        rotate(
            &server,
            "tailscale-key-1",
            &TailscaleAuthKeyRotationRequest {
                expected_revision: Some(revision),
                auth_key: kernel_api::SecretValue::new("tskey-auth-first-reusable-secret"),
            },
        )
        .await?
        .status(),
        StatusCode::ACCEPTED
    );
    assert_eq!(
        store
            .get(&Keyspace::new(&cluster_id).tailscale_auth_key())
            .await?
            .ok_or("Tailscale auth-key override disappeared")?
            .version,
        stored.version
    );

    assert_eq!(
        rotate(
            &server,
            "tailscale-key-stale",
            &TailscaleAuthKeyRotationRequest {
                expected_revision: None,
                auth_key: kernel_api::SecretValue::new("tskey-auth-stale-reusable-secret"),
            },
        )
        .await?
        .status(),
        StatusCode::CONFLICT
    );
    assert_eq!(
        rotate(
            &server,
            "tailscale-key-2",
            &TailscaleAuthKeyRotationRequest {
                expected_revision: Some(revision),
                auth_key: kernel_api::SecretValue::new("tskey-auth-second-reusable-secret"),
            },
        )
        .await?
        .status(),
        StatusCode::ACCEPTED
    );
    Ok(())
}

#[tokio::test]
async fn auth_key_rotation_rejects_disabled_gateways_and_weak_keys()
-> Result<(), Box<dyn std::error::Error>> {
    let (store, cluster_id) = seeded_store().await?;
    let disabled = ApiServer::new(
        store.clone(),
        cluster_id.clone(),
        ServerSettings::new("127.0.0.1:3000".parse()?, None),
    )?
    .with_cluster_config(config(cluster_id.clone(), false)?);
    let request_body = TailscaleAuthKeyRotationRequest {
        expected_revision: None,
        auth_key: kernel_api::SecretValue::new("tskey-auth-reusable-secret"),
    };
    assert_eq!(
        rotate(&disabled, "tailscale-disabled", &request_body)
            .await?
            .status(),
        StatusCode::CONFLICT
    );

    let enabled = ApiServer::new(
        store,
        cluster_id.clone(),
        ServerSettings::new("127.0.0.1:3000".parse()?, None),
    )?
    .with_cluster_config(config(cluster_id, true)?);
    assert_eq!(
        rotate(
            &enabled,
            "tailscale-weak",
            &TailscaleAuthKeyRotationRequest {
                expected_revision: None,
                auth_key: kernel_api::SecretValue::new("too-short"),
            },
        )
        .await?
        .status(),
        StatusCode::BAD_REQUEST
    );
    Ok(())
}

async fn rotate(
    server: &ApiServer,
    idempotency_key: &str,
    body: &TailscaleAuthKeyRotationRequest,
) -> Result<axum::response::Response, Box<dyn std::error::Error>> {
    Ok(server
        .router()
        .oneshot(
            Request::builder()
                .method(Method::PUT)
                .uri("/api/cluster/tailscale/auth-key")
                .header(header::CONTENT_TYPE, "application/json")
                .header("Idempotency-Key", idempotency_key)
                .body(Body::from(serde_json::to_vec(body)?))?,
        )
        .await?)
}

fn config(
    cluster_id: kernel_api::ClusterId,
    tailscale: bool,
) -> Result<MaskedClusterConfig, Box<dyn std::error::Error>> {
    Ok(MaskedClusterConfig {
        cluster_id,
        name: "server-test".to_owned(),
        cluster_cidr: "10.42.0.0/16".to_owned(),
        node_limit: 254,
        node_prefix: 24,
        local_node_id: NodeId::new("node-1")?,
        nodes: vec![MaskedClusterConfigNode {
            node_id: NodeId::new("node-1")?,
            hostname: "node-1.internal".to_owned(),
            role: NodeRole::Master,
            host_address: "10.20.0.1".to_owned(),
            api_port: 8443,
            workload_subnet: "10.42.1.0/24".to_owned(),
        }],
        control_allow_cidrs: vec!["10.20.0.0/24".to_owned()],
        ports: MaskedClusterConfigPorts {
            gateway: 443,
            store_client: 2379,
            store_peer: 2380,
            wireguard: 51_820,
        },
        tailscale: tailscale.then_some(MaskedTailscaleConfig {
            advertise_routes: vec!["10.42.0.0/16".to_owned()],
            dns_nameservers: vec!["10.42.1.1".to_owned()],
            replicas: 1,
            tags: vec!["tag:maestro-gateway".to_owned()],
        }),
    })
}
