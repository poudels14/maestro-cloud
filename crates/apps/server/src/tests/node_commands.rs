use axum::body::Body;
use axum::http::{Method, Request, StatusCode, header};
use kernel_api::{ConditionState, Node, ResourceRevision};
use serde_json::json;
use tower::ServiceExt;

use crate::{ApiServer, ServerSettings};

use super::{decode, request, seeded_store};

#[tokio::test]
async fn node_drain_and_restore_are_optimistic_replayable_status_commands()
-> Result<(), Box<dyn std::error::Error>> {
    let (store, cluster_id) = seeded_store().await?;
    let server = ApiServer::new(
        store,
        cluster_id,
        ServerSettings::new("127.0.0.1:3000".parse()?, None),
    )?;
    let initial = get_node(&server).await?;

    let drained = command(&server, "drain", "node-drain", initial.meta.revision).await?;
    assert_eq!(drained.status(), StatusCode::ACCEPTED);
    assert_eq!(
        decode::<serde_json::Value>(drained).await?,
        json!({"nodeId": "node-1", "draining": true})
    );
    let replay = command(&server, "drain", "node-drain", initial.meta.revision).await?;
    assert_eq!(replay.status(), StatusCode::ACCEPTED);
    let current = get_node(&server).await?;
    assert_eq!(current.meta.generation, initial.meta.generation);
    assert!(drain_pending(&current));

    assert_eq!(
        command(
            &server,
            "restore",
            "stale-node-restore",
            initial.meta.revision,
        )
        .await?
        .status(),
        StatusCode::CONFLICT
    );
    let restored = command(&server, "restore", "node-restore", current.meta.revision).await?;
    assert_eq!(restored.status(), StatusCode::ACCEPTED);
    let current = get_node(&server).await?;
    assert!(!draining(&current));

    let revision = current.meta.revision;
    let no_op = command(
        &server,
        "restore",
        "node-restore-no-op",
        current.meta.revision,
    )
    .await?;
    assert_eq!(no_op.status(), StatusCode::ACCEPTED);
    assert_eq!(get_node(&server).await?.meta.revision, revision);
    Ok(())
}

async fn get_node(server: &ApiServer) -> Result<Node, Box<dyn std::error::Error>> {
    decode(request(server, "/api/cluster/nodes/node-1", None).await?).await
}

async fn command(
    server: &ApiServer,
    action: &str,
    idempotency_key: &str,
    expected_revision: ResourceRevision,
) -> Result<axum::response::Response, Box<dyn std::error::Error>> {
    Ok(server
        .router()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri(format!("/api/cluster/nodes/node-1/{action}"))
                .header(header::CONTENT_TYPE, "application/json")
                .header("Idempotency-Key", idempotency_key)
                .body(Body::from(serde_json::to_vec(
                    &json!({"expectedRevision": expected_revision}),
                )?))?,
        )
        .await?)
}

fn draining(node: &Node) -> bool {
    node.status.conditions.iter().any(|condition| {
        condition.condition_type.0 == "Draining" && condition.state == ConditionState::True
    })
}

fn drain_pending(node: &Node) -> bool {
    node.status.conditions.iter().any(|condition| {
        condition.condition_type.0 == "Draining"
            && condition.state == ConditionState::Unknown
            && condition.reason.0 == "ReplicatingArtifacts"
    })
}
