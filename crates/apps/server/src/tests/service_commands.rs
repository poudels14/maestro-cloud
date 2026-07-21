use std::sync::Arc;

use axum::body::Body;
use axum::http::{Method, Request, StatusCode, header};
use kernel_api::ClusterId;
use kernel_store::{InMemoryStore, TokioClock};
use serde_json::{Value, json};
use tower::ServiceExt;

use crate::{ApiServer, ServerSettings};

use super::{decode, request, seeded_store};

#[tokio::test]
async fn service_commands_replay_and_preserve_lifecycle_boundaries()
-> Result<(), Box<dyn std::error::Error>> {
    let (store, cluster_id) = seeded_store().await?;
    let server = ApiServer::new(
        store,
        cluster_id,
        ServerSettings::new("127.0.0.1:3000".parse()?, None),
    )?;
    let initial = get_service(&server).await?;
    let initial_revision = revision(&initial)?.clone();

    let redeploy = mutate(
        &server,
        Method::POST,
        "/api/services/api/redeploy",
        "service-redeploy",
        &json!({"expectedRevision": initial_revision}),
    )
    .await?;
    assert_eq!(redeploy.status(), StatusCode::ACCEPTED);
    assert_eq!(field(&decode(redeploy).await?, "generation")?, &json!(2));
    let replay = mutate(
        &server,
        Method::POST,
        "/api/services/api/redeploy",
        "service-redeploy",
        &json!({"expectedRevision": initial_revision}),
    )
    .await?;
    assert_eq!(replay.status(), StatusCode::ACCEPTED);
    assert_eq!(field(&decode(replay).await?, "generation")?, &json!(2));
    assert_eq!(
        mutate(
            &server,
            Method::POST,
            "/api/services/api/freeze",
            "service-redeploy",
            &json!({"expectedRevision": initial_revision}),
        )
        .await?
        .status(),
        StatusCode::CONFLICT
    );
    assert_eq!(
        mutate(
            &server,
            Method::POST,
            "/api/services/api/freeze",
            "service-stale-revision",
            &json!({"expectedRevision": initial_revision}),
        )
        .await?
        .status(),
        StatusCode::CONFLICT
    );

    let current = get_service(&server).await?;
    let current_revision = revision(&current)?.clone();
    let frozen = mutate(
        &server,
        Method::POST,
        "/api/services/api/freeze",
        "service-freeze",
        &json!({"expectedRevision": current_revision}),
    )
    .await?;
    assert_eq!(frozen.status(), StatusCode::ACCEPTED);
    let frozen = decode(frozen).await?;
    assert_eq!(field(&frozen, "generation")?, &json!(2));
    assert_eq!(field(&frozen, "rollout")?, &json!("frozen"));

    let current = get_service(&server).await?;
    let current_revision = revision(&current)?.clone();
    let scaled = mutate(
        &server,
        Method::PUT,
        "/api/services/api/replicas",
        "service-scale",
        &json!({"expectedRevision": current_revision, "replicas": 3}),
    )
    .await?;
    assert_eq!(scaled.status(), StatusCode::ACCEPTED);
    assert_eq!(field(&decode(scaled).await?, "replicaOverride")?, &json!(3));

    let current = get_service(&server).await?;
    let current_revision = revision(&current)?.clone();
    let cleared = mutate(
        &server,
        Method::PUT,
        "/api/services/api/replicas",
        "service-scale-clear",
        &json!({"expectedRevision": current_revision, "replicas": null}),
    )
    .await?;
    assert_eq!(cleared.status(), StatusCode::ACCEPTED);
    assert!(
        decode::<Value>(cleared)
            .await?
            .get("replicaOverride")
            .is_none()
    );

    let current = get_service(&server).await?;
    let current_revision = revision(&current)?.clone();
    let deleted = mutate(
        &server,
        Method::DELETE,
        "/api/services/api",
        "service-delete",
        &json!({"expectedRevision": current_revision}),
    )
    .await?;
    assert_eq!(deleted.status(), StatusCode::ACCEPTED);
    assert!(field(&decode(deleted).await?, "deletionTimestamp")?.is_number());

    let deleting = get_service(&server).await?;
    assert_eq!(
        mutate(
            &server,
            Method::POST,
            "/api/services/api/unfreeze",
            "service-unfreeze-after-delete",
            &json!({"expectedRevision": revision(&deleting)?}),
        )
        .await?
        .status(),
        StatusCode::CONFLICT
    );
    Ok(())
}

#[tokio::test]
async fn service_commands_require_current_revisions_and_idempotency_keys()
-> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(InMemoryStore::new(Arc::new(TokioClock::new())));
    let server = ApiServer::new(
        store,
        ClusterId::new("service-command-validation")?,
        ServerSettings::new("127.0.0.1:3000".parse()?, None),
    )?;
    assert_eq!(
        mutate(
            &server,
            Method::POST,
            "/api/services/missing/redeploy",
            "missing-service",
            &json!({"expectedRevision": 1}),
        )
        .await?
        .status(),
        StatusCode::NOT_FOUND
    );
    let (store, cluster_id) = seeded_store().await?;
    let server = ApiServer::new(
        store,
        cluster_id,
        ServerSettings::new("127.0.0.1:3000".parse()?, None),
    )?;
    let current = get_service(&server).await?;
    assert_eq!(
        mutate(
            &server,
            Method::PUT,
            "/api/services/api/replicas",
            "missing-replicas",
            &json!({"expectedRevision": revision(&current)?}),
        )
        .await?
        .status(),
        StatusCode::BAD_REQUEST
    );
    let request = Request::builder()
        .method(Method::POST)
        .uri("/api/services/missing/redeploy")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(r#"{"expectedRevision":1}"#))?;
    assert_eq!(
        server.router().oneshot(request).await?.status(),
        StatusCode::BAD_REQUEST
    );
    Ok(())
}

async fn get_service(server: &ApiServer) -> Result<Value, Box<dyn std::error::Error>> {
    decode(request(server, "/api/services/api", None).await?).await
}

async fn mutate(
    server: &ApiServer,
    method: Method,
    uri: &str,
    idempotency_key: &str,
    payload: &Value,
) -> Result<axum::response::Response, Box<dyn std::error::Error>> {
    Ok(server
        .router()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
                .header(header::CONTENT_TYPE, "application/json")
                .header("Idempotency-Key", idempotency_key)
                .body(Body::from(serde_json::to_vec(payload)?))?,
        )
        .await?)
}

fn revision(service: &Value) -> Result<&Value, std::io::Error> {
    field(field(service, "meta")?, "revision")
}

fn field<'a>(value: &'a Value, name: &str) -> Result<&'a Value, std::io::Error> {
    value
        .get(name)
        .ok_or_else(|| std::io::Error::other(format!("field `{name}` is missing")))
}
