use axum::body::Body;
use axum::http::{Method, Request, StatusCode, header};
use kernel_api::{ResourceRevision, UpgradePhase, UpgradeRun};
use serde_json::{Value, json};
use tower::ServiceExt;

use crate::{ApiServer, ServerSettings};

use super::{decode, request, seeded_store};

#[tokio::test]
async fn upgrades_are_validated_created_observed_and_canceled_optimistically()
-> Result<(), Box<dyn std::error::Error>> {
    let (store, cluster_id) = seeded_store().await?;
    let server = ApiServer::new(
        store,
        cluster_id,
        ServerSettings::new("127.0.0.1:3000".parse()?, None),
    )?;

    assert_eq!(
        start(
            &server,
            "bad-version",
            json!({
                "upgradeRunId": "upgrade-bad",
                "spec": {"targetVersion": "not-semver", "mode": "rolling"}
            }),
        )
        .await?
        .status(),
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        start(
            &server,
            "duplicate-nodes",
            json!({
                "upgradeRunId": "upgrade-duplicate",
                "spec": {
                    "targetVersion": "2.0.0",
                    "mode": "rolling",
                    "nodeIds": ["node-1", "node-1"]
                }
            }),
        )
        .await?
        .status(),
        StatusCode::BAD_REQUEST
    );

    let create_payload = json!({
        "upgradeRunId": "upgrade-1",
        "spec": {
            "targetVersion": " 2.0.0 ",
            "mode": "rolling",
            "nodeIds": ["node-1"]
        }
    });
    let created = start(&server, "start-upgrade", create_payload.clone()).await?;
    assert_eq!(created.status(), StatusCode::ACCEPTED);
    assert_eq!(
        decode::<Value>(created).await?,
        json!({
            "upgradeRunId": "upgrade-1",
            "generation": 1,
            "phase": "pending"
        })
    );
    assert_eq!(
        start(&server, "start-upgrade", create_payload.clone())
            .await?
            .status(),
        StatusCode::ACCEPTED
    );
    assert_eq!(
        start(&server, "new-request-same-id", create_payload)
            .await?
            .status(),
        StatusCode::CONFLICT
    );

    let run = get(&server, "upgrade-1").await?;
    assert_eq!(run.spec.target_version, "2.0.0");
    assert_eq!(run.status.phase, UpgradePhase::Pending);
    assert_eq!(list(&server).await?.len(), 1);
    assert_eq!(
        cancel(
            &server,
            "stale-cancel",
            ResourceRevision(run.meta.revision.0.saturating_sub(1)),
        )
        .await?
        .status(),
        StatusCode::CONFLICT
    );

    let canceled = cancel(&server, "cancel-upgrade", run.meta.revision).await?;
    assert_eq!(canceled.status(), StatusCode::ACCEPTED);
    let body = decode::<Value>(canceled).await?;
    assert_eq!(
        body.get("phase"),
        Some(&Value::String("pending".to_string()))
    );
    assert!(
        body.get("deletionTimestamp")
            .and_then(Value::as_i64)
            .is_some()
    );
    assert_eq!(
        cancel(&server, "cancel-upgrade", run.meta.revision)
            .await?
            .status(),
        StatusCode::ACCEPTED
    );
    let deleting = get(&server, "upgrade-1").await?;
    assert!(deleting.meta.deletion_timestamp.is_some());
    let revision = deleting.meta.revision;
    assert_eq!(
        cancel(&server, "cancel-upgrade-no-op", revision)
            .await?
            .status(),
        StatusCode::ACCEPTED
    );
    assert_eq!(get(&server, "upgrade-1").await?.meta.revision, revision);
    Ok(())
}

async fn list(server: &ApiServer) -> Result<Vec<UpgradeRun>, Box<dyn std::error::Error>> {
    decode(request(server, "/api/cluster/upgrades", None).await?).await
}

async fn get(
    server: &ApiServer,
    upgrade_run_id: &str,
) -> Result<UpgradeRun, Box<dyn std::error::Error>> {
    decode(
        request(
            server,
            &format!("/api/cluster/upgrades/{upgrade_run_id}"),
            None,
        )
        .await?,
    )
    .await
}

async fn start(
    server: &ApiServer,
    idempotency_key: &str,
    payload: Value,
) -> Result<axum::response::Response, Box<dyn std::error::Error>> {
    mutate(
        server,
        Method::POST,
        "/api/cluster/upgrades".to_string(),
        idempotency_key,
        payload,
    )
    .await
}

async fn cancel(
    server: &ApiServer,
    idempotency_key: &str,
    expected_revision: ResourceRevision,
) -> Result<axum::response::Response, Box<dyn std::error::Error>> {
    mutate(
        server,
        Method::DELETE,
        "/api/cluster/upgrades/upgrade-1".to_string(),
        idempotency_key,
        json!({"expectedRevision": expected_revision}),
    )
    .await
}

async fn mutate(
    server: &ApiServer,
    method: Method,
    uri: String,
    idempotency_key: &str,
    payload: Value,
) -> Result<axum::response::Response, Box<dyn std::error::Error>> {
    Ok(server
        .router()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
                .header(header::CONTENT_TYPE, "application/json")
                .header("Idempotency-Key", idempotency_key)
                .body(Body::from(serde_json::to_vec(&payload)?))?,
        )
        .await?)
}
