use axum::body::Body;
use axum::http::{Method, Request, StatusCode, header};
use kernel_api::{
    RESTART_TARGET_VERSION, ResourceRevision, UpgradeCancelResponse, UpgradeOperation,
    UpgradePhase, UpgradeRun,
};
use serde_json::{Value, json};
use tower::ServiceExt;

use crate::{ApiServer, ServerSettings};

use super::{decode, put, request, seeded_store};

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
                "spec": {
                    "operation": "upgrade",
                    "targetVersion": "not-semver",
                    "mode": "rolling"
                }
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
                    "operation": "upgrade",
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
            "operation": "upgrade",
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
    assert_eq!(run.spec.operation, UpgradeOperation::Upgrade);
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

#[tokio::test]
async fn restart_runs_require_the_restart_sentinel_and_persist_the_operation()
-> Result<(), Box<dyn std::error::Error>> {
    let (store, cluster_id) = seeded_store().await?;
    let server = ApiServer::new(
        store,
        cluster_id,
        ServerSettings::new("127.0.0.1:3000".parse()?, None),
    )?;

    let wrong_target = start(
        &server,
        "restart-wrong-target",
        json!({
            "upgradeRunId": "restart-bad",
            "spec": {
                "operation": "restart",
                "targetVersion": "2.0.0",
                "mode": "rolling"
            }
        }),
    )
    .await?;
    assert_eq!(wrong_target.status(), StatusCode::BAD_REQUEST);

    let created = start(
        &server,
        "restart-node-1",
        json!({
            "upgradeRunId": "restart-1",
            "spec": {
                "operation": "restart",
                "targetVersion": RESTART_TARGET_VERSION,
                "mode": "rolling",
                "nodeIds": ["node-1"]
            }
        }),
    )
    .await?;
    assert_eq!(created.status(), StatusCode::ACCEPTED);
    let run = get(&server, "restart-1").await?;
    assert_eq!(run.spec.operation, UpgradeOperation::Restart);
    assert_eq!(run.spec.target_version, RESTART_TARGET_VERSION);
    assert_eq!(run.spec.node_ids.len(), 1);
    Ok(())
}

#[tokio::test]
async fn force_atomically_supersedes_existing_maintenance() -> Result<(), Box<dyn std::error::Error>>
{
    let (store, cluster_id) = seeded_store().await?;
    let server = ApiServer::new(
        store,
        cluster_id,
        ServerSettings::new("127.0.0.1:3000".parse()?, None),
    )?;
    let old = upgrade_payload("upgrade-old", false);
    assert_eq!(
        start(&server, "start-old", old).await?.status(),
        StatusCode::ACCEPTED
    );
    assert_eq!(
        start(
            &server,
            "start-blocked",
            upgrade_payload("upgrade-blocked", false),
        )
        .await?
        .status(),
        StatusCode::CONFLICT
    );

    let forced = start(
        &server,
        "start-forced",
        upgrade_payload("upgrade-forced", true),
    )
    .await?;
    assert_eq!(forced.status(), StatusCode::ACCEPTED);
    assert!(
        get(&server, "upgrade-old")
            .await?
            .meta
            .deletion_timestamp
            .is_some()
    );
    let replacement = get(&server, "upgrade-forced").await?;
    assert_eq!(replacement.status.phase, UpgradePhase::Pending);
    assert!(replacement.meta.deletion_timestamp.is_none());
    Ok(())
}

#[tokio::test]
async fn current_and_all_cancellation_select_maintenance_server_side()
-> Result<(), Box<dyn std::error::Error>> {
    let (store, cluster_id) = seeded_store().await?;
    let active = run("upgrade-active", "verifying")?;
    let queued = run("upgrade-queued", "pending")?;
    put(
        store.as_ref(),
        &cluster_id,
        "UpgradeRun",
        "upgrade-active",
        &active,
    )
    .await?;
    put(
        store.as_ref(),
        &cluster_id,
        "UpgradeRun",
        "upgrade-queued",
        &queued,
    )
    .await?;
    let server = ApiServer::new(
        store,
        cluster_id,
        ServerSettings::new("127.0.0.1:3000".parse()?, None),
    )?;

    let current = cancel_current(&server, "cancel-current", false).await?;
    assert_eq!(current.status(), StatusCode::ACCEPTED);
    let current = decode::<UpgradeCancelResponse>(current).await?;
    assert_eq!(
        current
            .upgrade_run_ids
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>(),
        vec!["upgrade-active"]
    );
    assert!(
        get(&server, "upgrade-active")
            .await?
            .meta
            .deletion_timestamp
            .is_some()
    );
    assert!(
        get(&server, "upgrade-queued")
            .await?
            .meta
            .deletion_timestamp
            .is_none()
    );

    let all = cancel_current(&server, "cancel-all", true).await?;
    assert_eq!(all.status(), StatusCode::ACCEPTED);
    let all = decode::<UpgradeCancelResponse>(all).await?;
    assert_eq!(
        all.upgrade_run_ids
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>(),
        vec!["upgrade-queued"]
    );
    assert!(
        get(&server, "upgrade-queued")
            .await?
            .meta
            .deletion_timestamp
            .is_some()
    );
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

async fn cancel_current(
    server: &ApiServer,
    idempotency_key: &str,
    all: bool,
) -> Result<axum::response::Response, Box<dyn std::error::Error>> {
    mutate(
        server,
        Method::DELETE,
        "/api/cluster/upgrades".to_string(),
        idempotency_key,
        json!({"all": all}),
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

fn upgrade_payload(upgrade_run_id: &str, force: bool) -> Value {
    json!({
        "upgradeRunId": upgrade_run_id,
        "spec": {
            "operation": "upgrade",
            "targetVersion": "2.0.0",
            "mode": "rolling"
        },
        "force": force
    })
}

fn run(id: &str, phase: &str) -> Result<UpgradeRun, serde_json::Error> {
    serde_json::from_value(json!({
        "meta": {"id": id, "revision": 0, "generation": 1},
        "spec": {
            "operation": "upgrade",
            "targetVersion": "2.0.0",
            "mode": "rolling"
        },
        "status": {"phase": phase}
    }))
}
