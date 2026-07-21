use axum::body::Body;
use axum::http::{Method, Request, StatusCode, header};
use kernel_api::{Deployment, DeploymentPhase};
use serde_json::{Value, json};
use tower::ServiceExt;

use crate::{ApiServer, ServerSettings};

use super::deployments::{deployment, deployment_in_phase};
use super::{decode, put, request, seeded_store};

#[tokio::test]
async fn deployment_commands_restart_cancel_remove_and_enforce_ownership()
-> Result<(), Box<dyn std::error::Error>> {
    let (store, cluster_id) = seeded_store().await?;
    let ready = deployment("ready-deployment", "api")?;
    let queued = deployment_in_phase("queued-deployment", "api", DeploymentPhase::Queued)?;
    let unrelated = deployment("other-deployment", "other")?;
    for value in [&ready, &queued, &unrelated] {
        put(
            &store,
            &cluster_id,
            "Deployment",
            value.meta.id.as_str(),
            value,
        )
        .await?;
    }
    let server = ApiServer::new(
        store,
        cluster_id,
        ServerSettings::new("127.0.0.1:3000".parse()?, None),
    )?;

    let current = get_deployment(&server, "ready-deployment").await?;
    let initial_revision = current.meta.revision;
    let restarted = command(
        &server,
        "ready-deployment",
        "restart",
        "deployment-restart",
        initial_revision.0,
    )
    .await?;
    assert_eq!(restarted.status(), StatusCode::ACCEPTED);
    let restarted: Value = decode(restarted).await?;
    assert_eq!(restarted.get("generation"), Some(&json!(2)));
    assert_eq!(restarted.get("restartGeneration"), Some(&json!(2)));
    let replay = command(
        &server,
        "ready-deployment",
        "restart",
        "deployment-restart",
        initial_revision.0,
    )
    .await?;
    assert_eq!(replay.status(), StatusCode::ACCEPTED);
    assert_eq!(
        decode::<Value>(replay).await?.get("restartGeneration"),
        Some(&json!(2))
    );

    let current = get_deployment(&server, "ready-deployment").await?;
    let removed = command(
        &server,
        "ready-deployment",
        "remove",
        "deployment-remove",
        current.meta.revision.0,
    )
    .await?;
    assert_eq!(removed.status(), StatusCode::ACCEPTED);
    assert_eq!(
        decode::<Value>(removed).await?.get("goal"),
        Some(&json!("remove"))
    );
    let current = get_deployment(&server, "ready-deployment").await?;
    assert_eq!(
        command(
            &server,
            "ready-deployment",
            "restart",
            "restart-removing",
            current.meta.revision.0,
        )
        .await?
        .status(),
        StatusCode::CONFLICT
    );

    let current = get_deployment(&server, "queued-deployment").await?;
    let canceled = command(
        &server,
        "queued-deployment",
        "cancel",
        "deployment-cancel",
        current.meta.revision.0,
    )
    .await?;
    assert_eq!(canceled.status(), StatusCode::ACCEPTED);
    assert_eq!(
        decode::<Value>(canceled).await?.get("goal"),
        Some(&json!("cancel"))
    );
    assert_eq!(
        command_for_service(
            &server,
            "api",
            "other-deployment",
            "remove",
            "cross-service-remove",
            1,
        )
        .await?
        .status(),
        StatusCode::NOT_FOUND
    );
    Ok(())
}

async fn get_deployment(
    server: &ApiServer,
    deployment_id: &str,
) -> Result<Deployment, Box<dyn std::error::Error>> {
    decode(
        request(
            server,
            &format!("/api/services/api/deployments/{deployment_id}"),
            None,
        )
        .await?,
    )
    .await
}

async fn command(
    server: &ApiServer,
    deployment_id: &str,
    action: &str,
    idempotency_key: &str,
    expected_revision: u64,
) -> Result<axum::response::Response, Box<dyn std::error::Error>> {
    command_for_service(
        server,
        "api",
        deployment_id,
        action,
        idempotency_key,
        expected_revision,
    )
    .await
}

async fn command_for_service(
    server: &ApiServer,
    service_id: &str,
    deployment_id: &str,
    action: &str,
    idempotency_key: &str,
    expected_revision: u64,
) -> Result<axum::response::Response, Box<dyn std::error::Error>> {
    Ok(server
        .router()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri(format!(
                    "/api/services/{service_id}/deployments/{deployment_id}/{action}"
                ))
                .header(header::CONTENT_TYPE, "application/json")
                .header("Idempotency-Key", idempotency_key)
                .body(Body::from(serde_json::to_vec(
                    &json!({"expectedRevision": expected_revision}),
                )?))?,
        )
        .await?)
}
