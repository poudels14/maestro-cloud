use axum::http::StatusCode;
use kernel_api::{
    Deployment, DeploymentGoal, DeploymentId, DeploymentPhase, DeploymentSpec, DeploymentStatus,
    ServiceId, Timestamp,
};

use crate::{ApiServer, ServerSettings};

use super::{decode, metadata, put, request, seeded_store, service};

#[tokio::test]
async fn deployment_history_is_service_scoped_and_masks_captured_secrets()
-> Result<(), Box<dyn std::error::Error>> {
    let (store, cluster_id) = seeded_store().await?;
    let owned = deployment("api-deployment", "api")?;
    let unrelated = deployment("other-deployment", "other")?;
    put(
        &store,
        &cluster_id,
        "Deployment",
        owned.meta.id.as_str(),
        &owned,
    )
    .await?;
    put(
        &store,
        &cluster_id,
        "Deployment",
        unrelated.meta.id.as_str(),
        &unrelated,
    )
    .await?;
    let server = ApiServer::new(
        store,
        cluster_id,
        ServerSettings::new("127.0.0.1:3000".parse()?, None),
    )?;

    let response = request(&server, "/api/services/api/deployments", None).await?;
    assert_eq!(response.status(), StatusCode::OK);
    let history: Vec<Deployment> = decode(response).await?;
    assert_eq!(history.len(), 1);
    let listed = history.first().ok_or("deployment history is empty")?;
    assert_eq!(listed.meta.id, owned.meta.id);
    assert!(listed.meta.revision.0 > 0);
    let encoded = serde_json::to_string(&history)?;
    assert!(!encoded.contains("database-password"));
    assert!(encoded.contains("••••word"));

    let response = request(
        &server,
        "/api/services/api/deployments/api-deployment",
        None,
    )
    .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let fetched: Deployment = decode(response).await?;
    assert_eq!(fetched.meta.id, owned.meta.id);
    assert_eq!(
        request(
            &server,
            "/api/services/api/deployments/other-deployment",
            None,
        )
        .await?
        .status(),
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        request(&server, "/api/services/missing/deployments", None)
            .await?
            .status(),
        StatusCode::NOT_FOUND
    );
    Ok(())
}

fn deployment(id: &str, service_id: &str) -> Result<Deployment, kernel_api::InvalidIdentifier> {
    let captured_service = service()?.spec;
    Ok(Deployment {
        meta: metadata(DeploymentId::new(id)?),
        spec: DeploymentSpec {
            service_id: ServiceId::new(service_id)?,
            service_generation: kernel_api::Generation(1),
            restart_generation: kernel_api::Generation(1),
            service: captured_service,
            goal: DeploymentGoal::Run,
            build_id: None,
        },
        status: DeploymentStatus {
            phase: DeploymentPhase::Ready,
            created_at: Timestamp(1_000),
            ready_at: Some(Timestamp(2_000)),
            draining_at: None,
            image_digest: Some("registry.test/api@sha256:abc".to_string()),
            conditions: Vec::new(),
        },
    })
}
