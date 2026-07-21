use axum::extract::{Path, State};
use axum::routing::get;
use axum::{Json, Router};
use kernel_api::{BuiltinKind, Deployment, DeploymentId, Service, ServiceId};

use crate::{ApiError, AppState, mask, resource};

pub(super) fn router() -> Router<AppState> {
    Router::new()
        .route(
            "/api/services/{service_id}/deployments",
            get(list_deployments),
        )
        .route(
            "/api/services/{service_id}/deployments/{deployment_id}",
            get(get_deployment),
        )
}

async fn list_deployments(
    State(state): State<AppState>,
    Path(service_id): Path<String>,
) -> Result<Json<Vec<Deployment>>, ApiError> {
    let service_id = parse_service_id(service_id)?;
    ensure_service(&state, service_id.clone()).await?;
    let deployments: Vec<Deployment> = resource::list(&state, BuiltinKind::Deployment).await?;
    Ok(Json(
        deployments
            .into_iter()
            .filter(|deployment| deployment.spec.service_id == service_id)
            .map(mask_deployment)
            .collect(),
    ))
}

async fn get_deployment(
    State(state): State<AppState>,
    Path((service_id, deployment_id)): Path<(String, String)>,
) -> Result<Json<Deployment>, ApiError> {
    let service_id = parse_service_id(service_id)?;
    ensure_service(&state, service_id.clone()).await?;
    Ok(Json(mask_deployment(
        owned_deployment(&state, &service_id, deployment_id).await?,
    )))
}

pub(super) fn parse_service_id(service_id: String) -> Result<ServiceId, ApiError> {
    ServiceId::new(service_id).map_err(|error| ApiError::bad_request(error.to_string()))
}

pub(super) async fn ensure_service(
    state: &AppState,
    service_id: ServiceId,
) -> Result<(), ApiError> {
    let _: Service = resource::get(state, BuiltinKind::Service, service_id).await?;
    Ok(())
}

pub(super) async fn owned_deployment(
    state: &AppState,
    service_id: &ServiceId,
    deployment_id: String,
) -> Result<Deployment, ApiError> {
    let deployment_id = DeploymentId::new(deployment_id)
        .map_err(|error| ApiError::bad_request(error.to_string()))?;
    let deployment: Deployment =
        resource::get(state, BuiltinKind::Deployment, deployment_id.clone()).await?;
    if deployment.spec.service_id != *service_id {
        return Err(ApiError::not_found(format!(
            "Deployment `{deployment_id}` does not exist for Service `{service_id}`"
        )));
    }
    Ok(deployment)
}

fn mask_deployment(mut deployment: Deployment) -> Deployment {
    mask::service_spec(&mut deployment.spec.service);
    deployment
}
