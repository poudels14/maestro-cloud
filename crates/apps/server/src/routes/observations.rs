use axum::extract::{Path, State};
use axum::routing::get;
use axum::{Json, Router};
use kernel_api::{
    Assignment, AssignmentId, Build, BuildId, BuiltinKind, DeploymentId, ReplicaState,
    ReplicaStateId, ServiceId,
};

use super::deployments::{ensure_service, owned_deployment, parse_service_id};
use crate::{ApiError, AppState, mask, resource};

pub(super) fn router() -> Router<AppState> {
    Router::new()
        .route(
            "/api/services/{service_id}/deployments/{deployment_id}/assignments",
            get(list_assignments),
        )
        .route(
            "/api/services/{service_id}/deployments/{deployment_id}/assignments/{assignment_id}",
            get(get_assignment),
        )
        .route(
            "/api/services/{service_id}/deployments/{deployment_id}/replicas",
            get(list_replicas),
        )
        .route(
            "/api/services/{service_id}/deployments/{deployment_id}/replicas/{replica_id}",
            get(get_replica),
        )
        .route("/api/services/{service_id}/builds", get(list_builds))
        .route(
            "/api/services/{service_id}/builds/{build_id}",
            get(get_build),
        )
}

async fn list_assignments(
    State(state): State<AppState>,
    Path((service_id, deployment_id)): Path<(String, String)>,
) -> Result<Json<Vec<Assignment>>, ApiError> {
    let (service_id, deployment_id) = scope(&state, service_id, deployment_id).await?;
    let assignments: Vec<Assignment> = resource::list(&state, BuiltinKind::Assignment).await?;
    Ok(Json(
        assignments
            .into_iter()
            .filter(|assignment| {
                assignment.spec.service_id == service_id
                    && assignment.spec.deployment_id == deployment_id
            })
            .collect(),
    ))
}

async fn get_assignment(
    State(state): State<AppState>,
    Path((service_id, deployment_id, assignment_id)): Path<(String, String, String)>,
) -> Result<Json<Assignment>, ApiError> {
    let (service_id, deployment_id) = scope(&state, service_id, deployment_id).await?;
    let assignment_id = AssignmentId::new(assignment_id)
        .map_err(|error| ApiError::bad_request(error.to_string()))?;
    let assignment: Assignment =
        resource::get(&state, BuiltinKind::Assignment, assignment_id.clone()).await?;
    ensure_assignment_scope(&assignment, &service_id, &deployment_id, &assignment_id)?;
    Ok(Json(assignment))
}

async fn list_replicas(
    State(state): State<AppState>,
    Path((service_id, deployment_id)): Path<(String, String)>,
) -> Result<Json<Vec<ReplicaState>>, ApiError> {
    let (service_id, deployment_id) = scope(&state, service_id, deployment_id).await?;
    let replicas: Vec<ReplicaState> = resource::list(&state, BuiltinKind::ReplicaState).await?;
    Ok(Json(
        replicas
            .into_iter()
            .filter(|replica| {
                replica.spec.service_id == service_id && replica.spec.deployment_id == deployment_id
            })
            .collect(),
    ))
}

async fn get_replica(
    State(state): State<AppState>,
    Path((service_id, deployment_id, replica_id)): Path<(String, String, String)>,
) -> Result<Json<ReplicaState>, ApiError> {
    let (service_id, deployment_id) = scope(&state, service_id, deployment_id).await?;
    let replica_id = ReplicaStateId::new(replica_id)
        .map_err(|error| ApiError::bad_request(error.to_string()))?;
    let replica: ReplicaState =
        resource::get(&state, BuiltinKind::ReplicaState, replica_id.clone()).await?;
    if replica.spec.service_id != service_id || replica.spec.deployment_id != deployment_id {
        return Err(ApiError::not_found(format!(
            "ReplicaState `{replica_id}` does not exist in this deployment"
        )));
    }
    Ok(Json(replica))
}

async fn list_builds(
    State(state): State<AppState>,
    Path(service_id): Path<String>,
) -> Result<Json<Vec<Build>>, ApiError> {
    let service_id = parse_service_id(service_id)?;
    ensure_service(&state, service_id.clone()).await?;
    let builds: Vec<Build> = resource::list(&state, BuiltinKind::Build).await?;
    Ok(Json(
        builds
            .into_iter()
            .filter(|build| build.spec.service_id == service_id)
            .map(mask::build)
            .collect(),
    ))
}

async fn get_build(
    State(state): State<AppState>,
    Path((service_id, build_id)): Path<(String, String)>,
) -> Result<Json<Build>, ApiError> {
    let service_id = parse_service_id(service_id)?;
    ensure_service(&state, service_id.clone()).await?;
    let build = owned_build(&state, &service_id, build_id).await?;
    Ok(Json(mask::build(build)))
}

pub(super) async fn owned_build(
    state: &AppState,
    service_id: &ServiceId,
    build_id: String,
) -> Result<Build, ApiError> {
    let build_id =
        BuildId::new(build_id).map_err(|error| ApiError::bad_request(error.to_string()))?;
    let build: Build = resource::get(state, BuiltinKind::Build, build_id.clone()).await?;
    if build.spec.service_id != *service_id {
        return Err(ApiError::not_found(format!(
            "Build `{build_id}` does not exist for Service `{service_id}`"
        )));
    }
    Ok(build)
}

async fn scope(
    state: &AppState,
    service_id: String,
    deployment_id: String,
) -> Result<(ServiceId, DeploymentId), ApiError> {
    let service_id = parse_service_id(service_id)?;
    ensure_service(state, service_id.clone()).await?;
    let deployment = owned_deployment(state, &service_id, deployment_id).await?;
    Ok((service_id, deployment.meta.id))
}

fn ensure_assignment_scope(
    assignment: &Assignment,
    service_id: &ServiceId,
    deployment_id: &DeploymentId,
    assignment_id: &AssignmentId,
) -> Result<(), ApiError> {
    if assignment.spec.service_id != *service_id || assignment.spec.deployment_id != *deployment_id
    {
        Err(ApiError::not_found(format!(
            "Assignment `{assignment_id}` does not exist in this deployment"
        )))
    } else {
        Ok(())
    }
}
