use axum::extract::rejection::JsonRejection;
use axum::extract::{DefaultBodyLimit, State};
use axum::routing::{get, put};
use axum::{Json, Router};
use cluster::PreviewLaunchConfig;
use kernel_api::{
    MaskedClusterConfig, PreviewLaunchConfigUpdateRequest, PreviewLaunchConfigUpdateResponse,
};

use crate::mutation::{MAXIMUM_REQUEST_BYTES, json_rejection};
use crate::{ApiError, AppState};

pub(super) fn router() -> Router<AppState> {
    Router::new()
        .route("/api/config", get(get_config))
        .layer(DefaultBodyLimit::max(MAXIMUM_REQUEST_BYTES))
}

pub(super) fn admin_router() -> Router<AppState> {
    Router::new()
        .route("/api/config/preview", put(update_preview))
        .layer(DefaultBodyLimit::max(MAXIMUM_REQUEST_BYTES))
}

async fn get_config(State(state): State<AppState>) -> Result<Json<MaskedClusterConfig>, ApiError> {
    let config = state.cluster_config.as_ref().ok_or_else(|| {
        ApiError::service_unavailable("cluster configuration is not available on this API server")
    })?;
    Ok(Json(config.as_ref().clone()))
}

async fn update_preview(
    State(state): State<AppState>,
    payload: Result<Json<PreviewLaunchConfigUpdateRequest>, JsonRejection>,
) -> Result<Json<PreviewLaunchConfigUpdateResponse>, ApiError> {
    let payload = payload
        .map_err(|rejection| json_rejection(rejection, "preview launch-config update"))?
        .0;
    let admin = state.launch_config_admin.as_ref().ok_or_else(|| {
        ApiError::service_unavailable("node-local launch configuration is not available")
    })?;
    if admin.cluster_id() != &payload.cluster_id || admin.node_id() != &payload.node_id {
        return Err(ApiError::conflict(
            "launchConfigTargetMismatch",
            "contacted Admin endpoint does not match the requested cluster and node",
        ));
    }
    let changed = admin
        .replace_preview(PreviewLaunchConfig {
            domain: payload.domain,
            github_token: payload.github_token,
            max_concurrent_previews: payload.max_concurrent_previews,
        })
        .await
        .map_err(|error| {
            ApiError::internal(format!(
                "failed to update protected launch configuration: {error}"
            ))
        })?;
    Ok(Json(PreviewLaunchConfigUpdateResponse {
        node_id: admin.node_id().clone(),
        changed,
    }))
}
