use axum::extract::State;
use axum::routing::get;
use axum::{Json, Router};
use kernel_api::MaskedClusterConfig;

use crate::{ApiError, AppState};

pub(super) fn router() -> Router<AppState> {
    Router::new().route("/api/config", get(get_config))
}

async fn get_config(State(state): State<AppState>) -> Result<Json<MaskedClusterConfig>, ApiError> {
    let config = state.cluster_config.as_ref().ok_or_else(|| {
        ApiError::service_unavailable("cluster configuration is not available on this API server")
    })?;
    Ok(Json(config.as_ref().clone()))
}
