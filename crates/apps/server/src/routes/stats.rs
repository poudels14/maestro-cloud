use std::collections::BTreeMap;
use std::sync::Arc;

use axum::extract::State;
use axum::routing::get;
use axum::{Json, Router};
use futures_util::future::try_join_all;
use logs::{
    BackupStatsSnapshot, ClusterStatsResponse, ControllerStatsProvider, ControllerStatsSnapshot,
    ProbeStatsSnapshot, derive_stats_warnings,
};

use crate::{ApiError, AppState, NodeStatsQueryError, NodeStatsQueryStore};

pub(super) fn router() -> Router<AppState> {
    Router::new()
        .route("/api/cluster/stats", get(cluster_stats))
        .route("/api/cluster/stats/nodes", get(cluster_node_stats))
}

pub(super) fn node_router() -> Router<AppState> {
    Router::new().route("/api/node/stats", get(node_stats))
}

async fn cluster_stats(
    State(state): State<AppState>,
) -> Result<Json<ClusterStatsResponse>, ApiError> {
    let now = state.timestamp_clock.now().0;
    let controller = controller_provider(&state)?
        .controller_stats(now)
        .await
        .map_err(store_error)?;
    let heartbeat_age_ms = Some(
        now.saturating_sub(controller.reported_at_ms)
            .try_into()
            .unwrap_or_default(),
    );
    let backup = backup_stats(&state)?;
    let warnings = derive_stats_warnings(
        Some(&controller),
        &backup,
        heartbeat_age_ms,
        now,
        env!("CARGO_PKG_VERSION"),
    );
    Ok(Json(ClusterStatsResponse {
        generated_at_ms: now,
        probe: ProbeStatsSnapshot {
            version: env!("CARGO_PKG_VERSION").to_owned(),
            uptime_ms: state
                .started_at
                .elapsed()
                .as_millis()
                .try_into()
                .unwrap_or(u64::MAX),
        },
        controller: Some(controller),
        controller_heartbeat_age_ms: heartbeat_age_ms,
        backup,
        warnings,
    }))
}

async fn node_stats(
    State(state): State<AppState>,
) -> Result<Json<ControllerStatsSnapshot>, ApiError> {
    controller_provider(&state)?
        .controller_stats(state.timestamp_clock.now().0)
        .await
        .map(Json)
        .map_err(store_error)
}

async fn cluster_node_stats(
    State(state): State<AppState>,
) -> Result<Json<BTreeMap<kernel_api::NodeId, ControllerStatsSnapshot>>, ApiError> {
    let queries = cluster_provider(&state)?;
    let now = state.timestamp_clock.now().0;
    let futures = state.cluster_stats_nodes.iter().map(|node_id| {
        let queries = queries.clone();
        async move {
            let stats = queries
                .query_node_stats(node_id, now)
                .await
                .map_err(node_error)?;
            Ok::<_, ApiError>((node_id.clone(), stats))
        }
    });
    Ok(Json(try_join_all(futures).await?.into_iter().collect()))
}

fn controller_provider(state: &AppState) -> Result<Arc<dyn ControllerStatsProvider>, ApiError> {
    state
        .controller_stats
        .clone()
        .ok_or_else(|| ApiError::service_unavailable("controller stats provider is not configured"))
}

fn cluster_provider(state: &AppState) -> Result<Arc<dyn NodeStatsQueryStore>, ApiError> {
    state
        .cluster_stats_queries
        .clone()
        .ok_or_else(|| ApiError::service_unavailable("cluster stats query store is not configured"))
}

fn backup_stats(state: &AppState) -> Result<BackupStatsSnapshot, ApiError> {
    state.backup_stats.as_ref().map_or_else(
        || Ok(BackupStatsSnapshot::default()),
        |provider| {
            provider
                .backup_stats()
                .map_err(|error| ApiError::service_unavailable(error.to_string()))
        },
    )
}

fn store_error(error: logs::LogStatsStoreError) -> ApiError {
    match error {
        logs::LogStatsStoreError::Rejected { message } => ApiError::bad_request(message),
        logs::LogStatsStoreError::Unavailable { message } => ApiError::service_unavailable(message),
    }
}

fn node_error(error: NodeStatsQueryError) -> ApiError {
    match error {
        NodeStatsQueryError::Rejected { message } => ApiError::bad_request(message),
        NodeStatsQueryError::Unavailable { message } => ApiError::service_unavailable(message),
    }
}
