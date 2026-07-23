use std::collections::BTreeMap;
use std::sync::Arc;

use axum::extract::{Query, State};
use axum::routing::get;
use axum::{Json, Router};
use futures_util::future::try_join_all;
use logs::{
    BackupStatsSnapshot, ClusterStatsResponse, ControllerStatsProvider, ControllerStatsSnapshot,
    ProbeStatsSnapshot, StatsMetricPoint, StatsMetricQuery, StatsMetricStore,
    StatsMetricStoreError, derive_stats_warnings,
};
use serde::Deserialize;

use crate::{ApiError, AppState, NodeStatsQueryError, NodeStatsQueryStore};

pub(super) fn router() -> Router<AppState> {
    Router::new()
        .route("/api/cluster/stats", get(cluster_stats))
        .route("/api/cluster/stats/nodes", get(cluster_node_stats))
        .route("/api/metrics/stats", get(cluster_stats_metrics))
}

pub(super) fn node_router() -> Router<AppState> {
    Router::new()
        .route("/api/node/stats", get(node_stats))
        .route("/api/node/metrics/stats", get(node_stats_metrics))
}

const DEFAULT_RANGE_MS: i64 = 3_600_000;

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct StatsMetricParameters {
    name: Option<String>,
    from: Option<i64>,
    to: Option<i64>,
    limit: Option<usize>,
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
                .uptime_clock
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

async fn cluster_stats_metrics(
    State(state): State<AppState>,
    Query(parameters): Query<StatsMetricParameters>,
) -> Result<Json<Vec<StatsMetricPoint>>, ApiError> {
    let query = public_stats_metric_query(&state, parameters)?;
    let queries = cluster_provider(&state)?;
    if state.cluster_stats_nodes.is_empty() {
        return Ok(Json(Vec::new()));
    }
    let per_node_limit = query
        .limit()
        .saturating_add(state.cluster_stats_nodes.len().saturating_sub(1))
        / state.cluster_stats_nodes.len();
    let node_query = StatsMetricQuery::new(
        query.name().map(str::to_owned),
        query.from(),
        query.to(),
        per_node_limit.max(1),
    )
    .map_err(metric_store_error)?;
    let futures = state.cluster_stats_nodes.iter().map(|node_id| {
        let queries = queries.clone();
        let query = node_query.clone();
        async move {
            queries
                .query_node_stats_metrics(node_id, &query)
                .await
                .map_err(node_error)
        }
    });
    let mut points = try_join_all(futures)
        .await?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    points.sort_by(|left, right| {
        left.ts
            .cmp(&right.ts)
            .then_with(|| left.name.cmp(&right.name))
            .then_with(|| left.labels.cmp(&right.labels))
    });
    points.truncate(query.limit());
    Ok(Json(points))
}

async fn node_stats_metrics(
    State(state): State<AppState>,
    Query(parameters): Query<StatsMetricParameters>,
) -> Result<Json<Vec<StatsMetricPoint>>, ApiError> {
    let from = parameters
        .from
        .ok_or_else(|| ApiError::bad_request("node stats metric query requires `from`"))?;
    let to = parameters
        .to
        .ok_or_else(|| ApiError::bad_request("node stats metric query requires `to`"))?;
    let query = StatsMetricQuery::new(
        parameters.name,
        from,
        to,
        parameters
            .limit
            .unwrap_or(logs::MAXIMUM_STATS_METRIC_QUERY_LIMIT),
    )
    .map_err(metric_store_error)?;
    stats_metric_store(&state)?
        .query_stats_metrics(&query)
        .await
        .map(Json)
        .map_err(metric_store_error)
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

fn stats_metric_store(state: &AppState) -> Result<Arc<dyn StatsMetricStore>, ApiError> {
    state
        .stats_metrics
        .clone()
        .ok_or_else(|| ApiError::service_unavailable("stats metric query store is not configured"))
}

fn public_stats_metric_query(
    state: &AppState,
    parameters: StatsMetricParameters,
) -> Result<StatsMetricQuery, ApiError> {
    let to = parameters
        .to
        .unwrap_or_else(|| state.timestamp_clock.now().0);
    let from = parameters
        .from
        .unwrap_or_else(|| to.saturating_sub(DEFAULT_RANGE_MS));
    StatsMetricQuery::new(
        parameters.name,
        from,
        to,
        parameters
            .limit
            .unwrap_or(logs::MAXIMUM_STATS_METRIC_QUERY_LIMIT),
    )
    .map_err(metric_store_error)
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

fn metric_store_error(error: StatsMetricStoreError) -> ApiError {
    match error {
        StatsMetricStoreError::Rejected { message } => ApiError::bad_request(message),
        StatsMetricStoreError::Unavailable { message } => ApiError::service_unavailable(message),
    }
}

fn node_error(error: NodeStatsQueryError) -> ApiError {
    match error {
        NodeStatsQueryError::Rejected { message } => ApiError::bad_request(message),
        NodeStatsQueryError::Unavailable { message } => ApiError::service_unavailable(message),
    }
}
