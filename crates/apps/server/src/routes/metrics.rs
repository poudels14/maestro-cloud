use std::collections::BTreeMap;
use std::num::NonZeroU64;
use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::routing::get;
use axum::{Json, Router};
use futures_util::future::try_join_all;
use kernel_api::Timestamp;
use metrics::{
    DiskInfo, HostMetricComponent, HostMetricHistoryPoint, HostMetricQuery, HostMetricQueryStore,
    HostMetricQueryStoreError, LatestHostMetricQuery, ResourceMetricPoint, ResourceMetricSource,
    WorkloadMetricHistoryPoint, WorkloadMetricQuery, WorkloadMetricQueryStore,
    WorkloadMetricQueryStoreError, aggregate_workload_resource_metrics_by_bucket,
    project_host_resource_metrics, project_latest_disks, project_workload_resource_metrics,
};
use serde::Deserialize;

use crate::{ApiError, AppState};
use crate::{NodeMetricQueryError, NodeMetricQueryStore};

const DEFAULT_RANGE_MS: i64 = 3_600_000;
const DEFAULT_LIMIT: usize = 10_000;
const MAXIMUM_LIMIT: usize = 10_000;
const DEFAULT_WORKLOAD_BUCKET_MS: u64 = 5_000;
const MINIMUM_WORKLOAD_BUCKET_MS: u64 = 1_000;
const MAXIMUM_WORKLOAD_BUCKET_MS: u64 = 3_600_000;

pub(super) fn router() -> Router<AppState> {
    Router::new()
        .route("/api/metrics/node", get(node_resource_metrics))
        .route(
            "/api/metrics/containers",
            get(all_container_resource_metrics),
        )
        .route(
            "/api/services/{service_id}/metrics",
            get(service_resource_metrics),
        )
        .route(
            "/api/services/{service_id}/metrics/containers",
            get(container_resource_metrics),
        )
        .route("/api/disks", get(local_disks))
        .route("/api/disks/nodes", get(cluster_disks))
}

pub(super) fn node_router() -> Router<AppState> {
    Router::new()
        .route("/api/node/metrics/host", get(node_host_history))
        .route("/api/node/metrics/workloads", get(node_workload_history))
        .route("/api/node/disks", get(node_disks))
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RangeParameters {
    from: Option<i64>,
    to: Option<i64>,
    limit: Option<usize>,
    bucket_ms: Option<u64>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct HostParameters {
    from: i64,
    to: i64,
    component: String,
    limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct WorkloadParameters {
    from: i64,
    to: i64,
    service_id: Option<String>,
    deployment_id: Option<String>,
    limit: Option<usize>,
}

async fn node_resource_metrics(
    State(state): State<AppState>,
    Query(parameters): Query<RangeParameters>,
) -> Result<Json<Vec<ResourceMetricPoint>>, ApiError> {
    let (from, to, limit) = public_range(&state, &parameters)?;
    let query = local_host_query(&state, from, to, HostMetricComponent::Resources, limit)?;
    let history = host_store(&state)?
        .query_host_metrics(&query)
        .await
        .map_err(host_error)?;
    Ok(Json(project_host_resource_metrics(
        &history,
        ResourceMetricSource::Node(local_node(&state)?),
    )))
}

async fn all_container_resource_metrics(
    State(state): State<AppState>,
    Query(parameters): Query<RangeParameters>,
) -> Result<Json<Vec<ResourceMetricPoint>>, ApiError> {
    let bucket = workload_bucket(&parameters)?;
    let history = cluster_workload_history(&state, &parameters, None).await?;
    Ok(Json(aggregate_workload_resource_metrics_by_bucket(
        &history,
        ResourceMetricSource::AllContainers,
        bucket,
    )))
}

async fn service_resource_metrics(
    State(state): State<AppState>,
    Path(service_id): Path<String>,
    Query(parameters): Query<RangeParameters>,
) -> Result<Json<Vec<ResourceMetricPoint>>, ApiError> {
    let service_id = crate::routes::deployments::parse_service_id(service_id)?;
    crate::routes::deployments::ensure_service(&state, service_id.clone()).await?;
    let bucket = workload_bucket(&parameters)?;
    let history = cluster_workload_history(&state, &parameters, Some(service_id.clone())).await?;
    Ok(Json(aggregate_workload_resource_metrics_by_bucket(
        &history,
        ResourceMetricSource::Service(service_id),
        bucket,
    )))
}

async fn container_resource_metrics(
    State(state): State<AppState>,
    Path(service_id): Path<String>,
    Query(parameters): Query<RangeParameters>,
) -> Result<Json<Vec<ResourceMetricPoint>>, ApiError> {
    let service_id = crate::routes::deployments::parse_service_id(service_id)?;
    crate::routes::deployments::ensure_service(&state, service_id.clone()).await?;
    let history = cluster_workload_history(&state, &parameters, Some(service_id)).await?;
    Ok(Json(project_workload_resource_metrics(&history)))
}

async fn local_disks(State(state): State<AppState>) -> Result<Json<Vec<DiskInfo>>, ApiError> {
    node_disks(State(state)).await
}

async fn node_host_history(
    State(state): State<AppState>,
    Query(parameters): Query<HostParameters>,
) -> Result<Json<Vec<HostMetricHistoryPoint>>, ApiError> {
    let component = match parameters.component.as_str() {
        "any" => HostMetricComponent::Any,
        "resources" => HostMetricComponent::Resources,
        "disks" => HostMetricComponent::Disks,
        value => {
            return Err(ApiError::bad_request(format!(
                "unknown host metric component `{value}`"
            )));
        }
    };
    let query = local_host_query(
        &state,
        Timestamp(parameters.from),
        Timestamp(parameters.to),
        component,
        parameters.limit.unwrap_or(DEFAULT_LIMIT),
    )?;
    host_store(&state)?
        .query_host_metrics(&query)
        .await
        .map(Json)
        .map_err(host_error)
}

async fn node_workload_history(
    State(state): State<AppState>,
    Query(parameters): Query<WorkloadParameters>,
) -> Result<Json<Vec<WorkloadMetricHistoryPoint>>, ApiError> {
    let mut query = WorkloadMetricQuery::new(
        state.cluster_id.clone(),
        Timestamp(parameters.from),
        Timestamp(parameters.to),
        parameters.limit.unwrap_or(DEFAULT_LIMIT),
    )
    .map_err(|error| ApiError::bad_request(error.to_string()))?
    .with_node(local_node(&state)?);
    if let Some(service_id) = parameters.service_id {
        query = query.with_service(
            kernel_api::ServiceId::new(service_id)
                .map_err(|error| ApiError::bad_request(error.to_string()))?,
        );
    }
    if let Some(deployment_id) = parameters.deployment_id {
        query = query.with_deployment(
            kernel_api::DeploymentId::new(deployment_id)
                .map_err(|error| ApiError::bad_request(error.to_string()))?,
        );
    }
    workload_store(&state)?
        .query_workload_metrics(&query)
        .await
        .map(Json)
        .map_err(workload_error)
}

async fn node_disks(State(state): State<AppState>) -> Result<Json<Vec<DiskInfo>>, ApiError> {
    let node_id = local_node(&state)?;
    let query = LatestHostMetricQuery::new(
        state.cluster_id.clone(),
        HostMetricComponent::Disks,
        DEFAULT_LIMIT,
    )
    .map_err(|error| ApiError::bad_request(error.to_string()))?;
    let points = host_store(&state)?
        .latest_host_metrics(&query)
        .await
        .map_err(host_error)?;
    Ok(Json(
        project_latest_disks(&points)
            .remove(&node_id)
            .unwrap_or_default(),
    ))
}

async fn cluster_disks(
    State(state): State<AppState>,
) -> Result<Json<BTreeMap<kernel_api::NodeId, Vec<DiskInfo>>>, ApiError> {
    let queries = cluster_store(&state)?;
    let futures = state.cluster_metric_nodes.iter().map(|node_id| {
        let queries = queries.clone();
        let cluster_id = state.cluster_id.clone();
        async move {
            let disks = queries
                .query_node_disks(node_id, &cluster_id)
                .await
                .map_err(node_error)?;
            Ok::<_, ApiError>((node_id.clone(), disks))
        }
    });
    Ok(Json(try_join_all(futures).await?.into_iter().collect()))
}

async fn cluster_workload_history(
    state: &AppState,
    parameters: &RangeParameters,
    service_id: Option<kernel_api::ServiceId>,
) -> Result<Vec<WorkloadMetricHistoryPoint>, ApiError> {
    let (from, to, limit) = public_range(state, parameters)?;
    let queries = cluster_store(state)?;
    if state.cluster_metric_nodes.is_empty() {
        return Ok(Vec::new());
    }
    let per_node_limit = limit.saturating_add(state.cluster_metric_nodes.len().saturating_sub(1))
        / state.cluster_metric_nodes.len();
    let futures = state.cluster_metric_nodes.iter().map(|node_id| {
        let queries = queries.clone();
        let query =
            WorkloadMetricQuery::new(state.cluster_id.clone(), from, to, per_node_limit.max(1))
                .map_err(|error| ApiError::bad_request(error.to_string()));
        let service_id = service_id.clone();
        async move {
            let mut query = query?;
            if let Some(service_id) = service_id {
                query = query.with_service(service_id);
            }
            queries
                .query_node_workloads(node_id, &query)
                .await
                .map_err(node_error)
        }
    });
    let mut history = try_join_all(futures)
        .await?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    history.sort_by(|left, right| {
        left.point
            .id
            .collected_at
            .cmp(&right.point.id.collected_at)
            .then_with(|| left.point.id.node_id.cmp(&right.point.id.node_id))
            .then_with(|| left.point.id.workload_id.cmp(&right.point.id.workload_id))
    });
    history.truncate(limit);
    Ok(history)
}

fn public_range(
    state: &AppState,
    parameters: &RangeParameters,
) -> Result<(Timestamp, Timestamp, usize), ApiError> {
    let to = Timestamp(
        parameters
            .to
            .unwrap_or_else(|| state.timestamp_clock.now().0),
    );
    let from = Timestamp(
        parameters
            .from
            .unwrap_or_else(|| to.0.saturating_sub(DEFAULT_RANGE_MS)),
    );
    if from.0 > to.0 {
        return Err(ApiError::bad_request(
            "metric query start must not be after its end",
        ));
    }
    let limit = parameters.limit.unwrap_or(DEFAULT_LIMIT);
    if !(1..=MAXIMUM_LIMIT).contains(&limit) {
        return Err(ApiError::bad_request(format!(
            "metric query limit must be between 1 and {MAXIMUM_LIMIT}"
        )));
    }
    Ok((from, to, limit))
}

fn workload_bucket(parameters: &RangeParameters) -> Result<NonZeroU64, ApiError> {
    let bucket_ms = parameters.bucket_ms.unwrap_or(DEFAULT_WORKLOAD_BUCKET_MS);
    if !(MINIMUM_WORKLOAD_BUCKET_MS..=MAXIMUM_WORKLOAD_BUCKET_MS).contains(&bucket_ms) {
        return Err(ApiError::bad_request(format!(
            "metric bucketMs must be between {MINIMUM_WORKLOAD_BUCKET_MS} and {MAXIMUM_WORKLOAD_BUCKET_MS}"
        )));
    }
    NonZeroU64::new(bucket_ms)
        .ok_or_else(|| ApiError::bad_request("metric bucketMs must be non-zero"))
}

fn local_host_query(
    state: &AppState,
    from: Timestamp,
    to: Timestamp,
    component: HostMetricComponent,
    limit: usize,
) -> Result<HostMetricQuery, ApiError> {
    HostMetricQuery::new(
        state.cluster_id.clone(),
        Some(local_node(state)?),
        from,
        to,
        component,
        limit,
    )
    .map_err(|error| ApiError::bad_request(error.to_string()))
}

fn local_node(state: &AppState) -> Result<kernel_api::NodeId, ApiError> {
    state.local_metric_node.clone().ok_or_else(|| {
        ApiError::service_unavailable("node-local metric queries are not configured")
    })
}

fn host_store(state: &AppState) -> Result<Arc<dyn HostMetricQueryStore>, ApiError> {
    state
        .host_metric_queries
        .clone()
        .ok_or_else(|| ApiError::service_unavailable("host metric query store is not configured"))
}

fn workload_store(state: &AppState) -> Result<Arc<dyn WorkloadMetricQueryStore>, ApiError> {
    state.workload_metric_queries.clone().ok_or_else(|| {
        ApiError::service_unavailable("workload metric query store is not configured")
    })
}

fn cluster_store(state: &AppState) -> Result<Arc<dyn NodeMetricQueryStore>, ApiError> {
    state.cluster_metric_queries.clone().ok_or_else(|| {
        ApiError::service_unavailable("cluster metric query store is not configured")
    })
}

fn node_error(error: NodeMetricQueryError) -> ApiError {
    match error {
        NodeMetricQueryError::Rejected { message } => ApiError::bad_request(message),
        NodeMetricQueryError::Unavailable { message } => ApiError::service_unavailable(message),
    }
}

fn host_error(error: HostMetricQueryStoreError) -> ApiError {
    ApiError::service_unavailable(error.to_string())
}

fn workload_error(error: WorkloadMetricQueryStoreError) -> ApiError {
    ApiError::service_unavailable(error.to_string())
}
