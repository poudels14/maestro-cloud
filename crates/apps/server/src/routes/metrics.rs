use std::sync::Arc;

use axum::extract::{Query, State};
use axum::routing::get;
use axum::{Json, Router};
use kernel_api::Timestamp;
use metrics::{
    DiskInfo, HostMetricComponent, HostMetricHistoryPoint, HostMetricQuery, HostMetricQueryStore,
    HostMetricQueryStoreError, LatestHostMetricQuery, ResourceMetricPoint, ResourceMetricSource,
    WorkloadMetricHistoryPoint, WorkloadMetricQuery, WorkloadMetricQueryStore,
    WorkloadMetricQueryStoreError, project_host_resource_metrics, project_latest_disks,
};
use serde::Deserialize;

use crate::{ApiError, AppState};

const DEFAULT_RANGE_MS: i64 = 3_600_000;
const DEFAULT_LIMIT: usize = 10_000;

pub(super) fn router() -> Router<AppState> {
    Router::new()
        .route("/api/metrics/node", get(node_resource_metrics))
        .route("/api/disks", get(local_disks))
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
    let (from, to, limit) = public_range(&state, parameters)?;
    let query = local_host_query(&state, from, to, HostMetricComponent::Resources, limit)?;
    let history = host_store(&state)?
        .query_host_metrics(&query)
        .await
        .map_err(host_error)?;
    Ok(Json(project_host_resource_metrics(
        &history,
        ResourceMetricSource::Node,
    )))
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

fn public_range(
    state: &AppState,
    parameters: RangeParameters,
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
    Ok((from, to, parameters.limit.unwrap_or(DEFAULT_LIMIT)))
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

fn host_error(error: HostMetricQueryStoreError) -> ApiError {
    ApiError::service_unavailable(error.to_string())
}

fn workload_error(error: WorkloadMetricQueryStoreError) -> ApiError {
    ApiError::service_unavailable(error.to_string())
}
