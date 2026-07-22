use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::routing::get;
use axum::{Json, Router};
use kernel_api::{NodeId, ServiceId, Timestamp};
use logs::{
    ClusterTrafficQueryCoordinator, IngressTrafficBreakdown, IngressTrafficQuery,
    IngressTrafficScope, MAXIMUM_TRAFFIC_METRIC_LIMIT, ServiceTrafficQuery, TrafficMetricPoint,
    TrafficQueryError, TrafficQueryStore,
};
use serde::Deserialize;

use crate::routes::deployments::{ensure_service, parse_service_id};
use crate::{ApiError, AppState};

const DEFAULT_TRAFFIC_RANGE_MS: i64 = 3_600_000;
const DEFAULT_BREAKDOWN_LIMIT: usize = 100;

pub(super) fn router() -> Router<AppState> {
    Router::new()
        .route("/api/ingress/traffic", get(ingress_traffic))
        .route("/api/ingress/blocked-traffic", get(blocked_ingress_traffic))
        .route("/api/services/{service_id}/traffic", get(service_traffic))
        .route(
            "/api/services/{service_id}/traffic/breakdown",
            get(service_traffic_breakdown),
        )
}

pub(super) fn node_router() -> Router<AppState> {
    Router::new()
        .route("/api/node/traffic/ingress", get(node_ingress_traffic))
        .route("/api/node/traffic/service", get(node_service_traffic))
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TrafficParameters {
    from: Option<i64>,
    to: Option<i64>,
    limit: Option<usize>,
    node_id: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct NodeIngressTrafficParameters {
    scope: String,
    router_prefix: String,
    from: i64,
    to: i64,
    limit: usize,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct NodeServiceTrafficParameters {
    service_id: String,
    router_prefix: String,
    from: i64,
    to: i64,
    limit: usize,
}

async fn ingress_traffic(
    State(state): State<AppState>,
    Query(parameters): Query<TrafficParameters>,
) -> Result<Json<IngressTrafficBreakdown>, ApiError> {
    cluster_ingress_traffic(
        &state,
        IngressTrafficScope::Cluster {
            blocked_router_prefix: ingress::TRAEFIK_BLOCKED_ROUTER_PREFIX.to_owned(),
        },
        parameters,
    )
    .await
}

async fn blocked_ingress_traffic(
    State(state): State<AppState>,
    Query(parameters): Query<TrafficParameters>,
) -> Result<Json<IngressTrafficBreakdown>, ApiError> {
    cluster_ingress_traffic(
        &state,
        IngressTrafficScope::Blocked {
            router_prefix: ingress::TRAEFIK_BLOCKED_ROUTER_PREFIX.to_owned(),
        },
        parameters,
    )
    .await
}

async fn service_traffic_breakdown(
    State(state): State<AppState>,
    Path(service_id): Path<String>,
    Query(parameters): Query<TrafficParameters>,
) -> Result<Json<IngressTrafficBreakdown>, ApiError> {
    let service_id = owned_service(&state, service_id).await?;
    cluster_ingress_traffic(
        &state,
        IngressTrafficScope::Service {
            router_prefix: ingress::traefik_service_router_prefix(&service_id),
        },
        parameters,
    )
    .await
}

async fn service_traffic(
    State(state): State<AppState>,
    Path(service_id): Path<String>,
    Query(parameters): Query<TrafficParameters>,
) -> Result<Json<Vec<TrafficMetricPoint>>, ApiError> {
    let service_id = owned_service(&state, service_id).await?;
    let (from, to) = traffic_range(&state, parameters.from, parameters.to);
    let query = ServiceTrafficQuery::new(
        service_id.clone(),
        ingress::traefik_service_router_prefix(&service_id),
        from,
        to,
        parameters.limit.unwrap_or(MAXIMUM_TRAFFIC_METRIC_LIMIT),
    )
    .map_err(traffic_error)?;
    let nodes = selected_nodes(&state, parameters.node_id.as_deref())?;
    cluster_store(&state)?
        .query_service_traffic(&nodes, &query)
        .await
        .map(Json)
        .map_err(traffic_error)
}

async fn cluster_ingress_traffic(
    state: &AppState,
    scope: IngressTrafficScope,
    parameters: TrafficParameters,
) -> Result<Json<IngressTrafficBreakdown>, ApiError> {
    let (from, to) = traffic_range(state, parameters.from, parameters.to);
    let query = IngressTrafficQuery::new(
        scope,
        from,
        to,
        parameters.limit.unwrap_or(DEFAULT_BREAKDOWN_LIMIT),
    )
    .map_err(traffic_error)?;
    let nodes = selected_nodes(state, parameters.node_id.as_deref())?;
    cluster_store(state)?
        .query_ingress_traffic(&nodes, &query)
        .await
        .map(Json)
        .map_err(traffic_error)
}

async fn node_ingress_traffic(
    State(state): State<AppState>,
    Query(parameters): Query<NodeIngressTrafficParameters>,
) -> Result<Json<IngressTrafficBreakdown>, ApiError> {
    let scope = match parameters.scope.as_str() {
        "cluster" => IngressTrafficScope::Cluster {
            blocked_router_prefix: parameters.router_prefix,
        },
        "blocked" => IngressTrafficScope::Blocked {
            router_prefix: parameters.router_prefix,
        },
        "service" => IngressTrafficScope::Service {
            router_prefix: parameters.router_prefix,
        },
        value => {
            return Err(ApiError::bad_request(format!(
                "unknown node traffic scope `{value}`"
            )));
        }
    };
    let query = IngressTrafficQuery::new(
        scope,
        Timestamp(parameters.from),
        Timestamp(parameters.to),
        parameters.limit,
    )
    .map_err(traffic_error)?;
    local_store(&state)?
        .query_ingress_traffic(&query)
        .await
        .map(Json)
        .map_err(traffic_error)
}

async fn node_service_traffic(
    State(state): State<AppState>,
    Query(parameters): Query<NodeServiceTrafficParameters>,
) -> Result<Json<Vec<TrafficMetricPoint>>, ApiError> {
    let service_id = ServiceId::new(parameters.service_id)
        .map_err(|error| ApiError::bad_request(error.to_string()))?;
    let query = ServiceTrafficQuery::new(
        service_id,
        parameters.router_prefix,
        Timestamp(parameters.from),
        Timestamp(parameters.to),
        parameters.limit,
    )
    .map_err(traffic_error)?;
    local_store(&state)?
        .query_service_traffic(&query)
        .await
        .map(Json)
        .map_err(traffic_error)
}

async fn owned_service(state: &AppState, service_id: String) -> Result<ServiceId, ApiError> {
    let service_id = parse_service_id(service_id)?;
    ensure_service(state, service_id.clone()).await?;
    Ok(service_id)
}

fn traffic_range(state: &AppState, from: Option<i64>, to: Option<i64>) -> (Timestamp, Timestamp) {
    let to = Timestamp(to.unwrap_or_else(|| state.timestamp_clock.now().0));
    let from = Timestamp(from.unwrap_or_else(|| to.0.saturating_sub(DEFAULT_TRAFFIC_RANGE_MS)));
    (from, to)
}

fn selected_nodes(state: &AppState, node_id: Option<&str>) -> Result<Vec<NodeId>, ApiError> {
    let Some(node_id) = node_id else {
        return Ok(state.cluster_traffic_nodes.to_vec());
    };
    let node_id = NodeId::new(node_id).map_err(|error| ApiError::bad_request(error.to_string()))?;
    if state.cluster_traffic_nodes.binary_search(&node_id).is_err() {
        return Err(ApiError::bad_request(format!(
            "traffic node `{node_id}` is outside this topology"
        )));
    }
    Ok(vec![node_id])
}

fn local_store(state: &AppState) -> Result<Arc<dyn TrafficQueryStore>, ApiError> {
    state.traffic_queries.clone().ok_or_else(|| {
        ApiError::service_unavailable("traffic queries are not configured on this node")
    })
}

fn cluster_store(state: &AppState) -> Result<Arc<ClusterTrafficQueryCoordinator>, ApiError> {
    state.cluster_traffic_queries.clone().ok_or_else(|| {
        ApiError::service_unavailable("cluster traffic queries are not configured on this node")
    })
}

fn traffic_error(error: TrafficQueryError) -> ApiError {
    match error {
        TrafficQueryError::Rejected { message } => ApiError::bad_request(message),
        TrafficQueryError::Unavailable { message } => ApiError::service_unavailable(message),
    }
}
