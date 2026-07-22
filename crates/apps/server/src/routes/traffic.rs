use std::collections::{BTreeMap, BTreeSet};
use std::net::IpAddr;
use std::sync::Arc;

use axum::extract::DefaultBodyLimit;
use axum::extract::rejection::JsonRejection;
use axum::extract::{Path, Query, State};
use axum::routing::get;
use axum::{Json, Router};
use kernel_api::{
    BuiltinKind, Generation, IngressBlocklist, IngressBlocklistId, IngressBlocklistSpec,
    IngressBlocklistStatus, IngressRouting, NodeId, Object, ObjectMeta, ResourceRevision,
    ServiceId, Timestamp, TrafficGeneration,
};
use kernel_store::{CasOutcome, ExpectedVersion, Keyspace, PutRequest};
use logs::{
    ClusterTrafficQueryCoordinator, IngressTrafficBreakdown, IngressTrafficQuery,
    IngressTrafficScope, MAXIMUM_TRAFFIC_METRIC_LIMIT, ServiceTrafficQuery, TrafficMetricPoint,
    TrafficQueryError, TrafficQueryStore,
};
use serde::Deserialize;

use crate::routes::deployments::{ensure_service, parse_service_id};
use crate::routes::service_commands::next_generation;
use crate::{ApiError, AppState, mutation, resource};

const DEFAULT_TRAFFIC_RANGE_MS: i64 = 3_600_000;
const DEFAULT_BREAKDOWN_LIMIT: usize = 100;
const BLOCKLIST_ID: &str = "global";
const MAXIMUM_BLOCKLIST_CAS_ATTEMPTS: usize = 8;

pub(super) fn router() -> Router<AppState> {
    Router::new()
        .route("/api/ingress/traffic", get(ingress_traffic))
        .route("/api/ingress/routes", get(ingress_routes))
        .route("/api/ingress/blocked-traffic", get(blocked_ingress_traffic))
        .route(
            "/api/ingress/blocked-ips",
            get(blocked_ips).patch(set_blocked_ip),
        )
        .route("/api/services/{service_id}/traffic", get(service_traffic))
        .route(
            "/api/services/{service_id}/traffic/breakdown",
            get(service_traffic_breakdown),
        )
        .layer(DefaultBodyLimit::max(mutation::MAXIMUM_REQUEST_BYTES))
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

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct BlockedIpRequest {
    ip: String,
    blocked: bool,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct BlockedIpsResponse {
    blocked_ips: Vec<String>,
}

async fn blocked_ips(State(state): State<AppState>) -> Result<Json<BlockedIpsResponse>, ApiError> {
    Ok(Json(blocklist_response(
        read_blocklist(&state).await?.as_ref(),
    )))
}

async fn ingress_routes(
    State(state): State<AppState>,
) -> Result<Json<Vec<IngressRouting>>, ApiError> {
    let generations: Vec<TrafficGeneration> =
        resource::list(&state, BuiltinKind::TrafficGeneration).await?;
    Ok(Json(ingress::active_routing(&generations)))
}

async fn set_blocked_ip(
    State(state): State<AppState>,
    payload: Result<Json<BlockedIpRequest>, JsonRejection>,
) -> Result<Json<BlockedIpsResponse>, ApiError> {
    let request = payload
        .map_err(|rejection| mutation::json_rejection(rejection, "ingress blocklist"))?
        .0;
    let address = request
        .ip
        .trim()
        .parse::<IpAddr>()
        .map_err(|_| ApiError::bad_request(format!("invalid IP address `{}`", request.ip)))?;
    mutate_blocklist(&state, address, request.blocked)
        .await
        .map(|blocklist| Json(blocklist_response(blocklist.as_ref())))
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

async fn mutate_blocklist(
    state: &AppState,
    address: IpAddr,
    blocked: bool,
) -> Result<Option<IngressBlocklist>, ApiError> {
    let keys = Keyspace::new(&state.cluster_id);
    let kind = blocklist_kind()?;
    let id = blocklist_id()?;
    let key = keys.resource(&kind, &id.clone().into());
    for _attempt in 0..MAXIMUM_BLOCKLIST_CAS_ATTEMPTS {
        let stored = state.store.get(&key).await.map_err(|error| {
            ApiError::internal(format!("failed to read IngressBlocklist: {error}"))
        })?;
        let (mut blocklist, expected) = match stored.as_ref() {
            Some(stored) => (
                resource::decode(stored, &keys, &kind, BuiltinKind::IngressBlocklist)?,
                ExpectedVersion::Exact(stored.version),
            ),
            None if !blocked => return Ok(None),
            None => (new_blocklist(id.clone()), ExpectedVersion::Missing),
        };
        let mut addresses = blocklist
            .spec
            .addresses
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        let changed = if blocked {
            addresses.insert(address)
        } else {
            addresses.remove(&address)
        };
        let normalized = addresses.into_iter().collect::<Vec<_>>();
        if !changed && blocklist.spec.addresses == normalized {
            return Ok(Some(blocklist));
        }
        if stored.is_some() {
            blocklist.meta.generation =
                next_generation(blocklist.meta.generation, "IngressBlocklist")?;
        }
        blocklist.spec.addresses = normalized;
        let value = serde_json::to_vec(&blocklist).map_err(|error| {
            ApiError::internal(format!("failed to encode IngressBlocklist: {error}"))
        })?;
        match state
            .store
            .put_cas(PutRequest {
                key: key.clone(),
                value,
                expected,
                session: None,
            })
            .await
            .map_err(|error| {
                ApiError::internal(format!("failed to write IngressBlocklist: {error}"))
            })? {
            CasOutcome::Applied(stored) => {
                blocklist.meta.revision = stored.version.resource_revision();
                return Ok(Some(blocklist));
            }
            CasOutcome::Conflict { .. } => {}
        }
    }
    Err(ApiError::conflict(
        "revisionConflict",
        "IngressBlocklist kept changing during the update",
    ))
}

async fn read_blocklist(state: &AppState) -> Result<Option<IngressBlocklist>, ApiError> {
    resource::get_optional(state, BuiltinKind::IngressBlocklist, blocklist_id()?).await
}

fn new_blocklist(id: IngressBlocklistId) -> IngressBlocklist {
    Object {
        meta: ObjectMeta {
            id,
            labels: BTreeMap::new(),
            annotations: BTreeMap::new(),
            revision: ResourceRevision::default(),
            generation: Generation(1),
            owner_refs: Vec::new(),
            finalizers: BTreeSet::new(),
            deletion_timestamp: None,
        },
        spec: IngressBlocklistSpec {
            addresses: Vec::new(),
        },
        status: IngressBlocklistStatus {
            applied_generation: Generation::default(),
            configuration_digest: None,
            conditions: Vec::new(),
        },
    }
}

fn blocklist_response(blocklist: Option<&IngressBlocklist>) -> BlockedIpsResponse {
    BlockedIpsResponse {
        blocked_ips: blocklist
            .into_iter()
            .flat_map(|blocklist| &blocklist.spec.addresses)
            .map(ToString::to_string)
            .collect(),
    }
}

fn blocklist_id() -> Result<IngressBlocklistId, ApiError> {
    IngressBlocklistId::new(BLOCKLIST_ID).map_err(|error| ApiError::internal(error.to_string()))
}

fn blocklist_kind() -> Result<kernel_api::ResourceKind, ApiError> {
    kernel_api::ResourceKind::new(BuiltinKind::IngressBlocklist.as_str())
        .map_err(|error| ApiError::internal(error.to_string()))
}
