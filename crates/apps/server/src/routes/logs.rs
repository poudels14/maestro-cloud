use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::routing::get;
use axum::{Json, Router};
use kernel_api::{BuildId, DeploymentId, NodeId, ServiceId, Timestamp};
use logs::{
    ClusterLogCursor, ClusterLogPage, ClusterLogQueryCoordinator, LogHistogramBucket,
    LogHistogramGroupBy, LogHistogramQuery, LogQueryParseError, LogQueryScope, LogQueryStore,
    LogQueryStoreError, LogReadCursor, LogReadOrder, LogReadQuery, LogSequence, SequencedLogEntry,
};
use serde::Deserialize;

use crate::routes::deployments::{ensure_service, owned_deployment, parse_service_id};
use crate::routes::observations::owned_build;
use crate::{ApiError, AppState};

const DEFAULT_TAIL: usize = 100;
const DEFAULT_HISTOGRAM_RANGE_MS: i64 = 3_600_000;
const DEFAULT_HISTOGRAM_BUCKET_MS: i64 = 60_000;

pub(super) fn router() -> Router<AppState> {
    Router::new()
        .route("/api/logs", get(all_logs))
        .route("/api/logs/histogram", get(all_histogram))
        .route("/api/system/logs", get(system_logs))
        .route("/api/system/logs/histogram", get(system_histogram))
        .route("/api/services/{service_id}/logs", get(service_logs))
        .route(
            "/api/services/{service_id}/logs/histogram",
            get(service_histogram),
        )
        .route(
            "/api/services/{service_id}/deployments/{deployment_id}/logs",
            get(deployment_logs),
        )
        .route(
            "/api/services/{service_id}/deployments/{deployment_id}/logs/histogram",
            get(deployment_histogram),
        )
        .route(
            "/api/services/{service_id}/builds/{build_id}/logs",
            get(build_logs),
        )
        .route(
            "/api/services/{service_id}/builds/{build_id}/logs/histogram",
            get(build_histogram),
        )
}

pub(super) fn node_router() -> Router<AppState> {
    Router::new()
        .route("/api/node/logs", get(node_logs))
        .route("/api/node/logs/histogram", get(node_histogram))
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ReadParameters {
    tail: Option<usize>,
    after: Option<u64>,
    before: Option<u64>,
    cursor: Option<String>,
    before_cursor: Option<String>,
    from: Option<i64>,
    to: Option<i64>,
    query: Option<String>,
    component: Option<String>,
    node_id: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct HistogramParameters {
    from: Option<i64>,
    to: Option<i64>,
    bucket_ms: Option<i64>,
    group_by: Option<String>,
    query: Option<String>,
    component: Option<String>,
    node_id: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct NodeReadParameters {
    scope: String,
    scope_id: Option<String>,
    tail: Option<usize>,
    after: Option<u64>,
    before: Option<u64>,
    from: Option<i64>,
    to: Option<i64>,
    query: Option<String>,
    order: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct NodeHistogramParameters {
    scope: String,
    scope_id: Option<String>,
    from: Option<i64>,
    to: Option<i64>,
    bucket_ms: Option<i64>,
    group_by: Option<String>,
    query: Option<String>,
}

async fn node_logs(
    State(state): State<AppState>,
    Query(parameters): Query<NodeReadParameters>,
) -> Result<Json<Vec<SequencedLogEntry>>, ApiError> {
    let scope = node_scope(&parameters.scope, parameters.scope_id.as_deref())?;
    let order = parameters.order.as_deref();
    let parameters = ReadParameters {
        tail: parameters.tail,
        after: parameters.after,
        before: parameters.before,
        cursor: None,
        before_cursor: None,
        from: parameters.from,
        to: parameters.to,
        query: parameters.query,
        component: None,
        node_id: None,
    };
    let mut query = read_query(scope, parameters)?;
    query = query.with_order(match order {
        None | Some("newest") => LogReadOrder::NewestFirst,
        Some("oldest") => LogReadOrder::OldestFirst,
        Some(value) => {
            return Err(ApiError::bad_request(format!(
                "unknown node log order `{value}`"
            )));
        }
    });
    query_store(&state)?
        .query_logs(&query)
        .await
        .map(Json)
        .map_err(query_error)
}

async fn node_histogram(
    State(state): State<AppState>,
    Query(parameters): Query<NodeHistogramParameters>,
) -> Result<Json<Vec<LogHistogramBucket>>, ApiError> {
    let scope = node_scope(&parameters.scope, parameters.scope_id.as_deref())?;
    let query = histogram_query(
        &state,
        scope,
        HistogramParameters {
            from: parameters.from,
            to: parameters.to,
            bucket_ms: parameters.bucket_ms,
            group_by: parameters.group_by,
            query: parameters.query,
            component: None,
            node_id: None,
        },
    )?;
    query_store(&state)?
        .query_log_histogram(&query)
        .await
        .map(Json)
        .map_err(query_error)
}

async fn all_logs(
    State(state): State<AppState>,
    Query(parameters): Query<ReadParameters>,
) -> Result<Json<ClusterLogPage>, ApiError> {
    cluster_read(&state, LogQueryScope::All, parameters).await
}

async fn system_logs(
    State(state): State<AppState>,
    Query(parameters): Query<ReadParameters>,
) -> Result<Json<ClusterLogPage>, ApiError> {
    let scope = system_scope(parameters.component.as_deref())?;
    cluster_read(&state, scope, parameters).await
}

async fn service_logs(
    State(state): State<AppState>,
    Path(service_id): Path<String>,
    Query(parameters): Query<ReadParameters>,
) -> Result<Json<ClusterLogPage>, ApiError> {
    let service_id = parse_service_id(service_id)?;
    ensure_service(&state, service_id.clone()).await?;
    cluster_read(&state, LogQueryScope::Service(service_id), parameters).await
}

async fn deployment_logs(
    State(state): State<AppState>,
    Path((service_id, deployment_id)): Path<(String, String)>,
    Query(parameters): Query<ReadParameters>,
) -> Result<Json<ClusterLogPage>, ApiError> {
    let deployment_id = owned_deployment_id(&state, service_id, deployment_id).await?;
    cluster_read(&state, LogQueryScope::Deployment(deployment_id), parameters).await
}

async fn build_logs(
    State(state): State<AppState>,
    Path((service_id, build_id)): Path<(String, String)>,
    Query(parameters): Query<ReadParameters>,
) -> Result<Json<ClusterLogPage>, ApiError> {
    let build_id = owned_build_id(&state, service_id, build_id).await?;
    cluster_read(&state, LogQueryScope::Build(build_id), parameters).await
}

async fn all_histogram(
    State(state): State<AppState>,
    Query(parameters): Query<HistogramParameters>,
) -> Result<Json<Vec<LogHistogramBucket>>, ApiError> {
    cluster_histogram(&state, LogQueryScope::All, parameters).await
}

async fn system_histogram(
    State(state): State<AppState>,
    Query(parameters): Query<HistogramParameters>,
) -> Result<Json<Vec<LogHistogramBucket>>, ApiError> {
    let scope = system_scope(parameters.component.as_deref())?;
    cluster_histogram(&state, scope, parameters).await
}

async fn service_histogram(
    State(state): State<AppState>,
    Path(service_id): Path<String>,
    Query(parameters): Query<HistogramParameters>,
) -> Result<Json<Vec<LogHistogramBucket>>, ApiError> {
    let service_id = parse_service_id(service_id)?;
    ensure_service(&state, service_id.clone()).await?;
    cluster_histogram(&state, LogQueryScope::Service(service_id), parameters).await
}

async fn deployment_histogram(
    State(state): State<AppState>,
    Path((service_id, deployment_id)): Path<(String, String)>,
    Query(parameters): Query<HistogramParameters>,
) -> Result<Json<Vec<LogHistogramBucket>>, ApiError> {
    let deployment_id = owned_deployment_id(&state, service_id, deployment_id).await?;
    cluster_histogram(&state, LogQueryScope::Deployment(deployment_id), parameters).await
}

async fn build_histogram(
    State(state): State<AppState>,
    Path((service_id, build_id)): Path<(String, String)>,
    Query(parameters): Query<HistogramParameters>,
) -> Result<Json<Vec<LogHistogramBucket>>, ApiError> {
    let build_id = owned_build_id(&state, service_id, build_id).await?;
    cluster_histogram(&state, LogQueryScope::Build(build_id), parameters).await
}

async fn cluster_read(
    state: &AppState,
    scope: LogQueryScope,
    parameters: ReadParameters,
) -> Result<Json<ClusterLogPage>, ApiError> {
    if parameters.after.is_some() || parameters.before.is_some() {
        return Err(ApiError::bad_request(
            "cluster log queries use cursor instead of after or before",
        ));
    }
    let node_ids = selected_cluster_log_nodes(state, parameters.node_id.as_deref())?;
    let cursor = parameters
        .cursor
        .as_deref()
        .map(|cursor| parse_cluster_cursor(state, cursor))
        .transpose()?;
    let before_cursor = parameters
        .before_cursor
        .as_deref()
        .map(|cursor| parse_cluster_cursor(state, cursor))
        .transpose()?;
    let query = read_query(scope, parameters)?;
    cluster_query_store(state)?
        .query_logs(&node_ids, &query, cursor.as_ref(), before_cursor.as_ref())
        .await
        .map(Json)
        .map_err(query_error)
}

async fn cluster_histogram(
    state: &AppState,
    scope: LogQueryScope,
    parameters: HistogramParameters,
) -> Result<Json<Vec<LogHistogramBucket>>, ApiError> {
    let node_ids = selected_cluster_log_nodes(state, parameters.node_id.as_deref())?;
    let query = histogram_query(state, scope, parameters)?;
    cluster_query_store(state)?
        .query_histogram(&node_ids, &query)
        .await
        .map(Json)
        .map_err(query_error)
}

fn read_query(scope: LogQueryScope, parameters: ReadParameters) -> Result<LogReadQuery, ApiError> {
    if parameters.after.is_some() && parameters.before.is_some() {
        return Err(ApiError::bad_request(
            "log query cannot specify both after and before",
        ));
    }
    let order = if parameters.after.is_some() {
        LogReadOrder::OldestFirst
    } else {
        LogReadOrder::NewestFirst
    };
    let mut query = LogReadQuery::new(scope, order, parameters.tail.unwrap_or(DEFAULT_TAIL))
        .map_err(|error| ApiError::bad_request(error.to_string()))?
        .within(parameters.from.map(Timestamp), parameters.to.map(Timestamp))
        .map_err(|error| ApiError::bad_request(error.to_string()))?;
    if let Some(search) = parameters.query {
        query = query.with_search(
            search
                .parse()
                .map_err(|error: LogQueryParseError| ApiError::bad_request(error.to_string()))?,
        );
    }
    if let Some(after) = parameters.after {
        query = query.with_cursor(LogReadCursor::After(LogSequence(after)));
    }
    if let Some(before) = parameters.before {
        query = query.with_cursor(LogReadCursor::Before(LogSequence(before)));
    }
    Ok(query)
}

fn histogram_query(
    state: &AppState,
    scope: LogQueryScope,
    parameters: HistogramParameters,
) -> Result<LogHistogramQuery, ApiError> {
    let to = Timestamp(
        parameters
            .to
            .unwrap_or_else(|| state.timestamp_clock.now().0),
    );
    let from = Timestamp(
        parameters
            .from
            .unwrap_or_else(|| to.0.saturating_sub(DEFAULT_HISTOGRAM_RANGE_MS)),
    );
    let group_by = match parameters.group_by.as_deref() {
        None | Some("level") => LogHistogramGroupBy::Level,
        Some("status") => LogHistogramGroupBy::HttpStatusClass,
        Some(value) => {
            return Err(ApiError::bad_request(format!(
                "unknown log histogram groupBy `{value}`"
            )));
        }
    };
    let mut query = LogHistogramQuery::new(
        scope,
        from,
        to,
        parameters.bucket_ms.unwrap_or(DEFAULT_HISTOGRAM_BUCKET_MS),
        group_by,
    )
    .map_err(|error| ApiError::bad_request(error.to_string()))?;
    if let Some(search) = parameters.query {
        query = query.with_search(
            search
                .parse()
                .map_err(|error: LogQueryParseError| ApiError::bad_request(error.to_string()))?,
        );
    }
    Ok(query)
}

fn system_scope(component: Option<&str>) -> Result<LogQueryScope, ApiError> {
    match component {
        Some(component) => {
            let scope = LogQueryScope::SystemComponent(component.to_owned());
            LogReadQuery::new(scope.clone(), LogReadOrder::NewestFirst, 1)
                .map(|_| scope)
                .map_err(|error| ApiError::bad_request(error.to_string()))
        }
        None => Ok(LogQueryScope::System),
    }
}

fn selected_cluster_log_nodes(
    state: &AppState,
    node_id: Option<&str>,
) -> Result<Vec<NodeId>, ApiError> {
    let Some(node_id) = node_id else {
        return Ok(state.cluster_log_nodes.to_vec());
    };
    let node_id = NodeId::new(node_id).map_err(|error| ApiError::bad_request(error.to_string()))?;
    if state.cluster_log_nodes.binary_search(&node_id).is_err() {
        return Err(ApiError::bad_request(format!(
            "node `{node_id}` is outside this topology"
        )));
    }
    Ok(vec![node_id])
}

fn node_scope(scope: &str, scope_id: Option<&str>) -> Result<LogQueryScope, ApiError> {
    let missing_id = || ApiError::bad_request(format!("node log scope `{scope}` requires scopeId"));
    match scope {
        "all" if scope_id.is_none() => Ok(LogQueryScope::All),
        "system" if scope_id.is_none() => Ok(LogQueryScope::System),
        "service" => ServiceId::new(scope_id.ok_or_else(missing_id)?)
            .map(LogQueryScope::Service)
            .map_err(|error| ApiError::bad_request(error.to_string())),
        "deployment" => DeploymentId::new(scope_id.ok_or_else(missing_id)?)
            .map(LogQueryScope::Deployment)
            .map_err(|error| ApiError::bad_request(error.to_string())),
        "systemComponent" => Ok(LogQueryScope::SystemComponent(
            scope_id.ok_or_else(missing_id)?.to_owned(),
        )),
        "build" => BuildId::new(scope_id.ok_or_else(missing_id)?)
            .map(LogQueryScope::Build)
            .map_err(|error| ApiError::bad_request(error.to_string())),
        "all" | "system" => Err(ApiError::bad_request(format!(
            "node log scope `{scope}` cannot specify scopeId"
        ))),
        value => Err(ApiError::bad_request(format!(
            "unknown node log scope `{value}`"
        ))),
    }
}

async fn owned_deployment_id(
    state: &AppState,
    service_id: String,
    deployment_id: String,
) -> Result<DeploymentId, ApiError> {
    let service_id =
        ServiceId::new(service_id).map_err(|error| ApiError::bad_request(error.to_string()))?;
    ensure_service(state, service_id.clone()).await?;
    Ok(owned_deployment(state, &service_id, deployment_id)
        .await?
        .meta
        .id)
}

async fn owned_build_id(
    state: &AppState,
    service_id: String,
    build_id: String,
) -> Result<BuildId, ApiError> {
    let service_id =
        ServiceId::new(service_id).map_err(|error| ApiError::bad_request(error.to_string()))?;
    ensure_service(state, service_id.clone()).await?;
    Ok(owned_build(state, &service_id, build_id).await?.meta.id)
}

fn query_store(state: &AppState) -> Result<Arc<dyn LogQueryStore>, ApiError> {
    state
        .log_queries
        .clone()
        .ok_or_else(|| ApiError::service_unavailable("log queries are not configured on this node"))
}

fn cluster_query_store(state: &AppState) -> Result<Arc<ClusterLogQueryCoordinator>, ApiError> {
    state.cluster_log_queries.clone().ok_or_else(|| {
        ApiError::service_unavailable("cluster log queries are not configured on this node")
    })
}

fn parse_cluster_cursor(state: &AppState, encoded: &str) -> Result<ClusterLogCursor, ApiError> {
    if encoded.len() > 64 * 1_024 {
        return Err(ApiError::bad_request(
            "cluster log cursor cannot exceed 64 KiB",
        ));
    }
    let cursor = serde_json::from_str::<ClusterLogCursor>(encoded)
        .map_err(|_| ApiError::bad_request("cluster log cursor is invalid"))?;
    if cursor
        .positions()
        .keys()
        .any(|node_id| state.cluster_log_nodes.binary_search(node_id).is_err())
    {
        return Err(ApiError::bad_request(
            "cluster log cursor contains a node outside this topology",
        ));
    }
    Ok(cursor)
}

fn query_error(error: LogQueryStoreError) -> ApiError {
    match error {
        LogQueryStoreError::Rejected { message } => ApiError::bad_request(message),
        LogQueryStoreError::Unavailable { message } => ApiError::service_unavailable(message),
    }
}
