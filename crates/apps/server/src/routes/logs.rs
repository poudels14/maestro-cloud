use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::routing::get;
use axum::{Json, Router};
use kernel_api::{DeploymentId, ServiceId, Timestamp};
use logs::{
    LogHistogramBucket, LogHistogramGroupBy, LogHistogramQuery, LogQueryParseError, LogQueryScope,
    LogQueryStore, LogQueryStoreError, LogReadCursor, LogReadOrder, LogReadQuery, LogSequence,
    SequencedLogEntry,
};
use serde::Deserialize;

use crate::routes::deployments::{ensure_service, owned_deployment, parse_service_id};
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
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ReadParameters {
    tail: Option<usize>,
    after: Option<u64>,
    before: Option<u64>,
    from: Option<i64>,
    to: Option<i64>,
    query: Option<String>,
    component: Option<String>,
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
}

async fn all_logs(
    State(state): State<AppState>,
    Query(parameters): Query<ReadParameters>,
) -> Result<Json<Vec<SequencedLogEntry>>, ApiError> {
    read(&state, LogQueryScope::All, parameters).await
}

async fn system_logs(
    State(state): State<AppState>,
    Query(parameters): Query<ReadParameters>,
) -> Result<Json<Vec<SequencedLogEntry>>, ApiError> {
    let scope = system_scope(parameters.component.as_deref())?;
    read(&state, scope, parameters).await
}

async fn service_logs(
    State(state): State<AppState>,
    Path(service_id): Path<String>,
    Query(parameters): Query<ReadParameters>,
) -> Result<Json<Vec<SequencedLogEntry>>, ApiError> {
    let service_id = parse_service_id(service_id)?;
    ensure_service(&state, service_id.clone()).await?;
    read(&state, LogQueryScope::Service(service_id), parameters).await
}

async fn deployment_logs(
    State(state): State<AppState>,
    Path((service_id, deployment_id)): Path<(String, String)>,
    Query(parameters): Query<ReadParameters>,
) -> Result<Json<Vec<SequencedLogEntry>>, ApiError> {
    let deployment_id = owned_deployment_id(&state, service_id, deployment_id).await?;
    read(&state, LogQueryScope::Deployment(deployment_id), parameters).await
}

async fn all_histogram(
    State(state): State<AppState>,
    Query(parameters): Query<HistogramParameters>,
) -> Result<Json<Vec<LogHistogramBucket>>, ApiError> {
    histogram(&state, LogQueryScope::All, parameters).await
}

async fn system_histogram(
    State(state): State<AppState>,
    Query(parameters): Query<HistogramParameters>,
) -> Result<Json<Vec<LogHistogramBucket>>, ApiError> {
    let scope = system_scope(parameters.component.as_deref())?;
    histogram(&state, scope, parameters).await
}

async fn service_histogram(
    State(state): State<AppState>,
    Path(service_id): Path<String>,
    Query(parameters): Query<HistogramParameters>,
) -> Result<Json<Vec<LogHistogramBucket>>, ApiError> {
    let service_id = parse_service_id(service_id)?;
    ensure_service(&state, service_id.clone()).await?;
    histogram(&state, LogQueryScope::Service(service_id), parameters).await
}

async fn deployment_histogram(
    State(state): State<AppState>,
    Path((service_id, deployment_id)): Path<(String, String)>,
    Query(parameters): Query<HistogramParameters>,
) -> Result<Json<Vec<LogHistogramBucket>>, ApiError> {
    let deployment_id = owned_deployment_id(&state, service_id, deployment_id).await?;
    histogram(&state, LogQueryScope::Deployment(deployment_id), parameters).await
}

async fn read(
    state: &AppState,
    scope: LogQueryScope,
    parameters: ReadParameters,
) -> Result<Json<Vec<SequencedLogEntry>>, ApiError> {
    let store = query_store(state)?;
    let query = read_query(scope, parameters)?;
    store
        .query_logs(&query)
        .await
        .map(Json)
        .map_err(query_error)
}

async fn histogram(
    state: &AppState,
    scope: LogQueryScope,
    parameters: HistogramParameters,
) -> Result<Json<Vec<LogHistogramBucket>>, ApiError> {
    let store = query_store(state)?;
    let query = histogram_query(state, scope, parameters)?;
    store
        .query_log_histogram(&query)
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

fn query_store(state: &AppState) -> Result<Arc<dyn LogQueryStore>, ApiError> {
    state
        .log_queries
        .clone()
        .ok_or_else(|| ApiError::service_unavailable("log queries are not configured on this node"))
}

fn query_error(error: LogQueryStoreError) -> ApiError {
    match error {
        LogQueryStoreError::Rejected { message } => ApiError::bad_request(message),
        LogQueryStoreError::Unavailable { message } => ApiError::service_unavailable(message),
    }
}
