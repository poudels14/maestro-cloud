use std::sync::Arc;

use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::error::TelemetryError;
use crate::metrics::MetricPoint;
use crate::search::LogSearchQuery;
use crate::store::{LogQuery, LogStore, MetricsQuery, StoredLog};

const DEFAULT_LIMIT: usize = 100;
const MAX_LIMIT: usize = 1_000;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct LogsParams {
    query: Option<String>,
    limit: Option<usize>,
    before: Option<i64>,
    before_ts: Option<i64>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct LogsPage {
    records: Vec<StoredLog>,
    next_cursor: Option<i64>,
}

pub fn router(store: Arc<LogStore>) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/logs", get(logs))
        .route("/metrics", get(metrics))
        .with_state(store)
}

async fn health() -> Json<serde_json::Value> {
    Json(json!({ "status": "ok" }))
}

async fn logs(
    State(store): State<Arc<LogStore>>,
    Query(params): Query<LogsParams>,
) -> Result<Json<LogsPage>, TelemetryError> {
    let search = params
        .query
        .as_deref()
        .map(str::trim)
        .filter(|query| !query.is_empty())
        .map(str::parse::<LogSearchQuery>)
        .transpose()
        .map_err(TelemetryError::Query)?;
    let limit = params.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT);
    let records = store
        .read_logs(LogQuery {
            search,
            before: params.before,
            before_ts: params.before_ts,
            limit,
        })
        .await?;
    let next_cursor = (records.len() == limit)
        .then(|| records.last().map(|record| record.seq))
        .flatten();
    Ok(Json(LogsPage {
        records,
        next_cursor,
    }))
}

async fn metrics(
    State(store): State<Arc<LogStore>>,
    Query(query): Query<MetricsQuery>,
) -> Result<Json<Vec<MetricPoint>>, TelemetryError> {
    Ok(Json(store.read_metrics(query).await?))
}

impl IntoResponse for TelemetryError {
    fn into_response(self) -> Response {
        let status = match self {
            Self::Query(_) => StatusCode::BAD_REQUEST,
            _ => {
                tracing::error!(error = %self, "log query failed");
                StatusCode::INTERNAL_SERVER_ERROR
            }
        };
        (status, Json(json!({ "error": self.to_string() }))).into_response()
    }
}
