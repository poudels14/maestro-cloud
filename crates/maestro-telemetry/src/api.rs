use std::sync::Arc;

use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::access_log::IngressTrafficBreakdown;
use crate::error::TelemetryError;
use crate::metrics::MetricPoint;
use crate::search::LogSearchQuery;
use crate::store::{
    HistogramGroup, HistogramQuery, IngressTrafficQuery, LogHistogram, LogQuery, LogStore,
    MAX_HISTOGRAM_RANGE_MS, MetricsQuery, StoredLog, TrafficQuery, histogram_bucket,
};
use crate::traffic::TrafficPoint;

const DEFAULT_LIMIT: usize = 100;
const MAX_LIMIT: usize = 1_000;
const DEFAULT_HISTOGRAM_RANGE_MS: i64 = 60 * 60 * 1000;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct LogsParams {
    query: Option<String>,
    limit: Option<usize>,
    before: Option<i64>,
    before_ts: Option<i64>,
    after_ts: Option<i64>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct HistogramParams {
    query: Option<String>,
    from: Option<i64>,
    to: Option<i64>,
    bucket_ms: Option<i64>,
    #[serde(default)]
    group_by: HistogramGroup,
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
        .route("/logs/histogram", get(histogram))
        .route("/metrics", get(metrics))
        .route("/traffic", get(traffic))
        .route("/ingress-traffic", get(ingress_traffic))
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
            after_ts: params.after_ts,
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

async fn histogram(
    State(store): State<Arc<LogStore>>,
    Query(params): Query<HistogramParams>,
) -> Result<Json<LogHistogram>, TelemetryError> {
    let to = params.to.unwrap_or_else(now_ms);
    let from = params.from.unwrap_or(to - DEFAULT_HISTOGRAM_RANGE_MS);
    let search = params
        .query
        .as_deref()
        .map(str::trim)
        .filter(|query| !query.is_empty())
        .map(str::parse::<LogSearchQuery>)
        .transpose()
        .map_err(TelemetryError::Query)?;
    match from <= to && to - from <= MAX_HISTOGRAM_RANGE_MS {
        false => Err(TelemetryError::Query(
            "the histogram range must be at most 7 days with from <= to".to_string(),
        )),
        true => Ok(Json(
            store
                .log_histogram(HistogramQuery {
                    search,
                    from,
                    to,
                    bucket_ms: histogram_bucket(from, to, params.bucket_ms),
                    group_by: params.group_by,
                })
                .await?,
        )),
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_millis() as i64)
}

async fn metrics(
    State(store): State<Arc<LogStore>>,
    Query(query): Query<MetricsQuery>,
) -> Result<Json<Vec<MetricPoint>>, TelemetryError> {
    Ok(Json(store.read_metrics(query).await?))
}

async fn traffic(
    State(store): State<Arc<LogStore>>,
    Query(query): Query<TrafficQuery>,
) -> Result<Json<Vec<TrafficPoint>>, TelemetryError> {
    Ok(Json(store.read_traffic(query).await?))
}

async fn ingress_traffic(
    State(store): State<Arc<LogStore>>,
    Query(query): Query<IngressTrafficQuery>,
) -> Result<Json<IngressTrafficBreakdown>, TelemetryError> {
    Ok(Json(store.read_ingress_traffic(query).await?))
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
