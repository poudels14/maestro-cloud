use std::collections::BTreeMap;

use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use maestro_core::{NodeId, NodeRecord, NodeState, ServiceId};
use maestro_telemetry::MetricPoint;
use serde::Deserialize;
use serde_json::json;

use crate::node_service::{ClusterAccess, NodeRequest, call_nodes, url_query};

const METRICS_BUCKET_MS: i64 = 15_000;
const DEFAULT_RANGE_MS: i64 = 60 * 60 * 1000;
const CONTAINER_SOURCE_PREFIX: &str = "container:";

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RangeQuery {
    from: Option<i64>,
    to: Option<i64>,
    node_id: Option<u64>,
}

pub async fn node_metrics(
    State(access): State<ClusterAccess>,
    Query(range): Query<RangeQuery>,
) -> Response {
    let node_id = range
        .node_id
        .map(NodeId)
        .unwrap_or(access.consensus.metrics().node_id);
    let selected = nodes(&access).map(|nodes| {
        nodes
            .into_iter()
            .filter(|node| node.id == node_id)
            .collect::<Vec<_>>()
    });
    respond(match selected {
        Ok(selected) => Ok(gather_metrics(&access, selected, &range, "source", "node").await),
        Err(error) => Err(error),
    })
}

pub async fn cluster_metrics(
    State(access): State<ClusterAccess>,
    Query(range): Query<RangeQuery>,
) -> Response {
    respond(match nodes(&access) {
        Ok(nodes) => Ok(sum_buckets(
            gather_metrics(&access, nodes, &range, "source", "cluster").await,
            "cluster",
        )),
        Err(error) => Err(error),
    })
}

pub async fn service_metrics(
    State(access): State<ClusterAccess>,
    Path(service_id): Path<String>,
    Query(range): Query<RangeQuery>,
) -> Response {
    let source = format!("service:{service_id}");
    respond(match nodes(&access) {
        Ok(nodes) => Ok(sum_buckets(
            gather_metrics(&access, nodes, &range, "source", &source).await,
            &source,
        )),
        Err(error) => Err(error),
    })
}

pub async fn container_metrics(
    State(access): State<ClusterAccess>,
    Path(service_id): Path<String>,
    Query(range): Query<RangeQuery>,
) -> Response {
    let deployments = access
        .consensus
        .state()
        .deployments(&ServiceId::new(service_id))
        .map(|deployments| {
            deployments
                .into_iter()
                .map(|deployment| format!("{CONTAINER_SOURCE_PREFIX}{}-", deployment.id))
                .collect::<Vec<_>>()
        })
        .map_err(|error| error.to_string());
    respond(match (nodes(&access), deployments) {
        (Ok(nodes), Ok(deployments)) => {
            let points = gather_metrics(
                &access,
                nodes,
                &range,
                "sourcePrefix",
                CONTAINER_SOURCE_PREFIX,
            )
            .await
            .into_iter()
            .filter(|point| {
                deployments
                    .iter()
                    .any(|prefix| point.source.starts_with(prefix))
            })
            .collect::<Vec<_>>();
            Ok(points)
        }
        (Err(error), _) | (_, Err(error)) => Err(error),
    })
}

async fn gather_metrics(
    access: &ClusterAccess,
    nodes: Vec<NodeRecord>,
    range: &RangeQuery,
    filter: &str,
    value: &str,
) -> Vec<MetricPoint> {
    let to = range.to.unwrap_or_else(now_ms);
    let from = range.from.unwrap_or(to - DEFAULT_RANGE_MS);
    let path = format!(
        "/metrics?{}",
        url_query(&[
            (filter, value.to_string()),
            ("from", from.to_string()),
            ("to", to.to_string())
        ])
    );
    call_nodes(&access.clients, nodes, NodeRequest::Telemetry { path })
        .await
        .into_iter()
        .filter_map(|(_, node_name, reply)| match reply {
            Ok(points) => serde_json::from_value::<Vec<MetricPoint>>(points).ok(),
            Err(status) => {
                tracing::warn!(node = %node_name, error = %status.message(), "node metrics query failed");
                None
            }
        })
        .flatten()
        .collect()
}

fn sum_buckets(points: Vec<MetricPoint>, source: &str) -> Vec<MetricPoint> {
    points
        .into_iter()
        .fold(BTreeMap::<i64, MetricPoint>::new(), |mut buckets, point| {
            let bucket = point.ts - point.ts.rem_euclid(METRICS_BUCKET_MS);
            let total = buckets.entry(bucket).or_insert_with(|| MetricPoint {
                ts: bucket,
                source: source.to_string(),
                ..MetricPoint::default()
            });
            total.cpu_percent += point.cpu_percent;
            total.memory_bytes += point.memory_bytes;
            total.memory_limit_bytes += point.memory_limit_bytes;
            total.net_rx_bytes += point.net_rx_bytes;
            total.net_tx_bytes += point.net_tx_bytes;
            buckets
        })
        .into_values()
        .collect()
}

fn nodes(access: &ClusterAccess) -> Result<Vec<NodeRecord>, String> {
    access
        .consensus
        .state()
        .nodes()
        .map(|nodes| {
            nodes
                .into_iter()
                .filter(|node| node.state != NodeState::Removed)
                .collect()
        })
        .map_err(|error| error.to_string())
}

fn respond<T: serde::Serialize>(result: Result<T, String>) -> Response {
    match result {
        Ok(value) => Json(value).into_response(),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": error })),
        )
            .into_response(),
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_millis() as i64)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn point(ts: i64, cpu: f64, memory: u64) -> MetricPoint {
        MetricPoint {
            ts,
            source: "cluster".to_string(),
            cpu_percent: cpu,
            memory_bytes: memory,
            ..MetricPoint::default()
        }
    }

    #[test]
    fn sums_node_samples_into_shared_buckets() {
        let summed = sum_buckets(
            vec![
                point(15_100, 10.0, 1),
                point(29_900, 5.0, 2),
                point(30_050, 1.0, 4),
            ],
            "cluster",
        );
        assert_eq!(summed.len(), 2);
        assert_eq!(
            (summed[0].ts, summed[0].cpu_percent, summed[0].memory_bytes),
            (15_000, 15.0, 3)
        );
        assert_eq!((summed[1].ts, summed[1].memory_bytes), (30_000, 4));
    }
}
