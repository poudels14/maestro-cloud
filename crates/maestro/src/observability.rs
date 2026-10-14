use std::collections::BTreeMap;

use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use maestro_core::{NodeId, NodeRecord, NodeState, ServiceId};
use maestro_telemetry::{
    IngressTrafficBreakdown, MetricPoint, TrafficBreakdownEntry, TrafficPoint,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::node_service::{ClusterAccess, NodeRequest, call_nodes, url_query};

const METRICS_BUCKET_MS: i64 = 15_000;
const DEFAULT_RANGE_MS: i64 = 60 * 60 * 1000;
const CONTAINER_SOURCE_PREFIX: &str = "container:";
const DEFAULT_BREAKDOWN_LIMIT: usize = 100;
const MAX_BREAKDOWN_LIMIT: usize = 500;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RangeQuery {
    from: Option<i64>,
    to: Option<i64>,
    node_id: Option<u64>,
    limit: Option<usize>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct MergedBreakdown {
    #[serde(flatten)]
    breakdown: IngressTrafficBreakdown,
    partial: bool,
    unavailable_nodes: usize,
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

pub async fn ingress_traffic(state: State<ClusterAccess>, range: Query<RangeQuery>) -> Response {
    traffic_breakdown(state, range, "cluster", None).await
}

pub async fn blocked_ingress_traffic(
    state: State<ClusterAccess>,
    range: Query<RangeQuery>,
) -> Response {
    traffic_breakdown(state, range, "blocked", None).await
}

pub async fn service_traffic_breakdown(
    state: State<ClusterAccess>,
    Path(service_id): Path<String>,
    range: Query<RangeQuery>,
) -> Response {
    traffic_breakdown(state, range, "service", Some(service_id)).await
}

async fn traffic_breakdown(
    State(access): State<ClusterAccess>,
    Query(range): Query<RangeQuery>,
    scope: &str,
    service_id: Option<String>,
) -> Response {
    let to = range.to.unwrap_or_else(now_ms);
    let from = range.from.unwrap_or(to - DEFAULT_RANGE_MS);
    let limit = range
        .limit
        .unwrap_or(DEFAULT_BREAKDOWN_LIMIT)
        .clamp(1, MAX_BREAKDOWN_LIMIT);
    match (from <= to, nodes(&access)) {
        (false, _) => (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "traffic range `from` cannot be after `to`" })),
        )
            .into_response(),
        (true, Err(error)) => respond::<()>(Err(error)),
        (true, Ok(nodes)) => {
            let selected = nodes
                .into_iter()
                .filter(|node| {
                    range
                        .node_id
                        .is_none_or(|node_id| node.id == NodeId(node_id))
                })
                .collect::<Vec<_>>();
            let requested = selected.len();
            let mut pairs = vec![
                ("scope", scope.to_string()),
                ("from", from.to_string()),
                ("to", to.to_string()),
                ("limit", (limit * 2).to_string()),
            ];
            pairs.extend(service_id.map(|service_id| ("serviceId", service_id)));
            let path = format!("/ingress-traffic?{}", url_query(&pairs));
            let breakdowns = gather(&access, selected, path)
                .await
                .into_iter()
                .filter_map(|value| serde_json::from_value::<IngressTrafficBreakdown>(value).ok())
                .collect::<Vec<_>>();
            let unavailable_nodes = requested - breakdowns.len();
            Json(MergedBreakdown {
                breakdown: IngressTrafficBreakdown {
                    by_ip: merge_entries(
                        breakdowns
                            .iter()
                            .flat_map(|breakdown| breakdown.by_ip.clone()),
                        limit,
                    ),
                    by_path: merge_entries(
                        breakdowns
                            .iter()
                            .flat_map(|breakdown| breakdown.by_path.clone()),
                        limit,
                    ),
                },
                partial: unavailable_nodes > 0,
                unavailable_nodes,
            })
            .into_response()
        }
    }
}

fn merge_entries(
    entries: impl Iterator<Item = TrafficBreakdownEntry>,
    limit: usize,
) -> Vec<TrafficBreakdownEntry> {
    let merged = entries.fold(
        BTreeMap::<(String, u16), TrafficBreakdownEntry>::new(),
        |mut merged, entry| {
            let total = merged
                .entry((entry.value.clone(), entry.status_code))
                .or_insert_with(|| TrafficBreakdownEntry {
                    requests: 0,
                    ..entry.clone()
                });
            total.requests += entry.requests;
            total.last_seen_at_ms = total.last_seen_at_ms.max(entry.last_seen_at_ms);
            merged
        },
    );
    let mut totals = merged
        .values()
        .fold(
            BTreeMap::<String, (u64, i64)>::new(),
            |mut totals, entry| {
                let total = totals.entry(entry.value.clone()).or_default();
                total.0 += entry.requests;
                total.1 = total.1.max(entry.last_seen_at_ms);
                totals
            },
        )
        .into_iter()
        .collect::<Vec<_>>();
    totals.sort_by(|(left_value, left), (right_value, right)| {
        right
            .0
            .cmp(&left.0)
            .then(right.1.cmp(&left.1))
            .then(left_value.cmp(right_value))
    });
    let ranks = totals
        .into_iter()
        .take(limit)
        .enumerate()
        .map(|(rank, (value, _))| (value, rank))
        .collect::<BTreeMap<_, _>>();
    let mut kept = merged
        .into_values()
        .filter(|entry| ranks.contains_key(&entry.value))
        .collect::<Vec<_>>();
    kept.sort_by_key(|entry| (ranks[&entry.value], entry.status_code));
    kept
}

pub async fn service_traffic(
    State(access): State<ClusterAccess>,
    Path(service_id): Path<String>,
    Query(range): Query<RangeQuery>,
) -> Response {
    let to = range.to.unwrap_or_else(now_ms);
    let from = range.from.unwrap_or(to - DEFAULT_RANGE_MS);
    let path = format!(
        "/traffic?{}",
        url_query(&[
            ("serviceId", service_id),
            ("from", from.to_string()),
            ("to", to.to_string()),
        ])
    );
    respond(match nodes(&access) {
        Ok(nodes) => {
            let mut points = gather(&access, nodes, path)
                .await
                .into_iter()
                .filter_map(|value| serde_json::from_value::<Vec<TrafficPoint>>(value).ok())
                .flatten()
                .collect::<Vec<_>>();
            points.sort_by_key(|point| point.ts);
            Ok(points)
        }
        Err(error) => Err(error),
    })
}

async fn gather(access: &ClusterAccess, nodes: Vec<NodeRecord>, path: String) -> Vec<Value> {
    call_nodes(&access.clients, nodes, NodeRequest::Telemetry { path })
        .await
        .into_iter()
        .filter_map(|(_, node_name, reply)| match reply {
            Ok(value) => Some(value),
            Err(status) => {
                tracing::warn!(node = %node_name, error = %status.message(), "node telemetry query failed");
                None
            }
        })
        .collect()
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
    gather(access, nodes, path)
        .await
        .into_iter()
        .filter_map(|points| serde_json::from_value::<Vec<MetricPoint>>(points).ok())
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

    #[test]
    fn merges_breakdowns_across_nodes_and_ranks_by_total() {
        let entry = |value: &str, status: u16, requests: u64, seen: i64| TrafficBreakdownEntry {
            value: value.to_string(),
            status_code: status,
            requests,
            last_seen_at_ms: seen,
        };
        let merged = merge_entries(
            vec![
                entry("a", 200, 3, 1),
                entry("b", 200, 4, 2),
                entry("a", 200, 2, 5),
                entry("a", 404, 1, 3),
                entry("c", 200, 1, 9),
            ]
            .into_iter(),
            2,
        );
        let summary = merged
            .iter()
            .map(|entry| {
                (
                    entry.value.as_str(),
                    entry.status_code,
                    entry.requests,
                    entry.last_seen_at_ms,
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            summary,
            [("a", 200, 5, 5), ("a", 404, 1, 3), ("b", 200, 4, 2)]
        );
    }
}
