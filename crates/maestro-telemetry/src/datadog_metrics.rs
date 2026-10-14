use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use serde_json::{Value, json};

use crate::datadog::DatadogConfig;
use crate::disks::Disk;
use crate::metrics::MetricPoint;
use crate::resolver::ReplicaResolver;

const GAUGE: u8 = 3;
const SEND_ATTEMPTS: u32 = 3;
const INITIAL_BACKOFF: Duration = Duration::from_millis(200);
const MAX_BACKOFF: Duration = Duration::from_secs(5);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(15);
const CONTAINER_PREFIX: &str = "container:";
const SERVICE_PREFIX: &str = "service:";

pub struct DatadogMetrics {
    config: DatadogConfig,
    resolver: Arc<dyn ReplicaResolver>,
    http: reqwest::Client,
    previous_network: HashMap<String, (u64, u64, i64)>,
}

impl DatadogMetrics {
    pub fn new(config: DatadogConfig, resolver: Arc<dyn ReplicaResolver>) -> Self {
        Self {
            config,
            resolver,
            http: reqwest::Client::new(),
            previous_network: HashMap::new(),
        }
    }

    pub async fn publish(&mut self, points: &[MetricPoint], disks: &[Disk]) {
        let series = self.series(points, disks);
        let body = json!({ "series": series });
        let mut backoff = INITIAL_BACKOFF;
        let mut attempt = 1;
        let mut result = self.send(&body).await;
        while result.is_err() && attempt < SEND_ATTEMPTS {
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(MAX_BACKOFF);
            attempt += 1;
            result = self.send(&body).await;
        }
        if let Err(error) = result {
            tracing::warn!(%error, "datadog metrics delivery failed");
        }
    }

    pub fn series(&mut self, points: &[MetricPoint], disks: &[Disk]) -> Vec<Value> {
        let base_tags = self
            .config
            .tags
            .iter()
            .cloned()
            .chain([
                format!("cluster:{}", self.config.cluster),
                format!("host:{}", self.config.node),
            ])
            .collect::<Vec<_>>();
        let point_series = points.iter().flat_map(|point| {
            let (scope, extra) = self.scope_and_tags(&point.source);
            let tags = base_tags.iter().cloned().chain(extra).collect::<Vec<_>>();
            let timestamp = point.ts / 1000;
            let rates = self.network_rates(point);
            let mut metrics = vec![
                gauge(
                    &format!("maestro.{scope}.cpu.percent"),
                    timestamp,
                    point.cpu_percent,
                    &tags,
                    "percent",
                ),
                gauge(
                    &format!("maestro.{scope}.memory.bytes"),
                    timestamp,
                    point.memory_bytes as f64,
                    &tags,
                    "byte",
                ),
            ];
            metrics.extend(rates.into_iter().flat_map(|(rx, tx)| {
                [
                    gauge(
                        &format!("maestro.{scope}.network.rx.bytes_per_sec"),
                        timestamp,
                        rx,
                        &tags,
                        "byte",
                    ),
                    gauge(
                        &format!("maestro.{scope}.network.tx.bytes_per_sec"),
                        timestamp,
                        tx,
                        &tags,
                        "byte",
                    ),
                ]
            }));
            metrics
        });
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |elapsed| elapsed.as_secs() as i64);
        let disk_series = disks.iter().flat_map(|disk| {
            let tags = base_tags
                .iter()
                .cloned()
                .chain([format!("mount:{}", disk.mount_point)])
                .collect::<Vec<_>>();
            let used = disk.total_bytes.saturating_sub(disk.available_bytes) as f64;
            let usage = match disk.total_bytes {
                0 => 0.0,
                total => used * 100.0 / total as f64,
            };
            [
                gauge("maestro.node.disk.used.bytes", now, used, &tags, "byte"),
                gauge(
                    "maestro.node.disk.total.bytes",
                    now,
                    disk.total_bytes as f64,
                    &tags,
                    "byte",
                ),
                gauge(
                    "maestro.node.disk.usage.percent",
                    now,
                    usage,
                    &tags,
                    "percent",
                ),
            ]
        });
        point_series
            .collect::<Vec<_>>()
            .into_iter()
            .chain(disk_series)
            .collect()
    }

    fn scope_and_tags(&self, source: &str) -> (&'static str, Vec<String>) {
        match (
            source.strip_prefix(CONTAINER_PREFIX),
            source.strip_prefix(SERVICE_PREFIX),
        ) {
            (Some(replica), _) => {
                let identity = self.resolver.resolve(replica);
                let tags = identity
                    .service_id
                    .map(|service| format!("service:{service}"))
                    .into_iter()
                    .chain(
                        identity
                            .deployment_id
                            .map(|deployment| format!("deployment:{deployment}")),
                    )
                    .chain([format!("replica:{replica}")])
                    .collect();
                ("service", tags)
            }
            (None, Some(service)) => ("service", vec![format!("service:{service}")]),
            (None, None) if source == "node" => ("node", Vec::new()),
            (None, None) => ("cluster", Vec::new()),
        }
    }

    fn network_rates(&mut self, point: &MetricPoint) -> Option<(f64, f64)> {
        let previous = self.previous_network.insert(
            point.source.clone(),
            (point.net_rx_bytes, point.net_tx_bytes, point.ts),
        );
        previous.and_then(|(rx, tx, ts)| {
            let seconds = (point.ts - ts) as f64 / 1000.0;
            (seconds > 0.0).then(|| {
                (
                    point.net_rx_bytes.saturating_sub(rx) as f64 / seconds,
                    point.net_tx_bytes.saturating_sub(tx) as f64 / seconds,
                )
            })
        })
    }

    async fn send(&self, body: &Value) -> Result<(), String> {
        let response = self
            .http
            .post(&self.config.metrics_url)
            .timeout(REQUEST_TIMEOUT)
            .header("DD-API-KEY", &self.config.api_key)
            .json(body)
            .send()
            .await
            .map_err(|error| error.without_url().to_string())?;
        match response.status().is_success() {
            true => Ok(()),
            false => Err(format!("datadog returned {}", response.status())),
        }
    }
}

fn gauge(metric: &str, timestamp: i64, value: f64, tags: &[String], unit: &str) -> Value {
    json!({
        "metric": metric,
        "type": GAUGE,
        "points": [{ "timestamp": timestamp, "value": value }],
        "tags": tags,
        "unit": unit,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::resolver::ReplicaIdentity;

    #[test]
    fn builds_tagged_series_with_network_rates() {
        let resolver: HashMap<String, ReplicaIdentity> = HashMap::from([(
            "d1-0".to_string(),
            ReplicaIdentity {
                service_id: Some("web".to_string()),
                deployment_id: Some("d1".to_string()),
                healthcheck_path: None,
            },
        )]);
        let mut metrics = DatadogMetrics::new(
            DatadogConfig {
                cluster: "prod".to_string(),
                node: "node1".to_string(),
                tags: vec!["team:core".to_string()],
                ..DatadogConfig::default()
            },
            Arc::new(resolver),
        );
        let point = |ts: i64, rx: u64| MetricPoint {
            ts,
            source: "container:d1-0".to_string(),
            cpu_percent: 5.0,
            memory_bytes: 10,
            memory_limit_bytes: 20,
            net_rx_bytes: rx,
            net_tx_bytes: 0,
        };
        let first = metrics.series(&[point(15_000, 100)], &[]);
        assert_eq!(first.len(), 2);
        assert_eq!(first[0]["metric"], "maestro.service.cpu.percent");
        let tags = first[0]["tags"].as_array().unwrap();
        for expected in [
            "team:core",
            "cluster:prod",
            "host:node1",
            "service:web",
            "deployment:d1",
            "replica:d1-0",
        ] {
            assert!(tags.contains(&json!(expected)), "{expected}");
        }
        let second = metrics.series(&[point(30_000, 400)], &[]);
        let rx = second
            .iter()
            .find(|series| series["metric"] == "maestro.service.network.rx.bytes_per_sec")
            .unwrap();
        assert_eq!(rx["points"][0]["value"], 20.0);
    }
}
