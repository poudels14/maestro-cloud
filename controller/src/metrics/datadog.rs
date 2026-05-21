use std::collections::HashMap;
use std::time::Duration;

use anyhow::{Result, anyhow};
use backon::{BackoffBuilder, ExponentialBuilder};
use serde::Serialize;
use tokio::sync::broadcast;

use crate::logs::Logger;
use crate::metrics::{DiskPoint, MetricBatch, MetricPoint};
use crate::signal::ShutdownEvent;
use crate::utils::crypto::SecretString;

const SEND_TIMEOUT: Duration = Duration::from_secs(15);
const RETRY_MAX_ATTEMPTS: usize = 3;
const DATADOG_GAUGE: u8 = 3;

pub struct DatadogMetricsSink {
    client: reqwest::Client,
    endpoint: String,
    api_key: SecretString,
    rx: flume::Receiver<MetricBatch>,
    cluster_name: String,
    hostname: String,
    global_tags: Vec<String>,
    signal_rx: broadcast::Receiver<ShutdownEvent>,
    logger: Logger,
    previous_net: HashMap<String, NetSample>,
}

#[derive(Clone, Copy)]
struct NetSample {
    ts: i64,
    rx_bytes: i64,
    tx_bytes: i64,
}

impl DatadogMetricsSink {
    pub fn new(
        site: &str,
        api_key: SecretString,
        rx: flume::Receiver<MetricBatch>,
        cluster_name: String,
        global_tags: Vec<String>,
        signal_rx: broadcast::Receiver<ShutdownEvent>,
        logger: Logger,
    ) -> Self {
        let hostname = sysinfo::System::host_name().unwrap_or_else(|| "unknown".to_string());
        Self {
            client: reqwest::Client::builder()
                .timeout(SEND_TIMEOUT)
                .build()
                .expect("failed to build datadog metrics http client"),
            endpoint: format!("https://api.{site}/api/v2/series"),
            api_key,
            rx,
            cluster_name,
            hostname,
            global_tags,
            signal_rx,
            logger,
            previous_net: HashMap::new(),
        }
    }

    pub fn spawn(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move { self.run().await })
    }

    async fn run(mut self) {
        self.logger.emit(
            "info",
            &format!(
                "datadog metrics sink started (endpoint: {}, host: {})",
                self.endpoint, self.hostname
            ),
        );
        loop {
            tokio::select! {
                signal = self.signal_rx.recv() => {
                    match signal {
                        Ok(ShutdownEvent::Graceful) | Ok(ShutdownEvent::Force)
                        | Err(broadcast::error::RecvError::Closed) => {
                            self.logger.emit("info", "datadog metrics sink shutting down");
                            return;
                        }
                        Err(broadcast::error::RecvError::Lagged(_)) => {}
                    }
                }
                batch = self.rx.recv_async() => {
                    let Ok(batch) = batch else {
                        self.logger.emit("info", "datadog metrics channel closed");
                        return;
                    };
                    let series = self.build_series(&batch);
                    if let Err(err) = self.send_with_retry(&series).await {
                        self.logger.emit("warn", &format!("datadog metrics send failed after retries: {err}"));
                    }
                }
            }
        }
    }

    fn build_series(&mut self, batch: &MetricBatch) -> Vec<DdSeries> {
        let base_tags = self.base_tags();
        let mut series: Vec<DdSeries> = Vec::new();
        for point in &batch.points {
            self.append_point_series(point, &base_tags, &mut series);
        }
        for disk in &batch.disks {
            append_disk_series(disk, &base_tags, &mut series);
        }
        series
    }

    fn append_point_series(
        &mut self,
        point: &MetricPoint,
        base_tags: &[String],
        out: &mut Vec<DdSeries>,
    ) {
        let timestamp_secs = point.ts / 1000;

        if point.source == "node" {
            out.push(gauge(
                "maestro.node.cpu.percent",
                timestamp_secs,
                point.cpu_percent,
                base_tags.to_vec(),
                Some("percent"),
            ));
            out.push(gauge(
                "maestro.node.memory.bytes",
                timestamp_secs,
                point.memory_bytes as f64,
                base_tags.to_vec(),
                Some("byte"),
            ));
            self.append_net_rate(
                point,
                base_tags,
                "maestro.node.network.rx.bytes_per_sec",
                "maestro.node.network.tx.bytes_per_sec",
                out,
            );
            return;
        }

        if point.source == "cluster" {
            out.push(gauge(
                "maestro.cluster.cpu.percent",
                timestamp_secs,
                point.cpu_percent,
                base_tags.to_vec(),
                Some("percent"),
            ));
            out.push(gauge(
                "maestro.cluster.memory.bytes",
                timestamp_secs,
                point.memory_bytes as f64,
                base_tags.to_vec(),
                Some("byte"),
            ));
            self.append_net_rate(
                point,
                base_tags,
                "maestro.cluster.network.rx.bytes_per_sec",
                "maestro.cluster.network.tx.bytes_per_sec",
                out,
            );
            return;
        }

        if let Some(container_name) = point.source.strip_prefix("container:") {
            let Some(parsed) = parse_user_container_name(container_name) else {
                return;
            };
            let mut tags = base_tags.to_vec();
            tags.push(format!("service:{}", parsed.service_id));
            tags.push(format!("deployment:{}", parsed.deployment_short));
            tags.push(format!("replica:{}", parsed.replica_index));
            out.push(gauge(
                "maestro.service.cpu.percent",
                timestamp_secs,
                point.cpu_percent,
                tags.clone(),
                Some("percent"),
            ));
            out.push(gauge(
                "maestro.service.memory.bytes",
                timestamp_secs,
                point.memory_bytes as f64,
                tags.clone(),
                Some("byte"),
            ));
            self.append_net_rate(
                point,
                &tags,
                "maestro.service.network.rx.bytes_per_sec",
                "maestro.service.network.tx.bytes_per_sec",
                out,
            );
        }
    }

    fn append_net_rate(
        &mut self,
        point: &MetricPoint,
        tags: &[String],
        rx_metric: &str,
        tx_metric: &str,
        out: &mut Vec<DdSeries>,
    ) {
        let timestamp_secs = point.ts / 1000;
        let prev = self.previous_net.insert(
            point.source.clone(),
            NetSample {
                ts: point.ts,
                rx_bytes: point.net_rx_bytes,
                tx_bytes: point.net_tx_bytes,
            },
        );
        let Some(prev) = prev else {
            return;
        };
        let dt_secs = (point.ts - prev.ts) as f64 / 1000.0;
        if dt_secs <= 0.0 {
            return;
        }
        let rx_delta = point.net_rx_bytes.saturating_sub(prev.rx_bytes).max(0) as f64;
        let tx_delta = point.net_tx_bytes.saturating_sub(prev.tx_bytes).max(0) as f64;
        out.push(gauge(
            rx_metric,
            timestamp_secs,
            rx_delta / dt_secs,
            tags.to_vec(),
            Some("byte"),
        ));
        out.push(gauge(
            tx_metric,
            timestamp_secs,
            tx_delta / dt_secs,
            tags.to_vec(),
            Some("byte"),
        ));
    }

    fn base_tags(&self) -> Vec<String> {
        let mut tags = Vec::with_capacity(self.global_tags.len() + 2);
        tags.push(format!("cluster:{}", self.cluster_name));
        tags.push(format!("host:{}", self.hostname));
        tags.extend(self.global_tags.iter().cloned());
        tags
    }

    async fn send_with_retry(&self, series: &[DdSeries]) -> Result<()> {
        if series.is_empty() {
            return Ok(());
        }
        let mut backoff = ExponentialBuilder::default()
            .with_min_delay(Duration::from_millis(200))
            .with_max_delay(Duration::from_secs(5))
            .with_factor(2.0)
            .with_max_times(RETRY_MAX_ATTEMPTS - 1)
            .build();
        loop {
            match self.send_once(series).await {
                Ok(()) => return Ok(()),
                Err(err) => match backoff.next() {
                    Some(delay) => {
                        self.logger.emit(
                            "warn",
                            &format!("datadog metrics send error, retrying in {delay:?}: {err}"),
                        );
                        tokio::time::sleep(delay).await;
                    }
                    None => return Err(err),
                },
            }
        }
    }

    async fn send_once(&self, series: &[DdSeries]) -> Result<()> {
        let response = self
            .client
            .post(&self.endpoint)
            .header("DD-API-KEY", self.api_key.as_str())
            .json(&DdPayload { series })
            .send()
            .await
            .map_err(|err| anyhow!("HTTP error: {err}"))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(anyhow!("datadog returned {status}: {body}"));
        }
        Ok(())
    }
}

fn append_disk_series(disk: &DiskPoint, base_tags: &[String], out: &mut Vec<DdSeries>) {
    let timestamp_secs = disk.ts / 1000;
    let used = (disk.total_bytes - disk.available_bytes).max(0);
    let mut tags = base_tags.to_vec();
    tags.push(format!("mount:{}", disk.mount_point));
    out.push(gauge(
        "maestro.node.disk.used.bytes",
        timestamp_secs,
        used as f64,
        tags.clone(),
        Some("byte"),
    ));
    out.push(gauge(
        "maestro.node.disk.total.bytes",
        timestamp_secs,
        disk.total_bytes as f64,
        tags.clone(),
        Some("byte"),
    ));
    if disk.total_bytes > 0 {
        let pct = (used as f64 / disk.total_bytes as f64) * 100.0;
        out.push(gauge(
            "maestro.node.disk.usage.percent",
            timestamp_secs,
            pct,
            tags,
            Some("percent"),
        ));
    }
}

#[derive(Debug, Serialize)]
struct DdPayload<'a> {
    series: &'a [DdSeries],
}

#[derive(Debug, Serialize)]
struct DdSeries {
    metric: String,
    #[serde(rename = "type")]
    metric_type: u8,
    points: Vec<DdPoint>,
    tags: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    unit: Option<&'static str>,
}

#[derive(Debug, Serialize)]
struct DdPoint {
    timestamp: i64,
    value: f64,
}

fn gauge(
    metric: &str,
    timestamp: i64,
    value: f64,
    tags: Vec<String>,
    unit: Option<&'static str>,
) -> DdSeries {
    DdSeries {
        metric: metric.to_string(),
        metric_type: DATADOG_GAUGE,
        points: vec![DdPoint { timestamp, value }],
        tags,
        unit,
    }
}

struct ParsedContainer {
    service_id: String,
    deployment_short: String,
    replica_index: u32,
}

fn parse_user_container_name(name: &str) -> Option<ParsedContainer> {
    if name.starts_with("maestro-") {
        return None;
    }
    let mut working = name;
    let mut replica_index: u32 = 0;
    if let Some(idx) = working.rfind('-') {
        let tail = &working[idx + 1..];
        if let Ok(parsed) = tail.parse::<u32>() {
            replica_index = parsed;
            working = &working[..idx];
        }
    }
    let idx = working.rfind('-')?;
    let tail = &working[idx + 1..];
    if tail.len() != 6 || !tail.chars().all(|c| c.is_ascii_alphanumeric()) {
        return None;
    }
    let service_id = working[..idx].to_string();
    if service_id.is_empty() {
        return None;
    }
    Some(ParsedContainer {
        service_id,
        deployment_short: tail.to_string(),
        replica_index,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_replica_zero() {
        let parsed = parse_user_container_name("app-Erxt1b").unwrap();
        assert_eq!(parsed.service_id, "app");
        assert_eq!(parsed.deployment_short, "Erxt1b");
        assert_eq!(parsed.replica_index, 0);
    }

    #[test]
    fn parses_higher_replica_index() {
        let parsed = parse_user_container_name("redis-PAmRwS-3").unwrap();
        assert_eq!(parsed.service_id, "redis");
        assert_eq!(parsed.deployment_short, "PAmRwS");
        assert_eq!(parsed.replica_index, 3);
    }

    #[test]
    fn parses_hyphenated_service_id() {
        let parsed = parse_user_container_name("agent-server-291Qho").unwrap();
        assert_eq!(parsed.service_id, "agent-server");
        assert_eq!(parsed.deployment_short, "291Qho");
        assert_eq!(parsed.replica_index, 0);
    }

    #[test]
    fn skips_system_containers() {
        assert!(parse_user_container_name("maestro-etcd-baton-prod").is_none());
        assert!(parse_user_container_name("maestro-tailscale-cluster").is_none());
    }

    #[test]
    fn skips_malformed_names() {
        assert!(parse_user_container_name("standalone").is_none());
        assert!(parse_user_container_name("svc-TOOLONG").is_none());
        assert!(parse_user_container_name("svc-tooSh").is_none());
    }
}
