use std::collections::HashMap;
use std::time::Duration;

use anyhow::{Result, anyhow};
use sysinfo::System;
use tokio::process::Command;
use tokio::sync::broadcast;

use crate::signal::ShutdownEvent;

const COLLECT_INTERVAL: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MetricPoint {
    pub ts: i64,
    pub source: String,
    pub cpu_percent: f64,
    pub memory_bytes: i64,
    pub memory_limit_bytes: i64,
    pub net_rx_bytes: i64,
    pub net_tx_bytes: i64,
}

pub struct MetricsCollector {
    endpoint: String,
    cluster_suffix: String,
    runtime_cli: String,
    client: reqwest::Client,
    signal_rx: broadcast::Receiver<ShutdownEvent>,
    logger: crate::logs::SystemLogger,
}

impl MetricsCollector {
    pub fn new(
        endpoint: String,
        cluster_name: String,
        runtime_cli: String,
        signal_rx: broadcast::Receiver<ShutdownEvent>,
        logger: crate::logs::SystemLogger,
    ) -> Self {
        Self {
            endpoint,
            cluster_suffix: format!("-{cluster_name}"),
            runtime_cli,
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .expect("failed to build metrics http client"),
            signal_rx,
            logger,
        }
    }

    pub async fn run(mut self) {
        self.logger.emit(
            "info",
            &format!(
                "metrics collector started (interval: {}s)",
                COLLECT_INTERVAL.as_secs()
            ),
        );
        loop {
            tokio::select! {
                signal = self.signal_rx.recv() => {
                    match signal {
                        Ok(ShutdownEvent::Graceful) | Ok(ShutdownEvent::Force)
                        | Err(broadcast::error::RecvError::Closed) => {
                            self.logger.emit("info", "metrics collector shutting down");
                            return;
                        }
                        Err(broadcast::error::RecvError::Lagged(_)) => {}
                    }
                }
                _ = tokio::time::sleep(COLLECT_INTERVAL) => {
                    if let Err(err) = self.collect_and_send().await {
                        self.logger.emit("error", &format!("metrics collection error: {err}"));
                    }
                }
            }
        }
    }

    async fn collect_and_send(&self) -> Result<()> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;

        let mut points: Vec<MetricPoint> = Vec::new();

        points.push(collect_node_metrics(now));

        let raw_stats = collect_container_stats(&self.runtime_cli)
            .await
            .unwrap_or_default();
        let mut service_agg: HashMap<String, MetricPoint> = HashMap::new();
        let mut cluster = MetricPoint {
            ts: now,
            source: "cluster".to_string(),
            cpu_percent: 0.0,
            memory_bytes: 0,
            memory_limit_bytes: 0,
            net_rx_bytes: 0,
            net_tx_bytes: 0,
        };

        for stat in &raw_stats {
            let container_point = MetricPoint {
                ts: now,
                source: format!("container:{}", stat.name),
                cpu_percent: stat.cpu_percent,
                memory_bytes: stat.memory_bytes,
                memory_limit_bytes: stat.memory_limit_bytes,
                net_rx_bytes: stat.net_rx_bytes,
                net_tx_bytes: stat.net_tx_bytes,
            };
            points.push(container_point);

            cluster.cpu_percent += stat.cpu_percent;
            cluster.memory_bytes += stat.memory_bytes;
            cluster.memory_limit_bytes = stat.memory_limit_bytes;
            cluster.net_rx_bytes += stat.net_rx_bytes;
            cluster.net_tx_bytes += stat.net_tx_bytes;

            let service_id = extract_service_id(&stat.name, &self.cluster_suffix);
            let entry = service_agg
                .entry(service_id.clone())
                .or_insert_with(|| MetricPoint {
                    ts: now,
                    source: format!("service:{service_id}"),
                    cpu_percent: 0.0,
                    memory_bytes: 0,
                    memory_limit_bytes: stat.memory_limit_bytes,
                    net_rx_bytes: 0,
                    net_tx_bytes: 0,
                });
            entry.cpu_percent += stat.cpu_percent;
            entry.memory_bytes += stat.memory_bytes;
            entry.net_rx_bytes += stat.net_rx_bytes;
            entry.net_tx_bytes += stat.net_tx_bytes;
        }

        points.push(cluster);
        points.extend(service_agg.into_values());

        self.client
            .post(&self.endpoint)
            .json(&points)
            .send()
            .await
            .map_err(|err| anyhow!("failed to send metrics: {err}"))?;

        Ok(())
    }
}

fn collect_node_metrics(ts: i64) -> MetricPoint {
    let mut sys = System::new();
    sys.refresh_cpu_all();
    std::thread::sleep(Duration::from_millis(200));
    sys.refresh_cpu_all();
    sys.refresh_memory();

    let cpu_percent = sys.global_cpu_usage() as f64;
    let memory_bytes = sys.used_memory() as i64;
    let memory_limit_bytes = sys.total_memory() as i64;

    let networks = sysinfo::Networks::new_with_refreshed_list();
    let mut net_rx: i64 = 0;
    let mut net_tx: i64 = 0;
    for (_name, data) in &networks {
        net_rx += data.total_received() as i64;
        net_tx += data.total_transmitted() as i64;
    }

    MetricPoint {
        ts,
        source: "node".to_string(),
        cpu_percent,
        memory_bytes,
        memory_limit_bytes,
        net_rx_bytes: net_rx,
        net_tx_bytes: net_tx,
    }
}

struct ContainerStats {
    name: String,
    cpu_percent: f64,
    memory_bytes: i64,
    memory_limit_bytes: i64,
    net_rx_bytes: i64,
    net_tx_bytes: i64,
}

async fn collect_container_stats(runtime_cli: &str) -> Result<Vec<ContainerStats>> {
    let output = Command::new(runtime_cli)
        .args([
            "stats",
            "--no-stream",
            "--no-trunc",
            "--format",
            "{{json .}}",
        ])
        .output()
        .await
        .map_err(|err| anyhow!("failed to run {runtime_cli} stats: {err}"))?;

    if !output.status.success() {
        return Ok(Vec::new());
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut results = Vec::new();

    for line in stdout.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(line) {
            let name = parsed["Name"].as_str().unwrap_or("").to_string();
            if name.is_empty() {
                continue;
            }
            let cpu_percent = parse_percent(parsed["CPUPerc"].as_str().unwrap_or("0%"));
            let (memory_bytes, memory_limit_bytes) =
                parse_mem_usage(parsed["MemUsage"].as_str().unwrap_or("0B / 0B"));
            let (net_rx_bytes, net_tx_bytes) =
                parse_net_io(parsed["NetIO"].as_str().unwrap_or("0B / 0B"));

            results.push(ContainerStats {
                name,
                cpu_percent,
                memory_bytes,
                memory_limit_bytes,
                net_rx_bytes,
                net_tx_bytes,
            });
        }
    }

    Ok(results)
}

fn extract_service_id(container_name: &str, cluster_suffix: &str) -> String {
    if let Some(base) = container_name.strip_suffix(cluster_suffix) {
        return base.to_string();
    }
    let mut name = container_name;
    if let Some(idx) = name.rfind('-') {
        let tail = &name[idx + 1..];
        if tail.parse::<u32>().is_ok() {
            name = &name[..idx];
        }
    }
    if let Some(idx) = name.rfind('-') {
        let suffix = &name[idx + 1..];
        if suffix.len() == 6 && suffix.chars().all(|c| c.is_ascii_alphanumeric()) {
            return name[..idx].to_string();
        }
    }
    name.to_string()
}

fn parse_percent(s: &str) -> f64 {
    s.trim_end_matches('%').trim().parse().unwrap_or(0.0)
}

fn parse_byte_value(s: &str) -> i64 {
    let s = s.trim();
    let (num_str, multiplier): (&str, i64) = if let Some(n) = s.strip_suffix("GiB") {
        (n, 1024 * 1024 * 1024)
    } else if let Some(n) = s.strip_suffix("MiB") {
        (n, 1024 * 1024)
    } else if let Some(n) = s.strip_suffix("KiB") {
        (n, 1024)
    } else if let Some(n) = s.strip_suffix("GB") {
        (n, 1_000_000_000)
    } else if let Some(n) = s.strip_suffix("MB") {
        (n, 1_000_000)
    } else if let Some(n) = s.strip_suffix("kB") {
        (n, 1_000)
    } else if let Some(n) = s.strip_suffix('B') {
        (n, 1)
    } else {
        (s, 1)
    };
    let num: f64 = num_str.trim().parse().unwrap_or(0.0);
    (num * multiplier as f64) as i64
}

fn parse_mem_usage(s: &str) -> (i64, i64) {
    let parts: Vec<&str> = s.split('/').collect();
    if parts.len() == 2 {
        (parse_byte_value(parts[0]), parse_byte_value(parts[1]))
    } else {
        (0, 0)
    }
}

fn parse_net_io(s: &str) -> (i64, i64) {
    let parts: Vec<&str> = s.split('/').collect();
    if parts.len() == 2 {
        (parse_byte_value(parts[0]), parse_byte_value(parts[1]))
    } else {
        (0, 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_percent_values() {
        assert_eq!(parse_percent("0.00%"), 0.0);
        assert_eq!(parse_percent("12.34%"), 12.34);
        assert_eq!(parse_percent("100.00%"), 100.0);
    }

    #[test]
    fn parse_byte_values() {
        assert_eq!(parse_byte_value("0B"), 0);
        assert_eq!(parse_byte_value("5.36kB"), 5360);
        assert!(parse_byte_value("2.891MiB") > 3_000_000);
        assert!(parse_byte_value("15.6GiB") > 16_000_000_000);
        assert_eq!(parse_byte_value("1.78MB"), 1780000);
    }

    #[test]
    fn parse_mem_usage_string() {
        let (used, limit) = parse_mem_usage("2.891MiB / 15.6GiB");
        assert!(used > 3_000_000);
        assert!(limit > 16_000_000_000);
    }

    #[test]
    fn parse_net_io_string() {
        let (rx, tx) = parse_net_io("5.36kB / 2kB");
        assert_eq!(rx, 5360);
        assert_eq!(tx, 2000);
    }

    #[test]
    fn extract_service_id_from_container_name() {
        let suffix = "-cluster-1";
        assert_eq!(extract_service_id("whoami-aBc123", suffix), "whoami");
        assert_eq!(
            extract_service_id("my-service-aBc123", suffix),
            "my-service"
        );
        assert_eq!(
            extract_service_id("maestro-etcd-cluster-1", suffix),
            "maestro-etcd"
        );
        assert_eq!(
            extract_service_id("maestro-probe-cluster-1", suffix),
            "maestro-probe"
        );
        assert_eq!(extract_service_id("my-svc-aBc123-1", suffix), "my-svc");
        assert_eq!(extract_service_id("standalone", suffix), "standalone");
    }
}
