//! Lightweight per-process counters for the cluster subsystem. Surfaced via
//! `/api/cluster/metrics`; scrape into Prometheus or Datadog from there.
//!
//! Atomic counters so emit sites don't need locks. The metrics deliberately
//! avoid the full [`crate::metrics::MetricsCollector`] pipeline — these are
//! observability for the cluster *control plane*, not the workload resources.
//!
//! Cross-process plumbing: the daemon increments counters locally; a periodic
//! task publishes a snapshot to etcd at `cluster/metrics/{node-id}`. The probe
//! container's HTTP server reads from etcd to surface them via the API.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::{Result, anyhow};
use etcd_client::{Client as EtcdClient, PutOptions};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, broadcast};

#[derive(Debug, Default)]
pub struct ClusterMetrics {
    pub leader_campaigns_started: AtomicU64,
    pub leader_campaigns_won: AtomicU64,
    pub leader_resigns: AtomicU64,
    pub scheduling_ticks: AtomicU64,
    pub scheduling_tick_failures: AtomicU64,
    pub scheduling_tick_duration_ms_total: AtomicU64,
    pub assignments_started: AtomicU64,
    pub assignments_stopped: AtomicU64,
    pub assignments_start_failures: AtomicU64,
    pub stale_assignments_swept: AtomicU64,
    pub current_unhealthy_replicas: AtomicU64,
}

pub const METRICS_PREFIX: &str = "cluster/metrics/";
pub const METRICS_LEASE_TTL_SECS: i64 = 60;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterMetricsSnapshot {
    pub leader_campaigns_started: u64,
    pub leader_campaigns_won: u64,
    pub leader_resigns: u64,
    pub scheduling_ticks: u64,
    pub scheduling_tick_failures: u64,
    pub scheduling_tick_avg_duration_ms: f64,
    pub assignments_started: u64,
    pub assignments_stopped: u64,
    pub assignments_start_failures: u64,
    pub stale_assignments_swept: u64,
    pub current_unhealthy_replicas: u64,
}

impl ClusterMetrics {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn incr(counter: &AtomicU64) {
        counter.fetch_add(1, Ordering::Relaxed);
    }

    pub fn add(counter: &AtomicU64, value: u64) {
        counter.fetch_add(value, Ordering::Relaxed);
    }

    pub fn set(counter: &AtomicU64, value: u64) {
        counter.store(value, Ordering::Relaxed);
    }

    /// Spawn a task that writes [`Self::snapshot`] to etcd every `interval`
    /// under `cluster/metrics/{node_id}` with a leased TTL so it disappears
    /// when this node dies.
    pub fn spawn_publisher(
        self: &Arc<Self>,
        client: Arc<Mutex<EtcdClient>>,
        node_id: String,
        interval: Duration,
        mut shutdown_rx: broadcast::Receiver<crate::signal::ShutdownEvent>,
    ) -> tokio::task::JoinHandle<()> {
        let metrics = Arc::clone(self);
        tokio::spawn(async move {
            let key = format!("{METRICS_PREFIX}{node_id}");
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            ticker.tick().await;
            loop {
                tokio::select! {
                    _ = shutdown_rx.recv() => return,
                    _ = ticker.tick() => {
                        if let Err(err) = publish_snapshot(&client, &key, &metrics.snapshot()).await {
                            eprintln!("cluster metrics publish failed: {err}");
                        }
                    }
                }
            }
        })
    }

    pub fn snapshot(&self) -> ClusterMetricsSnapshot {
        let ticks = self.scheduling_ticks.load(Ordering::Relaxed);
        let total_ms = self
            .scheduling_tick_duration_ms_total
            .load(Ordering::Relaxed);
        let avg = if ticks > 0 {
            total_ms as f64 / ticks as f64
        } else {
            0.0
        };
        ClusterMetricsSnapshot {
            leader_campaigns_started: self.leader_campaigns_started.load(Ordering::Relaxed),
            leader_campaigns_won: self.leader_campaigns_won.load(Ordering::Relaxed),
            leader_resigns: self.leader_resigns.load(Ordering::Relaxed),
            scheduling_ticks: ticks,
            scheduling_tick_failures: self.scheduling_tick_failures.load(Ordering::Relaxed),
            scheduling_tick_avg_duration_ms: avg,
            assignments_started: self.assignments_started.load(Ordering::Relaxed),
            assignments_stopped: self.assignments_stopped.load(Ordering::Relaxed),
            assignments_start_failures: self.assignments_start_failures.load(Ordering::Relaxed),
            stale_assignments_swept: self.stale_assignments_swept.load(Ordering::Relaxed),
            current_unhealthy_replicas: self.current_unhealthy_replicas.load(Ordering::Relaxed),
        }
    }
}

async fn publish_snapshot(
    client: &Arc<Mutex<EtcdClient>>,
    key: &str,
    snapshot: &ClusterMetricsSnapshot,
) -> Result<()> {
    let body = serde_json::to_vec(snapshot)
        .map_err(|err| anyhow!("failed to serialize metrics snapshot: {err}"))?;
    let mut etcd = client.lock().await;
    let lease = etcd
        .lease_grant(METRICS_LEASE_TTL_SECS, None)
        .await
        .map_err(|err| anyhow!("failed to grant metrics lease: {err}"))?;
    etcd.put(key, body, Some(PutOptions::new().with_lease(lease.id())))
        .await
        .map_err(|err| anyhow!("failed to put metrics snapshot: {err}"))?;
    Ok(())
}

/// Read all per-node metrics snapshots from etcd. Used by the API server in
/// the probe container, which doesn't have direct access to the live counters
/// in the daemon process.
pub async fn read_all_snapshots(
    client: &Arc<Mutex<EtcdClient>>,
) -> Result<Vec<(String, ClusterMetricsSnapshot)>> {
    let mut etcd = client.lock().await;
    let response = etcd
        .get(
            METRICS_PREFIX,
            Some(etcd_client::GetOptions::new().with_prefix()),
        )
        .await
        .map_err(|err| anyhow!("failed to list metrics snapshots: {err}"))?;
    let mut entries = Vec::with_capacity(response.kvs().len());
    for kv in response.kvs() {
        let key = match kv.key_str() {
            Ok(key) => key,
            Err(_) => continue,
        };
        let node_id = key.trim_start_matches(METRICS_PREFIX).to_string();
        match serde_json::from_slice::<ClusterMetricsSnapshot>(kv.value()) {
            Ok(snapshot) => entries.push((node_id, snapshot)),
            Err(err) => eprintln!("skipping malformed metrics snapshot for {node_id}: {err}"),
        }
    }
    entries.sort_by(|left, right| left.0.cmp(&right.0));
    Ok(entries)
}
