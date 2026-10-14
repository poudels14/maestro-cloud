use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::net::UnixListener;
use tokio::signal::unix::{SignalKind, signal};

use crate::api::router;
use crate::error::Result;
use crate::metrics::MetricsCollector;
use crate::resolver::{DeploymentFromReplicaId, ReplicaResolver};
use crate::store::LogStore;
use crate::tailer::Tailer;

const SWEEP_INTERVAL: Duration = Duration::from_secs(60);
const METRICS_RETENTION_MS: i64 = 7 * 24 * 60 * 60 * 1000;
const METRICS_PRUNE_INTERVAL: Duration = Duration::from_secs(24 * 60 * 60);

#[derive(Clone)]
pub struct TelemetrySettings {
    pub log_dir: PathBuf,
    pub data_dir: PathBuf,
    pub socket: PathBuf,
    pub poll_interval: Duration,
    pub idle_retention: Duration,
    pub rollover_interval: Duration,
    pub metrics_interval: Duration,
    pub cgroup_root: PathBuf,
    pub proc_root: PathBuf,
    pub resolver: Arc<dyn ReplicaResolver>,
}

pub async fn run(settings: TelemetrySettings) -> Result<()> {
    let data_dir = settings.data_dir.clone();
    let store = Arc::new(tokio::task::spawn_blocking(move || LogStore::open(&data_dir)).await??);
    let tailer = {
        let store = store.clone();
        let log_dir = settings.log_dir.clone();
        let resolver = settings.resolver.clone();
        tokio::task::spawn_blocking(move || Tailer::new(log_dir, store, resolver)).await??
    };
    let listener = bind_socket(&settings.socket)?;
    let mut terminate = signal(SignalKind::terminate())?;
    tracing::info!(
        log_dir = %settings.log_dir.display(),
        socket = %settings.socket.display(),
        "telemetry started"
    );
    tokio::select! {
        result = tail(tailer, settings.poll_interval, settings.idle_retention) => result,
        result = rollover(store.clone(), settings.rollover_interval) => result,
        result = collect_metrics(
            store.clone(),
            MetricsCollector::new(
                settings.cgroup_root.clone(),
                settings.proc_root.clone(),
                settings.resolver.clone(),
            ),
            settings.metrics_interval,
        ) => result,
        result = axum::serve(listener, router(store)) => Ok(result?),
        _ = tokio::signal::ctrl_c() => Ok(()),
        _ = terminate.recv() => Ok(()),
    }
}

impl Default for TelemetrySettings {
    fn default() -> Self {
        Self {
            log_dir: PathBuf::from("/var/lib/maestro/logs"),
            data_dir: PathBuf::from("/var/lib/maestro/telemetry"),
            socket: PathBuf::from("/run/maestro/telemetry.sock"),
            poll_interval: Duration::from_millis(500),
            idle_retention: Duration::from_secs(3600),
            rollover_interval: Duration::from_secs(3600),
            metrics_interval: Duration::from_secs(15),
            cgroup_root: PathBuf::from("/sys/fs/cgroup/maestro.slice/maestro-workloads.slice"),
            proc_root: PathBuf::from("/proc"),
            resolver: Arc::new(DeploymentFromReplicaId),
        }
    }
}

async fn collect_metrics(
    store: Arc<LogStore>,
    mut collector: MetricsCollector,
    interval: Duration,
) -> Result<()> {
    let mut ticker = tokio::time::interval(interval);
    let mut last_prune = Instant::now();
    loop {
        ticker.tick().await;
        let points = collector.collect(now_ms());
        let prune = last_prune.elapsed() >= METRICS_PRUNE_INTERVAL;
        let writer = store.clone();
        let written = tokio::task::spawn_blocking(move || {
            writer.insert_metrics(&points)?;
            if prune {
                writer.prune_metrics(now_ms() - METRICS_RETENTION_MS)?;
            }
            Ok::<_, crate::error::TelemetryError>(())
        })
        .await?;
        if let Err(error) = written {
            tracing::warn!(%error, "failed to record metrics");
        }
        if prune {
            last_prune = Instant::now();
        }
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_millis() as i64)
}

async fn tail(mut tailer: Tailer, poll_interval: Duration, idle_retention: Duration) -> Result<()> {
    let mut last_sweep = Instant::now();
    loop {
        let sweep_due = last_sweep.elapsed() >= SWEEP_INTERVAL;
        tailer = tokio::task::spawn_blocking(move || {
            if let Err(error) = tailer.poll() {
                tracing::warn!(%error, "log tailing failed");
            }
            if sweep_due && let Err(error) = tailer.sweep_idle(idle_retention) {
                tracing::warn!(%error, "log dir retention failed");
            }
            tailer
        })
        .await?;
        if sweep_due {
            last_sweep = Instant::now();
        }
        tokio::time::sleep(poll_interval).await;
    }
}

async fn rollover(store: Arc<LogStore>, interval: Duration) -> Result<()> {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        ticker.tick().await;
        if let Err(error) = store.rollover().await {
            tracing::warn!(%error, "parquet rollover failed");
        }
    }
}

fn bind_socket(path: &Path) -> Result<UnixListener> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    match std::fs::remove_file(path) {
        Err(error) if error.kind() != std::io::ErrorKind::NotFound => Err(error)?,
        _ => {}
    }
    Ok(UnixListener::bind(path)?)
}
