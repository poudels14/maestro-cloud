use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use kernel_api::{ClusterId, NodeId, Timestamp, WorkloadId};
use kernel_store::Clock;
use runtime::{WorkloadMetadata, WorkloadRuntime, WorkloadState};
use tokio::sync::watch;

use crate::StatusClock;
use crate::cgroup_stats::{CgroupStats, CgroupStatsReader};

/// Node and cluster scope used to discover owned runtime workloads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkloadStatsSettings {
    /// Cluster whose workloads are sampled.
    pub cluster_id: ClusterId,
    /// Local node whose runtime objects are sampled.
    pub node_id: NodeId,
    /// Delay between complete workload sampling passes.
    pub poll_interval: Duration,
}

/// One timestamped workload sample retaining durable ownership labels.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkloadStatsSample {
    /// Runtime ownership metadata used for service and deployment tags.
    pub metadata: WorkloadMetadata,
    /// Wall-clock collection time.
    pub collected_at: Timestamp,
    /// Backend-neutral cgroup v2 counters.
    pub stats: CgroupStats,
}

/// Point in collection at which one workload could not be sampled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkloadStatsFailureStage {
    /// The runtime could not resolve its native object to a cgroup path.
    ResolveCgroup,
    /// The resolved cgroup could not be read safely.
    ReadCgroup,
}

/// Isolated per-workload sampling failure that does not discard healthy samples.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkloadStatsFailure {
    /// Workload that failed collection.
    pub workload_id: WorkloadId,
    /// Collection stage that failed.
    pub stage: WorkloadStatsFailureStage,
    /// Safe diagnostic from the runtime or filesystem reader.
    pub message: String,
}

/// Result of one node-local stats sweep.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WorkloadStatsReport {
    /// Running or paused workloads selected for sampling.
    pub observed: usize,
    /// Successfully collected samples.
    pub samples: Vec<WorkloadStatsSample>,
    /// Samples accepted by the configured sink.
    pub delivered: usize,
    /// Batch delivery failure isolated from the next sampling pass.
    pub delivery_failure: Option<WorkloadStatsSinkError>,
    /// Failures isolated to individual workloads.
    pub failures: Vec<WorkloadStatsFailure>,
}

/// Delivery boundary for one complete node-local workload stats batch.
#[async_trait]
pub trait WorkloadStatsSink: Send + Sync + 'static {
    /// Accepts one timestamped batch without retaining borrowed sample data.
    async fn ingest(&self, samples: &[WorkloadStatsSample]) -> Result<(), WorkloadStatsSinkError>;
}

/// A workload stats sink could not accept a valid batch.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum WorkloadStatsSinkError {
    /// The batch permanently violates the sink's contract.
    #[error("workload stats sink rejected batch: {message}")]
    Rejected {
        /// Stable rejection detail.
        message: String,
    },
    /// The sink is temporarily unable to accept the batch.
    #[error("workload stats sink is unavailable: {message}")]
    Unavailable {
        /// Safe availability detail.
        message: String,
    },
}

/// Collects uniform cgroup stats without invoking backend-specific stats APIs.
pub struct WorkloadStatsAgent {
    runtime: Arc<dyn WorkloadRuntime>,
    reader: Arc<dyn CgroupStatsReader>,
    settings: WorkloadStatsSettings,
    clock: Arc<dyn StatusClock>,
    monotonic_clock: Arc<dyn Clock>,
    sink: Arc<dyn WorkloadStatsSink>,
}

impl WorkloadStatsAgent {
    /// Constructs a node-local collector without starting background work.
    pub fn new(
        runtime: Arc<dyn WorkloadRuntime>,
        reader: Arc<dyn CgroupStatsReader>,
        sink: Arc<dyn WorkloadStatsSink>,
        settings: WorkloadStatsSettings,
        clock: Arc<dyn StatusClock>,
        monotonic_clock: Arc<dyn Clock>,
    ) -> Result<Self, WorkloadStatsAgentError> {
        if settings.poll_interval.is_zero() {
            return Err(WorkloadStatsAgentError::InvalidPollInterval);
        }
        Ok(Self {
            runtime,
            reader,
            sink,
            settings,
            clock,
            monotonic_clock,
        })
    }

    /// Repeatedly samples and delivers finite snapshots until shutdown.
    pub async fn run(
        &self,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<(), WorkloadStatsAgentError> {
        loop {
            if *shutdown.borrow() {
                return Ok(());
            }
            let _report = self.collect().await?;
            let next_poll = self
                .monotonic_clock
                .now()
                .saturating_add(self.settings.poll_interval);
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return Ok(());
                    }
                }
                () = self.monotonic_clock.sleep_until(next_poll) => {}
            }
        }
    }

    /// Samples every running or paused local workload once.
    pub async fn collect(&self) -> Result<WorkloadStatsReport, WorkloadStatsAgentError> {
        let workloads = self
            .runtime
            .list(&self.settings.cluster_id, &self.settings.node_id)
            .await?;
        let selected = workloads
            .into_iter()
            .filter(|workload| {
                matches!(
                    workload.status.state,
                    WorkloadState::Running | WorkloadState::Paused
                )
            })
            .collect::<Vec<_>>();
        let mut report = WorkloadStatsReport {
            observed: selected.len(),
            ..Default::default()
        };
        for workload in selected {
            let path = match self.runtime.stats_handle(&workload.handle).await {
                Ok(path) => path,
                Err(error) => {
                    report.failures.push(WorkloadStatsFailure {
                        workload_id: workload.metadata.workload_id,
                        stage: WorkloadStatsFailureStage::ResolveCgroup,
                        message: error.to_string(),
                    });
                    continue;
                }
            };
            match self.reader.read(&path).await {
                Ok(stats) => report.samples.push(WorkloadStatsSample {
                    metadata: workload.metadata,
                    collected_at: self.clock.now(),
                    stats,
                }),
                Err(error) => report.failures.push(WorkloadStatsFailure {
                    workload_id: workload.metadata.workload_id,
                    stage: WorkloadStatsFailureStage::ReadCgroup,
                    message: error.to_string(),
                }),
            }
        }
        report
            .samples
            .sort_by(|left, right| left.metadata.workload_id.cmp(&right.metadata.workload_id));
        report
            .failures
            .sort_by(|left, right| left.workload_id.cmp(&right.workload_id));
        if !report.samples.is_empty() {
            match self.sink.ingest(&report.samples).await {
                Ok(()) => report.delivered = report.samples.len(),
                Err(error) => report.delivery_failure = Some(error),
            }
        }
        Ok(report)
    }
}

/// Failure to list the runtime snapshot for a stats sweep.
#[derive(Debug, thiserror::Error)]
pub enum WorkloadStatsAgentError {
    /// A zero poll interval would create an unbounded hot loop.
    #[error("workload stats poll interval must be non-zero")]
    InvalidPollInterval,
    /// The runtime ownership snapshot was unavailable.
    #[error(transparent)]
    Runtime(#[from] runtime::RuntimeError),
}
