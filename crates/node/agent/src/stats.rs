use std::sync::Arc;

use kernel_api::{ClusterId, NodeId, Timestamp, WorkloadId};
use runtime::{WorkloadMetadata, WorkloadRuntime, WorkloadState};

use crate::StatusClock;
use crate::cgroup_stats::{CgroupStats, CgroupStatsReader};

/// Node and cluster scope used to discover owned runtime workloads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkloadStatsSettings {
    /// Cluster whose workloads are sampled.
    pub cluster_id: ClusterId,
    /// Local node whose runtime objects are sampled.
    pub node_id: NodeId,
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
    /// Failures isolated to individual workloads.
    pub failures: Vec<WorkloadStatsFailure>,
}

/// Collects uniform cgroup stats without invoking backend-specific stats APIs.
pub struct WorkloadStatsAgent {
    runtime: Arc<dyn WorkloadRuntime>,
    reader: Arc<dyn CgroupStatsReader>,
    settings: WorkloadStatsSettings,
    clock: Arc<dyn StatusClock>,
}

impl WorkloadStatsAgent {
    /// Constructs a node-local collector without starting background work.
    pub fn new(
        runtime: Arc<dyn WorkloadRuntime>,
        reader: Arc<dyn CgroupStatsReader>,
        settings: WorkloadStatsSettings,
        clock: Arc<dyn StatusClock>,
    ) -> Self {
        Self {
            runtime,
            reader,
            settings,
            clock,
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
        Ok(report)
    }
}

/// Failure to list the runtime snapshot for a stats sweep.
#[derive(Debug, thiserror::Error)]
pub enum WorkloadStatsAgentError {
    /// The runtime ownership snapshot was unavailable.
    #[error(transparent)]
    Runtime(#[from] runtime::RuntimeError),
}
