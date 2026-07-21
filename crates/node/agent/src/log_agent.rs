use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use kernel_api::{ClusterId, NodeId, Timestamp, WorkloadId};
use kernel_store::Clock;
use runtime::{LogMode, LogRequest, LogSource, WorkloadMetadata, WorkloadRuntime};
use tokio::sync::watch;

use crate::{LogCheckpointError, LogCheckpointStore, StatusClock};

/// Node ownership scope and fairness bound for one runtime log sweep.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeLogAgentSettings {
    /// Cluster whose runtime objects may be shipped.
    pub cluster_id: ClusterId,
    /// Local node whose runtime objects may be shipped.
    pub node_id: NodeId,
    /// Maximum frames examined for one workload in a single sweep.
    pub max_frames_per_workload: usize,
    /// Delay between complete runtime snapshots.
    pub poll_interval: Duration,
}

/// One unmodified runtime frame enriched with durable workload ownership.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkloadLogEntry {
    /// Runtime ownership labels used to route and index the record.
    pub metadata: WorkloadMetadata,
    /// Node wall-clock time at which the frame reached the shipping boundary.
    pub received_at: Timestamp,
    /// Backend-native cursor used as the record's replay identity.
    pub cursor: runtime::LogCursor,
    /// Runtime stream that produced the frame.
    pub source: LogSource,
    /// Unmodified bytes; parsing and line assembly belong to observability.
    pub payload: Vec<u8>,
}

/// Delivery boundary implemented by the configured observability pipeline.
///
/// Implementations must make `(workload_id, cursor)` idempotent. The agent commits a cursor only
/// after this method succeeds, so a process failure between delivery and checkpoint replacement
/// deliberately replays the same entry rather than losing it.
#[async_trait]
pub trait WorkloadLogSink: Send + Sync + 'static {
    /// Accepts one ownership-enriched runtime frame.
    async fn ingest(&self, entry: WorkloadLogEntry) -> Result<(), WorkloadLogSinkError>;
}

/// A sink refused or could not durably accept one log entry.
#[derive(Debug, thiserror::Error)]
pub enum WorkloadLogSinkError {
    /// The entry is permanently invalid for this sink.
    #[error("log sink rejected entry: {message}")]
    Rejected {
        /// Safe rejection detail.
        message: String,
    },
    /// The sink is temporarily unable to accept entries.
    #[error("log sink is unavailable: {message}")]
    Unavailable {
        /// Safe availability detail.
        message: String,
    },
}

/// Point in one workload's log path at which progress stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeLogFailureStage {
    /// The last durable cursor could not be read.
    LoadCheckpoint,
    /// The runtime-native stream could not be opened.
    OpenStream,
    /// An opened stream disconnected or returned invalid data.
    ReadStream,
    /// The sink did not durably accept a frame.
    Deliver,
    /// A delivered cursor could not be atomically persisted.
    CommitCheckpoint,
}

/// Isolated per-workload failure from a log sweep.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeLogFailure {
    /// Workload whose stream stopped making progress.
    pub workload_id: WorkloadId,
    /// Shipping stage that failed.
    pub stage: RuntimeLogFailureStage,
    /// Safe diagnostic from the runtime, sink, or checkpoint store.
    pub message: String,
}

/// Outcome of one bounded pass over all locally owned runtime objects.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RuntimeLogReport {
    /// Runtime objects observed for this cluster and node.
    pub observed: usize,
    /// Frames accepted by the sink, including a frame whose checkpoint then failed.
    pub delivered: usize,
    /// Workloads that reached the per-sweep fairness bound.
    pub saturated: usize,
    /// Checkpoints removed because their runtime objects no longer exist.
    pub stale_checkpoints_removed: usize,
    /// Failures isolated to individual workloads.
    pub failures: Vec<RuntimeLogFailure>,
}

/// Ships runtime-native logs with durable, at-least-once resume positions.
pub struct RuntimeLogAgent {
    runtime: Arc<dyn WorkloadRuntime>,
    checkpoints: Arc<dyn LogCheckpointStore>,
    sink: Arc<dyn WorkloadLogSink>,
    settings: RuntimeLogAgentSettings,
    clock: Arc<dyn StatusClock>,
    monotonic_clock: Arc<dyn Clock>,
}

impl RuntimeLogAgent {
    /// Constructs a bounded log shipper without starting background work.
    pub fn new(
        runtime: Arc<dyn WorkloadRuntime>,
        checkpoints: Arc<dyn LogCheckpointStore>,
        sink: Arc<dyn WorkloadLogSink>,
        settings: RuntimeLogAgentSettings,
        clock: Arc<dyn StatusClock>,
        monotonic_clock: Arc<dyn Clock>,
    ) -> Result<Self, RuntimeLogAgentError> {
        if settings.max_frames_per_workload == 0 {
            return Err(RuntimeLogAgentError::InvalidFrameLimit);
        }
        if settings.poll_interval.is_zero() {
            return Err(RuntimeLogAgentError::InvalidPollInterval);
        }
        Ok(Self {
            runtime,
            checkpoints,
            sink,
            settings,
            clock,
            monotonic_clock,
        })
    }

    /// Repeatedly ships finite snapshots until shutdown, preserving durable cursors per sweep.
    pub async fn run(
        &self,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<(), RuntimeLogAgentError> {
        loop {
            if *shutdown.borrow() {
                return Ok(());
            }
            let _report = self.collect_once().await?;
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

    /// Delivers one finite, fair snapshot from every locally owned runtime object.
    pub async fn collect_once(&self) -> Result<RuntimeLogReport, RuntimeLogAgentError> {
        let mut workloads = self
            .runtime
            .list(&self.settings.cluster_id, &self.settings.node_id)
            .await?;
        workloads.sort_by(|left, right| left.metadata.workload_id.cmp(&right.metadata.workload_id));
        let active_workloads = workloads
            .iter()
            .map(|workload| workload.metadata.workload_id.clone())
            .collect::<BTreeSet<_>>();
        let stale_checkpoints_removed = self.checkpoints.cleanup_stale(&active_workloads).await?;
        let mut report = RuntimeLogReport {
            observed: workloads.len(),
            stale_checkpoints_removed,
            ..Default::default()
        };

        for workload in workloads {
            let workload_id = workload.metadata.workload_id.clone();
            let mut committed = match self.checkpoints.load(&workload_id).await {
                Ok(cursor) => cursor,
                Err(error) => {
                    push_failure(
                        &mut report,
                        workload_id,
                        RuntimeLogFailureStage::LoadCheckpoint,
                        error,
                    );
                    continue;
                }
            };
            let mut stream = match self
                .runtime
                .logs(
                    &workload.handle,
                    LogRequest {
                        after: committed.clone(),
                        mode: LogMode::Snapshot,
                    },
                )
                .await
            {
                Ok(stream) => stream,
                Err(error) => {
                    push_failure(
                        &mut report,
                        workload_id,
                        RuntimeLogFailureStage::OpenStream,
                        error,
                    );
                    continue;
                }
            };
            let mut examined = 0_usize;
            while examined < self.settings.max_frames_per_workload {
                let frame = match stream.next().await {
                    Ok(Some(frame)) => frame,
                    Ok(None) => break,
                    Err(error) => {
                        push_failure(
                            &mut report,
                            workload_id.clone(),
                            RuntimeLogFailureStage::ReadStream,
                            error,
                        );
                        break;
                    }
                };
                examined = examined.saturating_add(1);
                if committed.as_ref() == Some(&frame.cursor) {
                    continue;
                }
                let cursor = frame.cursor.clone();
                let entry = WorkloadLogEntry {
                    metadata: workload.metadata.clone(),
                    received_at: self.clock.now(),
                    cursor: cursor.clone(),
                    source: frame.source,
                    payload: frame.payload,
                };
                if let Err(error) = self.sink.ingest(entry).await {
                    push_failure(
                        &mut report,
                        workload_id.clone(),
                        RuntimeLogFailureStage::Deliver,
                        error,
                    );
                    break;
                }
                report.delivered = report.delivered.saturating_add(1);
                if let Err(error) = self.checkpoints.commit(&workload_id, &cursor).await {
                    push_failure(
                        &mut report,
                        workload_id.clone(),
                        RuntimeLogFailureStage::CommitCheckpoint,
                        error,
                    );
                    break;
                }
                committed = Some(cursor);
            }
            if examined == self.settings.max_frames_per_workload {
                report.saturated = report.saturated.saturating_add(1);
            }
        }
        report
            .failures
            .sort_by(|left, right| left.workload_id.cmp(&right.workload_id));
        Ok(report)
    }
}

fn push_failure(
    report: &mut RuntimeLogReport,
    workload_id: WorkloadId,
    stage: RuntimeLogFailureStage,
    error: impl std::fmt::Display,
) {
    report.failures.push(RuntimeLogFailure {
        workload_id,
        stage,
        message: error.to_string(),
    });
}

/// Fatal configuration or node-wide snapshot/checkpoint failure.
#[derive(Debug, thiserror::Error)]
pub enum RuntimeLogAgentError {
    /// A zero frame bound could never make progress.
    #[error("runtime log max_frames_per_workload must be greater than zero")]
    InvalidFrameLimit,
    /// A zero interval would hot-loop over runtime snapshots.
    #[error("runtime log poll interval must be greater than zero")]
    InvalidPollInterval,
    /// The authoritative local runtime snapshot was unavailable.
    #[error(transparent)]
    Runtime(#[from] runtime::RuntimeError),
    /// Stale-checkpoint collection failed before workload delivery began.
    #[error(transparent)]
    Checkpoint(#[from] LogCheckpointError),
}
