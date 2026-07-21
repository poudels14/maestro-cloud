use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use kernel_api::{ClusterId, NodeId, Timestamp};
use kernel_store::Clock;
use tokio::sync::watch;

use crate::{HostDiskReader, HostDiskStats, HostResourceStats, HostStatsReader, StatusClock};

/// Cluster identity and cadence for node-local host telemetry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostTelemetrySettings {
    /// Cluster that owns the node sample.
    pub cluster_id: ClusterId,
    /// Node whose host resources are sampled.
    pub node_id: NodeId,
    /// Delay between complete host sampling passes.
    pub poll_interval: Duration,
}

/// One timestamped partial-or-complete host telemetry snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostTelemetrySample {
    /// Cluster that owns the sample.
    pub cluster_id: ClusterId,
    /// Node that collected the sample.
    pub node_id: NodeId,
    /// Wall-clock sample time.
    pub collected_at: Timestamp,
    /// Host CPU, memory, and network values, absent after a resource read failure.
    pub resources: Option<HostResourceStats>,
    /// Complete disk inventory, absent after a mount-table read failure.
    pub disks: Option<Vec<HostDiskStats>>,
}

/// Point in collection at which host telemetry failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HostTelemetryFailureStage {
    /// Aggregate CPU, memory, or network reading failed.
    ReadResources,
    /// The host mount table could not be read safely.
    ReadDisks,
    /// One discovered mount could not report capacity.
    ReadDiskCapacity,
}

/// Isolated host telemetry failure that does not discard healthy sample parts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostTelemetryFailure {
    /// Collection stage that failed.
    pub stage: HostTelemetryFailureStage,
    /// Affected mount point for per-disk failures.
    pub mount_point: Option<String>,
    /// Safe diagnostic from the reader.
    pub message: String,
}

/// Outcome of one host telemetry sweep.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HostTelemetryReport {
    /// Sample offered to the configured sink, absent when both top-level readers failed.
    pub sample: Option<HostTelemetrySample>,
    /// Whether the sample crossed the sink boundary.
    pub delivered: bool,
    /// Downstream failure isolated from the next sampling pass.
    pub delivery_failure: Option<HostTelemetrySinkError>,
    /// Resource, disk inventory, and per-mount failures.
    pub failures: Vec<HostTelemetryFailure>,
}

/// Delivery boundary for one timestamped host telemetry sample.
#[async_trait]
pub trait HostTelemetrySink: Send + Sync + 'static {
    /// Accepts a partial-or-complete sample without retaining borrowed data.
    async fn ingest(&self, sample: &HostTelemetrySample) -> Result<(), HostTelemetrySinkError>;
}

/// A host telemetry sink could not accept a valid sample.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum HostTelemetrySinkError {
    /// The sample permanently violates the sink contract.
    #[error("host telemetry sink rejected sample: {message}")]
    Rejected {
        /// Stable rejection detail.
        message: String,
    },
    /// The sink is temporarily unable to accept the sample.
    #[error("host telemetry sink is unavailable: {message}")]
    Unavailable {
        /// Safe availability detail.
        message: String,
    },
}

/// Periodically collects independently-failable host resource and disk snapshots.
pub struct HostTelemetryAgent {
    resource_reader: Arc<dyn HostStatsReader>,
    disk_reader: Arc<dyn HostDiskReader>,
    sink: Arc<dyn HostTelemetrySink>,
    settings: HostTelemetrySettings,
    clock: Arc<dyn StatusClock>,
    monotonic_clock: Arc<dyn Clock>,
}

impl HostTelemetryAgent {
    /// Constructs a node-local collector without starting background work.
    pub fn new(
        resource_reader: Arc<dyn HostStatsReader>,
        disk_reader: Arc<dyn HostDiskReader>,
        sink: Arc<dyn HostTelemetrySink>,
        settings: HostTelemetrySettings,
        clock: Arc<dyn StatusClock>,
        monotonic_clock: Arc<dyn Clock>,
    ) -> Result<Self, HostTelemetryAgentError> {
        if settings.poll_interval.is_zero() {
            return Err(HostTelemetryAgentError::InvalidPollInterval);
        }
        Ok(Self {
            resource_reader,
            disk_reader,
            sink,
            settings,
            clock,
            monotonic_clock,
        })
    }

    /// Samples immediately and on each injected deadline until shutdown.
    pub async fn run(
        &self,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<(), HostTelemetryAgentError> {
        loop {
            if *shutdown.borrow() {
                return Ok(());
            }
            let _report = self.collect().await;
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

    /// Collects one partial-or-complete host snapshot with isolated failures.
    pub async fn collect(&self) -> HostTelemetryReport {
        let collected_at = self.clock.now();
        let (resources, disks) = tokio::join!(self.resource_reader.read(), self.disk_reader.read());
        let mut report = HostTelemetryReport::default();
        let resources = match resources {
            Ok(resources) => Some(resources),
            Err(error) => {
                report.failures.push(HostTelemetryFailure {
                    stage: HostTelemetryFailureStage::ReadResources,
                    mount_point: None,
                    message: error.to_string(),
                });
                None
            }
        };
        let disks = match disks {
            Ok(disks) => {
                report
                    .failures
                    .extend(
                        disks
                            .failures
                            .into_iter()
                            .map(|failure| HostTelemetryFailure {
                                stage: HostTelemetryFailureStage::ReadDiskCapacity,
                                mount_point: Some(failure.mount_point),
                                message: failure.message,
                            }),
                    );
                Some(disks.disks)
            }
            Err(error) => {
                report.failures.push(HostTelemetryFailure {
                    stage: HostTelemetryFailureStage::ReadDisks,
                    mount_point: None,
                    message: error.to_string(),
                });
                None
            }
        };
        if resources.is_none() && disks.is_none() {
            return report;
        }
        let sample = HostTelemetrySample {
            cluster_id: self.settings.cluster_id.clone(),
            node_id: self.settings.node_id.clone(),
            collected_at,
            resources,
            disks,
        };
        match self.sink.ingest(&sample).await {
            Ok(()) => report.delivered = true,
            Err(error) => report.delivery_failure = Some(error),
        }
        report.sample = Some(sample);
        report
    }
}

/// Invalid host telemetry lifecycle configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum HostTelemetryAgentError {
    /// A zero poll interval would create an unbounded hot loop.
    #[error("host telemetry poll interval must be non-zero")]
    InvalidPollInterval,
}
