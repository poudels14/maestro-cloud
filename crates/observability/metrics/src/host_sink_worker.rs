use std::sync::Arc;

use tokio::sync::watch;

use crate::{
    HostMetricDeliveryStore, HostMetricDeliveryStoreError, HostMetricSequence, HostMetricSink,
    MetricSinkError, MetricSinkSleeper, MetricSinkWorkerSettings, SequencedHostMetricPoint,
};

/// Bounded independently checkpointed delivery worker for one host-metric sink.
pub struct HostMetricSinkWorker {
    store: Arc<dyn HostMetricDeliveryStore>,
    sink: Arc<dyn HostMetricSink>,
    sleeper: Arc<dyn MetricSinkSleeper>,
    settings: MetricSinkWorkerSettings,
}

impl HostMetricSinkWorker {
    /// Constructs a host worker without spawning background tasks.
    pub fn new(
        store: Arc<dyn HostMetricDeliveryStore>,
        sink: Arc<dyn HostMetricSink>,
        sleeper: Arc<dyn MetricSinkSleeper>,
        settings: MetricSinkWorkerSettings,
    ) -> Result<Self, crate::MetricSinkWorkerSettingsError> {
        Ok(Self {
            store,
            sink,
            sleeper,
            settings: settings.validate()?,
        })
    }

    /// Delivers at most the configured host batches and durably advances progress.
    pub async fn drain_once(
        &self,
    ) -> Result<HostMetricSinkWorkerReport, HostMetricSinkWorkerError> {
        let mut cursor = self
            .store
            .load_host_metric_sink_cursor(self.sink.id())
            .await
            .map_err(|source| store_error("load host metric sink cursor", source))?;
        let mut report = HostMetricSinkWorkerReport {
            cursor,
            ..HostMetricSinkWorkerReport::default()
        };
        for _ in 0..self.settings.max_batches_per_run {
            let points = self
                .store
                .read_host_metrics_after(cursor, self.settings.batch_size)
                .await
                .map_err(|source| store_error("read host metric delivery batch", source))?;
            if points.is_empty() {
                break;
            }
            validate_batch(cursor, &points)?;
            let last_sequence = points
                .last()
                .map(|point| point.sequence)
                .ok_or_else(|| invalid("non-empty host metric batch had no last point"))?;
            let retries = self.send_with_retry(&points).await?;
            self.store
                .commit_host_metric_sink_cursor(self.sink.id(), last_sequence)
                .await
                .map_err(|source| store_error("commit host metric sink cursor", source))?;
            report.batches = report.batches.saturating_add(1);
            report.handled_points = report.handled_points.saturating_add(points.len());
            report.retries = report.retries.saturating_add(retries);
            cursor = Some(last_sequence);
            report.cursor = cursor;
            if points.len() < self.settings.batch_size {
                break;
            }
        }
        Ok(report)
    }

    /// Repeatedly performs bounded drains until shutdown, retrying failed passes later.
    pub async fn run(&self, mut shutdown: watch::Receiver<bool>) {
        loop {
            if *shutdown.borrow() {
                return;
            }
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return;
                    }
                }
                _result = self.drain_once() => {}
            }
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return;
                    }
                }
                () = self.sleeper.sleep(self.settings.poll_interval) => {}
            }
        }
    }

    async fn send_with_retry(
        &self,
        points: &[SequencedHostMetricPoint],
    ) -> Result<u32, HostMetricSinkWorkerError> {
        let mut delay = self.settings.initial_retry_delay;
        for attempt in 1..=self.settings.max_attempts {
            match self.sink.send_host_metrics(points).await {
                Ok(()) => return Ok(attempt.saturating_sub(1)),
                Err(source @ MetricSinkError::Rejected { .. }) => {
                    return Err(HostMetricSinkWorkerError::Sink {
                        attempts: attempt,
                        source,
                    });
                }
                Err(source) if attempt == self.settings.max_attempts => {
                    return Err(HostMetricSinkWorkerError::Sink {
                        attempts: attempt,
                        source,
                    });
                }
                Err(_source) => {
                    self.sleeper.sleep(delay).await;
                    delay = delay.saturating_mul(2).min(self.settings.max_retry_delay);
                }
            }
        }
        Err(invalid(
            "host metric retry loop ended without a destination outcome",
        ))
    }
}

/// Observable progress from one bounded host-metric worker drain.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct HostMetricSinkWorkerReport {
    /// Completely handled batches.
    pub batches: usize,
    /// Host points handled across completed batches.
    pub handled_points: usize,
    /// Additional destination attempts after initial failures.
    pub retries: u32,
    /// Last durable host cursor after this drain.
    pub cursor: Option<HostMetricSequence>,
}

/// One bounded host-metric worker drain stopped without claiming unsafe progress.
#[derive(Debug, thiserror::Error)]
pub enum HostMetricSinkWorkerError {
    /// Durable host reads or cursor updates failed.
    #[error("failed to {action}: {source}")]
    Store {
        /// Operation that failed.
        action: &'static str,
        /// Typed host delivery-store failure.
        #[source]
        source: HostMetricDeliveryStoreError,
    },
    /// The host destination exhausted its bounded attempts.
    #[error("host metric sink failed after {attempts} attempts: {source}")]
    Sink {
        /// Total attempted sends.
        attempts: u32,
        /// Last destination failure.
        #[source]
        source: MetricSinkError,
    },
    /// A store returned an internally inconsistent host batch.
    #[error("invalid host metric sink batch: {message}")]
    InvalidBatch {
        /// Stable contract violation detail.
        message: String,
    },
}

fn validate_batch(
    cursor: Option<HostMetricSequence>,
    points: &[SequencedHostMetricPoint],
) -> Result<(), HostMetricSinkWorkerError> {
    let mut previous = cursor;
    for point in points {
        if previous.is_some_and(|sequence| point.sequence <= sequence) {
            return Err(invalid(
                "host metric delivery sequences were not strictly increasing after the cursor",
            ));
        }
        previous = Some(point.sequence);
    }
    Ok(())
}

fn store_error(
    action: &'static str,
    source: HostMetricDeliveryStoreError,
) -> HostMetricSinkWorkerError {
    HostMetricSinkWorkerError::Store { action, source }
}

fn invalid(message: impl Into<String>) -> HostMetricSinkWorkerError {
    HostMetricSinkWorkerError::InvalidBatch {
        message: message.into(),
    }
}
