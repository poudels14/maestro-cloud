use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::watch;

use crate::{
    MetricDeliveryStore, MetricDeliveryStoreError, MetricSequence, MetricSink, MetricSinkError,
    SequencedMetricPoint,
};

/// Explicit fairness and retry bounds for one metric sink drain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetricSinkWorkerSettings {
    /// Maximum points read in one destination request.
    pub batch_size: usize,
    /// Maximum batches handled by one `drain_once` call.
    pub max_batches_per_run: usize,
    /// Total destination attempts per batch, including the first.
    pub max_attempts: u32,
    /// Delay after the first failed attempt.
    pub initial_retry_delay: Duration,
    /// Maximum delay after exponential growth.
    pub max_retry_delay: Duration,
    /// Delay between bounded drain passes.
    pub poll_interval: Duration,
}

impl MetricSinkWorkerSettings {
    /// Validates bounded non-zero worker settings.
    pub fn validate(self) -> Result<Self, MetricSinkWorkerSettingsError> {
        if self.batch_size == 0
            || self.max_batches_per_run == 0
            || self.max_attempts == 0
            || self.initial_retry_delay.is_zero()
            || self.max_retry_delay < self.initial_retry_delay
            || self.poll_interval.is_zero()
        {
            return Err(MetricSinkWorkerSettingsError);
        }
        Ok(self)
    }
}

impl Default for MetricSinkWorkerSettings {
    fn default() -> Self {
        Self {
            batch_size: 200,
            max_batches_per_run: 20,
            max_attempts: 5,
            initial_retry_delay: Duration::from_millis(500),
            max_retry_delay: Duration::from_secs(8),
            poll_interval: Duration::from_secs(5),
        }
    }
}

/// Metric sink worker bounds were zero or the retry range was inverted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("metric sink worker bounds and retry delays must be non-zero and ordered")]
pub struct MetricSinkWorkerSettingsError;

/// Injected metric retry timing boundary.
#[async_trait]
pub trait MetricSinkSleeper: Send + Sync {
    /// Waits before the next delivery attempt.
    async fn sleep(&self, duration: Duration);
}

/// Tokio-backed metric retry timing used by production composition roots.
#[derive(Debug, Clone, Copy, Default)]
pub struct TokioMetricSinkSleeper;

#[async_trait]
impl MetricSinkSleeper for TokioMetricSinkSleeper {
    async fn sleep(&self, duration: Duration) {
        tokio::time::sleep(duration).await;
    }
}

/// Bounded independently checkpointed delivery worker for one metric sink.
pub struct MetricSinkWorker {
    store: Arc<dyn MetricDeliveryStore>,
    sink: Arc<dyn MetricSink>,
    sleeper: Arc<dyn MetricSinkSleeper>,
    settings: MetricSinkWorkerSettings,
}

impl MetricSinkWorker {
    /// Constructs a worker without spawning background tasks.
    pub fn new(
        store: Arc<dyn MetricDeliveryStore>,
        sink: Arc<dyn MetricSink>,
        sleeper: Arc<dyn MetricSinkSleeper>,
        settings: MetricSinkWorkerSettings,
    ) -> Result<Self, MetricSinkWorkerSettingsError> {
        Ok(Self {
            store,
            sink,
            sleeper,
            settings: settings.validate()?,
        })
    }

    /// Delivers at most the configured number of batches and durably advances progress.
    pub async fn drain_once(&self) -> Result<MetricSinkWorkerReport, MetricSinkWorkerError> {
        let mut cursor = self
            .store
            .load_sink_cursor(self.sink.id())
            .await
            .map_err(|source| store_error("load metric sink cursor", source))?;
        let mut report = MetricSinkWorkerReport {
            cursor,
            ..MetricSinkWorkerReport::default()
        };
        for _ in 0..self.settings.max_batches_per_run {
            let points = self
                .store
                .read_after(cursor, self.settings.batch_size)
                .await
                .map_err(|source| store_error("read metric delivery batch", source))?;
            if points.is_empty() {
                break;
            }
            validate_batch(cursor, &points)?;
            let last_sequence = points
                .last()
                .map(|point| point.sequence)
                .ok_or_else(|| invalid("non-empty metric batch had no last point"))?;
            let retries = self.send_with_retry(&points).await?;
            self.store
                .commit_sink_cursor(self.sink.id(), last_sequence)
                .await
                .map_err(|source| store_error("commit metric sink cursor", source))?;
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
        points: &[SequencedMetricPoint],
    ) -> Result<u32, MetricSinkWorkerError> {
        let mut delay = self.settings.initial_retry_delay;
        for attempt in 1..=self.settings.max_attempts {
            match self.sink.send(points).await {
                Ok(()) => return Ok(attempt.saturating_sub(1)),
                Err(source @ MetricSinkError::Rejected { .. }) => {
                    return Err(MetricSinkWorkerError::Sink {
                        attempts: attempt,
                        source,
                    });
                }
                Err(source) if attempt == self.settings.max_attempts => {
                    return Err(MetricSinkWorkerError::Sink {
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
            "metric retry loop ended without a destination outcome",
        ))
    }
}

/// Observable progress from one bounded metric worker drain.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MetricSinkWorkerReport {
    /// Completely handled batches.
    pub batches: usize,
    /// Points handled across every completed batch.
    pub handled_points: usize,
    /// Additional destination attempts after initial failures.
    pub retries: u32,
    /// Last durable cursor after this drain.
    pub cursor: Option<MetricSequence>,
}

/// One bounded metric worker drain stopped without claiming unsafe progress.
#[derive(Debug, thiserror::Error)]
pub enum MetricSinkWorkerError {
    /// Durable reads or cursor updates failed.
    #[error("failed to {action}: {source}")]
    Store {
        /// Operation that failed.
        action: &'static str,
        /// Typed store failure.
        #[source]
        source: MetricDeliveryStoreError,
    },
    /// The destination exhausted its bounded attempts.
    #[error("metric sink failed after {attempts} attempts: {source}")]
    Sink {
        /// Total attempted sends.
        attempts: u32,
        /// Last destination failure.
        #[source]
        source: MetricSinkError,
    },
    /// A store returned an internally inconsistent batch.
    #[error("invalid metric sink batch: {message}")]
    InvalidBatch {
        /// Stable contract violation detail.
        message: String,
    },
}

fn validate_batch(
    cursor: Option<MetricSequence>,
    points: &[SequencedMetricPoint],
) -> Result<(), MetricSinkWorkerError> {
    let mut previous = cursor;
    for point in points {
        if previous.is_some_and(|sequence| point.sequence <= sequence) {
            return Err(invalid(
                "metric delivery sequences were not strictly increasing after the cursor",
            ));
        }
        previous = Some(point.sequence);
    }
    Ok(())
}

fn store_error(action: &'static str, source: MetricDeliveryStoreError) -> MetricSinkWorkerError {
    MetricSinkWorkerError::Store { action, source }
}

fn invalid(message: impl Into<String>) -> MetricSinkWorkerError {
    MetricSinkWorkerError::InvalidBatch {
        message: message.into(),
    }
}
