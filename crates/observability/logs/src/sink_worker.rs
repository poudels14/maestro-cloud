use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

use crate::{
    LogDeliveryStore, LogDeliveryStoreError, LogSequence, LogSink, LogSinkError, LogSinkOutcome,
    SequencedLogEntry,
};

/// Explicit fairness and retry bounds for one sink drain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SinkWorkerSettings {
    /// Maximum entries read in one destination request.
    pub batch_size: usize,
    /// Maximum batches handled by one `drain_once` call.
    pub max_batches_per_run: usize,
    /// Total destination attempts per batch, including the first.
    pub max_attempts: u32,
    /// Delay after the first failed attempt.
    pub initial_retry_delay: Duration,
    /// Maximum delay after exponential growth.
    pub max_retry_delay: Duration,
}

impl SinkWorkerSettings {
    /// Validates bounded non-zero worker settings.
    pub fn validate(self) -> Result<Self, SinkWorkerSettingsError> {
        if self.batch_size == 0
            || self.max_batches_per_run == 0
            || self.max_attempts == 0
            || self.initial_retry_delay.is_zero()
            || self.max_retry_delay < self.initial_retry_delay
        {
            return Err(SinkWorkerSettingsError);
        }
        Ok(self)
    }
}

/// Sink worker bounds were zero or the retry range was inverted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("sink worker bounds and retry delays must be non-zero and ordered")]
pub struct SinkWorkerSettingsError;

/// Injected retry timing boundary.
#[async_trait]
pub trait SinkSleeper: Send + Sync {
    /// Waits before the next delivery attempt.
    async fn sleep(&self, duration: Duration);
}

/// Tokio-backed retry timing used by production composition roots.
#[derive(Debug, Clone, Copy, Default)]
pub struct TokioSinkSleeper;

#[async_trait]
impl SinkSleeper for TokioSinkSleeper {
    async fn sleep(&self, duration: Duration) {
        tokio::time::sleep(duration).await;
    }
}

/// Bounded independently checkpointed delivery worker for one sink.
pub struct SinkWorker {
    store: Arc<dyn LogDeliveryStore>,
    sink: Arc<dyn LogSink>,
    sleeper: Arc<dyn SinkSleeper>,
    settings: SinkWorkerSettings,
}

impl SinkWorker {
    /// Constructs a worker without spawning background tasks.
    pub fn new(
        store: Arc<dyn LogDeliveryStore>,
        sink: Arc<dyn LogSink>,
        sleeper: Arc<dyn SinkSleeper>,
        settings: SinkWorkerSettings,
    ) -> Result<Self, SinkWorkerSettingsError> {
        Ok(Self {
            store,
            sink,
            sleeper,
            settings: settings.validate()?,
        })
    }

    /// Delivers at most the configured number of batches and durably advances progress.
    pub async fn drain_once(&self) -> Result<SinkWorkerReport, SinkWorkerError> {
        let mut cursor = self
            .store
            .load_sink_cursor(self.sink.id())
            .await
            .map_err(|source| SinkWorkerError::Store {
                action: "load sink cursor",
                source,
            })?;
        let mut report = SinkWorkerReport {
            cursor,
            ..SinkWorkerReport::default()
        };

        for _ in 0..self.settings.max_batches_per_run {
            let entries = self
                .store
                .read_after(cursor, self.settings.batch_size)
                .await
                .map_err(|source| SinkWorkerError::Store {
                    action: "read delivery batch",
                    source,
                })?;
            if entries.is_empty() {
                break;
            }
            validate_batch(cursor, &entries)?;
            let Some(last_sequence) = entries.last().map(|entry| entry.sequence) else {
                break;
            };
            let (outcome, retries) = self.send_with_retry(&entries).await?;
            validate_outcome(&entries, outcome)?;
            self.store
                .commit_sink_cursor(self.sink.id(), last_sequence)
                .await
                .map_err(|source| SinkWorkerError::Store {
                    action: "commit sink cursor",
                    source,
                })?;

            report.batches = report.batches.saturating_add(1);
            report.handled_entries = report.handled_entries.saturating_add(entries.len());
            report.filtered_entries = report
                .filtered_entries
                .saturating_add(outcome.filtered_entries);
            report.quarantined_entries = report
                .quarantined_entries
                .saturating_add(outcome.quarantined_entries);
            report.retries = report.retries.saturating_add(retries);
            cursor = Some(last_sequence);
            report.cursor = cursor;
            if entries.len() < self.settings.batch_size {
                break;
            }
        }
        Ok(report)
    }

    async fn send_with_retry(
        &self,
        entries: &[SequencedLogEntry],
    ) -> Result<(LogSinkOutcome, u32), SinkWorkerError> {
        let mut delay = self.settings.initial_retry_delay;
        for attempt in 1..=self.settings.max_attempts {
            match self.sink.send(entries).await {
                Ok(outcome) => return Ok((outcome, attempt.saturating_sub(1))),
                Err(source) if attempt == self.settings.max_attempts => {
                    return Err(SinkWorkerError::Sink {
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
        Err(SinkWorkerError::InvalidBatch {
            message: "retry loop ended without a destination outcome".to_owned(),
        })
    }
}

/// Observable progress from one bounded worker drain.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SinkWorkerReport {
    /// Completely handled batches.
    pub batches: usize,
    /// Records handled across sent, filtered, and quarantined outcomes.
    pub handled_entries: usize,
    /// Records removed by sink-local filters.
    pub filtered_entries: usize,
    /// Poison records durably quarantined by the sink.
    pub quarantined_entries: usize,
    /// Additional destination attempts after initial failures.
    pub retries: u32,
    /// Last durable cursor after this drain.
    pub cursor: Option<LogSequence>,
}

/// One bounded worker drain stopped without claiming unsafe progress.
#[derive(Debug, thiserror::Error)]
pub enum SinkWorkerError {
    /// Durable reads or cursor updates failed.
    #[error("failed to {action}: {source}")]
    Store {
        /// Operation that failed.
        action: &'static str,
        /// Typed store failure.
        #[source]
        source: LogDeliveryStoreError,
    },
    /// The destination exhausted its bounded attempts.
    #[error("log sink failed after {attempts} attempts: {source}")]
    Sink {
        /// Total attempted sends.
        attempts: u32,
        /// Last destination failure.
        #[source]
        source: LogSinkError,
    },
    /// A store or sink returned an internally inconsistent batch or outcome.
    #[error("invalid sink batch: {message}")]
    InvalidBatch {
        /// Stable contract violation detail.
        message: String,
    },
}

fn validate_batch(
    cursor: Option<LogSequence>,
    entries: &[SequencedLogEntry],
) -> Result<(), SinkWorkerError> {
    let mut previous = cursor;
    for entry in entries {
        if previous.is_some_and(|sequence| entry.sequence <= sequence) {
            return Err(SinkWorkerError::InvalidBatch {
                message: "delivery sequences were not strictly increasing after the cursor"
                    .to_owned(),
            });
        }
        previous = Some(entry.sequence);
    }
    Ok(())
}

fn validate_outcome(
    entries: &[SequencedLogEntry],
    outcome: LogSinkOutcome,
) -> Result<(), SinkWorkerError> {
    if outcome
        .filtered_entries
        .saturating_add(outcome.quarantined_entries)
        > entries.len()
    {
        return Err(SinkWorkerError::InvalidBatch {
            message: "sink reported more filtered and quarantined records than the batch contained"
                .to_owned(),
        });
    }
    Ok(())
}
