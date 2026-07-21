use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::IngestLogEntry;

/// Store-assigned total ordering used only for downstream delivery progress.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct LogSequence(pub u64);

/// One normalized record paired with its durable store ordering.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SequencedLogEntry {
    /// Monotonic store-local sequence.
    pub sequence: LogSequence,
    /// Normalized record delivered to the sink.
    pub entry: IngestLogEntry,
}

/// Validated stable identity for one independently checkpointed sink.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct LogSinkId(String);

impl LogSinkId {
    /// Validates a non-empty bounded identifier safe for storage and diagnostics.
    pub fn new(value: impl Into<String>) -> Result<Self, LogSinkIdError> {
        let value = value.into();
        if value.is_empty() || value.len() > 128 {
            return Err(LogSinkIdError);
        }
        if !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        {
            return Err(LogSinkIdError);
        }
        Ok(Self(value))
    }

    /// Returns the validated identifier.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub(crate) fn built_in(value: &'static str) -> Self {
        Self(value.to_owned())
    }
}

/// A sink identifier was empty, oversized, or contained unsafe characters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("log sink identifiers must be 1-128 ASCII letters, digits, dots, dashes, or underscores")]
pub struct LogSinkIdError;

/// Outcome of one completely handled sink batch.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LogSinkOutcome {
    /// Records intentionally removed by this sink's configured filters.
    pub filtered_entries: usize,
    /// Poison records durably quarantined by the sink instead of being sent.
    pub quarantined_entries: usize,
}

/// External destination for ordered normalized log batches.
#[async_trait]
pub trait LogSink: Send + Sync {
    /// Stable cursor namespace for this sink.
    fn id(&self) -> &LogSinkId;

    /// Handles the complete batch or returns an error without claiming progress.
    ///
    /// Cancellation may leave the destination updated while the local cursor remains unchanged,
    /// so implementations must tolerate exact replay.
    async fn send(&self, entries: &[SequencedLogEntry]) -> Result<LogSinkOutcome, LogSinkError>;
}

/// Read and cursor boundary required by independently checkpointed sink workers.
#[async_trait]
pub trait LogDeliveryStore: Send + Sync {
    /// Reads the next store-ordered records strictly after `cursor`.
    async fn read_after(
        &self,
        cursor: Option<LogSequence>,
        limit: usize,
    ) -> Result<Vec<SequencedLogEntry>, LogDeliveryStoreError>;

    /// Loads this sink's last completely committed sequence.
    async fn load_sink_cursor(
        &self,
        sink_id: &LogSinkId,
    ) -> Result<Option<LogSequence>, LogDeliveryStoreError>;

    /// Atomically advances a cursor; regressions must be rejected.
    async fn commit_sink_cursor(
        &self,
        sink_id: &LogSinkId,
        sequence: LogSequence,
    ) -> Result<(), LogDeliveryStoreError>;
}

/// A sink could not completely handle a batch.
#[derive(Debug, thiserror::Error)]
pub enum LogSinkError {
    /// The batch is permanently invalid for this destination.
    #[error("log sink rejected batch: {message}")]
    Rejected {
        /// Safe destination diagnostic.
        message: String,
    },
    /// The destination is temporarily unavailable.
    #[error("log sink is unavailable: {message}")]
    Unavailable {
        /// Safe destination diagnostic.
        message: String,
    },
}

/// Durable read or cursor state could not be completed.
#[derive(Debug, thiserror::Error)]
pub enum LogDeliveryStoreError {
    /// The requested cursor transition or read bound is invalid.
    #[error("log delivery store rejected operation: {message}")]
    Rejected {
        /// Stable validation detail.
        message: String,
    },
    /// Durable delivery state is temporarily unavailable.
    #[error("log delivery store is unavailable: {message}")]
    Unavailable {
        /// Safe backend diagnostic.
        message: String,
    },
}
