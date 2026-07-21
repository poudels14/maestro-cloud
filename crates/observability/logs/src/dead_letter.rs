use async_trait::async_trait;
use kernel_api::Timestamp;
use serde::{Deserialize, Serialize};

use crate::{LogSequence, LogSinkId};

/// One poison sink payload retained for inspection, export, or explicit purge.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SinkDeadLetter {
    /// Sink whose destination rejected this payload.
    pub sink_id: LogSinkId,
    /// Source record sequence used as the idempotency key within the sink.
    pub source_sequence: LogSequence,
    /// Destination status code when the transport supplies one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status_code: Option<u16>,
    /// Bounded safe failure description.
    pub reason: String,
    /// Exact outbound payload for operator export and diagnosis.
    pub payload: Vec<u8>,
    /// Time at which the payload was quarantined.
    pub recorded_at: Timestamp,
}

/// Aggregate retained dead-letter usage for one sink.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SinkDeadLetterStats {
    /// Retained poison records.
    pub count: u64,
    /// Bytes retained across their exact outbound payloads.
    pub payload_bytes: u64,
}

/// Durable idempotent dead-letter administration boundary.
#[async_trait]
pub trait DeadLetterStore: Send + Sync {
    /// Records a poison payload, deduplicating an exact `(sink, source_sequence)` replay.
    async fn record(&self, dead_letter: &SinkDeadLetter) -> Result<(), DeadLetterStoreError>;

    /// Lists retained records for a sink in ascending source order.
    async fn list(
        &self,
        sink_id: &LogSinkId,
        limit: usize,
    ) -> Result<Vec<SinkDeadLetter>, DeadLetterStoreError>;

    /// Reports retained row and payload-byte counts for one sink.
    async fn stats(&self, sink_id: &LogSinkId)
    -> Result<SinkDeadLetterStats, DeadLetterStoreError>;

    /// Purges records through an inclusive sequence, or every record when absent.
    async fn purge(
        &self,
        sink_id: &LogSinkId,
        through: Option<LogSequence>,
    ) -> Result<u64, DeadLetterStoreError>;
}

/// A dead-letter operation failed validation or durable storage.
#[derive(Debug, thiserror::Error)]
pub enum DeadLetterStoreError {
    /// Content or identity violates the dead-letter contract.
    #[error("dead-letter store rejected record: {message}")]
    Rejected {
        /// Stable validation detail.
        message: String,
    },
    /// Durable dead-letter state is temporarily unavailable.
    #[error("dead-letter store is unavailable: {message}")]
    Unavailable {
        /// Safe backend diagnostic.
        message: String,
    },
}
