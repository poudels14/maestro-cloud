use async_trait::async_trait;
use kernel_api::Timestamp;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

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

impl SinkDeadLetter {
    /// Returns operator-safe metadata without copying or exposing the retained payload.
    pub fn metadata(&self) -> SinkDeadLetterMetadata {
        SinkDeadLetterMetadata {
            sink_id: self.sink_id.clone(),
            source_sequence: self.source_sequence,
            status_code: self.status_code,
            reason: self.reason.clone(),
            payload_sha256: hex(&Sha256::digest(&self.payload)),
            payload_bytes: u64::try_from(self.payload.len()).unwrap_or(u64::MAX),
            recorded_at: self.recorded_at,
        }
    }
}

/// Operator-safe dead-letter metadata used by list output and export manifests.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SinkDeadLetterMetadata {
    /// Sink whose destination rejected the payload.
    pub sink_id: LogSinkId,
    /// Source record sequence used as the durable quarantine key.
    pub source_sequence: LogSequence,
    /// Destination status code when one was available.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status_code: Option<u16>,
    /// Bounded safe rejection description.
    pub reason: String,
    /// SHA-256 digest of the exact retained bytes.
    pub payload_sha256: String,
    /// Exact retained payload size.
    pub payload_bytes: u64,
    /// Time at which the payload entered quarantine.
    pub recorded_at: Timestamp,
}

/// Aggregate retained dead-letter usage for one sink.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SinkDeadLetterStats {
    /// Retained poison records.
    pub count: u64,
    /// Bytes retained across their exact outbound payloads.
    pub payload_bytes: u64,
}

/// Durable idempotent dead-letter administration boundary.
#[async_trait]
pub trait DeadLetterStore: Send + Sync {
    /// Records a poison payload, deduplicating the same outbound payload on source replay.
    async fn record(&self, dead_letter: &SinkDeadLetter) -> Result<(), DeadLetterStoreError>;

    /// Lists retained records after an optional exclusive sequence in ascending source order.
    async fn list(
        &self,
        sink_id: &LogSinkId,
        after: Option<LogSequence>,
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

fn hex(bytes: &[u8]) -> String {
    bytes
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<Vec<_>>()
        .concat()
}
