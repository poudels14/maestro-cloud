use async_trait::async_trait;
use kernel_api::{NodeId, Timestamp, WorkloadId};
use runtime::WorkloadMetadata;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Largest encoded OTLP request accepted by the durable node-local spool.
pub const MAXIMUM_OTLP_ENVELOPE_BYTES: usize = 4 * 1024 * 1024;

/// OTLP signal carried by one replay-safe envelope.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OtlpSignal {
    /// Application metric export.
    Metrics,
    /// Application trace export.
    Traces,
}

impl OtlpSignal {
    /// Returns the stable database representation.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Metrics => "metrics",
            Self::Traces => "traces",
        }
    }
}

/// SHA-256 identity of one encoded OTLP request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct OtlpEnvelopeDigest([u8; 32]);

impl OtlpEnvelopeDigest {
    /// Returns the fixed-width digest bytes.
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

/// Replay identity for one authenticated workload OTLP request.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OtlpEnvelopeId {
    /// Node that authenticated and accepted the request.
    pub node_id: NodeId,
    /// Workload bound to the private node API socket.
    pub workload_id: WorkloadId,
    /// OTLP signal encoded in the payload.
    pub signal: OtlpSignal,
    /// Content digest used to make transport retries idempotent.
    pub digest: OtlpEnvelopeDigest,
}

/// Lossless encoded OTLP request with authenticated workload ownership.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OtlpEnvelope {
    /// Replay-safe request identity.
    pub id: OtlpEnvelopeId,
    /// First node-local observation time.
    pub observed_at: Timestamp,
    /// Durable ownership supplied by the authenticated workload socket.
    pub metadata: WorkloadMetadata,
    /// Protobuf-encoded OTLP collector request.
    pub payload: Vec<u8>,
}

impl OtlpEnvelope {
    /// Creates and validates one lossless OTLP envelope.
    pub fn new(
        signal: OtlpSignal,
        metadata: WorkloadMetadata,
        observed_at: Timestamp,
        payload: Vec<u8>,
    ) -> Result<Self, OtlpEnvelopeValidationError> {
        let digest = OtlpEnvelopeDigest(Sha256::digest(&payload).into());
        let envelope = Self {
            id: OtlpEnvelopeId {
                node_id: metadata.node_id.clone(),
                workload_id: metadata.workload_id.clone(),
                signal,
                digest,
            },
            observed_at,
            metadata,
            payload,
        };
        envelope.validate()?;
        Ok(envelope)
    }

    /// Validates size, ownership, and content-addressed identity.
    pub fn validate(&self) -> Result<(), OtlpEnvelopeValidationError> {
        if self.payload.len() > MAXIMUM_OTLP_ENVELOPE_BYTES {
            return Err(OtlpEnvelopeValidationError::PayloadTooLarge);
        }
        if self.id.node_id != self.metadata.node_id
            || self.id.workload_id != self.metadata.workload_id
        {
            return Err(OtlpEnvelopeValidationError::Ownership);
        }
        if self.id.digest.0 != <[u8; 32]>::from(Sha256::digest(&self.payload)) {
            return Err(OtlpEnvelopeValidationError::Digest);
        }
        Ok(())
    }
}

/// An OTLP envelope violated the durable spool contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum OtlpEnvelopeValidationError {
    /// Encoded request exceeds the node API and spool bound.
    #[error("OTLP envelope exceeds {MAXIMUM_OTLP_ENVELOPE_BYTES} bytes")]
    PayloadTooLarge,
    /// Replay identity and authenticated workload ownership differ.
    #[error("OTLP envelope identity does not match workload ownership")]
    Ownership,
    /// Payload bytes do not match their content digest.
    #[error("OTLP envelope payload does not match its digest")]
    Digest,
}

/// Outcome of one atomic replay-safe OTLP append.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct OtlpEnvelopeAppendReport {
    /// New envelopes committed by the append.
    pub committed: usize,
    /// Exact request replays already present in the store.
    pub deduplicated: usize,
}

/// Durable lossless spool for application metric and trace exports.
#[async_trait]
pub trait OtlpEnvelopeStore: Send + Sync {
    /// Atomically appends envelopes and deduplicates exact transport retries.
    async fn append_otlp_envelopes(
        &self,
        envelopes: &[OtlpEnvelope],
    ) -> Result<OtlpEnvelopeAppendReport, OtlpEnvelopeStoreError>;
}

/// A lossless OTLP batch could not cross the durable spool boundary.
#[derive(Debug, thiserror::Error)]
pub enum OtlpEnvelopeStoreError {
    /// Content or replay identity permanently violates the store contract.
    #[error("OTLP envelope store rejected request: {message}")]
    Rejected {
        /// Stable rejection detail.
        message: String,
    },
    /// The store is temporarily unable to accept a valid request.
    #[error("OTLP envelope store is unavailable: {message}")]
    Unavailable {
        /// Safe availability detail.
        message: String,
    },
}
