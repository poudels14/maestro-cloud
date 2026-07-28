use kernel_api::NodeId;
use kernel_store::StoredValue;
use serde::{Deserialize, Serialize};

use super::AdmissionCoordinatorError;
use crate::JoinPayload;

/// Persisted state of one topology-bound join.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) enum NodeJoinRecordState {
    /// A legacy manually approved key has not joined yet.
    Approved,
    /// One exact configured-node request was accepted and is replayable.
    Admitted,
}

/// Durable binding between one configured node and its join key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct NodeJoinRecord {
    /// Declared node allowed to join.
    pub node_id: NodeId,
    /// SHA-256 fingerprint of the node's persisted X25519 public key.
    pub public_key_sha256: String,
    /// First trust decision time in Unix milliseconds.
    pub approved_at_unix_ms: i64,
    /// Current one-time join state.
    pub state: NodeJoinRecordState,
    /// Time when an exact request was accepted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub admitted_at_unix_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct StoredJoin {
    pub(super) view: NodeJoinRecord,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) accepted: Option<AcceptedJoin>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct AcceptedJoin {
    pub(super) request_sha256: String,
    pub(super) payload: JoinPayload,
}

pub(super) fn decode_record(stored: &StoredValue) -> Result<StoredJoin, AdmissionCoordinatorError> {
    serde_json::from_slice(&stored.value).map_err(Into::into)
}

pub(super) fn encode_record(record: &StoredJoin) -> Result<Vec<u8>, AdmissionCoordinatorError> {
    serde_json::to_vec(record).map_err(Into::into)
}
