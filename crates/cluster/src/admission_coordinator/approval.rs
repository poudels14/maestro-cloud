use kernel_api::NodeId;
use kernel_store::StoredValue;
use serde::{Deserialize, Serialize};

use super::AdmissionCoordinatorError;
use crate::JoinPayload;

/// Operator-visible state of one topology-bound join approval.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum NodeJoinApprovalState {
    /// The approved key may submit its first matching signed request.
    Approved,
    /// One exact request was accepted and is replayable for transport recovery.
    Admitted,
}

/// Secret-free durable authorization for one node join key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NodeJoinApproval {
    /// Declared node allowed to join.
    pub node_id: NodeId,
    /// SHA-256 fingerprint of the node's persisted X25519 public key.
    pub public_key_sha256: String,
    /// Operator approval time in Unix milliseconds.
    pub approved_at_unix_ms: i64,
    /// Current one-time admission state.
    pub state: NodeJoinApprovalState,
    /// Admission time when an exact request has been accepted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub admitted_at_unix_ms: Option<i64>,
}

/// Operator request authorizing one persisted node join key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NodeJoinApprovalRequest {
    /// Declared node allowed to submit the signed join request.
    pub node_id: NodeId,
    /// SHA-256 fingerprint printed by that node before admission.
    pub public_key_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct StoredApproval {
    pub(super) view: NodeJoinApproval,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) accepted: Option<AcceptedJoin>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct AcceptedJoin {
    pub(super) request_sha256: String,
    pub(super) payload: JoinPayload,
}

pub(super) fn validate_fingerprint(fingerprint: &str) -> Result<(), AdmissionCoordinatorError> {
    let decoded =
        hex::decode(fingerprint).map_err(|_| AdmissionCoordinatorError::InvalidFingerprint)?;
    if decoded.len() == 32 && fingerprint == fingerprint.to_ascii_lowercase() {
        Ok(())
    } else {
        Err(AdmissionCoordinatorError::InvalidFingerprint)
    }
}

pub(super) fn decode_record(
    stored: &StoredValue,
) -> Result<StoredApproval, AdmissionCoordinatorError> {
    serde_json::from_slice(&stored.value).map_err(Into::into)
}

pub(super) fn encode_record(record: &StoredApproval) -> Result<Vec<u8>, AdmissionCoordinatorError> {
    serde_json::to_vec(record).map_err(Into::into)
}
