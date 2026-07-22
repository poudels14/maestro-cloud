use http::{HeaderValue, StatusCode};
use kernel_api::{AnnotationKey, BuiltinResource, NodeRole, RequestId};
use kernel_controller::RequestFingerprint;
use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::LegacyEntry;
use crate::legacy_convert::LegacyPlanError;
use crate::legacy_request_schema::LegacyClusterRequestReceipt;

const REQUESTS_PREFIX: &str = "/maetro/cluster/requests/";
const SUMMARY_ANNOTATION: &str = "migration.maestro.dev/legacy-request-receipts";
const BARRIER_DIGEST_DOMAIN: &[u8] = b"maestro-legacy-request-barrier-v1\0";
const MAXIMUM_LEGACY_REQUEST_ID_BYTES: usize = 128;
const MAXIMUM_CONTENT_TYPE_BYTES: usize = 1_024;
const MAXIMUM_RESPONSE_BYTES: usize = 1_024 * 1_024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LegacyRequestCatalog {
    barriers: Vec<(RequestId, RequestFingerprint)>,
    summary: LegacyRequestSummary,
    pub(crate) unclaimed: Vec<LegacyEntry>,
}

impl LegacyRequestCatalog {
    pub(crate) fn decode(entries: &[LegacyEntry]) -> Result<Self, LegacyRequestError> {
        let mut barriers = Vec::new();
        let mut rejected_by_rewrite = 0;
        let mut unclaimed = Vec::new();
        for entry in entries {
            let Some(request_id) = classify_key(entry.key())? else {
                unclaimed.push(entry.clone());
                continue;
            };
            validate_legacy_request_id(entry.key(), request_id)?;
            let receipt: LegacyClusterRequestReceipt = serde_json::from_slice(entry.value())
                .map_err(|error| invalid(entry.key(), format!("invalid JSON: {error}")))?;
            let legacy_fingerprint = validate_receipt(entry.key(), &receipt)?;
            if receipt.state == "in-progress" {
                return Err(LegacyRequestError::InProgress {
                    key: entry.key().to_owned(),
                });
            }
            match RequestId::new(request_id) {
                Ok(request_id) => {
                    let fingerprint =
                        collision_fingerprint(request_id.as_str(), legacy_fingerprint);
                    barriers.push((request_id, fingerprint));
                }
                Err(_) => rejected_by_rewrite += 1,
            }
        }
        let summary = LegacyRequestSummary {
            completed_receipts: barriers.len() + rejected_by_rewrite,
            collision_barriers: barriers.len(),
            rejected_by_rewrite,
        };
        Ok(Self {
            barriers,
            summary,
            unclaimed,
        })
    }

    pub(crate) fn barriers(&self) -> impl Iterator<Item = (RequestId, RequestFingerprint)> + '_ {
        self.barriers.iter().cloned()
    }

    pub(crate) fn annotate_master(
        &self,
        resources: &mut [BuiltinResource],
    ) -> Result<(), LegacyPlanError> {
        if self.summary.completed_receipts == 0 {
            return Ok(());
        }
        let value = serde_json::to_string(&self.summary).map_err(|error| {
            LegacyPlanError::InvalidClusterState {
                resource_id: "legacy-request-receipts".to_owned(),
                message: format!("could not preserve request-receipt summary: {error}"),
            }
        })?;
        let master = resources.iter_mut().find_map(|resource| match resource {
            BuiltinResource::Node(node) if node.spec.role == NodeRole::Master => Some(node),
            _ => None,
        });
        let master = master.ok_or_else(|| LegacyPlanError::InvalidClusterState {
            resource_id: "legacy-request-receipts".to_owned(),
            message: "converted master node is missing".to_owned(),
        })?;
        master
            .meta
            .annotations
            .insert(AnnotationKey(SUMMARY_ANNOTATION.to_owned()), value);
        Ok(())
    }
}

fn classify_key(key: &str) -> Result<Option<&str>, LegacyRequestError> {
    if key == REQUESTS_PREFIX.trim_end_matches('/') {
        return Err(invalid(key, "request key has no identity"));
    }
    let Some(request_id) = key.strip_prefix(REQUESTS_PREFIX) else {
        return Ok(None);
    };
    if request_id.is_empty() || request_id.contains('/') {
        Err(invalid(
            key,
            "request key must contain one identity segment",
        ))
    } else {
        Ok(Some(request_id))
    }
}

fn validate_legacy_request_id(key: &str, request_id: &str) -> Result<(), LegacyRequestError> {
    if request_id.is_empty()
        || request_id.len() > MAXIMUM_LEGACY_REQUEST_ID_BYTES
        || !request_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"-_.".contains(&byte))
    {
        Err(invalid(
            key,
            "request identity violates the legacy contract",
        ))
    } else {
        Ok(())
    }
}

fn validate_receipt(
    key: &str,
    receipt: &LegacyClusterRequestReceipt,
) -> Result<[u8; 32], LegacyRequestError> {
    let fingerprint = decode_fingerprint(key, &receipt.fingerprint)?;
    if receipt.updated_at_ms < 0 || receipt.body.len() > MAXIMUM_RESPONSE_BYTES {
        return Err(invalid(
            key,
            "receipt timestamp or response size is invalid",
        ));
    }
    match receipt.state.as_str() {
        "in-progress"
            if receipt.status_code.is_none()
                && receipt.content_type.is_none()
                && receipt.body.is_empty() => {}
        "complete" => validate_completed_receipt(key, receipt)?,
        "in-progress" => {
            return Err(invalid(
                key,
                "in-progress receipt contains completed response fields",
            ));
        }
        _ => return Err(invalid(key, "receipt state is not recognized")),
    }
    Ok(fingerprint)
}

fn validate_completed_receipt(
    key: &str,
    receipt: &LegacyClusterRequestReceipt,
) -> Result<(), LegacyRequestError> {
    let status_code = receipt
        .status_code
        .ok_or_else(|| invalid(key, "completed receipt has no status code"))?;
    StatusCode::from_u16(status_code)
        .map_err(|error| invalid(key, format!("status code is invalid: {error}")))?;
    if let Some(content_type) = &receipt.content_type
        && (content_type.len() > MAXIMUM_CONTENT_TYPE_BYTES
            || HeaderValue::try_from(content_type.as_str()).is_err())
    {
        return Err(invalid(key, "content type is invalid"));
    }
    Ok(())
}

fn decode_fingerprint(key: &str, value: &str) -> Result<[u8; 32], LegacyRequestError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(invalid(
            key,
            "fingerprint must be 64 lowercase hexadecimal characters",
        ));
    }
    let mut decoded = [0; 32];
    hex::decode_to_slice(value, &mut decoded)
        .map_err(|error| invalid(key, format!("fingerprint is invalid: {error}")))?;
    Ok(decoded)
}

fn collision_fingerprint(request_id: &str, legacy_fingerprint: [u8; 32]) -> RequestFingerprint {
    let mut digest = Sha256::new();
    digest.update(BARRIER_DIGEST_DOMAIN);
    digest.update(
        u64::try_from(request_id.len())
            .unwrap_or(u64::MAX)
            .to_be_bytes(),
    );
    digest.update(request_id.as_bytes());
    digest.update(legacy_fingerprint);
    RequestFingerprint::new(digest.finalize().into())
}

fn invalid(key: impl Into<String>, message: impl Into<String>) -> LegacyRequestError {
    LegacyRequestError::InvalidState {
        key: key.into(),
        message: message.into(),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
struct LegacyRequestSummary {
    completed_receipts: usize,
    collision_barriers: usize,
    rejected_by_rewrite: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum LegacyRequestError {
    #[error("legacy request receipt at `{key}` is invalid: {message}")]
    InvalidState { key: String, message: String },
    #[error("legacy request receipt `{key}` is still in progress; cutover is not quiescent")]
    InProgress { key: String },
}
