use axum::extract::rejection::JsonRejection;
use axum::http::{HeaderMap, StatusCode};
use kernel_api::RequestId;
use kernel_controller::{ControllerError, DedupOutcome, RequestFingerprint};
use kernel_store::{Keyspace, Transaction};
use serde::Serialize;
use serde::de::DeserializeOwned;
use sha2::{Digest, Sha256};

use crate::{ApiError, AppState, OperatorIdentity};

pub(crate) const MAXIMUM_REQUEST_BYTES: usize = 1024 * 1_024;
const IDEMPOTENCY_KEY: &str = "idempotency-key";

pub(crate) struct MutationRequest {
    request_id: RequestId,
    claim_key: kernel_store::StoreKey,
    fingerprint: RequestFingerprint,
}

impl MutationRequest {
    pub(crate) fn new<Payload: Serialize>(
        state: &AppState,
        headers: &HeaderMap,
        operator: &OperatorIdentity,
        operation: &str,
        path_parts: &[&str],
        payload: &Payload,
    ) -> Result<Self, ApiError> {
        let request_id = request_id(headers)?;
        let encoded = serde_json::to_vec(payload).map_err(|error| {
            ApiError::internal(format!("failed to fingerprint request: {error}"))
        })?;
        let mut digest = Sha256::new();
        update_digest(&mut digest, operation.as_bytes());
        update_digest(&mut digest, operator.0.as_bytes());
        for part in path_parts {
            update_digest(&mut digest, part.as_bytes());
        }
        update_digest(&mut digest, &encoded);
        let claim_key = Keyspace::new(&state.cluster_id).request_claim(&request_id);
        Ok(Self {
            request_id,
            claim_key,
            fingerprint: RequestFingerprint::new(digest.finalize().into()),
        })
    }

    pub(crate) fn request_id(&self) -> &RequestId {
        &self.request_id
    }

    pub(crate) async fn replay<Response: DeserializeOwned>(
        &self,
        state: &AppState,
    ) -> Result<Option<Response>, ApiError> {
        state
            .requests
            .replay(&self.claim_key, self.fingerprint)
            .await
            .map_err(dedup_error)?
            .map(|response| decode_response(&response))
            .transpose()
    }

    pub(crate) async fn commit<Response: Serialize + DeserializeOwned>(
        self,
        state: &AppState,
        response: Response,
        transaction: Transaction,
        conflict_code: &'static str,
        conflict_message: impl Into<String>,
    ) -> Result<Response, ApiError> {
        let encoded_response = serde_json::to_vec(&response)
            .map_err(|error| ApiError::internal(format!("failed to encode response: {error}")))?;
        let outcome = state
            .requests
            .deduplicate(
                self.claim_key,
                self.fingerprint,
                encoded_response,
                transaction,
            )
            .await
            .map_err(dedup_error)?;
        match outcome {
            DedupOutcome::Committed { .. } => Ok(response),
            DedupOutcome::Duplicate { response } => decode_response(&response),
            DedupOutcome::MutationConflict => {
                Err(ApiError::conflict(conflict_code, conflict_message))
            }
        }
    }
}

pub(crate) fn json_rejection(rejection: JsonRejection, subject: &str) -> ApiError {
    if rejection.status() == StatusCode::PAYLOAD_TOO_LARGE {
        ApiError::payload_too_large(format!(
            "{subject} request exceeds {MAXIMUM_REQUEST_BYTES} bytes"
        ))
    } else {
        ApiError::bad_request(rejection.body_text())
    }
}

fn request_id(headers: &HeaderMap) -> Result<RequestId, ApiError> {
    let value = headers
        .get(IDEMPOTENCY_KEY)
        .ok_or_else(|| ApiError::bad_request("Idempotency-Key header is required"))?
        .to_str()
        .map_err(|_| ApiError::bad_request("Idempotency-Key header is not valid text"))?;
    RequestId::new(value).map_err(|error| ApiError::bad_request(error.to_string()))
}

fn update_digest(digest: &mut Sha256, value: &[u8]) {
    digest.update(u64::try_from(value.len()).unwrap_or(u64::MAX).to_be_bytes());
    digest.update(value);
}

fn decode_response<Response: DeserializeOwned>(response: &[u8]) -> Result<Response, ApiError> {
    serde_json::from_slice(response)
        .map_err(|_| ApiError::internal("persisted request response is malformed"))
}

fn dedup_error(error: ControllerError) -> ApiError {
    match error {
        ControllerError::RequestCollision => ApiError::conflict(
            "idempotencyConflict",
            "Idempotency-Key was already used for another request",
        ),
        error => ApiError::internal(format!("request deduplication failed: {error}")),
    }
}
