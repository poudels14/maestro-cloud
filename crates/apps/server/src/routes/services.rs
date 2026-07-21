use std::collections::{BTreeMap, BTreeSet};

use axum::extract::rejection::JsonRejection;
use axum::extract::{DefaultBodyLimit, Extension, Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::get;
use axum::{Json, Router};
use kernel_api::{
    BuiltinKind, Generation, Object, ObjectMeta, RequestId, ResourceKind, ResourceRevision,
    RolloutState, Service, ServiceId, ServiceSpec, ServiceStatus,
};
use kernel_controller::{ControllerError, DedupOutcome, RequestFingerprint};
use kernel_store::{Compare, ExpectedVersion, Keyspace, Mutation, Transaction};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{ApiError, AppState, OperatorIdentity, mask, resource};

const MAXIMUM_SERVICE_REQUEST_BYTES: usize = 1024 * 1_024;
const IDEMPOTENCY_KEY: &str = "idempotency-key";

pub(super) fn router() -> Router<AppState> {
    Router::new()
        .route("/api/services", get(list_services))
        .route(
            "/api/services/{service_id}",
            get(get_service).put(put_service),
        )
        .layer(DefaultBodyLimit::max(MAXIMUM_SERVICE_REQUEST_BYTES))
}

async fn put_service(
    State(state): State<AppState>,
    Path(service_id): Path<String>,
    Extension(operator): Extension<OperatorIdentity>,
    headers: HeaderMap,
    payload: Result<Json<ServiceWriteRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<ServiceWriteResponse>), ApiError> {
    let service_id =
        ServiceId::new(service_id).map_err(|error| ApiError::bad_request(error.to_string()))?;
    let request_id = request_id(&headers)?;
    let payload = payload.map_err(json_rejection)?.0;
    payload
        .spec
        .validate()
        .map_err(|error| ApiError::bad_request(error.to_string()))?;
    let fingerprint = fingerprint(&operator, &service_id, &payload)?;
    let keys = Keyspace::new(&state.cluster_id);
    let claim_key = keys.request_claim(&request_id);
    if let Some(response) = state
        .requests
        .replay(&claim_key, fingerprint)
        .await
        .map_err(dedup_error)?
    {
        return Ok((StatusCode::ACCEPTED, Json(decode_response(&response)?)));
    }

    let kind = ResourceKind::new(BuiltinKind::Service.as_str())
        .map_err(|error| ApiError::internal(error.to_string()))?;
    let resource_key = keys.resource(&kind, &service_id.clone().into());
    let current = state
        .store
        .get(&resource_key)
        .await
        .map_err(|error| ApiError::internal(format!("failed to read Service: {error}")))?;
    let (service, expected, write) =
        plan_service_write(current.as_ref(), &keys, &kind, service_id.clone(), payload)?;
    let response = ServiceWriteResponse {
        service_id,
        generation: service.meta.generation,
    };
    let encoded_response = serde_json::to_vec(&response)
        .map_err(|error| ApiError::internal(format!("failed to encode response: {error}")))?;
    let mutations = if write {
        vec![Mutation::Put {
            key: resource_key.clone(),
            value: serde_json::to_vec(&service).map_err(|error| {
                ApiError::internal(format!("failed to encode Service resource: {error}"))
            })?,
            session: None,
        }]
    } else {
        Vec::new()
    };
    let outcome = state
        .requests
        .deduplicate(
            claim_key,
            fingerprint,
            encoded_response,
            Transaction {
                compares: vec![Compare {
                    key: resource_key,
                    expected,
                }],
                mutations,
            },
        )
        .await
        .map_err(dedup_error)?;
    match outcome {
        DedupOutcome::Committed { .. } => Ok((StatusCode::ACCEPTED, Json(response))),
        DedupOutcome::Duplicate { response } => {
            Ok((StatusCode::ACCEPTED, Json(decode_response(&response)?)))
        }
        DedupOutcome::MutationConflict => Err(ApiError::conflict(
            "revisionConflict",
            "Service changed after its expected revision was read",
        )),
    }
}

fn plan_service_write(
    current: Option<&kernel_store::StoredValue>,
    keys: &Keyspace,
    kind: &ResourceKind,
    service_id: ServiceId,
    payload: ServiceWriteRequest,
) -> Result<(Service, ExpectedVersion, bool), ApiError> {
    match (current, payload.expected_revision) {
        (None, None) => Ok((
            new_service(service_id, payload.spec),
            ExpectedVersion::Missing,
            true,
        )),
        (None, Some(_)) => Err(ApiError::conflict(
            "revisionConflict",
            "Service does not exist at the expected revision",
        )),
        (Some(stored), expected_revision) => {
            let mut current: Service = resource::decode(stored, keys, kind, BuiltinKind::Service)?;
            if current.meta.deletion_timestamp.is_some() {
                return Err(ApiError::conflict(
                    "deletionInProgress",
                    "Service deletion is already in progress",
                ));
            }
            let expected_matches = expected_revision == Some(current.meta.revision);
            let expected = if expected_matches {
                ExpectedVersion::Exact(stored.version)
            } else {
                ExpectedVersion::Missing
            };
            if expected_matches && current.spec != payload.spec {
                current.meta.generation =
                    Generation(current.meta.generation.0.checked_add(1).ok_or_else(|| {
                        ApiError::conflict("generationExhausted", "Service generation is exhausted")
                    })?);
                current.spec = payload.spec;
                Ok((current, expected, true))
            } else {
                Ok((current, expected, false))
            }
        }
    }
}

fn new_service(service_id: ServiceId, spec: ServiceSpec) -> Service {
    Object {
        meta: ObjectMeta {
            id: service_id,
            labels: BTreeMap::new(),
            annotations: BTreeMap::new(),
            revision: ResourceRevision::default(),
            generation: Generation(1),
            owner_refs: Vec::new(),
            finalizers: BTreeSet::new(),
            deletion_timestamp: None,
        },
        spec,
        status: ServiceStatus {
            active_deployment_id: None,
            replica_override: None,
            rollout: RolloutState::Active,
            conditions: Vec::new(),
        },
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

fn fingerprint(
    operator: &OperatorIdentity,
    service_id: &ServiceId,
    payload: &ServiceWriteRequest,
) -> Result<RequestFingerprint, ApiError> {
    let encoded = serde_json::to_vec(payload)
        .map_err(|error| ApiError::internal(format!("failed to fingerprint request: {error}")))?;
    let mut digest = Sha256::new();
    for part in [
        b"PUT /api/services".as_slice(),
        operator.0.as_bytes(),
        service_id.as_str().as_bytes(),
        encoded.as_slice(),
    ] {
        digest.update(u64::try_from(part.len()).unwrap_or(u64::MAX).to_be_bytes());
        digest.update(part);
    }
    Ok(RequestFingerprint::new(digest.finalize().into()))
}

fn decode_response(response: &[u8]) -> Result<ServiceWriteResponse, ApiError> {
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

fn json_rejection(rejection: JsonRejection) -> ApiError {
    if rejection.status() == StatusCode::PAYLOAD_TOO_LARGE {
        ApiError::payload_too_large(format!(
            "service request exceeds {MAXIMUM_SERVICE_REQUEST_BYTES} bytes"
        ))
    } else {
        ApiError::bad_request(rejection.body_text())
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ServiceWriteRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    expected_revision: Option<ResourceRevision>,
    spec: ServiceSpec,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ServiceWriteResponse {
    service_id: ServiceId,
    generation: Generation,
}

async fn list_services(State(state): State<AppState>) -> Result<Json<Vec<Service>>, ApiError> {
    let services = resource::list(&state, BuiltinKind::Service)
        .await?
        .into_iter()
        .map(mask::service)
        .collect();
    Ok(Json(services))
}

async fn get_service(
    State(state): State<AppState>,
    Path(service_id): Path<String>,
) -> Result<Json<Service>, ApiError> {
    let service_id =
        ServiceId::new(service_id).map_err(|error| ApiError::bad_request(error.to_string()))?;
    Ok(Json(mask::service(
        resource::get(&state, BuiltinKind::Service, service_id).await?,
    )))
}
