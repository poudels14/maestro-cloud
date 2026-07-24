use std::collections::{BTreeMap, BTreeSet};

use axum::extract::rejection::JsonRejection;
use axum::extract::{DefaultBodyLimit, Extension, Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::{get, post};
use axum::{Json, Router};
use kernel_api::{
    BuiltinKind, Generation, Object, ObjectMeta, ResourceKind, ResourceRevision, RolloutState,
    Service, ServiceDiffRequest, ServiceDiffResponse, ServiceDiffStatus, ServiceId, ServiceSpec,
    ServiceStatus, ServiceWriteRequest, ServiceWriteResponse,
};
use kernel_store::{Compare, ExpectedVersion, Keyspace, Mutation, Transaction};

use crate::mutation::{MAXIMUM_REQUEST_BYTES, MutationRequest};
use crate::routes::service_diff;
use crate::system_resources::ensure_user_resource_id;
use crate::{ApiError, AppState, OperatorIdentity, mask, mutation, resource};

use super::write_plan::WritePlan;

pub(super) fn router() -> Router<AppState> {
    Router::new()
        .route("/api/services", get(list_services))
        .route(
            "/api/services/{service_id}",
            get(get_service).put(put_service),
        )
        .route("/api/services/{service_id}/diff", post(diff_service))
        .layer(DefaultBodyLimit::max(MAXIMUM_REQUEST_BYTES))
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
    ensure_user_resource_id("Service", service_id.as_str())?;
    let payload = payload
        .map_err(|rejection| mutation::json_rejection(rejection, "service"))?
        .0;
    payload
        .spec
        .validate()
        .map_err(|error| ApiError::bad_request(error.to_string()))?;
    let request = MutationRequest::new(
        &state,
        &headers,
        &operator,
        "PUT /api/services",
        &[service_id.as_str()],
        &payload,
    )?;
    let keys = Keyspace::new(&state.cluster_id);
    if let Some(response) = request.replay(&state).await? {
        return Ok((StatusCode::ACCEPTED, Json(response)));
    }

    let kind = ResourceKind::new(BuiltinKind::Service.as_str())
        .map_err(|error| ApiError::internal(error.to_string()))?;
    let resource_key = keys.resource(&kind, &service_id.clone().into());
    let current = state
        .store
        .get(&resource_key)
        .await
        .map_err(|error| ApiError::internal(format!("failed to read Service: {error}")))?;
    let plan = plan_service_write(current.as_ref(), &keys, &kind, service_id.clone(), payload)?;
    let (service, expected, mutations) = match plan {
        WritePlan::Retain { resource, expected } => (resource, expected, Vec::new()),
        WritePlan::Put { resource, expected } => {
            let mutation = Mutation::Put {
                key: resource_key.clone(),
                value: serde_json::to_vec(&resource).map_err(|error| {
                    ApiError::internal(format!("failed to encode Service resource: {error}"))
                })?,
                session: None,
            };
            (resource, expected, vec![mutation])
        }
    };
    let response = ServiceWriteResponse {
        service_id,
        generation: service.meta.generation,
    };
    let response = request
        .commit(
            &state,
            response,
            Transaction {
                compares: vec![Compare {
                    key: resource_key,
                    expected,
                }],
                mutations,
            },
            "revisionConflict",
            "Service changed after its expected revision was read",
        )
        .await?;
    Ok((StatusCode::ACCEPTED, Json(response)))
}

pub(super) fn plan_service_write(
    current: Option<&kernel_store::StoredValue>,
    keys: &Keyspace,
    kind: &ResourceKind,
    service_id: ServiceId,
    payload: ServiceWriteRequest,
) -> Result<WritePlan<Service>, ApiError> {
    match (current, payload.expected_revision) {
        (None, None) => Ok(WritePlan::Put {
            resource: new_service(service_id, payload.spec),
            expected: ExpectedVersion::Missing,
        }),
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
                Ok(WritePlan::Put {
                    resource: current,
                    expected,
                })
            } else {
                Ok(WritePlan::Retain {
                    resource: current,
                    expected,
                })
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
            rollout_bypass_generation: None,
            conditions: Vec::new(),
        },
    }
}

async fn list_services(State(state): State<AppState>) -> Result<Json<Vec<Service>>, ApiError> {
    let services = resource::list(&state, BuiltinKind::Service)
        .await?
        .into_iter()
        .map(mask::service)
        .collect();
    Ok(Json(services))
}

async fn diff_service(
    State(state): State<AppState>,
    Path(service_id): Path<String>,
    payload: Result<Json<ServiceDiffRequest>, JsonRejection>,
) -> Result<Json<ServiceDiffResponse>, ApiError> {
    let service_id =
        ServiceId::new(service_id).map_err(|error| ApiError::bad_request(error.to_string()))?;
    let desired = payload
        .map_err(|rejection| mutation::json_rejection(rejection, "service diff"))?
        .0
        .spec;
    desired
        .validate()
        .map_err(|error| ApiError::bad_request(error.to_string()))?;
    let current: Option<Service> =
        resource::get_optional(&state, BuiltinKind::Service, service_id.clone()).await?;
    Ok(Json(match current {
        None => ServiceDiffResponse {
            service_id,
            expected_revision: None,
            status: ServiceDiffStatus::New,
            changes: Vec::new(),
        },
        Some(current) => ServiceDiffResponse {
            service_id,
            expected_revision: Some(current.meta.revision),
            status: if current.spec == desired {
                ServiceDiffStatus::Unchanged
            } else {
                ServiceDiffStatus::Changed
            },
            changes: service_diff::changes(&current.spec, &desired)?,
        },
    }))
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
