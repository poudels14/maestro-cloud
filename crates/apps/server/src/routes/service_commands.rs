use axum::extract::rejection::JsonRejection;
use axum::extract::{DefaultBodyLimit, Extension, Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::{delete, post, put};
use axum::{Json, Router};
use kernel_api::{
    BuiltinKind, CommandRequest, Generation, ResourceKind, ResourceRevision, RolloutState, Service,
    ServiceCommandResponse, ServiceId, ServiceReplicaOverrideRequest, Timestamp,
};
use kernel_store::{Compare, ExpectedVersion, Keyspace, Mutation, Transaction};
use serde::Serialize;

use crate::mutation::{MAXIMUM_REQUEST_BYTES, MutationRequest};
use crate::{ApiError, AppState, OperatorIdentity, mutation, resource};

pub(super) fn router() -> Router<AppState> {
    Router::new()
        .route("/api/services/{service_id}/redeploy", post(redeploy))
        .route("/api/services/{service_id}/freeze", post(freeze))
        .route("/api/services/{service_id}/unfreeze", post(unfreeze))
        .route("/api/services/{service_id}/replicas", put(set_replicas))
        .route("/api/services/{service_id}", delete(delete_service))
        .layer(DefaultBodyLimit::max(MAXIMUM_REQUEST_BYTES))
}

async fn redeploy(
    State(state): State<AppState>,
    Path(service_id): Path<String>,
    Extension(operator): Extension<OperatorIdentity>,
    headers: HeaderMap,
    payload: Result<Json<CommandRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<ServiceCommandResponse>), ApiError> {
    command(
        state,
        service_id,
        operator,
        headers,
        parse(payload)?,
        "POST /api/services/{serviceId}/redeploy",
        ServiceMutation::Redeploy,
    )
    .await
}

async fn freeze(
    State(state): State<AppState>,
    Path(service_id): Path<String>,
    Extension(operator): Extension<OperatorIdentity>,
    headers: HeaderMap,
    payload: Result<Json<CommandRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<ServiceCommandResponse>), ApiError> {
    command(
        state,
        service_id,
        operator,
        headers,
        parse(payload)?,
        "POST /api/services/{serviceId}/freeze",
        ServiceMutation::Rollout(RolloutState::Frozen),
    )
    .await
}

async fn unfreeze(
    State(state): State<AppState>,
    Path(service_id): Path<String>,
    Extension(operator): Extension<OperatorIdentity>,
    headers: HeaderMap,
    payload: Result<Json<CommandRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<ServiceCommandResponse>), ApiError> {
    command(
        state,
        service_id,
        operator,
        headers,
        parse(payload)?,
        "POST /api/services/{serviceId}/unfreeze",
        ServiceMutation::Rollout(RolloutState::Active),
    )
    .await
}

async fn set_replicas(
    State(state): State<AppState>,
    Path(service_id): Path<String>,
    Extension(operator): Extension<OperatorIdentity>,
    headers: HeaderMap,
    payload: Result<Json<ServiceReplicaOverrideRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<ServiceCommandResponse>), ApiError> {
    let payload = payload
        .map_err(|rejection| mutation::json_rejection(rejection, "service command"))?
        .0;
    let expected_revision = payload.expected_revision;
    let replicas = payload.replicas;
    command_with_payload(
        ServiceCommandInput {
            state,
            service_id,
            operator,
            headers,
            operation: "PUT /api/services/{serviceId}/replicas",
            action: ServiceMutation::Replicas(replicas),
        },
        payload,
        expected_revision,
    )
    .await
}

async fn delete_service(
    State(state): State<AppState>,
    Path(service_id): Path<String>,
    Extension(operator): Extension<OperatorIdentity>,
    headers: HeaderMap,
    payload: Result<Json<CommandRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<ServiceCommandResponse>), ApiError> {
    let now = state.timestamp_clock.now();
    command(
        state,
        service_id,
        operator,
        headers,
        parse(payload)?,
        "DELETE /api/services/{serviceId}",
        ServiceMutation::Delete(now),
    )
    .await
}

async fn command(
    state: AppState,
    service_id: String,
    operator: OperatorIdentity,
    headers: HeaderMap,
    payload: CommandRequest,
    operation: &'static str,
    action: ServiceMutation,
) -> Result<(StatusCode, Json<ServiceCommandResponse>), ApiError> {
    let expected_revision = payload.expected_revision;
    command_with_payload(
        ServiceCommandInput {
            state,
            service_id,
            operator,
            headers,
            operation,
            action,
        },
        payload,
        expected_revision,
    )
    .await
}

async fn command_with_payload<Payload: Serialize>(
    input: ServiceCommandInput,
    payload: Payload,
    expected_revision: ResourceRevision,
) -> Result<(StatusCode, Json<ServiceCommandResponse>), ApiError> {
    let ServiceCommandInput {
        state,
        service_id,
        operator,
        headers,
        operation,
        action,
    } = input;
    let service_id =
        ServiceId::new(service_id).map_err(|error| ApiError::bad_request(error.to_string()))?;
    let request = MutationRequest::new(
        &state,
        &headers,
        &operator,
        operation,
        &[service_id.as_str()],
        &payload,
    )?;
    if let Some(response) = request.replay(&state).await? {
        return Ok((StatusCode::ACCEPTED, Json(response)));
    }
    let keys = Keyspace::new(&state.cluster_id);
    let kind = ResourceKind::new(BuiltinKind::Service.as_str())
        .map_err(|error| ApiError::internal(error.to_string()))?;
    let key = keys.resource(&kind, &service_id.clone().into());
    let stored = state
        .store
        .get(&key)
        .await
        .map_err(|error| ApiError::internal(format!("failed to read Service: {error}")))?
        .ok_or_else(|| ApiError::not_found(format!("Service `{service_id}` does not exist")))?;
    let mut service: Service = resource::decode(&stored, &keys, &kind, BuiltinKind::Service)?;
    if service.meta.revision != expected_revision {
        return Err(ApiError::conflict(
            "revisionConflict",
            "Service is not at the expected revision",
        ));
    }
    let write = mutate_service(&mut service, action)?;
    let response = command_response(&service);
    let mutations = if write {
        vec![Mutation::Put {
            key: key.clone(),
            value: serde_json::to_vec(&service).map_err(|error| {
                ApiError::internal(format!("failed to encode Service resource: {error}"))
            })?,
            session: None,
        }]
    } else {
        Vec::new()
    };
    let response = request
        .commit(
            &state,
            response,
            Transaction {
                compares: vec![Compare {
                    key,
                    expected: ExpectedVersion::Exact(stored.version),
                }],
                mutations,
            },
            "revisionConflict",
            "Service changed after its expected revision was read",
        )
        .await?;
    Ok((StatusCode::ACCEPTED, Json(response)))
}

fn mutate_service(service: &mut Service, action: ServiceMutation) -> Result<bool, ApiError> {
    if service.meta.deletion_timestamp.is_some() {
        return match action {
            ServiceMutation::Delete(_) => Ok(false),
            _ => Err(ApiError::conflict(
                "deletionInProgress",
                "Service deletion is already in progress",
            )),
        };
    }
    match action {
        ServiceMutation::Redeploy => {
            service.meta.generation = next_generation(service.meta.generation, "Service")?;
            Ok(true)
        }
        ServiceMutation::Rollout(rollout) if service.status.rollout != rollout => {
            service.status.rollout = rollout;
            Ok(true)
        }
        ServiceMutation::Replicas(replicas) if service.status.replica_override != replicas => {
            service.status.replica_override = replicas;
            Ok(true)
        }
        ServiceMutation::Delete(timestamp) => {
            service.meta.deletion_timestamp = Some(timestamp);
            Ok(true)
        }
        ServiceMutation::Rollout(_) | ServiceMutation::Replicas(_) => Ok(false),
    }
}

pub(super) fn next_generation(current: Generation, kind: &str) -> Result<Generation, ApiError> {
    current.0.checked_add(1).map(Generation).ok_or_else(|| {
        ApiError::conflict(
            "generationExhausted",
            format!("{kind} generation is exhausted"),
        )
    })
}

fn parse(payload: Result<Json<CommandRequest>, JsonRejection>) -> Result<CommandRequest, ApiError> {
    Ok(payload
        .map_err(|rejection| mutation::json_rejection(rejection, "service command"))?
        .0)
}

fn command_response(service: &Service) -> ServiceCommandResponse {
    ServiceCommandResponse {
        service_id: service.meta.id.clone(),
        generation: service.meta.generation,
        rollout: service.status.rollout,
        replica_override: service.status.replica_override,
        deletion_timestamp: service.meta.deletion_timestamp,
    }
}

enum ServiceMutation {
    Redeploy,
    Rollout(RolloutState),
    Replicas(Option<u32>),
    Delete(Timestamp),
}

struct ServiceCommandInput {
    state: AppState,
    service_id: String,
    operator: OperatorIdentity,
    headers: HeaderMap,
    operation: &'static str,
    action: ServiceMutation,
}
