use axum::extract::rejection::JsonRejection;
use axum::extract::{DefaultBodyLimit, Extension, Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::post;
use axum::{Json, Router};
use kernel_api::{
    ServiceDiffChange, ServiceDiffStatus, ServiceId, ServiceRolloutDiffRequest,
    ServiceRolloutDiffResponse, ServiceRolloutRequest, ServiceRolloutResponse,
};
use kernel_store::Transaction;
use serde::Serialize;

use super::service_diff;
use super::service_rollout_validation::validate_desired;
use crate::mutation::{MAXIMUM_REQUEST_BYTES, MutationRequest};
use crate::system_resources::ensure_user_resource_id;
use crate::{ApiError, AppState, OperatorIdentity, mutation};

mod managed_resources;

use managed_resources::{FrozenRolloutPolicy, ManagedResources};

pub(super) fn router() -> Router<AppState> {
    Router::new()
        .route("/api/services/{service_id}/rollout", post(apply))
        .route("/api/services/{service_id}/rollout/diff", post(diff))
        .layer(DefaultBodyLimit::max(MAXIMUM_REQUEST_BYTES))
}

async fn diff(
    State(state): State<AppState>,
    Path(service_id): Path<String>,
    payload: Result<Json<ServiceRolloutDiffRequest>, JsonRejection>,
) -> Result<Json<ServiceRolloutDiffResponse>, ApiError> {
    let service_id = parse_service_id(service_id)?;
    let desired = validate_desired(
        &state,
        &service_id,
        payload
            .map_err(|rejection| mutation::json_rejection(rejection, "service rollout diff"))?
            .0
            .desired,
    )
    .await?;
    let managed = ManagedResources::load(&state, &service_id).await?;
    let current = managed.decode()?;
    let mut changes = match &current.service {
        Some(service) => service_diff::changes(&service.spec, &desired.service)?,
        None => Vec::new(),
    };
    push_auxiliary_change(
        &mut changes,
        "ingress",
        current.ingress.as_ref().map(|route| &route.spec),
        desired.ingress.as_ref(),
    )?;
    push_auxiliary_change(
        &mut changes,
        "egress",
        current.egress.as_ref().map(|policy| &policy.spec),
        desired.egress.as_ref(),
    )?;
    let unchanged = current
        .service
        .as_ref()
        .is_some_and(|service| service.spec == desired.service)
        && current.ingress.as_ref().map(|route| &route.spec) == desired.ingress.as_ref()
        && current.egress.as_ref().map(|policy| &policy.spec) == desired.egress.as_ref();
    Ok(Json(ServiceRolloutDiffResponse {
        service_id,
        expected_revisions: managed.revisions(),
        status: if current.service.is_none() {
            ServiceDiffStatus::New
        } else if unchanged {
            ServiceDiffStatus::Unchanged
        } else {
            ServiceDiffStatus::Changed
        },
        changes,
    }))
}

async fn apply(
    State(state): State<AppState>,
    Path(service_id): Path<String>,
    Extension(operator): Extension<OperatorIdentity>,
    headers: HeaderMap,
    payload: Result<Json<ServiceRolloutRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<ServiceRolloutResponse>), ApiError> {
    let service_id = parse_service_id(service_id)?;
    ensure_user_resource_id("Service", service_id.as_str())?;
    let payload = payload
        .map_err(|rejection| mutation::json_rejection(rejection, "service rollout"))?
        .0;
    let desired = validate_desired(&state, &service_id, payload.desired).await?;
    let payload = ServiceRolloutRequest {
        expected_revisions: payload.expected_revisions,
        force: payload.force,
        desired,
    };
    let request = MutationRequest::new(
        &state,
        &headers,
        &operator,
        "POST /api/services/{serviceId}/rollout",
        &[service_id.as_str()],
        &payload,
    )?;
    if let Some(response) = request.replay(&state).await? {
        return Ok((StatusCode::ACCEPTED, Json(response)));
    }
    let frozen_rollout = if payload.force {
        FrozenRolloutPolicy::BypassNextGeneration
    } else {
        FrozenRolloutPolicy::HonorFreeze
    };
    let managed = ManagedResources::load(&state, &service_id).await?;
    let (compares, mutations, response) = managed.plan(
        service_id,
        payload.expected_revisions,
        frozen_rollout,
        payload.desired,
    )?;
    let response = request
        .commit(
            &state,
            response,
            Transaction {
                compares,
                mutations,
            },
            "revisionConflict",
            "a managed service resource changed after the rollout preview",
        )
        .await?;
    Ok((StatusCode::ACCEPTED, Json(response)))
}

fn push_auxiliary_change<Value: Serialize + PartialEq>(
    changes: &mut Vec<ServiceDiffChange>,
    field: &str,
    current: Option<&Value>,
    desired: Option<&Value>,
) -> Result<(), ApiError> {
    if current == desired {
        return Ok(());
    }
    changes.push(ServiceDiffChange {
        field: field.to_string(),
        from: display(current)?,
        to: display(desired)?,
    });
    Ok(())
}

fn display<Value: Serialize>(value: Option<&Value>) -> Result<Option<String>, ApiError> {
    value
        .map(|value| {
            serde_json::to_string(value).map_err(|error| {
                ApiError::internal(format!("failed to encode rollout diff: {error}"))
            })
        })
        .transpose()
}

fn parse_service_id(value: String) -> Result<ServiceId, ApiError> {
    ServiceId::new(value).map_err(|error| ApiError::bad_request(error.to_string()))
}
