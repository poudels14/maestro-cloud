use axum::extract::rejection::JsonRejection;
use axum::extract::{DefaultBodyLimit, Extension, State};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::get;
use axum::{Json, Router};
use cluster::TailscaleAuthKeyRecord;
use kernel_api::{
    TailscaleAuthKeyRotationRequest, TailscaleAuthKeyRotationResponse, TailscaleAuthKeyStatus,
};
use kernel_store::{Compare, ExpectedVersion, Keyspace, Mutation, Transaction};

use crate::mutation::{MAXIMUM_REQUEST_BYTES, MutationRequest};
use crate::{ApiError, AppState, OperatorIdentity, mutation};

pub(super) fn router() -> Router<AppState> {
    Router::new()
        .route("/api/cluster/tailscale/auth-key", get(status).put(rotate))
        .layer(DefaultBodyLimit::max(MAXIMUM_REQUEST_BYTES))
}

async fn status(State(state): State<AppState>) -> Result<Json<TailscaleAuthKeyStatus>, ApiError> {
    ensure_enabled(&state)?;
    let key = Keyspace::new(&state.cluster_id).tailscale_auth_key();
    let current = state.store.get(&key).await.map_err(|error| {
        ApiError::internal(format!("failed to read Tailscale auth key: {error}"))
    })?;
    if let Some(current) = &current {
        decode_record(&current.value)?;
    }
    Ok(Json(TailscaleAuthKeyStatus {
        override_revision: current.map(|current| current.version.resource_revision()),
    }))
}

async fn rotate(
    State(state): State<AppState>,
    Extension(operator): Extension<OperatorIdentity>,
    headers: HeaderMap,
    payload: Result<Json<TailscaleAuthKeyRotationRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<TailscaleAuthKeyRotationResponse>), ApiError> {
    ensure_enabled(&state)?;
    let payload = payload
        .map_err(|rejection| mutation::json_rejection(rejection, "Tailscale auth-key rotation"))?
        .0;
    let record = TailscaleAuthKeyRecord::new(payload.auth_key.clone())
        .map_err(|error| ApiError::bad_request(error.to_string()))?;
    let request = MutationRequest::new(
        &state,
        &headers,
        &operator,
        "PUT /api/cluster/tailscale/auth-key",
        &[],
        &record.auth_key,
    )?;
    if let Some(response) = request.replay(&state).await? {
        return Ok((StatusCode::ACCEPTED, Json(response)));
    }

    let key = Keyspace::new(&state.cluster_id).tailscale_auth_key();
    let current = state.store.get(&key).await.map_err(|error| {
        ApiError::internal(format!("failed to read Tailscale auth key: {error}"))
    })?;
    if let Some(current) = &current {
        decode_record(&current.value)?;
    }
    let expected = match (&current, payload.expected_revision) {
        (None, None) => ExpectedVersion::Missing,
        (Some(current), Some(revision)) if current.version.resource_revision() == revision => {
            ExpectedVersion::Exact(current.version)
        }
        (None, Some(_)) | (Some(_), None) | (Some(_), Some(_)) => {
            return Err(ApiError::conflict(
                "revisionConflict",
                "Tailscale auth-key override is not at the expected revision",
            ));
        }
    };
    let response = TailscaleAuthKeyRotationResponse {
        request_id: request.request_id().clone(),
    };
    let response = request
        .commit(
            &state,
            response,
            Transaction {
                compares: vec![Compare {
                    key: key.clone(),
                    expected,
                }],
                mutations: vec![Mutation::Put {
                    key,
                    value: serde_json::to_vec(&record).map_err(|error| {
                        ApiError::internal(format!(
                            "failed to encode Tailscale auth-key override: {error}"
                        ))
                    })?,
                    session: None,
                }],
            },
            "revisionConflict",
            "Tailscale auth-key override changed after it was read",
        )
        .await?;
    Ok((StatusCode::ACCEPTED, Json(response)))
}

fn ensure_enabled(state: &AppState) -> Result<(), ApiError> {
    let config = state.cluster_config.as_ref().ok_or_else(|| {
        ApiError::service_unavailable("cluster configuration is not available on this API server")
    })?;
    if config.tailscale.is_none() {
        Err(ApiError::conflict(
            "tailscaleDisabled",
            "managed Tailscale gateways are not enabled",
        ))
    } else {
        Ok(())
    }
}

fn decode_record(value: &[u8]) -> Result<TailscaleAuthKeyRecord, ApiError> {
    let record = serde_json::from_slice::<TailscaleAuthKeyRecord>(value)
        .map_err(|_| ApiError::internal("stored Tailscale auth-key override is malformed"))?;
    record
        .validate()
        .map_err(|_| ApiError::internal("stored Tailscale auth-key override is invalid"))?;
    Ok(record)
}
