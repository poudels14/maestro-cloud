use axum::extract::rejection::JsonRejection;
use axum::extract::{DefaultBodyLimit, Extension, Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::{delete, post, put};
use axum::{Json, Router};
use kernel_api::{
    BuiltinKind, Generation, ResourceKind, ResourceRevision, Timestamp, Webhook, WebhookId,
};
use kernel_store::{Compare, ExpectedVersion, Keyspace, Mutation, Transaction};
use serde::{Deserialize, Serialize};

use self::write_plan::{WebhookWriteRequest, plan_write, validate_payload};
use super::write_plan::WritePlan;
use crate::mutation::{MAXIMUM_REQUEST_BYTES, MutationRequest};
use crate::{ApiError, AppState, OperatorIdentity, mutation, resource};

mod write_plan;

pub(super) fn router() -> Router<AppState> {
    Router::new()
        .route(
            "/api/webhooks/{webhook_id}",
            put(write).merge(delete(delete_webhook)),
        )
        .route("/api/webhooks/{webhook_id}/test", post(test_webhook))
        .layer(DefaultBodyLimit::max(MAXIMUM_REQUEST_BYTES))
}

async fn write(
    State(state): State<AppState>,
    Path(webhook_id): Path<String>,
    Extension(operator): Extension<OperatorIdentity>,
    headers: HeaderMap,
    payload: Result<Json<WebhookWriteRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<WebhookCommandResponse>), ApiError> {
    let webhook_id = parse_id(webhook_id)?;
    let payload = payload
        .map_err(|rejection| mutation::json_rejection(rejection, "webhook"))?
        .0;
    validate_payload(&payload)?;
    let request = MutationRequest::new(
        &state,
        &headers,
        &operator,
        "PUT /api/webhooks/{webhookId}",
        &[webhook_id.as_str()],
        &payload,
    )?;
    if let Some(response) = request.replay(&state).await? {
        return Ok((StatusCode::ACCEPTED, Json(response)));
    }
    let keys = Keyspace::new(&state.cluster_id);
    let kind = webhook_kind()?;
    let key = keys.resource(&kind, &webhook_id.clone().into());
    let current = state
        .store
        .get(&key)
        .await
        .map_err(|error| ApiError::internal(format!("failed to read Webhook: {error}")))?;
    let plan = plan_write(current.as_ref(), &keys, &kind, webhook_id, payload)?;
    let (webhook, expected, mutations) = match plan {
        WritePlan::Retain { resource, expected } => (resource, expected, Vec::new()),
        WritePlan::Put { resource, expected } => {
            let mutation = Mutation::Put {
                key: key.clone(),
                value: encode(&resource)?,
                session: None,
            };
            (resource, expected, vec![mutation])
        }
    };
    let response = WebhookCommandResponse::from(&webhook);
    let response = request
        .commit(
            &state,
            response,
            Transaction {
                compares: vec![Compare { key, expected }],
                mutations,
            },
            "revisionConflict",
            "Webhook changed after its expected revision was read",
        )
        .await?;
    Ok((StatusCode::ACCEPTED, Json(response)))
}

async fn delete_webhook(
    State(state): State<AppState>,
    Path(webhook_id): Path<String>,
    Extension(operator): Extension<OperatorIdentity>,
    headers: HeaderMap,
    payload: Result<Json<WebhookDeleteRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<WebhookCommandResponse>), ApiError> {
    let webhook_id = parse_id(webhook_id)?;
    let payload = payload
        .map_err(|rejection| mutation::json_rejection(rejection, "webhook command"))?
        .0;
    let request = MutationRequest::new(
        &state,
        &headers,
        &operator,
        "DELETE /api/webhooks/{webhookId}",
        &[webhook_id.as_str()],
        &payload,
    )?;
    if let Some(response) = request.replay(&state).await? {
        return Ok((StatusCode::ACCEPTED, Json(response)));
    }
    let keys = Keyspace::new(&state.cluster_id);
    let kind = webhook_kind()?;
    let key = keys.resource(&kind, &webhook_id.clone().into());
    let stored = state
        .store
        .get(&key)
        .await
        .map_err(|error| ApiError::internal(format!("failed to read Webhook: {error}")))?
        .ok_or_else(|| ApiError::not_found(format!("Webhook `{webhook_id}` does not exist")))?;
    let webhook: Webhook = resource::decode(&stored, &keys, &kind, BuiltinKind::Webhook)?;
    if webhook.meta.revision != payload.expected_revision {
        return Err(ApiError::conflict(
            "revisionConflict",
            "Webhook is not at the expected revision",
        ));
    }
    let response = WebhookCommandResponse::from(&webhook);
    let response = request
        .commit(
            &state,
            response,
            Transaction {
                compares: vec![Compare {
                    key: key.clone(),
                    expected: ExpectedVersion::Exact(stored.version),
                }],
                mutations: vec![Mutation::Delete { key }],
            },
            "revisionConflict",
            "Webhook changed after its expected revision was read",
        )
        .await?;
    Ok((StatusCode::ACCEPTED, Json(response)))
}

async fn test_webhook(
    State(state): State<AppState>,
    Path(webhook_id): Path<String>,
    Extension(operator): Extension<OperatorIdentity>,
    headers: HeaderMap,
    payload: Result<Json<WebhookTestRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<WebhookTestResponse>), ApiError> {
    let webhook_id = parse_id(webhook_id)?;
    let payload = payload
        .map_err(|rejection| mutation::json_rejection(rejection, "webhook test command"))?
        .0;
    let request = MutationRequest::new(
        &state,
        &headers,
        &operator,
        "POST /api/webhooks/{webhookId}/test",
        &[webhook_id.as_str()],
        &payload,
    )?;
    if let Some(response) = request.replay(&state).await? {
        return Ok((StatusCode::OK, Json(response)));
    }
    let keys = Keyspace::new(&state.cluster_id);
    let kind = webhook_kind()?;
    let key = keys.resource(&kind, &webhook_id.clone().into());
    let stored = state
        .store
        .get(&key)
        .await
        .map_err(|error| ApiError::internal(format!("failed to read Webhook: {error}")))?
        .ok_or_else(|| ApiError::not_found(format!("Webhook `{webhook_id}` does not exist")))?;
    let webhook: Webhook = resource::decode(&stored, &keys, &kind, BuiltinKind::Webhook)?;
    let event = webhook
        .spec
        .events
        .first()
        .copied()
        .ok_or_else(|| ApiError::internal("stored Webhook has no subscribed events"))?;
    let backend = state
        .webhook_backend
        .as_ref()
        .ok_or_else(|| ApiError::service_unavailable("webhook delivery is not configured"))?;
    let tested_at = state.timestamp_clock.now();
    let delivery = webhook::WebhookDelivery::test(
        state.cluster_id.clone(),
        webhook_id.clone(),
        event,
        request.request_id(),
        tested_at,
    )
    .map_err(|error| ApiError::internal(error.to_string()))?;
    backend
        .deliver(
            webhook.spec.endpoint.expose(),
            webhook.spec.format,
            webhook.spec.signing_secret.as_ref(),
            &delivery,
        )
        .await
        .map_err(|error| ApiError::bad_gateway(error.to_string()))?;
    let response = WebhookTestResponse {
        webhook_id,
        delivery_id: delivery.delivery_id,
        tested_at,
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
                mutations: Vec::new(),
            },
            "revisionConflict",
            "Webhook changed while its test delivery was in flight",
        )
        .await?;
    Ok((StatusCode::OK, Json(response)))
}

fn encode(webhook: &Webhook) -> Result<Vec<u8>, ApiError> {
    serde_json::to_vec(webhook)
        .map_err(|error| ApiError::internal(format!("failed to encode Webhook: {error}")))
}

fn parse_id(value: String) -> Result<WebhookId, ApiError> {
    WebhookId::new(value).map_err(|error| ApiError::bad_request(error.to_string()))
}

fn webhook_kind() -> Result<ResourceKind, ApiError> {
    ResourceKind::new(BuiltinKind::Webhook.as_str())
        .map_err(|error| ApiError::internal(error.to_string()))
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct WebhookDeleteRequest {
    expected_revision: ResourceRevision,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WebhookTestRequest {}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct WebhookTestResponse {
    webhook_id: WebhookId,
    delivery_id: String,
    tested_at: Timestamp,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct WebhookCommandResponse {
    webhook_id: WebhookId,
    generation: Generation,
}

impl From<&Webhook> for WebhookCommandResponse {
    fn from(webhook: &Webhook) -> Self {
        Self {
            webhook_id: webhook.meta.id.clone(),
            generation: webhook.meta.generation,
        }
    }
}
