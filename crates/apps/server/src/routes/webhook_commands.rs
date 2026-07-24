use std::collections::{BTreeMap, BTreeSet};

use axum::extract::rejection::JsonRejection;
use axum::extract::{DefaultBodyLimit, Extension, Path, State};
use axum::http::{HeaderMap, StatusCode, Uri};
use axum::routing::{delete, post, put};
use axum::{Json, Router};
use kernel_api::{
    BuiltinKind, Generation, Object, ObjectMeta, ResourceKind, ResourceRevision, SecretValue,
    Timestamp, Webhook, WebhookCategory, WebhookEvent, WebhookFormat, WebhookId, WebhookSpec,
    WebhookStatus,
};
use kernel_store::{Compare, ExpectedVersion, Keyspace, Mutation, Transaction};
use serde::{Deserialize, Serialize};

use super::service_commands::next_generation;
use super::write_plan::WritePlan;
use crate::mutation::{MAXIMUM_REQUEST_BYTES, MutationRequest};
use crate::{ApiError, AppState, OperatorIdentity, mutation, resource};

const MINIMUM_SIGNING_SECRET_BYTES: usize = 32;
const MAXIMUM_SIGNING_SECRET_BYTES: usize = 4_096;
const MAXIMUM_ENDPOINT_BYTES: usize = 2_048;
const MAXIMUM_NAME_BYTES: usize = 256;

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

fn plan_write(
    current: Option<&kernel_store::StoredValue>,
    keys: &Keyspace,
    kind: &ResourceKind,
    webhook_id: WebhookId,
    payload: WebhookWriteRequest,
) -> Result<WritePlan<Webhook>, ApiError> {
    match (current, payload.expected_revision) {
        (None, None) => {
            let format = payload.format.unwrap_or_default();
            let endpoint = payload.endpoint.clone().ok_or_else(|| {
                ApiError::bad_request("endpoint is required when creating a webhook")
            })?;
            let secret = payload.signing_secret.clone();
            if format == WebhookFormat::Maestro && secret.is_none() {
                return Err(ApiError::bad_request(
                    "signingSecret is required when creating a native Maestro webhook",
                ));
            }
            let spec = payload.spec(None, endpoint, secret);
            validate_spec(&spec)?;
            Ok(WritePlan::Put {
                resource: new_webhook(webhook_id, spec),
                expected: ExpectedVersion::Missing,
            })
        }
        (None, Some(_)) => Err(ApiError::conflict(
            "revisionConflict",
            "Webhook does not exist at the expected revision",
        )),
        (Some(stored), expected_revision) => {
            let mut current: Webhook = resource::decode(stored, keys, kind, BuiltinKind::Webhook)?;
            if expected_revision != Some(current.meta.revision) {
                return Err(ApiError::conflict(
                    "revisionConflict",
                    "Webhook is not at the expected revision",
                ));
            }
            let secret = payload
                .signing_secret
                .clone()
                .or_else(|| current.spec.signing_secret.clone());
            let endpoint = payload
                .endpoint
                .clone()
                .unwrap_or_else(|| current.spec.endpoint.clone());
            let spec = payload.spec(Some(&current.spec), endpoint, secret);
            if spec.format == WebhookFormat::Maestro && spec.signing_secret.is_none() {
                return Err(ApiError::bad_request(
                    "native Maestro webhooks require a signing secret",
                ));
            }
            validate_spec(&spec)?;
            if current.spec == spec {
                Ok(WritePlan::Retain {
                    resource: current,
                    expected: ExpectedVersion::Exact(stored.version),
                })
            } else {
                current.meta.generation = next_generation(current.meta.generation, "Webhook")?;
                current.spec = spec;
                Ok(WritePlan::Put {
                    resource: current,
                    expected: ExpectedVersion::Exact(stored.version),
                })
            }
        }
    }
}

fn validate_spec(spec: &WebhookSpec) -> Result<(), ApiError> {
    if spec.format == WebhookFormat::Slack && spec.name.is_empty() {
        Err(ApiError::bad_request("Slack webhooks require a name"))
    } else {
        Ok(())
    }
}

fn validate_payload(payload: &WebhookWriteRequest) -> Result<(), ApiError> {
    if let Some(name) = &payload.name {
        let name = name.trim();
        if name.is_empty() {
            return Err(ApiError::bad_request("webhook name cannot be empty"));
        }
        if name.len() > MAXIMUM_NAME_BYTES {
            return Err(ApiError::bad_request("webhook name is too long"));
        }
    }
    if let Some(raw_endpoint) = &payload.endpoint {
        if raw_endpoint.expose().len() > MAXIMUM_ENDPOINT_BYTES {
            return Err(ApiError::bad_request("webhook endpoint is too long"));
        }
        let endpoint = raw_endpoint
            .expose()
            .parse::<Uri>()
            .map_err(|error| ApiError::bad_request(format!("invalid webhook endpoint: {error}")))?;
        if endpoint.scheme_str() != Some("https") || endpoint.authority().is_none() {
            return Err(ApiError::bad_request(
                "webhook endpoint must be an absolute https URL",
            ));
        }
        if endpoint
            .authority()
            .is_some_and(|authority| authority.as_str().contains('@'))
        {
            return Err(ApiError::bad_request(
                "webhook endpoint must not contain credentials",
            ));
        }
    }
    if payload.events.is_empty() {
        return Err(ApiError::bad_request(
            "webhook must subscribe to at least one event",
        ));
    }
    for (index, event) in payload.events.iter().enumerate() {
        if payload
            .events
            .iter()
            .take(index)
            .any(|existing| existing == event)
        {
            return Err(ApiError::bad_request(
                "webhook events must not contain duplicates",
            ));
        }
    }
    if let Some(categories) = &payload.categories {
        if categories.is_empty() {
            return Err(ApiError::bad_request(
                "webhook must select at least one notification category",
            ));
        }
        for (index, category) in categories.iter().enumerate() {
            if categories
                .iter()
                .take(index)
                .any(|existing| existing == category)
            {
                return Err(ApiError::bad_request(
                    "webhook categories must not contain duplicates",
                ));
            }
        }
    }
    if let Some(secret) = &payload.signing_secret {
        let length = secret.expose().len();
        if !(MINIMUM_SIGNING_SECRET_BYTES..=MAXIMUM_SIGNING_SECRET_BYTES).contains(&length) {
            return Err(ApiError::bad_request(format!(
                "signingSecret must contain {MINIMUM_SIGNING_SECRET_BYTES} to {MAXIMUM_SIGNING_SECRET_BYTES} bytes"
            )));
        }
    }
    Ok(())
}

fn new_webhook(webhook_id: WebhookId, spec: WebhookSpec) -> Webhook {
    Object {
        meta: ObjectMeta {
            id: webhook_id,
            labels: BTreeMap::new(),
            annotations: BTreeMap::new(),
            revision: ResourceRevision::default(),
            generation: Generation(1),
            owner_refs: Vec::new(),
            finalizers: BTreeSet::new(),
            deletion_timestamp: None,
        },
        spec,
        status: WebhookStatus {
            last_success_at: None,
            consecutive_failures: 0,
            retry_at: None,
            observed_generation: None,
            observations: Vec::new(),
            conditions: Vec::new(),
        },
    }
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
struct WebhookWriteRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    expected_revision: Option<ResourceRevision>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    endpoint: Option<SecretValue>,
    events: Vec<WebhookEvent>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    categories: Option<Vec<WebhookCategory>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    enabled: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    format: Option<WebhookFormat>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    signing_secret: Option<SecretValue>,
}

impl WebhookWriteRequest {
    fn spec(
        &self,
        current: Option<&WebhookSpec>,
        endpoint: SecretValue,
        signing_secret: Option<SecretValue>,
    ) -> WebhookSpec {
        WebhookSpec {
            name: self
                .name
                .as_deref()
                .map(str::trim)
                .map(str::to_owned)
                .or_else(|| current.map(|spec| spec.name.clone()))
                .unwrap_or_default(),
            endpoint,
            events: self.events.clone(),
            categories: self
                .categories
                .clone()
                .or_else(|| current.map(|spec| spec.categories.clone()))
                .unwrap_or_else(|| vec![WebhookCategory::Info, WebhookCategory::Error]),
            enabled: self
                .enabled
                .or_else(|| current.map(|spec| spec.enabled))
                .unwrap_or(true),
            format: self
                .format
                .or_else(|| current.map(|spec| spec.format))
                .unwrap_or_default(),
            signing_secret,
        }
    }
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
