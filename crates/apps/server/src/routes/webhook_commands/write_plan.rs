use std::collections::{BTreeMap, BTreeSet};

use axum::http::Uri;
use kernel_api::{
    BuiltinKind, Generation, Object, ObjectMeta, ResourceKind, ResourceRevision, SecretValue,
    Webhook, WebhookCategory, WebhookEvent, WebhookFormat, WebhookId, WebhookSpec, WebhookStatus,
};
use kernel_store::{ExpectedVersion, Keyspace, StoredValue};
use serde::{Deserialize, Serialize};

use super::super::service_commands::next_generation;
use super::super::write_plan::WritePlan;
use crate::{ApiError, resource};

const MINIMUM_SIGNING_SECRET_BYTES: usize = 32;
const MAXIMUM_SIGNING_SECRET_BYTES: usize = 4_096;
const MAXIMUM_ENDPOINT_BYTES: usize = 2_048;
const MAXIMUM_NAME_BYTES: usize = 256;

pub(super) fn plan_write(
    current: Option<&StoredValue>,
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

pub(super) fn validate_payload(payload: &WebhookWriteRequest) -> Result<(), ApiError> {
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

fn validate_spec(spec: &WebhookSpec) -> Result<(), ApiError> {
    if spec.format == WebhookFormat::Slack && spec.name.is_empty() {
        Err(ApiError::bad_request("Slack webhooks require a name"))
    } else {
        Ok(())
    }
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

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct WebhookWriteRequest {
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
