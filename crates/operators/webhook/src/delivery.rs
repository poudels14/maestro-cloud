use async_trait::async_trait;
use kernel_api::{
    ClusterId, DeploymentPhase, PreviewPhase, RequestId, ResourceName, ResourceRevision,
    SecretValue, Timestamp, UpgradePhase, WebhookCategory, WebhookEvent, WebhookFormat, WebhookId,
    WebhookNodeAvailability, WebhookObservedState,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Stable signed payload sent for one observed resource transition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WebhookDelivery {
    /// Deterministic identity receivers can use to deduplicate retries.
    pub delivery_id: String,
    /// Cluster in which the transition was observed.
    pub cluster_id: ClusterId,
    /// Webhook configuration receiving this delivery.
    pub webhook_id: WebhookId,
    /// Subscription class of the transition.
    pub event: WebhookEvent,
    /// Whether this payload was explicitly requested by an operator test command.
    #[serde(default, skip_serializing_if = "is_false")]
    pub test: bool,
    /// Built-in resource identity within the event class.
    pub resource_id: ResourceName,
    /// Previously acknowledged state, absent for a newly created resource.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub previous: Option<WebhookObservedState>,
    /// Current state observed from the store.
    pub current: WebhookObservedState,
    /// Current source store revision.
    pub resource_revision: ResourceRevision,
    /// Slack-compatible human summary; typed fields remain authoritative.
    pub text: String,
    /// Time the active leader observed this transition.
    pub observed_at: Timestamp,
}

impl WebhookDelivery {
    pub(crate) fn new(
        cluster_id: ClusterId,
        webhook_id: WebhookId,
        resource_id: ResourceName,
        previous: Option<WebhookObservedState>,
        current: WebhookObservedState,
        resource_revision: ResourceRevision,
        observed_at: Timestamp,
    ) -> Result<Self, WebhookDeliveryError> {
        let event = current.event();
        let text = delivery_text(&cluster_id, &resource_id, previous, current)?;
        let identity = serde_json::to_vec(&(
            &cluster_id,
            &webhook_id,
            event,
            &resource_id,
            current,
            resource_revision,
        ))
        .map_err(|error| WebhookDeliveryError::Rejected {
            message: format!("failed to encode delivery identity: {error}"),
        })?;
        let delivery_id = format!("wd_{}", hex::encode(Sha256::digest(identity)));
        Ok(Self {
            delivery_id,
            cluster_id,
            webhook_id,
            event,
            test: false,
            resource_id,
            previous,
            current,
            resource_revision,
            text,
            observed_at,
        })
    }

    /// Creates a deterministic synthetic delivery for an operator test command.
    pub fn test(
        cluster_id: ClusterId,
        webhook_id: WebhookId,
        event: WebhookEvent,
        request_id: &RequestId,
        observed_at: Timestamp,
    ) -> Result<Self, WebhookDeliveryError> {
        let identity = serde_json::to_vec(&(&cluster_id, &webhook_id, "test", request_id))
            .map_err(|error| WebhookDeliveryError::Rejected {
                message: format!("failed to encode test delivery identity: {error}"),
            })?;
        Ok(Self {
            delivery_id: format!("wd_{}", hex::encode(Sha256::digest(identity))),
            cluster_id: cluster_id.clone(),
            webhook_id,
            event,
            test: true,
            resource_id: ResourceName::new("test").map_err(|error| {
                WebhookDeliveryError::Rejected {
                    message: error.to_string(),
                }
            })?,
            previous: None,
            current: test_state(event),
            resource_revision: ResourceRevision::default(),
            text: format!("Maestro `{cluster_id}` webhook test message."),
            observed_at,
        })
    }

    /// Classifies the transition using the legacy-compatible info/error split.
    pub const fn category(&self) -> WebhookCategory {
        match self.current {
            WebhookObservedState::DeploymentTransition(DeploymentPhase::Crashed)
            | WebhookObservedState::NodeAvailability(WebhookNodeAvailability::Unavailable)
            | WebhookObservedState::PreviewTransition(PreviewPhase::Failed)
            | WebhookObservedState::UpgradeTransition(UpgradePhase::Failed) => {
                WebhookCategory::Error
            }
            _ => WebhookCategory::Info,
        }
    }
}

const fn test_state(event: WebhookEvent) -> WebhookObservedState {
    match event {
        WebhookEvent::DeploymentTransition => {
            WebhookObservedState::DeploymentTransition(DeploymentPhase::Queued)
        }
        WebhookEvent::NodeAvailability => {
            WebhookObservedState::NodeAvailability(WebhookNodeAvailability::Available)
        }
        WebhookEvent::PreviewTransition => {
            WebhookObservedState::PreviewTransition(PreviewPhase::Pending)
        }
        WebhookEvent::UpgradeTransition => {
            WebhookObservedState::UpgradeTransition(UpgradePhase::Pending)
        }
    }
}

const fn is_false(value: &bool) -> bool {
    !*value
}

fn delivery_text(
    cluster_id: &ClusterId,
    resource_id: &ResourceName,
    previous: Option<WebhookObservedState>,
    current: WebhookObservedState,
) -> Result<String, WebhookDeliveryError> {
    let current = state_label(current)?;
    Ok(match previous {
        Some(previous) => format!(
            "Maestro `{cluster_id}`: `{resource_id}` transitioned from `{}` to `{current}`.",
            state_label(previous)?,
        ),
        None => format!("Maestro `{cluster_id}`: `{resource_id}` entered `{current}`.",),
    })
}

fn state_label(state: WebhookObservedState) -> Result<String, WebhookDeliveryError> {
    serde_json::to_value(state)
        .ok()
        .and_then(|value| {
            value
                .get("state")
                .and_then(serde_json::Value::as_str)
                .map(str::to_ascii_lowercase)
        })
        .ok_or_else(|| WebhookDeliveryError::Rejected {
            message: "failed to encode webhook transition state".to_string(),
        })
}

/// Injected side-effect boundary for one signed outbound delivery.
#[async_trait]
pub trait WebhookDeliveryBackend: Send + Sync {
    /// Delivers one deterministic event.
    ///
    /// Cancellation may occur after the receiver accepts the request. Retrying
    /// the same payload preserves `delivery_id` so receivers can deduplicate.
    async fn deliver(
        &self,
        endpoint: &str,
        format: WebhookFormat,
        signing_secret: Option<&SecretValue>,
        delivery: &WebhookDelivery,
    ) -> Result<(), WebhookDeliveryError>;
}

/// Matchable outbound transport failure.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum WebhookDeliveryError {
    /// The request could not reach or complete at the endpoint.
    #[error("webhook delivery is unavailable: {message}")]
    Unavailable {
        /// Transport-safe failure detail.
        message: String,
    },
    /// The endpoint or payload was rejected before successful delivery.
    #[error("webhook delivery was rejected: {message}")]
    Rejected {
        /// Endpoint-safe rejection detail.
        message: String,
    },
}
