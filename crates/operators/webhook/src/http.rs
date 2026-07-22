use std::time::Duration;

use async_trait::async_trait;
use hmac::{Hmac, Mac};
use kernel_api::{SecretValue, WebhookFormat};
use reqwest::header::{CONTENT_TYPE, HeaderValue, USER_AGENT};
use sha2::Sha256;

use crate::{WebhookDelivery, WebhookDeliveryBackend, WebhookDeliveryError};

const EVENT_HEADER: &str = "X-Maestro-Event";
const DELIVERY_HEADER: &str = "X-Maestro-Delivery";
const SIGNATURE_HEADER: &str = "X-Maestro-Signature-256";

/// Production HTTPS transport for signed webhook payloads.
pub struct HttpWebhookBackend {
    client: reqwest::Client,
}

impl HttpWebhookBackend {
    /// Builds a bounded rustls client without starting background work.
    pub fn new(timeout: Duration) -> Result<Self, HttpWebhookBackendError> {
        if timeout.is_zero() {
            return Err(HttpWebhookBackendError::ZeroTimeout);
        }
        let client = reqwest::Client::builder()
            .timeout(timeout)
            .https_only(true)
            .build()
            .map_err(HttpWebhookBackendError::Client)?;
        Ok(Self { client })
    }

    pub(crate) fn request(
        &self,
        endpoint: &str,
        format: WebhookFormat,
        signing_secret: Option<&SecretValue>,
        delivery: &WebhookDelivery,
    ) -> Result<reqwest::RequestBuilder, WebhookDeliveryError> {
        let body = match format {
            WebhookFormat::Maestro => serde_json::to_vec(delivery),
            WebhookFormat::Slack => serde_json::to_vec(&serde_json::json!({
                "text": delivery.text,
            })),
        }
        .map_err(|error| WebhookDeliveryError::Rejected {
            message: format!("failed to encode webhook payload: {error}"),
        })?;
        let request = self
            .client
            .post(endpoint)
            .header(CONTENT_TYPE, "application/json")
            .header(USER_AGENT, "maestro-webhook/1")
            .body(body.clone());
        if format == WebhookFormat::Slack {
            return Ok(request);
        }
        let signing_secret = signing_secret.ok_or_else(|| WebhookDeliveryError::Rejected {
            message: "native Maestro webhooks require a signing secret".to_owned(),
        })?;
        let signature = sign(signing_secret, &body)?;
        let event = serde_json::to_value(delivery.event)
            .ok()
            .and_then(|value| value.as_str().map(ToOwned::to_owned))
            .ok_or_else(|| WebhookDeliveryError::Rejected {
                message: "failed to encode webhook event header".to_string(),
            })?;
        let event =
            HeaderValue::from_str(&event).map_err(|error| WebhookDeliveryError::Rejected {
                message: format!("invalid webhook event header: {error}"),
            })?;
        let delivery_id = HeaderValue::from_str(&delivery.delivery_id).map_err(|error| {
            WebhookDeliveryError::Rejected {
                message: format!("invalid webhook delivery header: {error}"),
            }
        })?;
        let signature =
            HeaderValue::from_str(&signature).map_err(|error| WebhookDeliveryError::Rejected {
                message: format!("invalid webhook signature header: {error}"),
            })?;
        Ok(request
            .header(EVENT_HEADER, event)
            .header(DELIVERY_HEADER, delivery_id)
            .header(SIGNATURE_HEADER, signature))
    }
}

#[async_trait]
impl WebhookDeliveryBackend for HttpWebhookBackend {
    async fn deliver(
        &self,
        endpoint: &str,
        format: WebhookFormat,
        signing_secret: Option<&SecretValue>,
        delivery: &WebhookDelivery,
    ) -> Result<(), WebhookDeliveryError> {
        let response = self
            .request(endpoint, format, signing_secret, delivery)?
            .send()
            .await
            .map_err(|error| WebhookDeliveryError::Unavailable {
                message: error.without_url().to_string(),
            })?;
        if response.status().is_success() {
            Ok(())
        } else {
            Err(WebhookDeliveryError::Rejected {
                message: format!("endpoint returned HTTP {}", response.status().as_u16()),
            })
        }
    }
}

fn sign(secret: &SecretValue, body: &[u8]) -> Result<String, WebhookDeliveryError> {
    let mut signer =
        Hmac::<Sha256>::new_from_slice(secret.expose().as_bytes()).map_err(|error| {
            WebhookDeliveryError::Rejected {
                message: format!("invalid signing secret: {error}"),
            }
        })?;
    signer.update(body);
    Ok(format!(
        "sha256={}",
        hex::encode(signer.finalize().into_bytes())
    ))
}

/// Invalid production transport construction.
#[derive(Debug, thiserror::Error)]
pub enum HttpWebhookBackendError {
    /// A zero timeout could hang operator shutdown forever.
    #[error("webhook HTTP timeout must be greater than zero")]
    ZeroTimeout,
    /// Reqwest could not construct the bounded client.
    #[error("failed to construct webhook HTTP client: {0}")]
    Client(reqwest::Error),
}
