use std::sync::Arc;
use std::time::Duration;

use crate::logs::Logger;
use crate::utils::crypto::SecretString;

#[derive(Clone)]
pub struct SlackNotifier {
    inner: Option<Arc<NotifierInner>>,
}

struct NotifierInner {
    webhook_url: SecretString,
    cluster_name: String,
    client: reqwest::Client,
    logger: Logger,
}

impl SlackNotifier {
    pub fn new(webhook_url: Option<SecretString>, cluster_name: String, logger: Logger) -> Self {
        let inner = webhook_url.map(|url| {
            let client = reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .expect("failed to build reqwest client for slack notifier");
            Arc::new(NotifierInner {
                webhook_url: url,
                cluster_name,
                client,
                logger,
            })
        });
        Self { inner }
    }

    pub fn notify_deployment_queued(&self, service_id: &str, deployment_id: &str, version: &str) {
        if let Some(inner) = self.inner.clone() {
            let text = format!(
                ":rocket: [{}] deployment queued — `{service_id}/{}` v{version}",
                inner.cluster_name,
                short_id(deployment_id)
            );
            spawn_send(inner, text);
        }
    }

    pub fn notify_deployment_ready(&self, service_id: &str, deployment_id: &str) {
        if let Some(inner) = self.inner.clone() {
            let text = format!(
                ":white_check_mark: [{}] deployment ready — `{service_id}/{}`",
                inner.cluster_name,
                short_id(deployment_id)
            );
            spawn_send(inner, text);
        }
    }

    pub fn notify_deployment_crashed(&self, service_id: &str, deployment_id: &str, reason: &str) {
        if let Some(inner) = self.inner.clone() {
            let text = format!(
                ":x: [{}] deployment crashed — `{service_id}/{}`: {reason}",
                inner.cluster_name,
                short_id(deployment_id)
            );
            spawn_send(inner, text);
        }
    }

    pub fn notify_service_removed(&self, service_id: &str) {
        if let Some(inner) = self.inner.clone() {
            let text = format!(
                ":wastebasket: [{}] service removed — `{service_id}`",
                inner.cluster_name
            );
            spawn_send(inner, text);
        }
    }
}

fn short_id(deployment_id: &str) -> String {
    deployment_id.chars().take(6).collect()
}

fn spawn_send(inner: Arc<NotifierInner>, text: String) {
    tokio::spawn(async move {
        let payload = serde_json::json!({ "text": text });
        let result = inner
            .client
            .post(inner.webhook_url.as_str())
            .json(&payload)
            .send()
            .await;
        match result {
            Ok(response) if response.status().is_success() => {}
            Ok(response) => {
                inner.logger.emit(
                    "warn",
                    &format!(
                        "slack webhook returned status {} for notification",
                        response.status()
                    ),
                );
            }
            Err(err) => {
                inner
                    .logger
                    .emit("warn", &format!("slack webhook failed: {err}"));
            }
        }
    });
}
