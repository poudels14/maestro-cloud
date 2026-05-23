use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::deployment::store::ClusterStore;
use crate::logs::Logger;
use crate::utils::crypto::SecretString;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SlackCategory {
    Info,
    Error,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SlackWebhook {
    pub id: String,
    pub name: String,
    pub url: SecretString,
    pub categories: Vec<SlackCategory>,
    #[serde(default = "default_true")]
    pub enabled: bool,
}

fn default_true() -> bool {
    true
}

#[derive(Clone)]
pub struct SlackNotifier {
    inner: Arc<NotifierInner>,
}

struct NotifierInner {
    config_webhook: Option<SecretString>,
    store: Option<Arc<dyn ClusterStore>>,
    cluster_name: String,
    client: reqwest::Client,
    logger: Logger,
}

impl SlackNotifier {
    pub fn new(
        config_webhook: Option<SecretString>,
        store: Option<Arc<dyn ClusterStore>>,
        cluster_name: String,
        logger: Logger,
    ) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .expect("failed to build reqwest client for slack notifier");
        Self {
            inner: Arc::new(NotifierInner {
                config_webhook,
                store,
                cluster_name,
                client,
                logger,
            }),
        }
    }

    pub fn notify_deployment_queued(&self, service_id: &str, deployment_id: &str, version: &str) {
        let text = format!(
            ":rocket: *Deployment queued* — `{service_id}` (`{}`)\n> cluster: `{}` · version: `{}`",
            short_id(deployment_id),
            self.inner.cluster_name,
            short_version(version),
        );
        self.dispatch(SlackCategory::Info, text);
    }

    pub fn notify_deployment_ready(&self, service_id: &str, deployment_id: &str) {
        let text = format!(
            ":white_check_mark: *Deployment ready* — `{service_id}` (`{}`)\n> cluster: `{}`",
            short_id(deployment_id),
            self.inner.cluster_name,
        );
        self.dispatch(SlackCategory::Info, text);
    }

    pub fn notify_deployment_crashed(&self, service_id: &str, deployment_id: &str, reason: &str) {
        let text = format!(
            ":x: *Deployment crashed* — `{service_id}` (`{}`)\n> cluster: `{}`\n> reason: {reason}",
            short_id(deployment_id),
            self.inner.cluster_name,
        );
        self.dispatch(SlackCategory::Error, text);
    }

    pub fn notify_service_removed(&self, service_id: &str) {
        let text = format!(
            ":wastebasket: *Service removed* — `{service_id}`\n> cluster: `{}`",
            self.inner.cluster_name,
        );
        self.dispatch(SlackCategory::Info, text);
    }

    fn dispatch(&self, category: SlackCategory, text: String) {
        let inner = self.inner.clone();
        tokio::spawn(async move {
            if let Some(url) = inner.config_webhook.as_ref() {
                send(&inner.client, &inner.logger, url.as_str(), &text).await;
            }
            if let Some(store) = inner.store.as_ref() {
                match store.list_slack_webhooks().await {
                    Ok(webhooks) => {
                        for webhook in webhooks {
                            if !webhook.enabled || !webhook.categories.contains(&category) {
                                continue;
                            }
                            send(&inner.client, &inner.logger, webhook.url.as_str(), &text).await;
                        }
                    }
                    Err(err) => {
                        inner.logger.emit(
                            "warn",
                            &format!("failed to read slack webhooks from store: {err}"),
                        );
                    }
                }
            }
        });
    }
}

async fn send(client: &reqwest::Client, logger: &Logger, url: &str, text: &str) {
    let payload = serde_json::json!({ "text": text });
    let result = client.post(url).json(&payload).send().await;
    match result {
        Ok(response) if response.status().is_success() => {}
        Ok(response) => {
            logger.emit(
                "warn",
                &format!(
                    "slack webhook returned status {} for notification",
                    response.status()
                ),
            );
        }
        Err(err) => {
            logger.emit("warn", &format!("slack webhook failed: {err}"));
        }
    }
}

fn short_id(deployment_id: &str) -> String {
    deployment_id.chars().take(6).collect()
}

fn short_version(version: &str) -> String {
    let stripped = version.strip_prefix("cfg-").unwrap_or(version);
    let short: String = stripped.chars().take(7).collect();
    format!("cfg-{short}")
}
