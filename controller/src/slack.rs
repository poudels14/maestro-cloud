use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::cluster::types::NodeInfo;
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

    pub fn notify_node_down(&self, node: &NodeInfo) {
        let text = node_down_message(&self.inner.cluster_name, node);
        self.dispatch(SlackCategory::Error, text);
    }

    pub fn notify_node_recovered(&self, node: &NodeInfo, unavailable_for_ms: i64) {
        let text = node_recovered_message(&self.inner.cluster_name, node, unavailable_for_ms);
        self.dispatch(SlackCategory::Info, text);
    }

    pub fn notify_node_unavailable(&self, node: &NodeInfo, reason: &str, unavailable_for_ms: i64) {
        let text =
            node_unavailable_message(&self.inner.cluster_name, node, reason, unavailable_for_ms);
        self.dispatch(SlackCategory::Error, text);
    }

    pub fn notify_node_available(&self, node: &NodeInfo, unavailable_for_ms: i64) {
        let text = node_available_message(&self.inner.cluster_name, node, unavailable_for_ms);
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

fn node_label(node: &NodeInfo) -> String {
    format!("`{}` (`{}`)", node.hostname, node.node_id)
}

fn node_down_message(cluster_name: &str, node: &NodeInfo) -> String {
    format!(
        ":red_circle: *Node down* — {}\n> cluster: `{cluster_name}` · role: `{}` · address: `{}:{}`\n> control-plane heartbeat expired",
        node_label(node),
        node.role,
        node.cluster_host_ip,
        node.cluster_api_port,
    )
}

fn node_recovered_message(cluster_name: &str, node: &NodeInfo, unavailable_for_ms: i64) -> String {
    format!(
        ":large_green_circle: *Node recovered* — {}\n> cluster: `{cluster_name}` · role: `{}` · downtime: `{}`",
        node_label(node),
        node.role,
        format_duration(unavailable_for_ms),
    )
}

fn node_unavailable_message(
    cluster_name: &str,
    node: &NodeInfo,
    reason: &str,
    unavailable_for_ms: i64,
) -> String {
    format!(
        ":warning: *Node unavailable* — {}\n> cluster: `{cluster_name}` · role: `{}` · address: `{}:{}` · unavailable for: `{}`\n> reason: {reason}",
        node_label(node),
        node.role,
        node.cluster_host_ip,
        node.cluster_gateway_port,
        format_duration(unavailable_for_ms),
    )
}

fn node_available_message(cluster_name: &str, node: &NodeInfo, unavailable_for_ms: i64) -> String {
    format!(
        ":white_check_mark: *Node available* — {}\n> cluster: `{cluster_name}` · role: `{}` · unavailable for: `{}`",
        node_label(node),
        node.role,
        format_duration(unavailable_for_ms),
    )
}

fn format_duration(milliseconds: i64) -> String {
    let seconds = milliseconds.max(0) / 1_000;
    if seconds < 60 {
        return format!("{seconds}s");
    }
    let minutes = seconds / 60;
    let remaining_seconds = seconds % 60;
    if minutes < 60 {
        return format!("{minutes}m {remaining_seconds}s");
    }
    let hours = minutes / 60;
    let remaining_minutes = minutes % 60;
    format!("{hours}h {remaining_minutes}m")
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::cluster::types::NodeRole;

    fn node() -> NodeInfo {
        NodeInfo {
            node_id: "node00000001".to_string(),
            instance_id: "boot-1".to_string(),
            hostname: "worker-1".to_string(),
            role: NodeRole::Worker,
            cluster_host_ip: "10.20.0.11".parse().unwrap(),
            cluster_api_port: 3101,
            cluster_gateway_port: 3102,
            subnet: "172.22.1.0/24".to_string(),
            tailscale_ip: None,
            data_plane_ready: false,
            data_plane_checked_at_ms: 0,
            data_plane_error: None,
            version: "test".to_string(),
            started_at_ms: 1,
            labels: BTreeMap::new(),
        }
    }

    #[test]
    fn node_alert_messages_identify_the_cluster_node_and_failure() {
        let node = node();
        let down = node_down_message("staging", &node);
        assert!(down.contains("*Node down*"));
        assert!(down.contains("`staging`"));
        assert!(down.contains("`worker-1` (`node00000001`)"));
        assert!(down.contains("`10.20.0.11:3101`"));

        let unavailable = node_unavailable_message("staging", &node, "gateway timed out", 31_000);
        assert!(unavailable.contains("*Node unavailable*"));
        assert!(unavailable.contains("`10.20.0.11:3102`"));
        assert!(unavailable.contains("`31s`"));
        assert!(unavailable.contains("gateway timed out"));
    }

    #[test]
    fn recovery_messages_include_a_readable_incident_duration() {
        let node = node();
        assert!(node_recovered_message("prod", &node, 125_000).contains("`2m 5s`"));
        assert!(node_available_message("prod", &node, 7_440_000).contains("`2h 4m`"));
        assert_eq!(format_duration(-1), "0s");
    }
}
