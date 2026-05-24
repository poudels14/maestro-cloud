//! HTTP-driven [`NodeUpgrader`] that hits each node's existing
//! `/api/system/upgrade`. Drain/restore are implemented by toggling a node's
//! "drain" flag via the registry (drained nodes are marked unschedulable so
//! the scheduler skips them; restore clears the flag).
//!
//! Verification polls the node's `/_healthy` endpoint until it reports the
//! new version, with a timeout.

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use reqwest::Client as HttpClient;

use super::assignment_store::AssignmentStore;
use super::registry::NodeRegistry;
use super::types::NodeId;
use super::upgrade::NodeUpgrader;

pub struct HttpNodeUpgrader {
    pub registry: Arc<dyn NodeRegistry>,
    pub http_client: HttpClient,
    pub auth_token: Option<String>,
    pub verify_timeout: Duration,
    pub verify_poll_interval: Duration,
    /// When set, `drain` polls this until the node has zero assignments (or
    /// `drain_timeout` elapses) before returning. Without it, `drain` only
    /// flips the unschedulable flag — the orchestrator would proceed before
    /// replicas have actually been moved off the node.
    pub assignment_store: Option<Arc<dyn AssignmentStore>>,
    pub drain_timeout: Duration,
    pub drain_poll_interval: Duration,
}

impl HttpNodeUpgrader {
    pub fn new(registry: Arc<dyn NodeRegistry>, http_client: HttpClient) -> Self {
        Self {
            registry,
            http_client,
            auth_token: None,
            verify_timeout: Duration::from_secs(120),
            verify_poll_interval: Duration::from_secs(2),
            assignment_store: None,
            drain_timeout: Duration::from_secs(60),
            drain_poll_interval: Duration::from_secs(2),
        }
    }

    pub fn with_auth_token(mut self, token: impl Into<String>) -> Self {
        self.auth_token = Some(token.into());
        self
    }

    pub fn with_assignment_store(mut self, store: Arc<dyn AssignmentStore>) -> Self {
        self.assignment_store = Some(store);
        self
    }

    async fn node_base_url(&self, node_id: &NodeId) -> Result<String> {
        let node = self
            .registry
            .get_node(node_id)
            .await?
            .ok_or_else(|| anyhow!("node {node_id} not in registry"))?;
        let host = node
            .tailscale_ip
            .clone()
            .unwrap_or_else(|| node.hostname.clone());
        Ok(format!("http://{host}:{}", node.api_port))
    }

    fn add_auth(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match &self.auth_token {
            Some(token) => request.bearer_auth(token),
            None => request,
        }
    }
}

#[async_trait]
impl NodeUpgrader for HttpNodeUpgrader {
    async fn drain(&self, node_id: &NodeId) -> Result<()> {
        let url = format!("{}/api/cluster/drain", self.node_base_url(node_id).await?);
        let request = self.http_client.post(&url);
        let response = self
            .add_auth(request)
            .send()
            .await
            .map_err(|err| anyhow!("drain request to {node_id} failed: {err}"))?;
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(anyhow!(
                "drain request to {node_id} returned {status}: {body}"
            ));
        }
        if let Some(store) = self.assignment_store.as_ref() {
            let deadline = Instant::now() + self.drain_timeout;
            loop {
                let remaining = store.list_for_node(node_id).await.unwrap_or_default().len();
                if remaining == 0 {
                    return Ok(());
                }
                if Instant::now() >= deadline {
                    return Err(anyhow!(
                        "drain timeout: {node_id} still has {remaining} assignment(s) after {:?}",
                        self.drain_timeout
                    ));
                }
                tokio::time::sleep(self.drain_poll_interval).await;
            }
        }
        Ok(())
    }

    async fn upgrade(&self, node_id: &NodeId, _target_version: &str) -> Result<()> {
        let url = format!("{}/api/system/upgrade", self.node_base_url(node_id).await?);
        let request = self
            .http_client
            .post(&url)
            .json(&serde_json::json!({ "target": "system" }));
        let response = self
            .add_auth(request)
            .send()
            .await
            .map_err(|err| anyhow!("upgrade request to {node_id} failed: {err}"))?;
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(anyhow!(
                "upgrade request to {node_id} returned {status}: {body}"
            ));
        }
        Ok(())
    }

    async fn verify(&self, node_id: &NodeId, _target_version: &str) -> Result<()> {
        let base = self.node_base_url(node_id).await?;
        let healthy_url = format!("{base}/_healthy");
        let deadline = Instant::now() + self.verify_timeout;
        loop {
            let response = self.http_client.get(&healthy_url).send().await;
            if let Ok(response) = response {
                if response.status().is_success() {
                    return Ok(());
                }
            }
            if Instant::now() >= deadline {
                return Err(anyhow!("verify timeout for {node_id}"));
            }
            tokio::time::sleep(self.verify_poll_interval).await;
        }
    }

    async fn restore(&self, node_id: &NodeId) -> Result<()> {
        let url = format!("{}/api/cluster/restore", self.node_base_url(node_id).await?);
        let request = self.http_client.post(&url);
        let response = self
            .add_auth(request)
            .send()
            .await
            .map_err(|err| anyhow!("restore request to {node_id} failed: {err}"))?;
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(anyhow!(
                "restore request to {node_id} returned {status}: {body}"
            ));
        }
        Ok(())
    }
}
