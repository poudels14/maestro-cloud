use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;

use super::sink::LogSink;
use super::store::LogEntry;

pub struct DatadogSink {
    api_key: String,
    endpoint: String,
    include_system_logs: bool,
    client: reqwest::Client,
}

impl DatadogSink {
    pub fn new(api_key: String, site: &str, include_system_logs: bool) -> Self {
        let endpoint = format!("https://http-intake.logs.{site}/api/v2/logs");
        Self {
            api_key,
            endpoint,
            include_system_logs,
            client: reqwest::Client::builder()
                .http1_only()
                .timeout(Duration::from_secs(10))
                .build()
                .expect("failed to build http client"),
        }
    }
}

#[derive(serde::Serialize)]
struct DatadogLogEntry {
    message: String,
    hostname: String,
    service: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    ddsource: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    ddtags: Option<String>,
    status: String,
}

#[async_trait]
impl LogSink for DatadogSink {
    fn id(&self) -> &str {
        "datadog"
    }

    async fn send(&self, entries: &[LogEntry]) -> Result<()> {
        let dd_entries: Vec<DatadogLogEntry> = entries
            .iter()
            .filter(|entry| self.include_system_logs || !entry.system)
            .map(|entry| {
                let (ddtags, service, hostname) = build_dd_tags(&entry.tags);
                DatadogLogEntry {
                    message: entry.text.clone(),
                    hostname: hostname.unwrap_or_else(|| entry.source.to_string()),
                    service: service.unwrap_or_else(|| entry.source.to_string()),
                    ddsource: Some("maestro".to_string()),
                    ddtags,
                    status: dd_status(&entry.level),
                }
            })
            .collect();

        let response = self
            .client
            .post(&self.endpoint)
            .header("DD-API-KEY", &self.api_key)
            .header("Content-Type", "application/json")
            .json(&dd_entries)
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            anyhow::bail!("datadog log sink POST failed with {status}: {body}");
        }
        Ok(())
    }
}

fn build_dd_tags(
    tags: &Arc<serde_json::Value>,
) -> (Option<String>, Option<String>, Option<String>) {
    let mut service = None;
    let mut hostname = None;
    let mut tag_parts = Vec::new();

    if let serde_json::Value::Array(arr) = tags.as_ref() {
        for val in arr {
            if let serde_json::Value::String(tag) = val {
                if let Some(svc) = tag.strip_prefix("service:") {
                    service = Some(svc.to_string());
                } else if let Some(h) = tag.strip_prefix("hostname:") {
                    hostname = Some(h.to_string());
                } else {
                    tag_parts.push(tag.as_str());
                }
            }
        }
    }

    let ddtags = if tag_parts.is_empty() {
        None
    } else {
        Some(tag_parts.join(","))
    };

    (ddtags, service, hostname)
}

fn dd_status(level: &str) -> String {
    match level {
        "error" => "error",
        "warn" => "warn",
        "debug" => "debug",
        "trace" => "debug",
        _ => "info",
    }
    .to_string()
}
