use std::io::Write;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use flate2::{Compression, write::GzEncoder};
use reqwest::header::{CONTENT_ENCODING, CONTENT_TYPE};
use serde::Serialize;

use super::sink::LogSink;
use super::store::{LogEntry, LogOrigin};

pub struct DatadogSink {
    api_key: String,
    endpoint: String,
    include_ingress_logs: bool,
    include_tailscale_logs: bool,
    client: reqwest::Client,
}

impl DatadogSink {
    pub fn new(
        api_key: String,
        site: &str,
        include_ingress_logs: bool,
        include_tailscale_logs: bool,
    ) -> Self {
        let endpoint = format!("https://http-intake.logs.{site}/api/v2/logs");
        Self {
            api_key,
            endpoint,
            include_ingress_logs,
            include_tailscale_logs,
            client: reqwest::Client::builder()
                .http1_only()
                .timeout(Duration::from_secs(10))
                .build()
                .expect("failed to build http client"),
        }
    }

    fn should_send(&self, entry: &LogEntry) -> bool {
        match entry.origin {
            LogOrigin::Service => true,
            LogOrigin::Build => false,
            LogOrigin::System => {
                (self.include_ingress_logs && entry.source.as_ref() == "maestro-ingress")
                    || (self.include_tailscale_logs && entry.source.as_ref() == "maestro-tailscale")
            }
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
            .filter(|entry| self.should_send(entry))
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
        let compressed_body = gzip_json(&dd_entries)?;

        let response = self
            .client
            .post(&self.endpoint)
            .header("DD-API-KEY", &self.api_key)
            .header(CONTENT_TYPE, "application/json")
            .header(CONTENT_ENCODING, "gzip")
            .body(compressed_body)
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

fn gzip_json<T: Serialize + ?Sized>(value: &T) -> Result<Vec<u8>> {
    let payload = serde_json::to_vec(value)?;
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(&payload)?;
    let compressed = encoder
        .finish()
        .map_err(|err| anyhow::anyhow!("failed to finish gzip stream: {err}"))?;
    Ok(compressed)
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
