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
    #[serde(flatten, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    attrs: std::collections::BTreeMap<String, String>,
}

#[async_trait]
impl LogSink for DatadogSink {
    fn id(&self) -> &str {
        "datadog"
    }

    fn advance_cursor_on_retry_exhaustion(&self) -> bool {
        true
    }

    async fn send(&self, entries: &[LogEntry]) -> Result<()> {
        let dd_entries: Vec<DatadogLogEntry> = entries
            .iter()
            .filter(|entry| self.should_send(entry))
            .map(|entry| {
                let (ddtags, service, hostname) = build_dd_tags(&entry.tags);
                let attrs = entry
                    .attrs
                    .iter()
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect();
                DatadogLogEntry {
                    message: entry.text.clone(),
                    hostname: hostname.unwrap_or_else(|| entry.source.to_string()),
                    service: service.unwrap_or_else(|| entry.source.to_string()),
                    ddsource: Some(dd_source(&entry.source).to_string()),
                    ddtags,
                    status: dd_status(&entry.level),
                    attrs,
                }
            })
            .collect();
        if dd_entries.is_empty() {
            return Ok(());
        }

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
            return handle_response_failure(status, body);
        }
        Ok(())
    }
}

fn handle_response_failure(status: reqwest::StatusCode, body: String) -> Result<()> {
    let is_retryable_client_status = matches!(status.as_u16(), 408 | 409 | 425 | 429);
    let is_non_retryable_status = status.is_client_error() && !is_retryable_client_status;
    if is_non_retryable_status {
        eprintln!(
            "[maestro]: dropping datadog log batch after non-retryable response {status}: {body}"
        );
        return Ok(());
    }
    anyhow::bail!("datadog log sink POST failed with {status}: {body}");
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

fn dd_source(source: &str) -> &'static str {
    match source {
        "maestro-ingress" => "traefik",
        _ => "maestro",
    }
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    fn system_log(source: &str) -> LogEntry {
        LogEntry {
            seq: 1,
            ts: 1_700_000_000_000,
            level: Arc::from("info"),
            stream: Arc::from("stderr"),
            text: "controller startup".to_string(),
            source: Arc::from(source),
            origin: LogOrigin::System,
            tags: Arc::new(serde_json::json!([])),
            attrs: vec![],
        }
    }

    #[tokio::test]
    async fn send_skips_http_request_when_all_entries_are_filtered_out() {
        let sink = DatadogSink::new("test-api-key".to_string(), "invalid.invalid", false, false);
        assert!(sink.advance_cursor_on_retry_exhaustion());

        sink.send(&[system_log("maestro-controller")])
            .await
            .expect("filtered batch should be treated as delivered");
    }

    #[test]
    fn non_retryable_datadog_response_is_treated_as_delivered() {
        handle_response_failure(reqwest::StatusCode::BAD_REQUEST, "bad payload".to_string())
            .expect("non-retryable datadog response should not pin the cursor");
    }

    #[test]
    fn retryable_datadog_response_is_returned_as_error() {
        let err = handle_response_failure(
            reqwest::StatusCode::TOO_MANY_REQUESTS,
            "slow down".to_string(),
        )
        .expect_err("retryable datadog response should remain an error");
        assert!(
            err.to_string().contains("429"),
            "error should include retryable status: {err}"
        );
    }
}
