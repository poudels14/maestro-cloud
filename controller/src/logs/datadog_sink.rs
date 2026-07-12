use std::collections::VecDeque;
use std::io::Write;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, bail};
use async_trait::async_trait;
use flate2::{Compression, write::GzEncoder};
use reqwest::header::{CONTENT_ENCODING, CONTENT_TYPE};

use super::filter::{LogFilterSet, is_internal_tag};
use super::sink::{LogSink, SinkSendOutcome};
use super::store::{LogEntry, LogOrigin, LogStore};

const MAX_DATADOG_UNCOMPRESSED_BYTES: usize = 4_500_000;
const MAX_DATADOG_ERROR_BODY_CHARS: usize = 4_096;

pub struct DatadogSink {
    api_key: String,
    endpoint: String,
    include_ingress_logs: bool,
    include_tailscale_logs: bool,
    filters: LogFilterSet,
    dead_letter_store: Arc<LogStore>,
    client: reqwest::Client,
}

impl DatadogSink {
    pub fn new(
        api_key: String,
        site: &str,
        include_ingress_logs: bool,
        include_tailscale_logs: bool,
        filter_healthcheck: bool,
        dead_letter_store: Arc<LogStore>,
    ) -> Self {
        let endpoint = format!("https://http-intake.logs.{site}/api/v2/logs");
        Self::with_endpoint(
            api_key,
            endpoint,
            include_ingress_logs,
            include_tailscale_logs,
            filter_healthcheck,
            dead_letter_store,
        )
    }

    fn with_endpoint(
        api_key: String,
        endpoint: String,
        include_ingress_logs: bool,
        include_tailscale_logs: bool,
        filter_healthcheck: bool,
        dead_letter_store: Arc<LogStore>,
    ) -> Self {
        let filters = if filter_healthcheck {
            LogFilterSet::excluding_successful_healthchecks()
        } else {
            LogFilterSet::default()
        };
        Self {
            api_key,
            endpoint,
            include_ingress_logs,
            include_tailscale_logs,
            filters,
            dead_letter_store,
            client: reqwest::Client::builder()
                .http1_only()
                .timeout(Duration::from_secs(10))
                .build()
                .expect("failed to build http client"),
        }
    }

    fn should_send(&self, entry: &LogEntry) -> bool {
        let included_origin = match entry.origin {
            LogOrigin::Service => true,
            LogOrigin::Build => false,
            LogOrigin::System => {
                (self.include_ingress_logs && entry.source.as_ref() == "maestro-ingress")
                    || (self.include_tailscale_logs && entry.source.as_ref() == "maestro-tailscale")
            }
        };
        included_origin && !self.filters.excludes(entry)
    }

    fn prepare(&self, entry: &LogEntry) -> PreparedDatadogEntry {
        let (ddtags, service, hostname) = build_dd_tags(&entry.tags);
        let attrs = entry
            .attrs
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        PreparedDatadogEntry {
            seq: entry.seq,
            payload: DatadogLogEntry {
                message: entry.text.clone(),
                hostname: hostname.unwrap_or_else(|| entry.source.to_string()),
                service: service.unwrap_or_else(|| entry.source.to_string()),
                ddsource: Some(dd_source(&entry.source).to_string()),
                ddtags,
                status: dd_status(&entry.level),
                attrs,
            },
        }
    }

    async fn post_payload(&self, payload: &[u8]) -> Result<Option<(reqwest::StatusCode, String)>> {
        let compressed_body = gzip_bytes(payload)?;
        let response = self
            .client
            .post(&self.endpoint)
            .header("DD-API-KEY", &self.api_key)
            .header(CONTENT_TYPE, "application/json")
            .header(CONTENT_ENCODING, "gzip")
            .body(compressed_body)
            .send()
            .await?;
        if response.status().is_success() {
            return Ok(None);
        }
        let status = response.status();
        let body = response
            .text()
            .await
            .unwrap_or_default()
            .chars()
            .take(MAX_DATADOG_ERROR_BODY_CHARS)
            .collect();
        Ok(Some((status, body)))
    }

    async fn quarantine(
        &self,
        entry: &PreparedDatadogEntry,
        status: reqwest::StatusCode,
        body: &str,
        payload: &[u8],
    ) -> Result<()> {
        self.dead_letter_store
            .record_sink_dead_letter("datadog", entry.seq, status.as_u16(), body, payload)
            .await?;
        eprintln!(
            "[maestro]: quarantined Datadog poison log seq={} after permanent response {}: {}",
            entry.seq, status, body
        );
        Ok(())
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

struct PreparedDatadogEntry {
    seq: i64,
    payload: DatadogLogEntry,
}

#[async_trait]
impl LogSink for DatadogSink {
    fn id(&self) -> &str {
        "datadog"
    }

    async fn send(&self, entries: &[LogEntry]) -> Result<SinkSendOutcome> {
        let filtered_entries = entries
            .iter()
            .filter(|entry| self.filters.excludes(entry))
            .count()
            .try_into()
            .unwrap_or(u64::MAX);
        let dd_entries: Vec<PreparedDatadogEntry> = entries
            .iter()
            .filter(|entry| self.should_send(entry))
            .map(|entry| self.prepare(entry))
            .collect();
        if dd_entries.is_empty() {
            return Ok(SinkSendOutcome { filtered_entries });
        }

        let mut sizing = VecDeque::from([(0, dd_entries.len())]);
        let mut roots = Vec::new();
        while let Some((start, end)) = sizing.pop_front() {
            let payload = serialize_payload(&dd_entries[start..end])?;
            if payload.len() > MAX_DATADOG_UNCOMPRESSED_BYTES && end - start > 1 {
                push_split(&mut sizing, start, end);
                continue;
            }
            roots.push((start, end));
        }

        for (root_start, root_end) in roots {
            let mut pending = VecDeque::from([(root_start, root_end)]);
            let mut accepted_any = false;
            let mut bad_request_candidates = Vec::new();
            while let Some((start, end)) = pending.pop_front() {
                let payload = serialize_payload(&dd_entries[start..end])?;
                let Some((status, body)) = self.post_payload(&payload).await? else {
                    accepted_any = true;
                    continue;
                };
                match status.as_u16() {
                    400 if end - start > 1 => push_split(&mut pending, start, end),
                    400 => bad_request_candidates.push((start, body, payload)),
                    413 if end - start > 1 => push_split(&mut pending, start, end),
                    413 => {
                        self.quarantine(&dd_entries[start], status, &body, &payload)
                            .await?;
                    }
                    _ => bail!("datadog log sink POST failed with {status}: {body}"),
                }
            }

            if !bad_request_candidates.is_empty() && !accepted_any {
                bail!(
                    "datadog rejected every isolated payload with 400; treating this as a global failure and retaining the batch"
                );
            }
            for (index, body, payload) in bad_request_candidates {
                self.quarantine(
                    &dd_entries[index],
                    reqwest::StatusCode::BAD_REQUEST,
                    &body,
                    &payload,
                )
                .await?;
            }
        }
        Ok(SinkSendOutcome { filtered_entries })
    }
}

fn push_split(pending: &mut VecDeque<(usize, usize)>, start: usize, end: usize) {
    let middle = start + (end - start) / 2;
    pending.push_front((middle, end));
    pending.push_front((start, middle));
}

fn serialize_payload(entries: &[PreparedDatadogEntry]) -> Result<Vec<u8>> {
    Ok(serde_json::to_vec(
        &entries
            .iter()
            .map(|entry| &entry.payload)
            .collect::<Vec<_>>(),
    )?)
}

fn gzip_bytes(payload: &[u8]) -> Result<Vec<u8>> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(payload)?;
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
                } else if !is_internal_tag(tag) {
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
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use axum::{Router, http::StatusCode, routing::post};

    fn temp_store(label: &str) -> (Arc<LogStore>, PathBuf) {
        let path = std::env::temp_dir().join(format!(
            "maestro-datadog-{label}-{}-{}.sqlite",
            std::process::id(),
            crate::utils::nanoid::unique_id(8)
        ));
        (Arc::new(LogStore::open(&path).expect("log store")), path)
    }

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

    fn service_log(seq: i64, text: String) -> LogEntry {
        LogEntry {
            seq,
            ts: 1_700_000_000_000,
            level: Arc::from("info"),
            stream: Arc::from("stdout"),
            text,
            source: Arc::from("api/dep/replica0"),
            origin: LogOrigin::Service,
            tags: Arc::new(serde_json::json!(["service:api", "hostname:node-a"])),
            attrs: vec![],
        }
    }

    fn successful_healthcheck_log(seq: i64) -> LogEntry {
        let mut entry = service_log(seq, "request".into());
        entry.tags = Arc::new(serde_json::json!([
            "service:api",
            crate::logs::healthcheck_path_tag("/health")
        ]));
        entry.attrs = vec![
            ("http.method".into(), "GET".into()),
            ("http.status_code".into(), "200".into()),
            ("http.url_details.path".into(), "/health".into()),
        ];
        entry
    }

    async fn test_endpoint<F>(response: F) -> (String, Arc<AtomicUsize>)
    where
        F: Fn(usize) -> StatusCode + Clone + Send + Sync + 'static,
    {
        let calls = Arc::new(AtomicUsize::new(0));
        let handler_calls = calls.clone();
        let app = Router::new().route(
            "/",
            post(move || {
                let call = handler_calls.fetch_add(1, Ordering::SeqCst);
                let status = response.clone()(call);
                async move { (status, "test response") }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr().expect("address");
        tokio::spawn(async move {
            axum::serve(listener, app).await.ok();
        });
        (format!("http://{address}"), calls)
    }

    fn sink(store: Arc<LogStore>, endpoint: String) -> DatadogSink {
        DatadogSink::with_endpoint("test-api-key".into(), endpoint, false, false, false, store)
    }

    #[tokio::test]
    async fn send_skips_http_request_when_all_entries_are_filtered_out() {
        let (store, path) = temp_store("filtered");
        let sink = sink(store, "http://127.0.0.1:1".into());
        sink.send(&[system_log("maestro-controller")])
            .await
            .expect("filtered batch should be treated as delivered");
        drop(sink);
        std::fs::remove_file(path).ok();
    }

    #[tokio::test]
    async fn configured_healthcheck_filter_skips_only_successful_healthchecks() {
        let (store, path) = temp_store("healthcheck-filter");
        let (endpoint, calls) = test_endpoint(|_| StatusCode::ACCEPTED).await;
        let sink =
            DatadogSink::with_endpoint("test-api-key".into(), endpoint, false, false, true, store);

        let outcome = sink
            .send(&[successful_healthcheck_log(1)])
            .await
            .expect("successful healthcheck should be filtered");
        assert_eq!(outcome.filtered_entries, 1);
        assert_eq!(calls.load(Ordering::SeqCst), 0);

        let mut failed = successful_healthcheck_log(2);
        failed.attrs[1].1 = "503".into();
        let outcome = sink
            .send(&[failed])
            .await
            .expect("failed healthcheck should be sent");
        assert_eq!(outcome.filtered_entries, 0);
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        drop(sink);
        std::fs::remove_file(path).ok();
    }

    #[tokio::test]
    async fn payloads_are_split_before_exceeding_the_uncompressed_limit() {
        let (store, path) = temp_store("sized");
        let (endpoint, calls) = test_endpoint(|_| StatusCode::ACCEPTED).await;
        let sink = sink(store, endpoint);
        sink.send(&[
            service_log(1, "a".repeat(2_500_000)),
            service_log(2, "b".repeat(2_500_000)),
        ])
        .await
        .expect("size-aware send");
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        drop(sink);
        std::fs::remove_file(path).ok();
    }

    #[tokio::test]
    async fn payload_too_large_response_is_bisected_without_dropping_entries() {
        let (store, path) = temp_store("413-split");
        let dead_letters = store.clone();
        let (endpoint, calls) = test_endpoint(|call| {
            if call == 0 {
                StatusCode::PAYLOAD_TOO_LARGE
            } else {
                StatusCode::ACCEPTED
            }
        })
        .await;
        let sink = sink(store, endpoint);
        sink.send(&[
            service_log(1, "first".into()),
            service_log(2, "second".into()),
        ])
        .await
        .expect("bisected send");
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        assert_eq!(
            dead_letters
                .sink_dead_letter_count("datadog")
                .await
                .expect("dead letters"),
            0
        );
        drop(sink);
        drop(dead_letters);
        std::fs::remove_file(path).ok();
    }

    #[tokio::test]
    async fn payload_specific_bad_request_is_durably_quarantined() {
        let (store, path) = temp_store("400-poison");
        let dead_letters = store.clone();
        let (endpoint, calls) = test_endpoint(|call| match call {
            0 | 1 => StatusCode::BAD_REQUEST,
            _ => StatusCode::ACCEPTED,
        })
        .await;
        let sink = sink(store, endpoint);
        sink.send(&[
            service_log(11, "first".into()),
            service_log(12, "second".into()),
        ])
        .await
        .expect("isolated poison entry is quarantined");
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        assert_eq!(
            dead_letters
                .sink_dead_letter_count("datadog")
                .await
                .expect("dead letters"),
            1
        );
        drop(sink);
        drop(dead_letters);
        std::fs::remove_file(path).ok();
    }

    #[tokio::test]
    async fn global_bad_request_pins_the_sink_without_quarantine() {
        let (store, path) = temp_store("400-global");
        let dead_letters = store.clone();
        let (endpoint, calls) = test_endpoint(|_| StatusCode::BAD_REQUEST).await;
        let sink = sink(store, endpoint);
        let error = sink
            .send(&[
                service_log(11, "first".into()),
                service_log(12, "second".into()),
            ])
            .await
            .expect_err("global rejection must pin the cursor");
        assert!(error.to_string().contains("global failure"));
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        assert_eq!(
            dead_letters
                .sink_dead_letter_count("datadog")
                .await
                .expect("dead letters"),
            0
        );
        drop(sink);
        drop(dead_letters);
        std::fs::remove_file(path).ok();
    }

    #[tokio::test]
    async fn operational_failures_pin_the_sink_without_quarantine() {
        for status in [
            StatusCode::FORBIDDEN,
            StatusCode::TOO_MANY_REQUESTS,
            StatusCode::SERVICE_UNAVAILABLE,
        ] {
            let (store, path) = temp_store(&format!("{}-failure", status.as_u16()));
            let dead_letters = store.clone();
            let (endpoint, calls) = test_endpoint(move |_| status).await;
            let sink = sink(store, endpoint);
            let error = sink
                .send(&[service_log(1, "operational failure".into())])
                .await
                .expect_err("operational failure must pin the cursor");
            assert!(error.to_string().contains(&status.as_u16().to_string()));
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            assert_eq!(
                dead_letters
                    .sink_dead_letter_count("datadog")
                    .await
                    .expect("dead letters"),
                0
            );
            drop(sink);
            drop(dead_letters);
            std::fs::remove_file(path).ok();
        }
    }
}
