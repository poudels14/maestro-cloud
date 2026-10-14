use std::io::Write;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

use crate::access_log::INGRESS_REPLICA;
use crate::error::Result;
use crate::resolver::ReplicaResolver;
use crate::store::{LogStore, SinkRecord};

pub const DATADOG_SINK: &str = "datadog";
const TAILSCALE_REPLICA: &str = "maestro-tailscale";
const BATCH_SIZE: usize = 200;
const FLUSH_INTERVAL: Duration = Duration::from_secs(5);
const SEND_ATTEMPTS: u32 = 5;
const INITIAL_BACKOFF: Duration = Duration::from_millis(500);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_PAYLOAD_BYTES: usize = 4_500_000;
const DEAD_LETTER_CAPACITY: usize = 100_000;
const MAX_ERROR_LENGTH: usize = 4096;
const METHOD_FIELDS: [&str; 4] = ["http.method", "method", "req.method", "request.method"];
const STATUS_FIELDS: [&str; 6] = [
    "http.status_code",
    "status_code",
    "statuscode",
    "status",
    "http.status",
    "response.status_code",
];
const PATH_FIELDS: [&str; 8] = [
    "http.url_details.path",
    "http.path",
    "path",
    "url",
    "route",
    "uri",
    "request.path",
    "target",
];

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DatadogConfig {
    pub api_key: String,
    pub logs_url: String,
    pub metrics_url: String,
    pub tags: Vec<String>,
    pub cluster: String,
    pub node: String,
    pub include_ingress_logs: bool,
    pub include_tailscale_logs: bool,
    pub include_healthcheck: bool,
    pub include_metrics: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SinkStats {
    pub id: String,
    pub cursor: i64,
    pub pending_entries: i64,
    pub last_success_at_ms: Option<i64>,
    pub last_error_at_ms: Option<i64>,
    pub last_error: Option<String>,
    pub consecutive_failures: u32,
    pub dead_letters: i64,
    pub dead_letter_capacity: usize,
}

pub struct DatadogLogSink {
    config: DatadogConfig,
    store: Arc<LogStore>,
    resolver: Arc<dyn ReplicaResolver>,
    http: reqwest::Client,
    stats: Arc<Mutex<SinkStats>>,
}

enum Delivery {
    Accepted,
    Rejected { status: u16, body: String },
    Failed(String),
}

impl DatadogConfig {
    pub fn site_urls(site: &str) -> (String, String) {
        (
            format!("https://http-intake.logs.{site}/api/v2/logs"),
            format!("https://api.{site}/api/v2/series"),
        )
    }
}

impl DatadogLogSink {
    pub fn new(
        config: DatadogConfig,
        store: Arc<LogStore>,
        resolver: Arc<dyn ReplicaResolver>,
        stats: Arc<Mutex<SinkStats>>,
    ) -> Self {
        Self {
            config,
            store,
            resolver,
            http: reqwest::Client::new(),
            stats,
        }
    }

    pub async fn run(self) -> Result<()> {
        let mut cursor = match self.store.sink_cursor(DATADOG_SINK)? {
            Some(cursor) => cursor,
            None => {
                let start = self.store.latest_seq()?;
                self.store.set_sink_cursor(DATADOG_SINK, start)?;
                start
            }
        };
        let mut ticker = tokio::time::interval(FLUSH_INTERVAL);
        loop {
            ticker.tick().await;
            let mut draining = true;
            while draining {
                let records = self.store.records_after(cursor, BATCH_SIZE).await?;
                draining = match records.last().map(|record| record.seq) {
                    None => false,
                    Some(last) => {
                        let entries = records
                            .iter()
                            .filter(|record| self.should_send(record))
                            .map(|record| (record.seq, self.entry(record)))
                            .collect::<Vec<_>>();
                        match self.deliver(&entries).await {
                            Ok(()) => {
                                cursor = last;
                                self.store.set_sink_cursor(DATADOG_SINK, cursor)?;
                                self.record_success(cursor);
                                records.len() == BATCH_SIZE
                            }
                            Err(error) => {
                                self.record_failure(cursor, error);
                                false
                            }
                        }
                    }
                };
            }
            self.update_pending(cursor);
        }
    }

    fn should_send(&self, record: &SinkRecord) -> bool {
        let wanted = match (&record.service_id, record.replica_id.as_str()) {
            (Some(_), _) => true,
            (None, INGRESS_REPLICA) => self.config.include_ingress_logs,
            (None, TAILSCALE_REPLICA) => self.config.include_tailscale_logs,
            (None, _) => false,
        };
        let healthcheck_path = self.resolver.resolve(&record.replica_id).healthcheck_path;
        let drop_healthcheck = !self.config.include_healthcheck
            && record.service_id.is_some()
            && healthcheck_path
                .as_deref()
                .is_some_and(|path| is_healthcheck(&record.attributes, path));
        wanted && !drop_healthcheck
    }

    fn entry(&self, record: &SinkRecord) -> Value {
        let service = record
            .service_id
            .clone()
            .unwrap_or_else(|| record.replica_id.clone());
        let mut tags = self.config.tags.clone();
        tags.extend([
            format!("cluster:{}", self.config.cluster),
            format!("node:{}", self.config.node),
            format!("replica:{}", record.replica_id),
        ]);
        tags.extend(
            record
                .deployment_id
                .iter()
                .map(|id| format!("deployment_id:{id}")),
        );
        let mut entry = serde_json::from_str::<Map<String, Value>>(&record.attributes)
            .unwrap_or_default()
            .into_iter()
            .map(|(key, value)| match value {
                Value::String(text) => (key, Value::String(text)),
                other => (key, Value::String(other.to_string())),
            })
            .collect::<Map<_, _>>();
        entry.extend([
            ("message".to_string(), json!(record.line)),
            ("hostname".to_string(), json!(record.replica_id)),
            ("service".to_string(), json!(service)),
            (
                "ddsource".to_string(),
                json!(match record.replica_id.as_str() {
                    INGRESS_REPLICA => "traefik",
                    _ => "maestro",
                }),
            ),
            ("ddtags".to_string(), json!(tags.join(","))),
            ("status".to_string(), json!(status(&record.level))),
            ("timestamp".to_string(), json!(record.ts)),
        ]);
        Value::Object(entry)
    }

    async fn deliver(&self, entries: &[(i64, Value)]) -> std::result::Result<(), String> {
        match entries.is_empty() {
            true => Ok(()),
            false => {
                let mut pending = vec![entries.to_vec()];
                let mut accepted_any = false;
                let mut rejected = Vec::new();
                while let Some(batch) = pending.pop() {
                    let body = serde_json::to_vec(
                        &batch.iter().map(|(_, entry)| entry).collect::<Vec<_>>(),
                    )
                    .map_err(|error| error.to_string())?;
                    let delivery = match body.len() > MAX_PAYLOAD_BYTES && batch.len() > 1 {
                        true => Delivery::Rejected {
                            status: 413,
                            body: "payload too large".to_string(),
                        },
                        false => self.send_with_retries(&body).await,
                    };
                    match delivery {
                        Delivery::Accepted => accepted_any = true,
                        Delivery::Rejected { .. } if batch.len() > 1 => {
                            let (left, right) = batch.split_at(batch.len() / 2);
                            pending.push(right.to_vec());
                            pending.push(left.to_vec());
                        }
                        Delivery::Rejected { status, body } => {
                            rejected.push((batch[0].clone(), status, body));
                        }
                        Delivery::Failed(error) => Err(error)?,
                    }
                }
                let quarantine =
                    accepted_any || rejected.iter().all(|(_, status, _)| *status == 413);
                match (rejected.is_empty(), quarantine) {
                    (true, _) => Ok(()),
                    (false, false) => Err("datadog rejected every entry in the batch".to_string()),
                    (false, true) => {
                        rejected
                            .into_iter()
                            .try_for_each(|((seq, entry), status, body)| {
                                self.store
                                    .dead_letter(
                                        DATADOG_SINK,
                                        seq,
                                        status,
                                        &body.chars().take(MAX_ERROR_LENGTH).collect::<String>(),
                                        &entry.to_string(),
                                        DEAD_LETTER_CAPACITY,
                                    )
                                    .map_err(|error| error.to_string())
                            })
                    }
                }
            }
        }
    }

    async fn send_with_retries(&self, body: &[u8]) -> Delivery {
        let mut backoff = INITIAL_BACKOFF;
        let mut attempt = 1;
        loop {
            let delivery = self.send(body).await;
            match (&delivery, attempt < SEND_ATTEMPTS) {
                (Delivery::Failed(_), true) => {
                    tokio::time::sleep(backoff).await;
                    backoff *= 2;
                    attempt += 1;
                }
                _ => break delivery,
            }
        }
    }

    async fn send(&self, body: &[u8]) -> Delivery {
        let compressed = gzip(body);
        let response = self
            .http
            .post(&self.config.logs_url)
            .timeout(REQUEST_TIMEOUT)
            .header("DD-API-KEY", &self.config.api_key)
            .header("Content-Type", "application/json")
            .header("Content-Encoding", "gzip")
            .body(compressed)
            .send()
            .await;
        match response {
            Err(error) => Delivery::Failed(error.without_url().to_string()),
            Ok(response) if response.status().is_success() => Delivery::Accepted,
            Ok(response) => {
                let status = response.status().as_u16();
                let body = response.text().await.unwrap_or_default();
                match status {
                    400 | 413 => Delivery::Rejected { status, body },
                    _ => Delivery::Failed(format!("datadog returned {status}: {body}")),
                }
            }
        }
    }

    fn record_success(&self, cursor: i64) {
        let mut stats = lock(&self.stats);
        stats.cursor = cursor;
        stats.last_success_at_ms = Some(now_ms());
        stats.consecutive_failures = 0;
        stats.dead_letters = self
            .store
            .dead_letter_count(DATADOG_SINK)
            .unwrap_or_default();
    }

    fn record_failure(&self, cursor: i64, error: String) {
        tracing::warn!(%error, "datadog log delivery failed");
        let mut stats = lock(&self.stats);
        stats.cursor = cursor;
        stats.last_error_at_ms = Some(now_ms());
        stats.last_error = Some(error);
        stats.consecutive_failures += 1;
    }

    fn update_pending(&self, cursor: i64) {
        let latest = self.store.latest_seq().unwrap_or(cursor);
        lock(&self.stats).pending_entries = (latest - cursor).max(0);
    }
}

pub async fn run_datadog_logs(
    config: DatadogConfig,
    store: Arc<LogStore>,
    resolver: Arc<dyn ReplicaResolver>,
) -> Result<()> {
    DatadogLogSink::new(config, store, resolver, new_stats())
        .run()
        .await
}

pub fn new_stats() -> Arc<Mutex<SinkStats>> {
    Arc::new(Mutex::new(SinkStats {
        id: DATADOG_SINK.to_string(),
        dead_letter_capacity: DEAD_LETTER_CAPACITY,
        ..SinkStats::default()
    }))
}

fn is_healthcheck(attributes: &str, healthcheck_path: &str) -> bool {
    let attributes = serde_json::from_str::<Value>(attributes).unwrap_or_default();
    let field = |names: &[&str]| {
        names.iter().find_map(|name| {
            let value = name
                .split('.')
                .try_fold(&attributes, |value, part| value.get(part))
                .or_else(|| attributes.get(*name))?;
            match value {
                Value::String(text) => Some(text.clone()),
                Value::Number(number) => Some(number.to_string()),
                _ => None,
            }
        })
    };
    let normalize = |path: &str| {
        let path = path.split('?').next().unwrap_or(path);
        path.trim_end_matches('/').to_string()
    };
    let method = field(&METHOD_FIELDS).is_some_and(|method| method.eq_ignore_ascii_case("GET"));
    let status = field(&STATUS_FIELDS).is_some_and(|status| status == "200");
    let path =
        field(&PATH_FIELDS).is_some_and(|path| normalize(&path) == normalize(healthcheck_path));
    method && status && path
}

fn status(level: &str) -> &'static str {
    match level.to_ascii_lowercase().as_str() {
        "error" | "fatal" | "critical" => "error",
        "warn" | "warning" => "warn",
        "debug" | "trace" => "debug",
        _ => "info",
    }
}

fn gzip(body: &[u8]) -> Vec<u8> {
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    let _ = encoder.write_all(body);
    encoder.finish().unwrap_or_default()
}

fn lock(stats: &Mutex<SinkStats>) -> std::sync::MutexGuard<'_, SinkStats> {
    stats
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_millis() as i64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_successful_get_health_checks() {
        assert!(is_healthcheck(
            r#"{"method":"GET","status":200,"path":"/health/?probe=1"}"#,
            "/health"
        ));
        assert!(is_healthcheck(
            r#"{"http":{"method":"get","status_code":"200","path":"/health"}}"#,
            "/health"
        ));
        assert!(!is_healthcheck(
            r#"{"method":"GET","status":500,"path":"/health"}"#,
            "/health"
        ));
        assert!(!is_healthcheck(
            r#"{"method":"POST","status":200,"path":"/health"}"#,
            "/health"
        ));
        assert!(!is_healthcheck("not json", "/health"));
    }

    #[test]
    fn maps_levels_to_datadog_statuses() {
        assert_eq!(status("ERROR"), "error");
        assert_eq!(status("warning"), "warn");
        assert_eq!(status("trace"), "debug");
        assert_eq!(status("notice"), "info");
    }
}
