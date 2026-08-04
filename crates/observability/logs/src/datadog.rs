use std::collections::{BTreeMap, VecDeque};
use std::io::Write;
use std::sync::Arc;

use async_trait::async_trait;
use flate2::{Compression, write::GzEncoder};
use serde::Serialize;
use url::{Host, Url};

use crate::{
    DeadLetterStore, DeadLetterStoreError, HttpRequest, HttpTransport, IngestLogEntry, LogBody,
    LogFilterChain, LogFilterKind, LogOrigin, LogSequence, LogSink, LogSinkError, LogSinkId,
    LogSinkOutcome, SequencedLogEntry, SinkDeadLetter,
};

const MAX_UNCOMPRESSED_BYTES: usize = 4_500_000;
const INGRESS_COMPONENTS: &[&str] = &["ingress", "maestro-ingress"];
const TAILSCALE_COMPONENTS: &[&str] = &["tailscale", "tailscaled", "maestro-tailscale"];

/// Validated Datadog Logs intake configuration.
///
/// This type intentionally omits `Debug` so API credentials cannot be formatted accidentally.
pub struct DatadogLogSinkSettings {
    api_key: String,
    endpoint: String,
    ingress_logs: LogSourceInclusion,
    tailscale_logs: LogSourceInclusion,
    filters: Vec<LogFilterKind>,
}

/// Delivery policy for an optional Datadog log source.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogSourceInclusion {
    /// Filter records from the source before delivery.
    Exclude,
    /// Deliver records from the source.
    Include,
}

impl DatadogLogSinkSettings {
    /// Builds the official intake endpoint for a validated Datadog site.
    pub fn new(
        api_key: impl Into<String>,
        site: impl Into<String>,
    ) -> Result<Self, DatadogLogSinkSettingsError> {
        let site = site.into();
        if !valid_datadog_site(&site) {
            return Err(DatadogLogSinkSettingsError::InvalidSite);
        }
        Self::with_endpoint(
            api_key,
            format!("https://http-intake.logs.{site}/api/v2/logs"),
        )
    }

    pub(crate) fn with_endpoint(
        api_key: impl Into<String>,
        endpoint: String,
    ) -> Result<Self, DatadogLogSinkSettingsError> {
        let api_key = api_key.into();
        if api_key.is_empty() || api_key.len() > 1_024 {
            return Err(DatadogLogSinkSettingsError::InvalidApiKey);
        }
        if !valid_http_endpoint(&endpoint) {
            return Err(DatadogLogSinkSettingsError::InvalidEndpoint);
        }
        Ok(Self {
            api_key,
            endpoint,
            ingress_logs: LogSourceInclusion::Exclude,
            tailscale_logs: LogSourceInclusion::Exclude,
            filters: Vec::new(),
        })
    }

    /// Configures delivery of ingress system-service records and their access analytics.
    pub fn ingress_logs(mut self, inclusion: LogSourceInclusion) -> Self {
        self.ingress_logs = inclusion;
        self
    }

    /// Configures delivery of useful Tailscale records that survive source noise filtering.
    pub fn tailscale_logs(mut self, inclusion: LogSourceInclusion) -> Self {
        self.tailscale_logs = inclusion;
        self
    }

    /// Applies an ordered declarative per-sink filter chain.
    pub fn filters(mut self, filters: Vec<LogFilterKind>) -> Self {
        self.filters = filters;
        self
    }
}

/// Datadog sink configuration was unsafe or ambiguous.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum DatadogLogSinkSettingsError {
    /// API key was empty or unreasonably large.
    #[error("Datadog API key must be 1-1024 bytes")]
    InvalidApiKey,
    /// Site was empty, oversized, or contained URL syntax.
    #[error("Datadog site must be a 1-255 character DNS name")]
    InvalidSite,
    /// Test or internal endpoint was not absolute HTTP(S).
    #[error("Datadog endpoint must be an absolute HTTP(S) URL")]
    InvalidEndpoint,
}

/// Datadog Logs sink with size-aware bisection and durable poison quarantine.
pub struct DatadogLogSink {
    id: LogSinkId,
    settings: DatadogLogSinkSettings,
    filters: LogFilterChain,
    transport: Arc<dyn HttpTransport>,
    dead_letters: Arc<dyn DeadLetterStore>,
}

impl DatadogLogSink {
    /// Constructs a sink over injected HTTP and dead-letter boundaries.
    pub fn new(
        settings: DatadogLogSinkSettings,
        transport: Arc<dyn HttpTransport>,
        dead_letters: Arc<dyn DeadLetterStore>,
    ) -> Self {
        let filters = LogFilterChain::configured(settings.filters.iter().copied());
        Self {
            id: LogSinkId::built_in("datadog"),
            settings,
            filters,
            transport,
            dead_letters,
        }
    }

    fn should_send(&self, entry: &IngestLogEntry) -> bool {
        if self.filters.dropped_by(entry).is_some() {
            return false;
        }
        match &entry.origin {
            LogOrigin::Build { .. } => false,
            LogOrigin::System { component, .. } => self.includes_system_component(component),
            LogOrigin::Workload { metadata } => {
                self.includes_workload(metadata.service_id.as_str())
            }
        }
    }

    fn includes_system_component(&self, component: &str) -> bool {
        (INGRESS_COMPONENTS.contains(&component)
            && self.settings.ingress_logs == LogSourceInclusion::Include)
            || (TAILSCALE_COMPONENTS.contains(&component)
                && self.settings.tailscale_logs == LogSourceInclusion::Include)
    }

    fn includes_workload(&self, service_id: &str) -> bool {
        if INGRESS_COMPONENTS.contains(&service_id) {
            self.settings.ingress_logs == LogSourceInclusion::Include
        } else if TAILSCALE_COMPONENTS.contains(&service_id) {
            self.settings.tailscale_logs == LogSourceInclusion::Include
        } else {
            true
        }
    }

    fn prepare(&self, entry: &SequencedLogEntry) -> PreparedEntry {
        let (hostname, service, ddsource, tags) = datadog_identity(&entry.entry);
        PreparedEntry {
            sequence: entry.sequence,
            observed_at: entry.entry.observed_at,
            payload: DatadogEntry {
                message: body_text(&entry.entry.body),
                hostname,
                service,
                ddsource,
                ddtags: (!tags.is_empty()).then(|| tags.join(",")),
                status: datadog_status(&entry.entry.severity).to_owned(),
                attributes: entry.entry.attributes.clone(),
            },
        }
    }

    async fn post(&self, payload: Vec<u8>) -> Result<PostOutcome, LogSinkError> {
        let compressed = gzip(&payload)?;
        let response = self
            .transport
            .send(HttpRequest {
                url: self.settings.endpoint.clone(),
                headers: BTreeMap::from([
                    ("Content-Encoding".to_owned(), "gzip".to_owned()),
                    ("Content-Type".to_owned(), "application/json".to_owned()),
                    ("DD-API-KEY".to_owned(), self.settings.api_key.clone()),
                ]),
                body: compressed,
            })
            .await
            .map_err(|error| LogSinkError::Unavailable {
                message: error.to_string(),
            })?;
        if (200..300).contains(&response.status) {
            Ok(PostOutcome::Accepted)
        } else {
            Ok(PostOutcome::Rejected {
                status: response.status,
                body: response.body,
            })
        }
    }

    async fn quarantine(
        &self,
        entry: &PreparedEntry,
        status: u16,
        reason: String,
        payload: Vec<u8>,
    ) -> Result<(), LogSinkError> {
        self.dead_letters
            .record(&SinkDeadLetter {
                sink_id: self.id.clone(),
                source_sequence: entry.sequence,
                status_code: Some(status),
                reason,
                payload,
                recorded_at: entry.observed_at,
            })
            .await
            .map_err(map_dead_letter_error)
    }

    async fn send_root(
        &self,
        entries: &[PreparedEntry],
        start: usize,
        end: usize,
    ) -> Result<usize, LogSinkError> {
        let mut pending = VecDeque::from([(start, end)]);
        let mut accepted_any = false;
        let mut bad_requests = Vec::new();
        let mut quarantined = 0_usize;
        while let Some((start, end)) = pending.pop_front() {
            let slice = entries.get(start..end).ok_or_else(invalid_split)?;
            let payload = serialize(slice)?;
            match self.post(payload.clone()).await? {
                PostOutcome::Accepted => accepted_any = true,
                PostOutcome::Rejected {
                    status: 400,
                    body: _,
                } if slice.len() > 1 => push_split(&mut pending, start, end),
                PostOutcome::Rejected { status: 400, body } => {
                    bad_requests.push((start, body, payload));
                }
                PostOutcome::Rejected {
                    status: 413,
                    body: _,
                } if slice.len() > 1 => push_split(&mut pending, start, end),
                PostOutcome::Rejected { status: 413, body } => {
                    let entry = entries.get(start).ok_or_else(invalid_split)?;
                    self.quarantine(entry, 413, body, payload).await?;
                    quarantined = quarantined.saturating_add(1);
                }
                PostOutcome::Rejected { status, body } => {
                    return Err(LogSinkError::Unavailable {
                        message: format!("Datadog intake returned HTTP {status}: {body}"),
                    });
                }
            }
        }
        if !bad_requests.is_empty() && !accepted_any {
            return Err(LogSinkError::Rejected {
                message: "Datadog rejected every isolated payload with HTTP 400".to_owned(),
            });
        }
        for (index, body, payload) in bad_requests {
            let entry = entries.get(index).ok_or_else(invalid_split)?;
            self.quarantine(entry, 400, body, payload).await?;
            quarantined = quarantined.saturating_add(1);
        }
        Ok(quarantined)
    }
}

#[async_trait]
impl LogSink for DatadogLogSink {
    fn id(&self) -> &LogSinkId {
        &self.id
    }

    async fn send(&self, entries: &[SequencedLogEntry]) -> Result<LogSinkOutcome, LogSinkError> {
        let prepared = entries
            .iter()
            .filter(|entry| self.should_send(&entry.entry))
            .map(|entry| self.prepare(entry))
            .collect::<Vec<_>>();
        let filtered_entries = entries.len().saturating_sub(prepared.len());
        let mut sizing = VecDeque::from([(0, prepared.len())]);
        let mut roots = Vec::new();
        while let Some((start, end)) = sizing.pop_front() {
            let slice = prepared.get(start..end).ok_or_else(invalid_split)?;
            let payload = serialize(slice)?;
            if payload.len() > MAX_UNCOMPRESSED_BYTES && slice.len() > 1 {
                push_split(&mut sizing, start, end);
            } else if !slice.is_empty() {
                roots.push((start, end));
            }
        }
        let mut quarantined_entries = 0_usize;
        for (start, end) in roots {
            quarantined_entries =
                quarantined_entries.saturating_add(self.send_root(&prepared, start, end).await?);
        }
        Ok(LogSinkOutcome {
            filtered_entries,
            quarantined_entries,
        })
    }
}

#[derive(Serialize)]
struct DatadogEntry {
    message: String,
    hostname: String,
    service: String,
    ddsource: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    ddtags: Option<String>,
    status: String,
    #[serde(flatten, skip_serializing_if = "BTreeMap::is_empty")]
    attributes: BTreeMap<String, String>,
}

struct PreparedEntry {
    sequence: LogSequence,
    observed_at: kernel_api::Timestamp,
    payload: DatadogEntry,
}

enum PostOutcome {
    Accepted,
    Rejected { status: u16, body: String },
}

fn push_split(pending: &mut VecDeque<(usize, usize)>, start: usize, end: usize) {
    let middle = start.saturating_add(end.saturating_sub(start) / 2);
    pending.push_front((middle, end));
    pending.push_front((start, middle));
}

fn serialize(entries: &[PreparedEntry]) -> Result<Vec<u8>, LogSinkError> {
    serde_json::to_vec(
        &entries
            .iter()
            .map(|entry| &entry.payload)
            .collect::<Vec<_>>(),
    )
    .map_err(|error| LogSinkError::Rejected {
        message: format!("Datadog payload could not be encoded: {error}"),
    })
}

fn gzip(payload: &[u8]) -> Result<Vec<u8>, LogSinkError> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder
        .write_all(payload)
        .and_then(|()| encoder.finish())
        .map_err(|error| LogSinkError::Unavailable {
            message: format!("Datadog payload compression failed: {error}"),
        })
}

fn datadog_identity(entry: &IngestLogEntry) -> (String, String, String, Vec<String>) {
    match &entry.origin {
        LogOrigin::Workload { metadata } => {
            let mut tags = metadata
                .labels
                .iter()
                .filter(|(key, _)| !key.starts_with("maestro."))
                .map(|(key, value)| format!("{key}:{value}"))
                .collect::<Vec<_>>();
            tags.extend([
                format!("cluster:{}", metadata.cluster_id),
                format!("deployment:{}", metadata.deployment_id),
                format!("workload:{}", metadata.workload_id),
            ]);
            let component = metadata.service_id.as_str();
            (
                metadata.workload_id.to_string(),
                metadata.service_id.to_string(),
                datadog_source(component).to_owned(),
                tags,
            )
        }
        LogOrigin::System {
            cluster_id,
            node_id,
            component,
        } => (
            node_id
                .as_ref()
                .map_or_else(|| cluster_id.to_string(), ToString::to_string),
            component.clone(),
            datadog_source(component).to_owned(),
            vec![format!("cluster:{cluster_id}")],
        ),
        LogOrigin::Build { build_id, .. } => (
            build_id.to_string(),
            "build".to_owned(),
            "maestro".to_owned(),
            Vec::new(),
        ),
    }
}

fn body_text(body: &LogBody) -> String {
    match body {
        LogBody::Text(body) => body.clone(),
        LogBody::Bytes(body) => String::from_utf8_lossy(body).into_owned(),
    }
}

fn datadog_source(component: &str) -> &'static str {
    if INGRESS_COMPONENTS.contains(&component) {
        "traefik"
    } else {
        "maestro"
    }
}

fn datadog_status(severity: &str) -> &'static str {
    match severity {
        "error" => "error",
        "warn" => "warn",
        "debug" | "trace" => "debug",
        _ => "info",
    }
}

fn valid_datadog_site(site: &str) -> bool {
    !site.is_empty()
        && site.len() <= 255
        && site.is_ascii()
        && Host::parse(site).is_ok_and(
            |host| matches!(host, Host::Domain(domain) if domain.eq_ignore_ascii_case(site)),
        )
        && site.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= 63
                && label
                    .as_bytes()
                    .first()
                    .is_some_and(u8::is_ascii_alphanumeric)
                && label
                    .as_bytes()
                    .last()
                    .is_some_and(u8::is_ascii_alphanumeric)
        })
}

fn valid_http_endpoint(endpoint: &str) -> bool {
    Url::parse(endpoint).is_ok_and(|url| {
        matches!(url.scheme(), "http" | "https")
            && url.has_host()
            && url.username().is_empty()
            && url.password().is_none()
    })
}

fn invalid_split() -> LogSinkError {
    LogSinkError::Rejected {
        message: "Datadog bisection produced an invalid range".to_owned(),
    }
}

fn map_dead_letter_error(error: DeadLetterStoreError) -> LogSinkError {
    match error {
        DeadLetterStoreError::Rejected { message } => LogSinkError::Rejected { message },
        DeadLetterStoreError::Unavailable { message } => LogSinkError::Unavailable { message },
    }
}
