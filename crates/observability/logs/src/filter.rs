use std::borrow::Cow;
use std::sync::Arc;

use runtime::HEALTHCHECK_PATH_LABEL;
use serde::{Deserialize, Serialize};

use crate::{IngestLogEntry, LogBody, LogOrigin};

const HTTP_METHOD_KEYS: &[&str] = &[
    "http.method",
    "method",
    "req.method",
    "request.method",
    "RequestMethod",
];
const HTTP_STATUS_KEYS: &[&str] = &[
    "http.status_code",
    "status_code",
    "statuscode",
    "status",
    "http.status",
    "response.status_code",
    "DownstreamStatus",
];
const HTTP_PATH_KEYS: &[&str] = &[
    "http.url_details.path",
    "http.path",
    "path",
    "url",
    "route",
    "uri",
    "request.path",
    "target",
    "RequestPath",
];
const TAILSCALE_COMPONENTS: &[&str] = &["tailscale", "tailscaled", "maestro-tailscale"];
const TAILSCALE_NOISE_PREFIXES: &[&str] = &[
    "magicsock:",
    "derphttp.Client.",
    "netstack: UDP session",
    "netmap: suggested exit node",
    "client -> backend close connection",
    "backend -> client close connection",
    "proxy connection closed",
    "[RATELIMIT]",
];

/// One immutable predicate in an ordered normalized-log filter chain.
pub trait LogFilter: Send + Sync {
    /// Stable declarative name used in drop accounting and diagnostics.
    fn name(&self) -> &'static str;

    /// Returns true when this filter intentionally removes the record.
    fn should_drop(&self, entry: &IngestLogEntry) -> bool;
}

/// Built-in filters available to declarative source and sink configurations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum LogFilterKind {
    /// Drops repetitive transport chatter emitted by Tailscale components.
    TailscaleNoise,
    /// Drops successful GET access records for the workload's configured HTTP healthcheck.
    SuccessfulHealthcheck,
}

/// Ordered filter chain that reports the first predicate responsible for a drop.
#[derive(Clone, Default)]
pub struct LogFilterChain {
    filters: Vec<Arc<dyn LogFilter>>,
}

impl LogFilterChain {
    /// Builds a chain from custom in-process filters.
    pub fn new(filters: Vec<Arc<dyn LogFilter>>) -> Self {
        Self { filters }
    }

    /// Builds built-in filters in the exact declarative order supplied.
    pub fn configured(kinds: impl IntoIterator<Item = LogFilterKind>) -> Self {
        Self::new(
            kinds
                .into_iter()
                .map(|kind| match kind {
                    LogFilterKind::TailscaleNoise => {
                        Arc::new(TailscaleNoiseFilter) as Arc<dyn LogFilter>
                    }
                    LogFilterKind::SuccessfulHealthcheck => {
                        Arc::new(SuccessfulHealthcheckFilter) as Arc<dyn LogFilter>
                    }
                })
                .collect(),
        )
    }

    /// Returns the stable name of the first filter that drops the record.
    pub fn dropped_by(&self, entry: &IngestLogEntry) -> Option<&'static str> {
        self.filters
            .iter()
            .find(|filter| filter.should_drop(entry))
            .map(|filter| filter.name())
    }
}

/// Source-level defaults applied before records enter the durable local store.
pub fn standard_ingest_filters() -> LogFilterChain {
    LogFilterChain::configured([LogFilterKind::TailscaleNoise])
}

/// Drops legacy high-volume Tailscale chatter only for known Tailscale origins.
#[derive(Debug, Clone, Copy, Default)]
pub struct TailscaleNoiseFilter;

impl LogFilter for TailscaleNoiseFilter {
    fn name(&self) -> &'static str {
        "tailscaleNoise"
    }

    fn should_drop(&self, entry: &IngestLogEntry) -> bool {
        is_tailscale_origin(&entry.origin)
            && text_body(entry).is_some_and(|body| {
                TAILSCALE_NOISE_PREFIXES
                    .iter()
                    .any(|prefix| body.starts_with(prefix))
            })
    }
}

/// Drops successful access records matching a workload's configured HTTP probe path.
#[derive(Debug, Clone, Copy, Default)]
pub struct SuccessfulHealthcheckFilter;

impl LogFilter for SuccessfulHealthcheckFilter {
    fn name(&self) -> &'static str {
        "successfulHealthcheck"
    }

    fn should_drop(&self, entry: &IngestLogEntry) -> bool {
        let LogOrigin::Workload { metadata } = &entry.origin else {
            return false;
        };
        let Some(healthcheck_path) = metadata.labels.get(HEALTHCHECK_PATH_LABEL) else {
            return false;
        };
        let Some(method) = attribute_value(entry, HTTP_METHOD_KEYS) else {
            return false;
        };
        let Some(status) = attribute_value(entry, HTTP_STATUS_KEYS) else {
            return false;
        };
        let Some(request_path) = attribute_value(entry, HTTP_PATH_KEYS) else {
            return false;
        };

        method.eq_ignore_ascii_case("GET")
            && status.trim() == "200"
            && normalized_path(&request_path) == normalized_path(healthcheck_path)
    }
}

fn is_tailscale_origin(origin: &LogOrigin) -> bool {
    let component = match origin {
        LogOrigin::Workload { metadata } => metadata.service_id.as_str(),
        LogOrigin::System { component, .. } => component,
        LogOrigin::Build { .. } => return false,
    };
    TAILSCALE_COMPONENTS.contains(&component)
}

fn text_body(entry: &IngestLogEntry) -> Option<&str> {
    match &entry.body {
        LogBody::Text(body) => Some(body),
        LogBody::Bytes(_) => None,
    }
}

fn attribute_value<'a>(entry: &'a IngestLogEntry, keys: &[&str]) -> Option<Cow<'a, str>> {
    keys.iter().find_map(|candidate| {
        entry
            .attributes
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(candidate))
            .map(|(_, value)| Cow::Borrowed(value.as_str()))
            .or_else(|| nested_attribute_value(&entry.attributes, candidate))
    })
}

fn nested_attribute_value<'a>(
    attributes: &'a std::collections::BTreeMap<String, String>,
    path: &str,
) -> Option<Cow<'a, str>> {
    let (root, remainder) = path.split_once('.')?;
    let raw = attributes
        .iter()
        .find(|(key, _)| key.eq_ignore_ascii_case(root))?
        .1
        .as_str();
    let parsed = serde_json::from_str::<serde_json::Value>(raw).ok()?;
    let value = remainder.split('.').try_fold(&parsed, |value, segment| {
        value
            .as_object()?
            .iter()
            .find_map(|(key, value)| key.eq_ignore_ascii_case(segment).then_some(value))
    })?;
    match value {
        serde_json::Value::String(value) => Some(Cow::Owned(value.clone())),
        serde_json::Value::Number(value) => Some(Cow::Owned(value.to_string())),
        serde_json::Value::Bool(value) => Some(Cow::Owned(value.to_string())),
        _ => None,
    }
}

fn normalized_path(value: &str) -> Cow<'_, str> {
    let value = value.trim();
    let path = value.split_once("://").map_or(value, |(_, remainder)| {
        remainder.find('/').map_or("/", |start| &remainder[start..])
    });
    let path = path.split(['?', '#']).next().unwrap_or(path);
    if path.len() > 1 {
        Cow::Owned(path.trim_end_matches('/').to_owned())
    } else {
        Cow::Borrowed(path)
    }
}
