pub mod collector;
pub mod datadog_sink;
pub mod duck;
mod filter;
pub mod http_sink;
mod search;
pub mod sink;
pub mod store;

pub use collector::{LogCollector, LogConfig};
pub use datadog_sink::DatadogSink;
pub use duck::{BackupPartition, DuckLogStore, IngestLogEntry};
pub(crate) use filter::healthcheck_path_tag;
pub use http_sink::HttpSink;
pub use search::LogSearchQuery;
pub(crate) use search::{LogSearchValue, http_status_class_expression, sql_like_prefix};
pub use sink::SinkWorker;
pub use store::{LogEntry, LogOrigin, LogStore, Logger};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogReadScope {
    AllServices,
    AllSystem,
    Prefix(String),
    Sources(Vec<String>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct LogReadQuery {
    pub scope: LogReadScope,
    pub origin: Option<LogOrigin>,
    pub search: Option<LogSearchQuery>,
    pub from: Option<i64>,
    pub to: Option<i64>,
    pub after: Option<i64>,
    pub before: Option<i64>,
    pub limit: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum LogHistogramGroupBy {
    #[default]
    Level,
    HttpStatusClass,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LogHistogramQuery {
    pub scope: LogReadScope,
    pub origin: Option<LogOrigin>,
    pub search: Option<LogSearchQuery>,
    pub from: i64,
    pub to: i64,
    pub bucket_ms: i64,
    pub group_by: LogHistogramGroupBy,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LogHistogramBucket {
    pub ts: i64,
    pub count: u64,
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub levels: std::collections::BTreeMap<String, u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LogHistogram {
    pub from: i64,
    pub to: i64,
    pub bucket_ms: i64,
    pub buckets: Vec<LogHistogramBucket>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TrafficBreakdownEntry {
    pub value: String,
    pub status_code: u16,
    pub requests: u64,
    pub last_seen_at_ms: i64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IngressTrafficBreakdown {
    pub by_ip: Vec<TrafficBreakdownEntry>,
    pub by_path: Vec<TrafficBreakdownEntry>,
}

pub(crate) struct IngressTrafficEvent {
    pub bucket_at_ms: i64,
    pub last_seen_at_ms: i64,
    pub router: String,
    pub client_ip: String,
    pub path: String,
    pub status_code: u16,
}

pub(crate) fn parse_ingress_traffic_event(entry: &LogEntry) -> Option<IngressTrafficEvent> {
    if entry.source.as_ref() != "maestro-ingress" {
        return None;
    }
    let attr = |name: &str| {
        entry
            .attrs
            .iter()
            .find(|(candidate, _)| candidate == name)
            .map(|(_, value)| value.as_str())
    };
    let router = attr("RouterName")?;
    let client_ip = attr("maestro.client_ip")
        .or_else(|| attr("ClientHost"))?
        .parse::<std::net::IpAddr>()
        .ok()?
        .to_string();
    let status_code = attr("DownstreamStatus")?.parse::<u16>().ok()?;
    if !(100..=599).contains(&status_code) {
        return None;
    }
    let path = attr("RequestPath")?.split('?').next().unwrap_or("/");
    let path = if path.is_empty() { "/" } else { path };
    let path = path.chars().take(2_048).collect::<String>();
    Some(IngressTrafficEvent {
        bucket_at_ms: entry.ts - entry.ts.rem_euclid(60_000),
        last_seen_at_ms: entry.ts,
        router: router.to_string(),
        client_ip,
        path,
        status_code,
    })
}
