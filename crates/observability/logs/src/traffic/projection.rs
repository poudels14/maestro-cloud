use std::collections::BTreeMap;

use super::{
    IngressTrafficBreakdown, IngressTrafficQuery, ServiceTrafficQuery, TRAFFIC_BUCKET_MS,
    TrafficBreakdownEntry, TrafficMetricPoint,
};
use crate::{IngestLogEntry, LogOrigin};

const MAXIMUM_ACCESS_PATH_CHARS: usize = 2_048;
const MAXIMUM_METHOD_CHARS: usize = 32;

/// Aggregates trusted normalized entries for the in-memory store and contract tests.
pub fn project_ingress_traffic<'a>(
    entries: impl IntoIterator<Item = &'a IngestLogEntry>,
    query: &IngressTrafficQuery,
) -> IngressTrafficBreakdown {
    let events = entries
        .into_iter()
        .filter_map(ingress_traffic_event)
        .filter(|event| in_range(event.at_ms, query.from, query.to))
        .filter(|event| query.scope.matches(&event.router))
        .collect::<Vec<_>>();
    IngressTrafficBreakdown {
        by_ip: rank_dimension(&events, query.limit, |event| event.client_ip.as_str()),
        by_path: rank_dimension(&events, query.limit, |event| event.path.as_str()),
    }
}

/// Aggregates trusted normalized entries into the retired service traffic wire shape.
pub fn project_service_traffic<'a>(
    entries: impl IntoIterator<Item = &'a IngestLogEntry>,
    query: &ServiceTrafficQuery,
) -> Vec<TrafficMetricPoint> {
    let mut intervals = BTreeMap::<(i64, u16, String), TrafficAggregate>::new();
    for event in entries.into_iter().filter_map(ingress_traffic_event) {
        if !in_range(event.at_ms, query.from, query.to)
            || !event.router.starts_with(&query.router_prefix)
        {
            continue;
        }
        let bucket = event.at_ms - event.at_ms.rem_euclid(TRAFFIC_BUCKET_MS);
        intervals
            .entry((bucket, event.status_code, event.method.clone()))
            .or_default()
            .record(&event);
    }
    intervals
        .into_iter()
        .take(query.limit)
        .map(
            |((ts, status_code, method), aggregate)| TrafficMetricPoint {
                ts,
                service_id: query.service_id.as_str().to_owned(),
                deployment_id: None,
                status_code,
                method,
                requests: aggregate.requests,
                bytes_in: aggregate.bytes_in,
                bytes_out: aggregate.bytes_out,
                lat_le_1s: aggregate.lat_le_1s,
                lat_le_5s: aggregate.lat_le_5s,
                lat_le_10s: aggregate.lat_le_10s,
                lat_total: aggregate.lat_total,
            },
        )
        .collect()
}

/// Merges node breakdowns using the established value-level ranking semantics.
pub fn merge_ingress_traffic(
    breakdowns: impl IntoIterator<Item = IngressTrafficBreakdown>,
    limit: usize,
) -> IngressTrafficBreakdown {
    let mut by_ip = Vec::new();
    let mut by_path = Vec::new();
    for breakdown in breakdowns {
        by_ip.extend(breakdown.by_ip);
        by_path.extend(breakdown.by_path);
    }
    IngressTrafficBreakdown {
        by_ip: merge_dimension(by_ip, limit),
        by_path: merge_dimension(by_path, limit),
    }
}

/// Saturating-merges matching node service intervals and applies one global row limit.
pub fn merge_service_traffic(
    points: impl IntoIterator<Item = TrafficMetricPoint>,
    limit: usize,
) -> Vec<TrafficMetricPoint> {
    let mut merged =
        BTreeMap::<(i64, String, Option<String>, u16, String), TrafficMetricPoint>::new();
    for point in points {
        let key = (
            point.ts,
            point.service_id.clone(),
            point.deployment_id.clone(),
            point.status_code,
            point.method.clone(),
        );
        merged
            .entry(key)
            .and_modify(|current| {
                current.requests = current.requests.saturating_add(point.requests);
                current.bytes_in = current.bytes_in.saturating_add(point.bytes_in);
                current.bytes_out = current.bytes_out.saturating_add(point.bytes_out);
                current.lat_le_1s = current.lat_le_1s.saturating_add(point.lat_le_1s);
                current.lat_le_5s = current.lat_le_5s.saturating_add(point.lat_le_5s);
                current.lat_le_10s = current.lat_le_10s.saturating_add(point.lat_le_10s);
                current.lat_total = current.lat_total.saturating_add(point.lat_total);
            })
            .or_insert(point);
    }
    merged.into_values().take(limit).collect()
}

#[derive(Debug)]
struct IngressTrafficEvent {
    at_ms: i64,
    router: String,
    client_ip: String,
    path: String,
    status_code: u16,
    method: String,
    bytes_in: i64,
    bytes_out: i64,
    duration_ns: Option<i64>,
}

fn ingress_traffic_event(entry: &IngestLogEntry) -> Option<IngressTrafficEvent> {
    let component = match &entry.origin {
        LogOrigin::System { component, .. } => component.as_str(),
        LogOrigin::Workload { .. } | LogOrigin::Build { .. } => return None,
    };
    if !matches!(component, "ingress" | "maestro-ingress")
        || attribute(entry, "maestro.log_type") != Some("ingress_access")
    {
        return None;
    }
    let router = attribute(entry, "RouterName")?.to_owned();
    let client_ip = attribute(entry, "maestro.client_ip")?
        .parse::<std::net::IpAddr>()
        .ok()?
        .to_string();
    let status_code = attribute(entry, "DownstreamStatus")?.parse::<u16>().ok()?;
    if !(100..=599).contains(&status_code) {
        return None;
    }
    let path = bounded(attribute(entry, "RequestPath")?, MAXIMUM_ACCESS_PATH_CHARS);
    let path = if path.is_empty() {
        "/".to_owned()
    } else {
        path
    };
    let method = bounded(attribute(entry, "RequestMethod")?, MAXIMUM_METHOD_CHARS);
    Some(IngressTrafficEvent {
        at_ms: entry.event_at.0,
        router,
        client_ip,
        path,
        status_code,
        method,
        bytes_in: nonnegative_attribute(entry, "RequestContentSize"),
        bytes_out: nonnegative_attribute(entry, "DownstreamContentSize"),
        duration_ns: attribute(entry, "Duration")
            .and_then(|value| value.parse::<i64>().ok())
            .filter(|value| *value >= 0),
    })
}

#[derive(Debug, Default)]
struct TrafficAggregate {
    requests: i64,
    bytes_in: i64,
    bytes_out: i64,
    lat_le_1s: i64,
    lat_le_5s: i64,
    lat_le_10s: i64,
    lat_total: i64,
}

impl TrafficAggregate {
    fn record(&mut self, event: &IngressTrafficEvent) {
        self.requests = self.requests.saturating_add(1);
        self.bytes_in = self.bytes_in.saturating_add(event.bytes_in);
        self.bytes_out = self.bytes_out.saturating_add(event.bytes_out);
        if let Some(duration) = event.duration_ns {
            self.lat_total = self.lat_total.saturating_add(1);
            self.lat_le_1s = self
                .lat_le_1s
                .saturating_add(i64::from(duration <= 1_000_000_000));
            self.lat_le_5s = self
                .lat_le_5s
                .saturating_add(i64::from(duration <= 5_000_000_000));
            self.lat_le_10s = self
                .lat_le_10s
                .saturating_add(i64::from(duration <= 10_000_000_000));
        }
    }
}

fn rank_dimension(
    events: &[IngressTrafficEvent],
    limit: usize,
    value: impl Fn(&IngressTrafficEvent) -> &str,
) -> Vec<TrafficBreakdownEntry> {
    let mut grouped = BTreeMap::<(String, u16), (u64, i64)>::new();
    for event in events {
        let row = grouped
            .entry((value(event).to_owned(), event.status_code))
            .or_insert((0, i64::MIN));
        row.0 = row.0.saturating_add(1);
        row.1 = row.1.max(event.at_ms);
    }
    rank_grouped(grouped, limit)
}

fn merge_dimension(
    entries: Vec<TrafficBreakdownEntry>,
    limit: usize,
) -> Vec<TrafficBreakdownEntry> {
    let mut grouped = BTreeMap::<(String, u16), (u64, i64)>::new();
    for entry in entries {
        let row = grouped
            .entry((entry.value, entry.status_code))
            .or_insert((0, i64::MIN));
        row.0 = row.0.saturating_add(entry.requests);
        row.1 = row.1.max(entry.last_seen_at_ms);
    }
    rank_grouped(grouped, limit)
}

fn rank_grouped(
    grouped: BTreeMap<(String, u16), (u64, i64)>,
    limit: usize,
) -> Vec<TrafficBreakdownEntry> {
    let mut totals = BTreeMap::<String, (u64, i64)>::new();
    for ((value, _), (requests, last_seen)) in &grouped {
        let total = totals.entry(value.clone()).or_insert((0, i64::MIN));
        total.0 = total.0.saturating_add(*requests);
        total.1 = total.1.max(*last_seen);
    }
    let mut ranked = totals.into_iter().collect::<Vec<_>>();
    ranked.sort_by(|left, right| {
        right
            .1
            .0
            .cmp(&left.1.0)
            .then_with(|| right.1.1.cmp(&left.1.1))
            .then_with(|| left.0.cmp(&right.0))
    });
    let ranks = ranked
        .into_iter()
        .take(limit)
        .enumerate()
        .map(|(rank, (value, _))| (value, rank))
        .collect::<BTreeMap<_, _>>();
    let mut entries = grouped
        .into_iter()
        .filter(|((value, _), _)| ranks.contains_key(value))
        .map(
            |((value, status_code), (requests, last_seen_at_ms))| TrafficBreakdownEntry {
                value,
                status_code,
                requests,
                last_seen_at_ms,
            },
        )
        .collect::<Vec<_>>();
    entries.sort_by(|left, right| {
        ranks
            .get(&left.value)
            .cmp(&ranks.get(&right.value))
            .then_with(|| left.status_code.cmp(&right.status_code))
    });
    entries
}

fn attribute<'a>(entry: &'a IngestLogEntry, name: &str) -> Option<&'a str> {
    entry.attributes.get(name).map(String::as_str)
}

fn nonnegative_attribute(entry: &IngestLogEntry, name: &str) -> i64 {
    attribute(entry, name)
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or_default()
        .max(0)
}

fn bounded(value: &str, limit: usize) -> String {
    value.chars().take(limit).collect()
}

fn in_range(value: i64, from: kernel_api::Timestamp, to: kernel_api::Timestamp) -> bool {
    value >= from.0 && value <= to.0
}
