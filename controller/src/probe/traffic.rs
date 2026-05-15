use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, anyhow};

use crate::logs::LogStore;
use crate::metrics::TrafficPoint;

const SCRAPE_INTERVAL: Duration = Duration::from_secs(5);
const SCRAPE_TIMEOUT: Duration = Duration::from_secs(3);
const METRICS_URL: &str = "http://web:9100/metrics";

pub async fn run(log_store: Arc<LogStore>) {
    let http_client = match reqwest::Client::builder().timeout(SCRAPE_TIMEOUT).build() {
        Ok(c) => c,
        Err(err) => {
            eprintln!("traffic scraper: failed to build http client: {err}");
            return;
        }
    };

    let mut scraper = TrafficScraper {
        http_client,
        log_store,
        previous: HashMap::new(),
        has_scraped: false,
    };

    let mut interval = tokio::time::interval(SCRAPE_INTERVAL);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        interval.tick().await;
        if let Err(err) = scraper.scrape_once().await {
            eprintln!("traffic scrape error: {err}");
        }
    }
}

struct TrafficScraper {
    http_client: reqwest::Client,
    log_store: Arc<LogStore>,
    previous: HashMap<SeriesKey, Cumulative>,
    has_scraped: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SeriesKey {
    service_id: String,
    deployment_id: Option<String>,
    status_code: u16,
    method: String,
}

#[derive(Debug, Clone, Copy, Default)]
struct Cumulative {
    requests: i64,
    bytes_in: i64,
    bytes_out: i64,
    lat_le_1s: i64,
    lat_le_5s: i64,
    lat_le_10s: i64,
    lat_total: i64,
}

impl TrafficScraper {
    async fn scrape_once(&mut self) -> Result<()> {
        let response = self.http_client.get(METRICS_URL).send().await?;
        if !response.status().is_success() {
            return Err(anyhow!("traefik metrics returned {}", response.status()));
        }
        let body = response.text().await?;
        let current = parse_prometheus_text(&body);

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;

        let mut deltas: Vec<TrafficPoint> = Vec::new();

        if self.has_scraped {
            for (key, cur) in &current {
                let prev = self.previous.get(key).copied().unwrap_or_default();
                let delta = compute_delta(prev, *cur);
                deltas.push(TrafficPoint {
                    ts: now,
                    service_id: key.service_id.clone(),
                    deployment_id: key.deployment_id.clone(),
                    status_code: key.status_code,
                    method: key.method.clone(),
                    requests: delta.requests,
                    bytes_in: delta.bytes_in,
                    bytes_out: delta.bytes_out,
                    lat_le_1s: delta.lat_le_1s,
                    lat_le_5s: delta.lat_le_5s,
                    lat_le_10s: delta.lat_le_10s,
                    lat_total: delta.lat_total,
                });
            }
        }

        self.previous = current;
        self.has_scraped = true;

        if !deltas.is_empty() {
            self.log_store.append_traffic_metrics(&deltas).await?;
        }
        Ok(())
    }
}

fn compute_delta(prev: Cumulative, cur: Cumulative) -> Cumulative {
    let diff = |c: i64, p: i64| -> i64 { if c >= p { c - p } else { c } };
    Cumulative {
        requests: diff(cur.requests, prev.requests),
        bytes_in: diff(cur.bytes_in, prev.bytes_in),
        bytes_out: diff(cur.bytes_out, prev.bytes_out),
        lat_le_1s: diff(cur.lat_le_1s, prev.lat_le_1s),
        lat_le_5s: diff(cur.lat_le_5s, prev.lat_le_5s),
        lat_le_10s: diff(cur.lat_le_10s, prev.lat_le_10s),
        lat_total: diff(cur.lat_total, prev.lat_total),
    }
}

fn parse_prometheus_text(body: &str) -> HashMap<SeriesKey, Cumulative> {
    let mut out: HashMap<SeriesKey, Cumulative> = HashMap::new();
    for line in body.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some(sample) = parse_sample_line(line) else {
            continue;
        };
        let Some(svc_raw) = sample.labels.get("service") else {
            continue;
        };
        let Some((service_id, deployment_id)) = split_service_label(svc_raw) else {
            continue;
        };
        let method = sample
            .labels
            .get("method")
            .cloned()
            .unwrap_or_else(|| "GET".to_string());
        let status_code: u16 = sample
            .labels
            .get("code")
            .and_then(|c| c.parse().ok())
            .unwrap_or(0);

        let key = SeriesKey {
            service_id,
            deployment_id,
            status_code,
            method,
        };
        let entry = out.entry(key).or_default();

        match sample.name.as_str() {
            "traefik_service_requests_total" => {
                entry.requests = sample.value as i64;
            }
            "traefik_service_requests_bytes_total" => {
                entry.bytes_in = sample.value as i64;
            }
            "traefik_service_responses_bytes_total" => {
                entry.bytes_out = sample.value as i64;
            }
            "traefik_service_request_duration_seconds_count" => {
                entry.lat_total = sample.value as i64;
            }
            "traefik_service_request_duration_seconds_bucket" => {
                let Some(le) = sample.labels.get("le") else {
                    continue;
                };
                match le.as_str() {
                    "1" | "1.0" => entry.lat_le_1s = sample.value as i64,
                    "5" | "5.0" => entry.lat_le_5s = sample.value as i64,
                    "10" | "10.0" => entry.lat_le_10s = sample.value as i64,
                    _ => {}
                }
            }
            _ => {}
        }
    }
    out
}

struct Sample {
    name: String,
    labels: HashMap<String, String>,
    value: f64,
}

fn parse_sample_line(line: &str) -> Option<Sample> {
    if let Some(idx) = line.find('{') {
        let name = line[..idx].to_string();
        let after_brace = &line[idx + 1..];
        let close = after_brace.find('}')?;
        let labels_raw = &after_brace[..close];
        let value_str = after_brace[close + 1..].trim();
        let labels = parse_labels(labels_raw);
        let value: f64 = value_str.split_whitespace().next()?.parse().ok()?;
        Some(Sample {
            name,
            labels,
            value,
        })
    } else {
        let mut parts = line.split_whitespace();
        let name = parts.next()?.to_string();
        let value: f64 = parts.next()?.parse().ok()?;
        Some(Sample {
            name,
            labels: HashMap::new(),
            value,
        })
    }
}

fn parse_labels(raw: &str) -> HashMap<String, String> {
    let mut out = HashMap::new();
    let bytes = raw.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        while i < bytes.len() && (bytes[i] == b',' || bytes[i].is_ascii_whitespace()) {
            i += 1;
        }
        let key_start = i;
        while i < bytes.len() && bytes[i] != b'=' {
            i += 1;
        }
        if i >= bytes.len() {
            break;
        }
        let key = &raw[key_start..i];
        i += 1;
        if i >= bytes.len() || bytes[i] != b'"' {
            break;
        }
        i += 1;
        let mut value = String::new();
        while i < bytes.len() && bytes[i] != b'"' {
            if bytes[i] == b'\\' && i + 1 < bytes.len() {
                let next = bytes[i + 1];
                let ch = match next {
                    b'n' => '\n',
                    b't' => '\t',
                    b'"' => '"',
                    b'\\' => '\\',
                    other => other as char,
                };
                value.push(ch);
                i += 2;
            } else {
                value.push(bytes[i] as char);
                i += 1;
            }
        }
        if i < bytes.len() {
            i += 1;
        }
        out.insert(key.to_string(), value);
    }
    out
}

fn split_service_label(raw: &str) -> Option<(String, Option<String>)> {
    let stripped = raw.split('@').next().unwrap_or(raw);
    let (svc, dep) = match stripped.rsplit_once('-') {
        Some((head, tail))
            if tail.len() == 6 && tail.chars().all(|c| c.is_ascii_alphanumeric()) =>
        {
            (head.to_string(), Some(tail.to_string()))
        }
        _ => (stripped.to_string(), None),
    };
    if svc.is_empty() {
        return None;
    }
    Some((svc, dep))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_basic_counter() {
        let body = r#"
# HELP traefik_service_requests_total ...
# TYPE traefik_service_requests_total counter
traefik_service_requests_total{service="service-1-whLB04@docker",code="200",method="GET"} 42
"#;
        let result = parse_prometheus_text(body);
        assert_eq!(result.len(), 1);
        let (k, v) = result.iter().next().unwrap();
        assert_eq!(k.service_id, "service-1");
        assert_eq!(k.deployment_id.as_deref(), Some("whLB04"));
        assert_eq!(k.status_code, 200);
        assert_eq!(k.method, "GET");
        assert_eq!(v.requests, 42);
    }

    #[test]
    fn parses_histogram_buckets() {
        let body = r#"
traefik_service_request_duration_seconds_bucket{service="svc-aBc123",code="200",method="GET",le="1.0"} 5
traefik_service_request_duration_seconds_bucket{service="svc-aBc123",code="200",method="GET",le="5.0"} 8
traefik_service_request_duration_seconds_bucket{service="svc-aBc123",code="200",method="GET",le="10.0"} 9
traefik_service_request_duration_seconds_count{service="svc-aBc123",code="200",method="GET"} 10
"#;
        let result = parse_prometheus_text(body);
        let (_, v) = result.iter().next().unwrap();
        assert_eq!(v.lat_le_1s, 5);
        assert_eq!(v.lat_le_5s, 8);
        assert_eq!(v.lat_le_10s, 9);
        assert_eq!(v.lat_total, 10);
    }

    #[test]
    fn handles_counter_reset() {
        let prev = Cumulative {
            requests: 100,
            ..Default::default()
        };
        let cur = Cumulative {
            requests: 5,
            ..Default::default()
        };
        let delta = compute_delta(prev, cur);
        assert_eq!(delta.requests, 5);
    }

    #[test]
    fn computes_normal_delta() {
        let prev = Cumulative {
            requests: 100,
            ..Default::default()
        };
        let cur = Cumulative {
            requests: 150,
            ..Default::default()
        };
        let delta = compute_delta(prev, cur);
        assert_eq!(delta.requests, 50);
    }

    #[test]
    fn splits_service_label_with_provider_suffix() {
        let (svc, dep) = split_service_label("service-1-whLB04@docker").unwrap();
        assert_eq!(svc, "service-1");
        assert_eq!(dep.as_deref(), Some("whLB04"));
    }

    #[test]
    fn splits_service_label_without_deployment_suffix() {
        let (svc, dep) = split_service_label("standalone@docker").unwrap();
        assert_eq!(svc, "standalone");
        assert_eq!(dep, None);
    }

    #[test]
    fn ignores_unknown_metric_names() {
        let body = r#"
traefik_entrypoint_requests_total{entrypoint="web",code="200",method="GET"} 99
go_gc_duration_seconds{quantile="0"} 0.0001
"#;
        let result = parse_prometheus_text(body);
        assert!(result.is_empty());
    }
}
