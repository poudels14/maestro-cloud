use std::collections::HashMap;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const SERVICE_PREFIX: &str = "svc-";
const PROVIDER_SUFFIX: &str = "@file";
const REQUESTS: &str = "traefik_service_requests_total";
const BYTES_IN: &str = "traefik_service_requests_bytes_total";
const BYTES_OUT: &str = "traefik_service_responses_bytes_total";
const DURATION_BUCKET: &str = "traefik_service_request_duration_seconds_bucket";
const DURATION_COUNT: &str = "traefik_service_request_duration_seconds_count";
const SCRAPE_TIMEOUT: Duration = Duration::from_secs(3);

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TrafficPoint {
    pub ts: i64,
    pub service_id: String,
    pub deployment_id: Option<String>,
    pub status_code: u16,
    pub method: String,
    pub requests: u64,
    pub bytes_in: u64,
    pub bytes_out: u64,
    pub lat_le_1s: u64,
    pub lat_le_5s: u64,
    pub lat_le_10s: u64,
    pub lat_total: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct SeriesKey {
    service_id: String,
    deployment_id: Option<String>,
    status_code: u16,
    method: String,
}

#[derive(Clone, Copy, Debug, Default, PartialEq)]
struct Counters {
    requests: f64,
    bytes_in: f64,
    bytes_out: f64,
    lat_le_1s: f64,
    lat_le_5s: f64,
    lat_le_10s: f64,
    lat_total: f64,
}

#[derive(Default)]
pub struct TrafficScraper {
    previous: Option<HashMap<SeriesKey, Counters>>,
}

impl TrafficScraper {
    pub async fn scrape(&mut self, address: &str, ts: i64) -> Vec<TrafficPoint> {
        match fetch(address).await {
            Ok(body) => self.record(&body, ts),
            Err(error) => {
                tracing::debug!(%error, "traefik metrics scrape failed");
                Vec::new()
            }
        }
    }

    fn record(&mut self, exposition: &str, ts: i64) -> Vec<TrafficPoint> {
        let current = parse(exposition);
        let points = match &self.previous {
            Some(previous) => current
                .iter()
                .filter_map(|(key, counters)| {
                    let before = previous.get(key).copied().unwrap_or_default();
                    let delta = |now: f64, then: f64| match now >= then {
                        true => (now - then) as u64,
                        false => now as u64,
                    };
                    let point = TrafficPoint {
                        ts,
                        service_id: key.service_id.clone(),
                        deployment_id: key.deployment_id.clone(),
                        status_code: key.status_code,
                        method: key.method.clone(),
                        requests: delta(counters.requests, before.requests),
                        bytes_in: delta(counters.bytes_in, before.bytes_in),
                        bytes_out: delta(counters.bytes_out, before.bytes_out),
                        lat_le_1s: delta(counters.lat_le_1s, before.lat_le_1s),
                        lat_le_5s: delta(counters.lat_le_5s, before.lat_le_5s),
                        lat_le_10s: delta(counters.lat_le_10s, before.lat_le_10s),
                        lat_total: delta(counters.lat_total, before.lat_total),
                    };
                    (point.requests > 0 || point.lat_total > 0).then_some(point)
                })
                .collect(),
            None => Vec::new(),
        };
        self.previous = Some(current);
        points
    }
}

async fn fetch(address: &str) -> std::io::Result<String> {
    tokio::time::timeout(SCRAPE_TIMEOUT, async {
        let mut stream = tokio::net::TcpStream::connect(address).await?;
        stream
            .write_all(format!("GET /metrics HTTP/1.0\r\nHost: {address}\r\n\r\n").as_bytes())
            .await?;
        let mut response = String::new();
        stream.read_to_string(&mut response).await?;
        Ok(response
            .split_once("\r\n\r\n")
            .map(|(_, body)| body.to_string())
            .unwrap_or_default())
    })
    .await
    .unwrap_or_else(|_| {
        Err(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "scrape timed out",
        ))
    })
}

fn parse(exposition: &str) -> HashMap<SeriesKey, Counters> {
    exposition
        .lines()
        .filter(|line| !line.starts_with('#'))
        .filter_map(parse_sample)
        .fold(HashMap::new(), |mut series, (name, labels, value)| {
            if let Some(key) = series_key(&labels) {
                let counters = series.entry(key).or_insert_with(Counters::default);
                match (name.as_str(), labels.get("le").map(String::as_str)) {
                    (REQUESTS, _) => counters.requests += value,
                    (BYTES_IN, _) => counters.bytes_in += value,
                    (BYTES_OUT, _) => counters.bytes_out += value,
                    (DURATION_COUNT, _) => counters.lat_total += value,
                    (DURATION_BUCKET, Some("1" | "1.0")) => counters.lat_le_1s += value,
                    (DURATION_BUCKET, Some("5" | "5.0")) => counters.lat_le_5s += value,
                    (DURATION_BUCKET, Some("10" | "10.0")) => counters.lat_le_10s += value,
                    _ => {}
                }
            }
            series
        })
}

fn parse_sample(line: &str) -> Option<(String, HashMap<String, String>, f64)> {
    let (series, value) = line.rsplit_once(' ')?;
    let value = value.trim().parse::<f64>().ok()?;
    let (name, labels) = match series.split_once('{') {
        Some((name, rest)) => (name, rest.strip_suffix('}')?),
        None => (series, ""),
    };
    let labels = labels
        .split("\",")
        .filter_map(|pair| {
            let (key, value) = pair.split_once("=\"")?;
            Some((
                key.trim().to_string(),
                value.trim_end_matches('"').to_string(),
            ))
        })
        .collect();
    Some((name.to_string(), labels, value))
}

fn series_key(labels: &HashMap<String, String>) -> Option<SeriesKey> {
    let service = labels
        .get("service")?
        .strip_suffix(PROVIDER_SUFFIX)?
        .strip_prefix(SERVICE_PREFIX)?;
    let (service_id, deployment_id) = split_replica_service(service);
    Some(SeriesKey {
        service_id,
        deployment_id,
        status_code: labels
            .get("code")
            .and_then(|code| code.parse().ok())
            .unwrap_or_default(),
        method: labels
            .get("method")
            .cloned()
            .unwrap_or_else(|| "GET".to_string()),
    })
}

fn split_replica_service(service: &str) -> (String, Option<String>) {
    let replica = service.rsplit_once('-').and_then(|(rest, index)| {
        let (service_id, deployment_id) = rest.rsplit_once('-')?;
        let is_index =
            !index.is_empty() && index.chars().all(|character| character.is_ascii_digit());
        let is_deployment = deployment_id.len() >= 10
            && deployment_id.starts_with('d')
            && deployment_id
                .chars()
                .all(|character| character.is_ascii_lowercase() || character.is_ascii_digit());
        (is_index && is_deployment && !service_id.is_empty())
            .then(|| (service_id.to_string(), Some(deployment_id.to_string())))
    });
    replica.unwrap_or_else(|| (service.to_string(), None))
}

#[cfg(test)]
mod tests {
    use super::*;

    const FIRST: &str = r#"# HELP traefik_service_requests_total How many HTTP requests processed
traefik_service_requests_total{code="200",method="GET",protocol="http",service="svc-web@file"} 10
traefik_service_requests_total{code="404",method="GET",protocol="http",service="svc-web@file"} 1
traefik_service_requests_total{code="200",method="POST",protocol="http",service="svc-web-dmv7n1a4xao07-2@file"} 4
traefik_service_requests_bytes_total{code="200",method="GET",protocol="http",service="svc-web@file"} 100
traefik_service_request_duration_seconds_bucket{code="200",method="GET",protocol="http",service="svc-web@file",le="1.0"} 9
traefik_service_request_duration_seconds_bucket{code="200",method="GET",protocol="http",service="svc-web@file",le="5.0"} 10
traefik_service_request_duration_seconds_bucket{code="200",method="GET",protocol="http",service="svc-web@file",le="10.0"} 10
traefik_service_request_duration_seconds_count{code="200",method="GET",protocol="http",service="svc-web@file"} 10
traefik_service_requests_total{code="200",method="GET",protocol="http",service="maestro-denied@file"} 3
"#;

    #[test]
    fn records_per_scrape_deltas_by_service_and_replica() {
        let mut scraper = TrafficScraper::default();
        assert!(scraper.record(FIRST, 1).is_empty());
        let second = FIRST
            .replace("svc-web@file\"} 10\n", "svc-web@file\"} 15\n")
            .replace("le=\"1.0\"} 9", "le=\"1.0\"} 13");
        let mut points = scraper.record(&second, 2);
        points.sort_by_key(|point| {
            (
                point.service_id.clone(),
                point.status_code,
                point.method.clone(),
            )
        });
        assert_eq!(points.len(), 1);
        assert_eq!(points[0].service_id, "web");
        assert_eq!(
            (points[0].requests, points[0].lat_le_1s, points[0].lat_total),
            (5, 4, 5)
        );
    }

    #[test]
    fn maps_traefik_services_to_maestro_services() {
        assert_eq!(split_replica_service("web"), ("web".to_string(), None));
        assert_eq!(
            split_replica_service("my-api"),
            ("my-api".to_string(), None)
        );
        assert_eq!(
            split_replica_service("my-api-dmv7n1a4xao07-12"),
            ("my-api".to_string(), Some("dmv7n1a4xao07".to_string()))
        );
        let key = series_key(&HashMap::from([(
            "service".to_string(),
            "maestro-denied@file".to_string(),
        )]));
        assert!(key.is_none());
    }
}
