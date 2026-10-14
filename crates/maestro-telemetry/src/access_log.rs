use std::net::IpAddr;

use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const INGRESS_REPLICA: &str = "maestro-traefik";
pub const BLOCKED_ROUTER_PREFIX: &str = "maestro-blocked-";
const MINUTE_MS: i64 = 60_000;
const MAX_PATH_LENGTH: usize = 2048;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AccessEvent {
    pub bucket_at_ms: i64,
    pub router: String,
    pub client_ip: String,
    pub path: String,
    pub status_code: u16,
    pub at_ms: i64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TrafficBreakdownEntry {
    pub value: String,
    pub status_code: u16,
    pub requests: u64,
    pub last_seen_at_ms: i64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IngressTrafficBreakdown {
    pub by_ip: Vec<TrafficBreakdownEntry>,
    pub by_path: Vec<TrafficBreakdownEntry>,
}

pub fn parse_access_event(line: &str, ts: i64) -> Option<AccessEvent> {
    let entry: Value = serde_json::from_str(line).ok()?;
    let status_code = entry["DownstreamStatus"]
        .as_u64()
        .filter(|status| (100..600).contains(status))? as u16;
    let router = entry["RouterName"].as_str()?.to_string();
    let path = entry["RequestPath"].as_str()?;
    let path = path.split('?').next().unwrap_or(path);
    let path = path.chars().take(MAX_PATH_LENGTH).collect::<String>();
    let client_ip = client_ip(&entry)?;
    Some(AccessEvent {
        bucket_at_ms: ts - ts.rem_euclid(MINUTE_MS),
        router,
        client_ip,
        path,
        status_code,
        at_ms: ts,
    })
}

fn client_ip(entry: &Value) -> Option<String> {
    let direct = entry["ClientHost"].as_str()?.parse::<IpAddr>().ok()?;
    let header = |name: &str| {
        let key = format!("request_{name}");
        entry
            .as_object()?
            .iter()
            .find(|(field, _)| field.eq_ignore_ascii_case(&key))
            .and_then(|(_, value)| value.as_str())
            .map(str::trim)
    };
    let forwarded = header("CF-Connecting-IP")
        .and_then(|value| value.parse::<IpAddr>().ok())
        .or_else(|| header("X-Real-IP").and_then(|value| value.parse::<IpAddr>().ok()))
        .or_else(|| {
            header("X-Forwarded-For")?
                .split(',')
                .find_map(|value| value.trim().parse::<IpAddr>().ok())
        });
    let ip = match is_internal_proxy(&direct) {
        true => forwarded.unwrap_or(direct),
        false => direct,
    };
    Some(ip.to_string())
}

fn is_internal_proxy(ip: &IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => {
            let [first, second, ..] = ip.octets();
            ip.is_private()
                || ip.is_loopback()
                || ip.is_link_local()
                || (first == 100 && (64..128).contains(&second))
        }
        IpAddr::V6(ip) => {
            let first = ip.segments()[0];
            ip.is_loopback() || (first & 0xfe00) == 0xfc00 || (first & 0xffc0) == 0xfe80
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_access_lines_and_resolves_client_ips() {
        let line = r#"{"ClientHost":"10.0.0.5","DownstreamStatus":404,"RouterName":"svc-web@file","RequestPath":"/a/b?token=x","request_X-Forwarded-For":"bad, 203.0.113.9, 10.0.0.1"}"#;
        let event = parse_access_event(line, 125_000).unwrap();
        assert_eq!(event.bucket_at_ms, 120_000);
        assert_eq!(event.client_ip, "203.0.113.9");
        assert_eq!(event.path, "/a/b");
        assert_eq!(event.status_code, 404);

        let direct = r#"{"ClientHost":"198.51.100.7","DownstreamStatus":200,"RouterName":"svc-web@file","RequestPath":"/","request_CF-Connecting-IP":"203.0.113.1"}"#;
        assert_eq!(
            parse_access_event(direct, 0).unwrap().client_ip,
            "198.51.100.7"
        );

        let cloudflare = r#"{"ClientHost":"127.0.0.1","DownstreamStatus":200,"RouterName":"svc-web@file","RequestPath":"/","request_Cf-Connecting-Ip":"203.0.113.1","request_X-Real-Ip":"203.0.113.2"}"#;
        assert_eq!(
            parse_access_event(cloudflare, 0).unwrap().client_ip,
            "203.0.113.1"
        );

        assert!(parse_access_event("plain text", 0).is_none());
        assert!(parse_access_event(r#"{"level":"warn"}"#, 0).is_none());
    }
}
