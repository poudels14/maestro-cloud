use std::collections::BTreeMap;

use kernel_api::Timestamp;
use proptest::prelude::*;

use crate::{
    DatePrefixedLogParser, JsonLogParser, LogBody, LogParser, LogrusLogParser, PlainTextLogParser,
    Rfc3339PrefixedLogParser, standard_parsers,
};

#[test]
fn json_parser_extracts_and_normalizes_canonical_fields() {
    let parsed = JsonLogParser
        .parse(
            br#"{"time":"2026-05-21T07:05:29.899Z","level":"WARNING","msg":"slow","attempt":2,"obj":{"a":1}}"#,
        )
        .expect("JSON object should parse");

    assert_eq!(parsed.event_at, Some(Timestamp(1_779_347_129_899)));
    assert_eq!(parsed.severity.as_deref(), Some("warn"));
    assert_eq!(parsed.body, LogBody::Text("slow".to_owned()));
    assert_eq!(
        parsed.attributes,
        BTreeMap::from([
            ("attempt".to_owned(), "2".to_owned()),
            ("obj".to_owned(), r#"{"a":1}"#.to_owned()),
        ])
    );
}

#[test]
fn json_parser_accepts_numeric_timestamps_and_preserves_full_body_without_message() {
    let line = br#"{"ts":1710000000000,"level":"debug","data":"something"}"#;
    let parsed = JsonLogParser.parse(line).expect("JSON object should parse");

    assert_eq!(parsed.event_at, Some(Timestamp(1_710_000_000_000)));
    assert_eq!(parsed.severity.as_deref(), Some("debug"));
    assert_eq!(
        parsed.body,
        LogBody::Text(String::from_utf8_lossy(line).into_owned())
    );
    assert_eq!(
        parsed.attributes.get("data").map(String::as_str),
        Some("something")
    );
}

#[test]
fn traefik_access_log_sanitizes_path_and_trusts_internal_proxy_headers() {
    let parsed = JsonLogParser
        .parse(br#"{"ClientHost":"172.22.0.20","DownstreamStatus":200,"RequestHost":"app.example.com","RequestMethod":"GET","RequestPath":"/api/users?page=2","RouterName":"app@etcd","level":"info","msg":"","request_CF-Connecting-IP":"203.0.113.9","time":"2026-07-14T00:01:12Z"}"#)
        .expect("Traefik JSON should parse");

    let body = match &parsed.body {
        LogBody::Text(body) => Some(body.as_str()),
        LogBody::Bytes(_) => None,
    };
    assert!(body.is_some_and(|body| !body.contains("page=2")));
    assert_eq!(parsed.severity.as_deref(), Some("info"));
    assert_eq!(
        parsed.attributes.get("RequestPath").map(String::as_str),
        Some("/api/users")
    );
    assert_eq!(
        parsed
            .attributes
            .get("maestro.log_type")
            .map(String::as_str),
        Some("ingress_access")
    );
    assert_eq!(
        parsed
            .attributes
            .get("maestro.client_ip")
            .map(String::as_str),
        Some("203.0.113.9")
    );
}

#[test]
fn traefik_access_log_rejects_spoofed_forwarded_address_from_external_client() {
    let parsed = JsonLogParser
        .parse(br#"{"ClientHost":"198.51.100.8","DownstreamStatus":200,"RequestMethod":"GET","RequestPath":"/","request_X-Forwarded-For":"203.0.113.9"}"#)
        .expect("Traefik JSON should parse");

    assert_eq!(
        parsed
            .attributes
            .get("maestro.client_ip")
            .map(String::as_str),
        Some("198.51.100.8")
    );
}

#[test]
fn ordinary_json_is_not_marked_as_an_access_log() {
    let parsed = JsonLogParser
        .parse(br#"{"level":"info","msg":"ready"}"#)
        .expect("JSON object should parse");

    assert!(!parsed.attributes.contains_key("maestro.log_type"));
}

#[test]
fn rfc3339_prefix_extracts_known_levels_and_keeps_unknown_tokens() {
    let known = Rfc3339PrefixedLogParser
        .parse(b"2026-03-15T20:28:36Z ERR Provider error")
        .expect("timestamped log should parse");
    assert_eq!(known.severity.as_deref(), Some("error"));
    assert_eq!(known.body, LogBody::Text("Provider error".to_owned()));

    let unknown = Rfc3339PrefixedLogParser
        .parse(b"2026-03-15T20:28:36Z HTTPS://EXAMPLE invalid response")
        .expect("timestamped log should parse");
    assert_eq!(unknown.severity, None);
    assert_eq!(
        unknown.body,
        LogBody::Text("HTTPS://EXAMPLE invalid response".to_owned())
    );
}

#[test]
fn ansi_is_removed_before_structured_and_plain_parsing() {
    let structured = Rfc3339PrefixedLogParser
        .parse(b"2026-03-15T20:28:36Z \x1b[31mERR\x1b[0m something broke")
        .expect("ANSI timestamped log should parse");
    assert_eq!(structured.severity.as_deref(), Some("error"));
    assert_eq!(structured.body, LogBody::Text("something broke".to_owned()));

    let plain = PlainTextLogParser
        .parse(b"[23:41] \x1b[32mINFO\x1b[39m: request")
        .expect("fallback parser");
    assert_eq!(
        plain.body,
        LogBody::Text("[23:41] INFO: request".to_owned())
    );
}

#[test]
fn logrus_parser_handles_escaped_messages_and_extra_attributes() {
    let parsed = LogrusLogParser
        .parse(br#"time="2026-03-20T05:35:46Z" level=fatal msg="exec: \"iptables\" not found" entryPointName=web routerName=service-1@etcd"#)
        .expect("logrus record should parse");

    assert_eq!(parsed.event_at, Some(Timestamp(1_773_984_946_000)));
    assert_eq!(parsed.severity.as_deref(), Some("error"));
    assert_eq!(
        parsed.body,
        LogBody::Text("exec: \"iptables\" not found".to_owned())
    );
    assert_eq!(
        parsed.attributes,
        BTreeMap::from([
            ("entryPointName".to_owned(), "web".to_owned()),
            ("routerName".to_owned(), "service-1@etcd".to_owned()),
        ])
    );
}

#[test]
fn slash_date_parser_extracts_timestamp_and_trims_padding() {
    let parsed = DatePrefixedLogParser
        .parse(b"2026/03/18 08:39:04   Starting up")
        .expect("date-prefixed record should parse");

    assert_eq!(parsed.event_at, Some(Timestamp(1_773_823_144_000)));
    assert_eq!(parsed.severity, None);
    assert_eq!(parsed.body, LogBody::Text("Starting up".to_owned()));
}

#[test]
fn standard_chain_is_ordered_and_ends_in_a_byte_preserving_fallback() {
    let json = parse_standard(br#"{"msg":"structured"}"#);
    assert_eq!(json.body, LogBody::Text("structured".to_owned()));

    let invalid_utf8 = [0xff, b'a'];
    let fallback = parse_standard(&invalid_utf8);
    assert_eq!(fallback.body, LogBody::Bytes(invalid_utf8.to_vec()));
}

fn parse_standard(payload: &[u8]) -> crate::ParsedLog {
    standard_parsers()
        .iter()
        .find_map(|parser| parser.parse(payload))
        .expect("standard chain has a total fallback")
}

proptest! {
    #[test]
    fn every_parser_is_total_over_arbitrary_bytes(payload in proptest::collection::vec(any::<u8>(), 0..2048)) {
        let parsers = standard_parsers();
        for parser in &parsers {
            let _ = parser.parse(&payload);
        }
        prop_assert!(parsers.iter().any(|parser| parser.parse(&payload).is_some()));
    }
}
