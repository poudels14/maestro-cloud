use super::*;

#[test]
fn json_with_ts_level_msg() {
    let line = r#"{"ts":"2026-03-15T20:28:36Z","level":"error","msg":"connection refused"}"#;
    let parsed = parse_log_line(line);
    assert_eq!(parsed.text, "connection refused");
    assert_eq!(parsed.level.as_deref(), Some("error"));
    assert!(parsed.ts.is_some());
}

#[test]
fn json_with_timestamp_and_message_fields() {
    let line = r#"{"timestamp":"2026-01-01T00:00:00Z","level":"warn","message":"disk full"}"#;
    let parsed = parse_log_line(line);
    assert_eq!(parsed.text, "disk full");
    assert_eq!(parsed.level.as_deref(), Some("warn"));
    assert!(parsed.ts.is_some());
}

#[test]
fn json_with_numeric_ts() {
    let line = r#"{"ts":1710000000000,"level":"info","msg":"started"}"#;
    let parsed = parse_log_line(line);
    assert_eq!(parsed.text, "started");
    assert_eq!(parsed.ts, Some(1710000000000));
}

#[test]
fn json_with_time_field_for_timestamp() {
    let line = r#"{"level":"info","time":"2026-05-21T07:05:29.899Z","msg":"request"}"#;
    let parsed = parse_log_line(line);
    assert_eq!(parsed.text, "request");
    assert_eq!(parsed.level.as_deref(), Some("info"));
    assert!(parsed.ts.is_some());
}

#[test]
fn json_collects_extra_fields_as_attrs() {
    let line = r#"{"level":"info","time":"2026-05-21T07:05:29.899Z","http.method":"GET","http.url_details.path":"/api/trpc/users.getCurrentUser","http.status_code":200,"duration":933354417,"msg":"request"}"#;
    let parsed = parse_log_line(line);
    assert_eq!(parsed.text, "request");
    assert_eq!(parsed.level.as_deref(), Some("info"));
    assert!(parsed.ts.is_some());

    let attrs: std::collections::HashMap<_, _> = parsed.attrs.into_iter().collect();
    assert_eq!(attrs.len(), 4);
    assert_eq!(attrs.get("http.method").map(String::as_str), Some("GET"));
    assert_eq!(
        attrs.get("http.url_details.path").map(String::as_str),
        Some("/api/trpc/users.getCurrentUser")
    );
    assert_eq!(
        attrs.get("http.status_code").map(String::as_str),
        Some("200")
    );
    assert_eq!(attrs.get("duration").map(String::as_str), Some("933354417"));
}

#[test]
fn ingress_access_logs_use_forwarded_ip_only_for_internal_proxies() {
    let mut attrs = vec![
        ("ClientHost".to_string(), "172.22.0.20".to_string()),
        (
            "request_CF-Connecting-IP".to_string(),
            "203.0.113.9".to_string(),
        ),
    ];
    normalize_ingress_access_log_attrs(&mut attrs);
    assert_eq!(find_attr(&attrs, "maestro.client_ip"), Some("203.0.113.9"));

    let mut spoofed = vec![
        ("ClientHost".to_string(), "198.51.100.8".to_string()),
        (
            "request_CF-Connecting-IP".to_string(),
            "203.0.113.9".to_string(),
        ),
    ];
    normalize_ingress_access_log_attrs(&mut spoofed);
    assert_eq!(
        find_attr(&spoofed, "maestro.client_ip"),
        Some("198.51.100.8")
    );
}

#[test]
fn cloudflare_tunnel_access_log_keeps_the_normalized_visitor_ip() {
    let visitor = "2607:f598:f0e9:c000:b0aa:3a2e:a438:d81f";
    let mut attrs = vec![
        ("ClientAddr".to_string(), "10.100.0.255:55258".to_string()),
        ("ClientHost".to_string(), visitor.to_string()),
        ("request_Cf-Connecting-Ip".to_string(), visitor.to_string()),
        ("request_X-Forwarded-For".to_string(), visitor.to_string()),
    ];

    normalize_ingress_access_log_attrs(&mut attrs);

    assert_eq!(find_attr(&attrs, "maestro.client_ip"), Some(visitor));
}

#[test]
fn traefik_access_log_exposes_structured_request_fields() {
    let line = r#"{"ClientHost":"10.100.0.255","DownstreamStatus":200,"Duration":186492218,"RequestHost":"app.example.com","RequestMethod":"GET","RequestPath":"/api/users?page=2","RequestScheme":"https","RouterName":"app@etcd","ServiceName":"app@etcd","entryPointName":"web","level":"info","msg":"","request_CF-Connecting-IP":"203.0.113.9","time":"2026-07-14T00:01:12Z"}"#;
    let mut parsed = parse_log_line(line);
    normalize_ingress_access_log_attrs(&mut parsed.attrs);
    let emitted = sanitize_ingress_request_path(line, &mut parsed);

    assert!(!parsed.text.contains("page=2"));
    assert!(!emitted.contains("page=2"));
    assert_eq!(parsed.level.as_deref(), Some("info"));
    assert_eq!(find_attr(&parsed.attrs, "RequestMethod"), Some("GET"));
    assert_eq!(find_attr(&parsed.attrs, "RequestPath"), Some("/api/users"));
    assert_eq!(find_attr(&parsed.attrs, "DownstreamStatus"), Some("200"));
    assert_eq!(
        find_attr(&parsed.attrs, "RequestHost"),
        Some("app.example.com")
    );
    assert_eq!(find_attr(&parsed.attrs, "RouterName"), Some("app@etcd"));
    assert_eq!(
        find_attr(&parsed.attrs, "maestro.log_type"),
        Some("ingress_access")
    );
    assert_eq!(
        find_attr(&parsed.attrs, "maestro.client_ip"),
        Some("203.0.113.9")
    );
}

#[test]
fn ingress_lifecycle_output_is_not_marked_as_an_access_log() {
    let mut attrs = Vec::new();

    normalize_ingress_access_log_attrs(&mut attrs);

    assert_eq!(find_attr(&attrs, "maestro.log_type"), None);
}

#[test]
fn json_attrs_serialize_nested_values() {
    let line = r#"{"msg":"x","obj":{"a":1},"arr":[1,2],"flag":true,"nothing":null}"#;
    let parsed = parse_log_line(line);
    let attrs: std::collections::HashMap<_, _> = parsed.attrs.into_iter().collect();
    assert_eq!(attrs.get("obj").map(String::as_str), Some(r#"{"a":1}"#));
    assert_eq!(attrs.get("arr").map(String::as_str), Some("[1,2]"));
    assert_eq!(attrs.get("flag").map(String::as_str), Some("true"));
    assert_eq!(attrs.get("nothing").map(String::as_str), Some("null"));
}

#[test]
fn json_without_msg_uses_full_line() {
    let line = r#"{"level":"debug","data":"something"}"#;
    let parsed = parse_log_line(line);
    assert_eq!(parsed.text, line);
    assert_eq!(parsed.level.as_deref(), Some("debug"));
}

#[test]
fn iso_prefixed_with_level() {
    let line = "2026-03-15T20:28:36Z ERR Provider error, retrying in 5s";
    let parsed = parse_log_line(line);
    assert_eq!(parsed.text, "Provider error, retrying in 5s");
    assert_eq!(parsed.level.as_deref(), Some("error"));
    assert!(parsed.ts.is_some());
}

#[test]
fn iso_prefixed_with_info_level() {
    let line = "2026-03-15T20:28:36.123Z INFO server listening on :8080";
    let parsed = parse_log_line(line);
    assert_eq!(parsed.text, "server listening on :8080");
    assert_eq!(parsed.level.as_deref(), Some("info"));
    assert!(parsed.ts.is_some());
}

#[test]
fn iso_prefixed_unknown_token_is_message_not_level() {
    let line = "2026-03-15T20:28:36Z HTTPS://HTTPBIN.ORG/STATUS/200 error <urlopen error>";
    let parsed = parse_log_line(line);
    assert_eq!(
        parsed.text,
        "HTTPS://HTTPBIN.ORG/STATUS/200 error <urlopen error>"
    );
    assert!(parsed.level.is_none());
    assert!(parsed.ts.is_some());
}

#[test]
fn slash_timestamp_format() {
    let line = "2026/03/18 08:39:04 Starting up on port 80";
    let parsed = parse_log_line(line);
    assert_eq!(parsed.text, "Starting up on port 80");
    assert!(parsed.ts.is_some());
    assert!(parsed.level.is_none());
}

#[test]
fn slash_timestamp_with_extra_spaces() {
    let line = "2026/03/18 08:39:04   padded message";
    let parsed = parse_log_line(line);
    assert_eq!(parsed.text, "padded message");
    assert!(parsed.ts.is_some());
}

#[test]
fn plain_text() {
    let line = "just a plain log message";
    let parsed = parse_log_line(line);
    assert_eq!(parsed.text, "just a plain log message");
    assert!(parsed.ts.is_none());
    assert!(parsed.level.is_none());
}

#[test]
fn empty_line() {
    let parsed = parse_log_line("");
    assert_eq!(parsed.text, "");
    assert!(parsed.ts.is_none());
    assert!(parsed.level.is_none());
}

#[test]
fn normalize_level_aliases() {
    assert_eq!(normalize_level("ERR"), "error");
    assert_eq!(normalize_level("fatal"), "error");
    assert_eq!(normalize_level("PANIC"), "error");
    assert_eq!(normalize_level("warning"), "warn");
    assert_eq!(normalize_level("DBG"), "debug");
    assert_eq!(normalize_level("INFO"), "info");
    assert_eq!(normalize_level("TRACE"), "trace");
    assert_eq!(normalize_level("custom"), "custom");
}

#[test]
fn strip_ansi_codes() {
    let input = "\x1b[31mred text\x1b[0m";
    let stripped = strip_ansi(input);
    assert_eq!(stripped, "red text");
}

#[test]
fn parse_strips_ansi_from_input() {
    let line = "\x1b[31mERR\x1b[0m something broke";
    let parsed = parse_log_line(&format!("2026-03-15T20:28:36Z {line}"));
    assert_eq!(parsed.level.as_deref(), Some("error"));
    assert_eq!(parsed.text, "something broke");
}

#[test]
fn parse_strips_ansi_from_plain_text() {
    let line = "[23:41:21.644] \x1b[32mINFO\x1b[39m: \x1b[36mrequest\x1b[39m";
    let parsed = parse_log_line(line);
    assert_eq!(parsed.text, "[23:41:21.644] INFO: request");
}

#[test]
fn logrus_format() {
    let line = r#"time="2026-03-20T05:35:46Z" level=fatal msg="failed to load networking flags: exec: \"iptables\": executable file not found in $PATH""#;
    let parsed = parse_log_line(line);
    assert_eq!(parsed.level.as_deref(), Some("error"));
    assert!(parsed.ts.is_some());
    assert!(parsed.text.contains("iptables"));
}

#[test]
fn logrus_format_info() {
    let line = r#"time="2026-03-20T01:00:00Z" level=info msg="server started""#;
    let parsed = parse_log_line(line);
    assert_eq!(parsed.level.as_deref(), Some("info"));
    assert_eq!(parsed.text, "server started");
    assert!(parsed.ts.is_some());
}

#[test]
fn logrus_format_with_extra_attrs() {
    let line = r#"time="2026-03-20T01:00:00Z" level=error msg="the service does not exist" entryPointName=web routerName=service-1@etcd"#;
    let parsed = parse_log_line(line);
    assert_eq!(parsed.level.as_deref(), Some("error"));
    assert_eq!(parsed.text, "the service does not exist");
    assert_eq!(parsed.attrs.len(), 2);
    assert_eq!(
        parsed.attrs[0],
        ("entryPointName".to_string(), "web".to_string())
    );
    assert_eq!(
        parsed.attrs[1],
        ("routerName".to_string(), "service-1@etcd".to_string())
    );
    assert!(parsed.ts.is_some());
}
