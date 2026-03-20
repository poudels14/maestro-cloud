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
