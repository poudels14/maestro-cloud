use std::sync::Arc;

use tokio::io::AsyncBufReadExt;

struct ParsedLine {
    ts: Option<u64>,
    level: Option<String>,
    text: String,
}

pub async fn read_pipe_to_collector(
    reader: os_pipe::PipeReader,
    stream: &'static str,
    source: Arc<str>,
    log_config: crate::logs::LogConfig,
) {
    use std::os::unix::io::{FromRawFd, IntoRawFd};

    let fd = reader.into_raw_fd();
    let std_file = unsafe { std::fs::File::from_raw_fd(fd) };
    let async_file = tokio::fs::File::from_std(std_file);
    let buf_reader = tokio::io::BufReader::new(async_file);
    let mut lines = buf_reader.lines();

    let stream: Arc<str> = Arc::from(stream);
    let tags = log_config.build_tags();
    let now_millis = || {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64
    };

    while let Ok(Some(raw_line)) = lines.next_line().await {
        let line = strip_ansi(&raw_line);
        let parsed = parse_log_line(&line);

        let entry = crate::logs::LogEntry {
            seq: 0,
            ts: parsed.ts.map(|t| t as i64).unwrap_or_else(&now_millis),
            level: parsed
                .level
                .map(Arc::from)
                .unwrap_or_else(|| Arc::from("info")),
            stream: stream.clone(),
            text: parsed.text,
            source: source.clone(),
            system: log_config.system,
            tags: tags.clone(),
        };

        if log_config.sender.send_async(entry).await.is_err() {
            break;
        }
    }
}

/// Parse a log line, extracting timestamp, level, and message from:
/// - JSON logs: `{"ts":"...","level":"...","msg":"..."}`
/// - Prefixed logs: `2026-03-15T20:28:36Z ERR Provider error, retrying...`
/// - Plain text: everything else
fn parse_log_line(line: &str) -> ParsedLine {
    // Try JSON first
    if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(line) {
        let ts = parsed
            .get("ts")
            .or_else(|| parsed.get("timestamp"))
            .and_then(|v| match v {
                serde_json::Value::String(s) => parse_iso_timestamp(s),
                serde_json::Value::Number(n) => n.as_u64(),
                _ => None,
            });

        let level = parsed
            .get("level")
            .and_then(|v| v.as_str())
            .map(normalize_level);

        let text = parsed
            .get("msg")
            .or_else(|| parsed.get("message"))
            .and_then(|v| v.as_str())
            .unwrap_or(line)
            .to_string();

        return ParsedLine { ts, level, text };
    }

    // Try "2026-03-15T20:28:36Z ERR message..." format
    if line.len() > 20 && line.as_bytes()[4] == b'-' && line.as_bytes()[10] == b'T' {
        if let Some(space_idx) = line[..25.min(line.len())].find(' ') {
            let ts_str = &line[..space_idx];
            if let Some(ts) = parse_iso_timestamp(ts_str) {
                let rest = line[space_idx + 1..].trim_start();
                if let Some(msg_start) = rest.find(' ') {
                    let level_str = &rest[..msg_start];
                    let text = rest[msg_start + 1..].trim_start();
                    return ParsedLine {
                        ts: Some(ts),
                        level: Some(normalize_level(level_str)),
                        text: text.to_string(),
                    };
                }
            }
        }
    }

    // Try "2026/03/18 08:39:04 message..." format
    if line.len() > 19 && line.as_bytes()[4] == b'/' && line.as_bytes()[7] == b'/' {
        if let Some(ts) = parse_slash_timestamp(&line[..19]) {
            let text = line[19..].trim_start().to_string();
            return ParsedLine {
                ts: Some(ts),
                level: None,
                text,
            };
        }
    }

    ParsedLine {
        ts: None,
        level: None,
        text: line.to_string(),
    }
}

fn parse_iso_timestamp(s: &str) -> Option<u64> {
    let s = s.trim_matches('"');
    let dt = chrono::DateTime::parse_from_rfc3339(s)
        .or_else(|_| {
            chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f")
                .map(|naive| naive.and_utc().fixed_offset())
        })
        .ok()?;
    Some(dt.timestamp_millis() as u64)
}

fn normalize_level(s: &str) -> String {
    match s.to_ascii_lowercase().as_str() {
        "err" | "error" | "fatal" | "panic" => "error".to_string(),
        "warn" | "warning" => "warn".to_string(),
        "info" => "info".to_string(),
        "debug" | "dbg" => "debug".to_string(),
        "trace" => "trace".to_string(),
        other => other.to_lowercase(),
    }
}

fn parse_slash_timestamp(s: &str) -> Option<u64> {
    let naive = chrono::NaiveDateTime::parse_from_str(s, "%Y/%m/%d %H:%M:%S").ok()?;
    Some(naive.and_utc().timestamp_millis() as u64)
}

fn strip_ansi(s: &str) -> String {
    let bytes = strip_ansi_escapes::strip(s);
    String::from_utf8(bytes).unwrap_or_else(|e| String::from_utf8_lossy(e.as_bytes()).into_owned())
}

#[cfg(test)]
mod tests {
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
}
