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
            origin: log_config.origin,
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

    // Try logrus format: time="2026-03-20T05:35:46Z" level=fatal msg="..."
    if line.starts_with("time=\"") {
        if let Some(parsed) = parse_logrus_line(line) {
            return parsed;
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

fn parse_logrus_line(line: &str) -> Option<ParsedLine> {
    let extract_quoted = |key: &str| -> Option<String> {
        let prefix = format!("{key}=\"");
        let start = line.find(&prefix)? + prefix.len();
        let mut end = start;
        while end < line.len() {
            if line.as_bytes()[end] == b'"' && (end == start || line.as_bytes()[end - 1] != b'\\') {
                break;
            }
            end += 1;
        }
        Some(line[start..end].replace("\\\"", "\""))
    };
    let extract_bare = |key: &str| -> Option<&str> {
        let prefix = format!("{key}=");
        let start = line.find(&prefix)? + prefix.len();
        let end = line[start..]
            .find(' ')
            .map(|i| i + start)
            .unwrap_or(line.len());
        Some(&line[start..end])
    };

    let ts = extract_quoted("time")
        .as_deref()
        .and_then(|s| parse_iso_timestamp(s));
    let level = extract_bare("level").map(normalize_level);
    let msg = extract_quoted("msg").unwrap_or_else(|| line.to_string());
    Some(ParsedLine {
        ts,
        level,
        text: msg,
    })
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
#[path = "../tests/supervisor/logs.rs"]
mod tests;
