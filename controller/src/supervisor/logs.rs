use std::{
    io::Write,
    path::Path,
    sync::{Arc, Mutex as StdMutex},
};

use anyhow::Result;
use tokio::io::AsyncBufReadExt;

#[derive(serde::Serialize)]
struct LogEntry<'a> {
    ts: u64,
    level: &'a str,
    stream: &'a str,
    text: &'a str,
}

struct ParsedLine {
    ts: Option<u64>,
    level: Option<String>,
    text: String,
}

pub async fn read_jsonl_logs(path: &Path, tail: usize) -> Result<Vec<serde_json::Value>> {
    let content = match tokio::fs::read_to_string(path).await {
        Ok(content) => content,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => return Err(err.into()),
    };

    let lines: Vec<&str> = content.lines().filter(|line| !line.is_empty()).collect();
    let start = lines.len().saturating_sub(tail);
    let entries = lines[start..]
        .iter()
        .filter_map(|line| serde_json::from_str(line).ok())
        .collect();

    Ok(entries)
}

pub async fn read_pipe_to_jsonl(
    reader: os_pipe::PipeReader,
    stream: &'static str,
    log_file: Arc<StdMutex<std::fs::File>>,
) {
    use std::os::unix::io::{FromRawFd, IntoRawFd};

    let fd = reader.into_raw_fd();
    // SAFETY: fd is a valid owned file descriptor from os_pipe::PipeReader.
    let std_file = unsafe { std::fs::File::from_raw_fd(fd) };
    let async_file = tokio::fs::File::from_std(std_file);
    let buf_reader = tokio::io::BufReader::new(async_file);
    let mut lines = buf_reader.lines();

    let now_millis = || {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    };

    while let Ok(Some(raw_line)) = lines.next_line().await {
        let line = strip_ansi(&raw_line);
        let parsed = parse_log_line(&line);

        let entry = LogEntry {
            ts: parsed.ts.unwrap_or_else(&now_millis),
            level: parsed.level.as_deref().unwrap_or("info"),
            stream,
            text: &parsed.text,
        };

        if let Ok(json) = serde_json::to_string(&entry) {
            if let Ok(mut file) = log_file.lock() {
                let _ = writeln!(file, "{json}");
            }
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

fn strip_ansi(s: &str) -> String {
    let bytes = strip_ansi_escapes::strip(s);
    String::from_utf8(bytes).unwrap_or_else(|e| String::from_utf8_lossy(e.as_bytes()).into_owned())
}
