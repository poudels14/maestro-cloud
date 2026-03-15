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
    stream: &'a str,
    text: &'a str,
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

    while let Ok(Some(raw_line)) = lines.next_line().await {
        let line = strip_ansi(&raw_line);
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        // If the line is structured JSON with a msg/message field, use that as text.
        let text = if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&line) {
            parsed
                .get("msg")
                .or_else(|| parsed.get("message"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
        } else {
            None
        };

        let entry = LogEntry {
            ts,
            stream,
            text: text.as_deref().unwrap_or(&line),
        };

        if let Ok(json) = serde_json::to_string(&entry) {
            if let Ok(mut file) = log_file.lock() {
                let _ = writeln!(file, "{json}");
            }
        }
    }
}

fn strip_ansi(s: &str) -> String {
    let bytes = strip_ansi_escapes::strip(s);
    String::from_utf8(bytes).unwrap_or_else(|e| String::from_utf8_lossy(e.as_bytes()).into_owned())
}
