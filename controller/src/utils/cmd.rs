use std::ffi::OsStr;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Result, anyhow};
use tokio::io::AsyncBufReadExt;
use tokio::process::Command;

use crate::logs::{LogEntry, LogOrigin};

pub async fn run<S: AsRef<OsStr>>(program: &str, args: &[S]) -> Result<String> {
    let output = Command::new(program)
        .args(args)
        .output()
        .await
        .map_err(|err| anyhow!("failed to run {program}: {err}"))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(anyhow!("{} failed: {stderr}", label(program, args)));
    }
    Ok(String::from_utf8_lossy(&output.stdout).to_string())
}

pub async fn run_in<S: AsRef<OsStr>>(program: &str, args: &[S], dir: &Path) -> Result<String> {
    let output = Command::new(program)
        .args(args)
        .current_dir(dir)
        .output()
        .await
        .map_err(|err| anyhow!("failed to run {program}: {err}"))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(anyhow!("{} failed: {stderr}", label(program, args)));
    }
    Ok(String::from_utf8_lossy(&output.stdout).to_string())
}

pub async fn run_with_logs<S: AsRef<OsStr>>(
    program: &str,
    args: &[S],
    sender: &flume::Sender<LogEntry>,
    source: &str,
    origin: LogOrigin,
) -> Result<()> {
    let source: Arc<str> = Arc::from(source);
    let mut child = Command::new(program)
        .args(args)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .map_err(|err| anyhow!("failed to spawn {program}: {err}"))?;

    let stdout = child.stdout.take();
    let stderr = child.stderr.take();

    let sender_clone = sender.clone();
    let source_clone = source.clone();
    let stdout_task = tokio::spawn(async move {
        if let Some(out) = stdout {
            pipe_to_collector(out, "stdout", &source_clone, &sender_clone, origin).await;
        }
    });

    let sender_clone = sender.clone();
    let source_clone = source.clone();
    let stderr_task = tokio::spawn(async move {
        if let Some(err_stream) = stderr {
            pipe_to_collector(err_stream, "stderr", &source_clone, &sender_clone, origin).await;
        }
    });

    let _ = stdout_task.await;
    let _ = stderr_task.await;

    let status = child
        .wait()
        .await
        .map_err(|err| anyhow!("failed to wait for {program}: {err}"))?;
    if !status.success() {
        return Err(anyhow!("{} failed", label(program, args)));
    }
    Ok(())
}

async fn pipe_to_collector(
    reader: impl tokio::io::AsyncRead + Unpin,
    stream_name: &str,
    source: &Arc<str>,
    sender: &flume::Sender<LogEntry>,
    origin: LogOrigin,
) {
    let stream: Arc<str> = Arc::from(stream_name);
    let mut lines = tokio::io::BufReader::new(reader).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        let entry = LogEntry {
            seq: 0,
            ts,
            level: Arc::from("info"),
            stream: stream.clone(),
            text: line,
            source: source.clone(),
            origin,
            tags: Arc::new(serde_json::Value::Null),
            attrs: vec![],
        };
        if sender.send_async(entry).await.is_err() {
            break;
        }
    }
}

fn label<S: AsRef<OsStr>>(program: &str, args: &[S]) -> String {
    match args.first() {
        Some(sub) => format!("{program} {}", sub.as_ref().to_string_lossy()),
        None => program.to_string(),
    }
}
