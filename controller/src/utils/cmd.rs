use std::collections::HashMap;
use std::ffi::OsStr;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Result, anyhow};
use tokio::io::AsyncBufReadExt;
use tokio::process::Command;

use crate::logs::{LogEntry, LogOrigin};
use crate::supervisor::logs::parse_log_line;
use crate::utils::crypto::SecretString;

pub async fn run<S: AsRef<OsStr>>(program: &str, args: &[S]) -> Result<String> {
    exec(program, args).run().await
}

pub fn exec<'a, S: AsRef<OsStr>>(program: &'a str, args: &'a [S]) -> Cmd<'a, S> {
    Cmd {
        program,
        args,
        dir: None,
        env: None,
    }
}

pub struct Cmd<'a, S: AsRef<OsStr>> {
    program: &'a str,
    args: &'a [S],
    dir: Option<&'a Path>,
    env: Option<&'a HashMap<String, SecretString>>,
}

impl<'a, S: AsRef<OsStr>> Cmd<'a, S> {
    pub fn dir(mut self, dir: &'a Path) -> Self {
        self.dir = Some(dir);
        self
    }

    pub fn env(mut self, env: &'a HashMap<String, SecretString>) -> Self {
        self.env = Some(env);
        self
    }

    fn apply_options(&self, cmd: &mut Command) {
        if let Some(dir) = self.dir {
            cmd.current_dir(dir);
        }
        if let Some(env) = self.env {
            for (key, value) in env {
                cmd.env(key, value.as_str());
            }
        }
    }

    pub async fn run(self) -> Result<String> {
        let mut cmd = Command::new(self.program);
        cmd.args(self.args);
        self.apply_options(&mut cmd);
        let output = cmd
            .output()
            .await
            .map_err(|err| anyhow!("failed to run {}: {err}", self.program))?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(anyhow!(
                "{} failed: {stderr}",
                label(self.program, self.args)
            ));
        }
        Ok(String::from_utf8_lossy(&output.stdout).to_string())
    }

    pub async fn run_with_logs(
        self,
        sender: &flume::Sender<LogEntry>,
        source: &str,
        origin: LogOrigin,
    ) -> Result<()> {
        let source: Arc<str> = Arc::from(source);
        let mut cmd = Command::new(self.program);
        cmd.args(self.args)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());
        self.apply_options(&mut cmd);
        let mut child = cmd
            .spawn()
            .map_err(|err| anyhow!("failed to spawn {}: {err}", self.program))?;

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
            .map_err(|err| anyhow!("failed to wait for {}: {err}", self.program))?;
        if !status.success() {
            return Err(anyhow!("{} failed", label(self.program, self.args)));
        }
        Ok(())
    }
}

async fn pipe_to_collector(
    reader: impl tokio::io::AsyncRead + Unpin,
    stream_name: &str,
    source: &Arc<str>,
    sender: &flume::Sender<LogEntry>,
    origin: LogOrigin,
) {
    let stream: Arc<str> = Arc::from(stream_name);
    let now_millis = || {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64
    };
    let mut lines = tokio::io::BufReader::new(reader).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        let parsed = parse_log_line(&line);
        let entry = LogEntry {
            seq: 0,
            ts: parsed.ts.map(|t| t as i64).unwrap_or_else(&now_millis),
            level: parsed
                .level
                .map(Arc::from)
                .unwrap_or_else(|| Arc::from("info")),
            stream: stream.clone(),
            text: parsed.text,
            source: source.clone(),
            origin,
            tags: Arc::new(serde_json::Value::Null),
            attrs: parsed.attrs,
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
