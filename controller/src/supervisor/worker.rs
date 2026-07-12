use std::{path::PathBuf, process::Stdio, str::FromStr, sync::Arc, time::Duration};

use tokio::{
    sync::{mpsc, oneshot, watch},
    task::JoinHandle,
    time::{sleep, timeout},
};

use backon::{BackoffBuilder, ExponentialBuilder};

use crate::config::RuntimeType;
use crate::logs::{LogConfig, LogEntry, LogOrigin};
use crate::runtime;

use super::logs::read_pipe_to_collector;
use watchexec_supervisor::{
    ProcessEnd, Signal,
    command::{Command, Program, Shell, SpawnOptions},
    job::{CommandState, Job, start_job},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobCommand {
    Exec { program: String, args: Vec<String> },
    Shell(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContainerRef {
    pub name: String,
    pub runtime_cli: String,
}

#[derive(Clone)]
pub struct SupervisedJobConfig {
    pub id: String,
    pub name: String,
    pub command: JobCommand,
    pub restart_delay_ms: u64,
    pub max_restart_delay_ms: Option<u64>,
    pub max_restarts: Option<u32>,

    pub shutdown_grace_period_ms: u64,
    pub container: Option<ContainerRef>,
    pub secrets_mount: Option<SecretsMount>,
    pub log_config: Option<LogConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SecretsMount {
    pub host_path: PathBuf,
    pub container_path: String,
    pub content: String,
}

impl SecretsMount {
    pub fn write(&self) -> std::io::Result<()> {
        if let Some(parent) = self.host_path.parent() {
            std::fs::create_dir_all(parent)?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let _ = std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700));
            }
        }
        std::fs::write(&self.host_path, &self.content)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ =
                std::fs::set_permissions(&self.host_path, std::fs::Permissions::from_mode(0o600));
        }
        Ok(())
    }

    pub fn cleanup(&self) {
        if let Ok(len) = std::fs::metadata(&self.host_path).map(|m| m.len()) {
            let _ = std::fs::write(&self.host_path, vec![0u8; len as usize]);
        }
        let _ = std::fs::remove_file(&self.host_path);
        if let Some(parent) = self.host_path.parent() {
            let _ = std::fs::remove_dir(parent);
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SupervisedJobStatus {
    Pending,
    Running,
    Stopped,
    Completed,
    Crashed,
}

impl SupervisedJobStatus {
    pub fn finished(&self) -> bool {
        matches!(self, Self::Completed | Self::Stopped | Self::Crashed)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShutdownRequest {
    None,
    Graceful,
    Force,
}

enum WorkerOutcome {
    Shutdown(ShutdownRequest),
    Exited(Option<ProcessEnd>),
    DelayElapsed,
}

pub struct SupervisedJob {
    managed: SupervisedJobInner,
    handle: JoinHandle<SupervisedJobStatus>,
    shutdown_tx: watch::Sender<ShutdownRequest>,
}

impl SupervisedJob {
    #[inline]
    pub fn is_finished(&self) -> bool {
        self.handle.is_finished()
    }

    pub async fn status(&self) -> SupervisedJobStatus {
        let status = self.managed.status().await;
        status.unwrap_or_else(|| {
            if self.handle.is_finished() {
                SupervisedJobStatus::Stopped
            } else {
                SupervisedJobStatus::Running
            }
        })
    }

    #[inline]
    pub async fn join(&mut self) -> Result<SupervisedJobStatus, tokio::task::JoinError> {
        (&mut self.handle).await
    }

    #[inline]
    pub fn abort(&mut self) {
        self.handle.abort();
    }

    #[inline]
    pub fn shutdown(&self, request: ShutdownRequest) {
        let _ = self.shutdown_tx.send(request);
    }
}

struct SupervisedJobInner {
    inner: Job,
}

impl Clone for SupervisedJobInner {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl SupervisedJobInner {
    fn new(job: Job) -> Self {
        Self { inner: job }
    }

    async fn is_running(&self) -> bool {
        let (tx, rx) = oneshot::channel();
        self.inner
            .run(move |ctx| {
                let _ = tx.send(matches!(ctx.current, CommandState::Running { .. }));
            })
            .await;
        rx.await.unwrap_or(false)
    }

    async fn status(&self) -> Option<SupervisedJobStatus> {
        let (tx, rx) = oneshot::channel();
        self.inner
            .run(move |ctx| {
                let status = match ctx.current {
                    CommandState::Pending => SupervisedJobStatus::Pending,
                    CommandState::Running { .. } => SupervisedJobStatus::Running,
                    CommandState::Finished { status, .. } => match status {
                        ProcessEnd::Success => SupervisedJobStatus::Completed,
                        _ => SupervisedJobStatus::Stopped,
                    },
                };
                let _ = tx.send(status);
            })
            .await;
        rx.await.ok()
    }

    async fn wait(&self) -> Option<ProcessEnd> {
        self.inner.to_wait().await;
        let (tx, rx) = oneshot::channel();
        self.inner
            .run(move |ctx| {
                let status = if let CommandState::Finished { status, .. } = ctx.current {
                    Some(*status)
                } else {
                    None
                };
                let _ = tx.send(status);
            })
            .await;
        rx.await.ok().flatten()
    }

    async fn start(&self) {
        self.inner.start().await;
    }

    async fn graceful_stop(&self, grace: Duration, shutdown_timeout: Duration) {
        if !self.is_running().await {
            let _ = timeout(Duration::from_secs(2), self.inner.delete_now()).await;
            return;
        }

        let graceful_completed = tokio::select! {
            _ = self.wait() => true,
            result = timeout(
                shutdown_timeout,
                self.inner.stop_with_signal(Signal::Terminate, grace),
            ) => result.is_ok(),
        };

        if !graceful_completed && self.is_running().await {
            eprintln!("[maestro]: graceful stop timed out; forcing stop");
            let _ = timeout(Duration::from_secs(2), self.inner.signal(Signal::ForceStop)).await;
            let _ = timeout(Duration::from_secs(2), self.inner.stop()).await;
        }

        let _ = timeout(Duration::from_secs(2), self.inner.delete_now()).await;
    }

    async fn force_stop(&self) {
        if !self.is_running().await {
            let _ = timeout(Duration::from_secs(2), self.inner.delete_now()).await;
            return;
        }

        let _ = timeout(Duration::from_secs(1), self.inner.signal(Signal::ForceStop)).await;
        let _ = timeout(Duration::from_secs(2), self.inner.stop()).await;
        let _ = timeout(Duration::from_secs(2), self.inner.delete_now()).await;
    }

    async fn handle_shutdown(
        &self,
        request: ShutdownRequest,
        grace: Duration,
        shutdown_timeout: Duration,
        container: Option<&ContainerRef>,
    ) -> bool {
        match request {
            ShutdownRequest::Force => self.force_stop().await,
            ShutdownRequest::Graceful => self.graceful_stop(grace, shutdown_timeout).await,
            ShutdownRequest::None => return false,
        }
        if let Some(container) = container {
            let _ =
                crate::utils::cmd::run(&container.runtime_cli, &["kill", &container.name]).await;
        }
        true
    }

    async fn delete(&self) {
        let _ = timeout(Duration::from_secs(2), self.inner.delete_now()).await;
    }

    async fn log_process_ids(&self, name: &str, log_config: Option<&LogConfig>) {
        let (tx, rx) = oneshot::channel();
        self.inner
            .run(move |ctx| {
                let pid = match ctx.current {
                    CommandState::Running { child, .. } => child.id(),
                    _ => None,
                };
                let pgid = pid.and_then(get_pgid_for_pid);
                let _ = tx.send((pid, pgid));
            })
            .await;

        if let Ok((Some(pid), pgid)) = rx.await {
            let pgid_str = pgid.map_or("unknown".to_string(), |g| g.to_string());
            let text = format!("service '{name}' started: pid={pid} pgid={pgid_str}");
            eprintln!("[maestro]: {text}");
            if let Some(cfg) = log_config {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as i64;
                let _ = cfg.sender.try_send(LogEntry {
                    seq: 0,
                    ts: now,
                    level: Arc::from("info"),
                    stream: Arc::from("stderr"),
                    text,
                    source: Arc::from(name.to_string()),
                    origin: LogOrigin::System,
                    tags: cfg.build_tags(),
                    attrs: vec![],
                });
            }
        }
    }
}

pub struct SupervisedJobRunner;

impl SupervisedJobRunner {
    pub fn new() -> Self {
        Self
    }

    pub fn spawn(&self, config: SupervisedJobConfig) -> SupervisedJob {
        let program = match &config.command {
            JobCommand::Exec { program, args } => Program::Exec {
                prog: program.clone().into(),
                args: args.to_vec(),
            },
            JobCommand::Shell(cmd) => Program::Shell {
                shell: Shell::new("sh"),
                command: cmd.clone(),
                args: Vec::new(),
            },
        };
        let command = Arc::new(Command {
            program,
            options: SpawnOptions {
                grouped: true,
                ..Default::default()
            },
        });
        let (job, job_handle) = start_job(command);
        let managed = SupervisedJobInner::new(job);
        let run_managed = managed.clone();
        let (shutdown_tx, shutdown_rx) = watch::channel(ShutdownRequest::None);
        let handle = tokio::spawn(async move {
            Self::run_job(config, run_managed, job_handle, shutdown_rx).await
        });

        SupervisedJob {
            managed,
            handle,
            shutdown_tx,
        }
    }

    async fn setup_job(
        job: &SupervisedJobInner,
        config: &SupervisedJobConfig,
    ) -> Option<JoinHandle<()>> {
        let (pipe_tx, mut pipe_rx) =
            mpsc::unbounded_channel::<(os_pipe::PipeReader, os_pipe::PipeReader)>();

        job.inner
            .set_spawn_hook(move |command, _| {
                let Ok((stdout_reader, stdout_writer)) = os_pipe::pipe() else {
                    return;
                };
                let Ok((stderr_reader, stderr_writer)) = os_pipe::pipe() else {
                    return;
                };
                command.command_mut().stdout(Stdio::from(stdout_writer));
                command.command_mut().stderr(Stdio::from(stderr_writer));
                let _ = pipe_tx.send((stdout_reader, stderr_reader));
            })
            .await;

        let log_config = config.log_config.clone();
        let log_source: Arc<str> = Arc::from(config.name.as_str());

        let collector = tokio::spawn(async move {
            while let Some((stdout_reader, stderr_reader)) = pipe_rx.recv().await {
                if let Some(lc) = &log_config {
                    tokio::spawn(read_pipe_to_collector(
                        stdout_reader,
                        "stdout",
                        log_source.clone(),
                        lc.clone(),
                    ));
                    tokio::spawn(read_pipe_to_collector(
                        stderr_reader,
                        "stderr",
                        log_source.clone(),
                        lc.clone(),
                    ));
                }
            }
        });

        Some(collector)
    }

    async fn run_job(
        config: SupervisedJobConfig,
        job: SupervisedJobInner,
        mut job_handle: JoinHandle<()>,
        mut shutdown_rx: watch::Receiver<ShutdownRequest>,
    ) -> SupervisedJobStatus {
        let collector_handle = Self::setup_job(&job, &config).await;
        let name = config.name.clone();

        let shutdown_grace = Duration::from_millis(config.shutdown_grace_period_ms);
        let shutdown_timeout = shutdown_grace.saturating_add(Duration::from_secs(3));
        let max_delay = config
            .max_restart_delay_ms
            .unwrap_or(config.restart_delay_ms);
        let mut backoff = ExponentialBuilder::default()
            .with_min_delay(Duration::from_millis(config.restart_delay_ms))
            .with_max_delay(Duration::from_millis(max_delay))
            .with_factor(2.0)
            .without_max_times()
            .build();

        let mut restart_count = 0_u32;
        let mut exit_status = SupervisedJobStatus::Completed;

        loop {
            let current_shutdown = *shutdown_rx.borrow();
            if job
                .handle_shutdown(
                    current_shutdown,
                    shutdown_grace,
                    shutdown_timeout,
                    config.container.as_ref(),
                )
                .await
            {
                exit_status = SupervisedJobStatus::Stopped;
                break;
            }

            if let Some(secrets) = &config.secrets_mount
                && let Err(err) = secrets.write()
            {
                eprintln!("[maestro]: failed to write secrets for '{name}': {err}");
            }
            job.start().await;
            job.log_process_ids(&name, config.log_config.as_ref()).await;

            let outcome = tokio::select! {
                biased;
                status = job.wait() => WorkerOutcome::Exited(status),
                _ = shutdown_rx.changed() => WorkerOutcome::Shutdown(*shutdown_rx.borrow()),
            };

            match outcome {
                WorkerOutcome::Shutdown(request) => {
                    if job
                        .handle_shutdown(
                            request,
                            shutdown_grace,
                            shutdown_timeout,
                            config.container.as_ref(),
                        )
                        .await
                    {
                        exit_status = SupervisedJobStatus::Stopped;
                        break;
                    }
                }
                WorkerOutcome::Exited(Some(status)) if !matches!(status, ProcessEnd::Success) => {
                    if let Some(max) = config.max_restarts
                        && restart_count >= max
                    {
                        eprintln!(
                            "[maestro]: service '{name}' failed with {status:?} and hit maxRestarts={max} (stopping)"
                        );
                        exit_status = SupervisedJobStatus::Crashed;
                        break;
                    }

                    restart_count += 1;
                    cleanup_system_container(config.container.as_ref(), &config.log_config).await;
                    let delay = backoff.next().unwrap_or(Duration::from_millis(max_delay));
                    let delay_ms = delay.as_millis();
                    match config.max_restarts {
                        Some(max) => eprintln!(
                            "[maestro]: service '{name}' failed with {status:?}; restart {restart_count}/{max} in {delay_ms}ms",
                        ),
                        None => eprintln!(
                            "[maestro]: service '{name}' failed with {status:?}; restart {restart_count} in {delay_ms}ms",
                        ),
                    }

                    let delay_outcome = tokio::select! {
                        _ = shutdown_rx.changed() => WorkerOutcome::Shutdown(*shutdown_rx.borrow()),
                        _ = sleep(delay) => WorkerOutcome::DelayElapsed,
                    };

                    if let WorkerOutcome::Shutdown(request) = delay_outcome
                        && job
                            .handle_shutdown(
                                request,
                                shutdown_grace,
                                shutdown_timeout,
                                config.container.as_ref(),
                            )
                            .await
                    {
                        exit_status = SupervisedJobStatus::Stopped;
                        break;
                    }
                }
                WorkerOutcome::Exited(Some(_)) if config.max_restarts.is_none() => {
                    eprintln!("[maestro]: service '{name}' exited successfully; restarting");
                    cleanup_system_container(config.container.as_ref(), &config.log_config).await;
                    let delay = backoff.next().unwrap_or(Duration::from_millis(max_delay));
                    sleep(delay).await;
                }
                WorkerOutcome::Exited(Some(_)) => {
                    eprintln!("[maestro]: service '{name}' exited successfully (not restarting)");
                    break;
                }
                WorkerOutcome::Exited(None) => {
                    eprintln!("[maestro]: service '{name}' ended without an exit status");
                    exit_status = SupervisedJobStatus::Crashed;
                    break;
                }
                WorkerOutcome::DelayElapsed => {}
            }
        }

        if let Some(secrets) = &config.secrets_mount {
            secrets.cleanup();
        }
        job.delete().await;
        if timeout(Duration::from_secs(3), &mut job_handle)
            .await
            .is_err()
        {
            eprintln!(
                "[maestro]: supervisor job did not stop in time; aborting worker cleanup task"
            );
            job_handle.abort();
            let _ = timeout(Duration::from_secs(1), &mut job_handle).await;
        }
        if let Some(handle) = collector_handle {
            let _ = timeout(Duration::from_secs(2), handle).await;
        }
        exit_status
    }
}

async fn cleanup_system_container(
    container: Option<&ContainerRef>,
    log_config: &Option<LogConfig>,
) {
    let is_system_service = log_config
        .as_ref()
        .map(|cfg| cfg.origin == LogOrigin::System)
        .unwrap_or(false);
    if is_system_service && let Some(container_ref) = container {
        let runtime_type = RuntimeType::from_str(&container_ref.runtime_cli);
        if let Ok(runtime_type) = runtime_type {
            let provider = runtime::create_provider(runtime_type);
            let _ = provider.remove_container(&container_ref.name).await;
        }
    }
}

fn get_pgid_for_pid(pid: u32) -> Option<u32> {
    let pid = i32::try_from(pid).ok()?;
    // SAFETY: `getpgid` is called with a PID obtained from the spawned child process.
    let pgid = unsafe { libc::getpgid(pid) };
    if pgid < 0 {
        None
    } else {
        u32::try_from(pgid).ok()
    }
}
