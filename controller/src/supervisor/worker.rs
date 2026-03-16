use std::{
    fs::OpenOptions,
    path::PathBuf,
    process::Stdio,
    sync::{Arc, Mutex as StdMutex},
    time::Duration,
};

use tokio::{
    sync::{mpsc, oneshot, watch},
    task::JoinHandle,
    time::{sleep, timeout},
};

use super::logs::read_pipe_to_jsonl;
use watchexec_supervisor::{
    ProcessEnd, Signal,
    command::{Command, Program, Shell, SpawnOptions},
    job::{CommandState, Job, start_job},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SupervisedJobConfig {
    pub id: String,
    pub name: String,
    pub command: String,
    pub restart_delay_ms: u64,
    pub max_restarts: Option<u32>,
    pub shutdown_grace_period_ms: u64,
    pub logs_dir: Option<PathBuf>,
    pub docker_container: Option<String>,
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
    job: Job,
    handle: JoinHandle<SupervisedJobStatus>,
    shutdown_tx: watch::Sender<ShutdownRequest>,
}

impl SupervisedJob {
    #[inline]
    pub fn is_finished(&self) -> bool {
        self.handle.is_finished()
    }

    pub async fn status(&self) -> SupervisedJobStatus {
        let (tx, rx) = oneshot::channel();
        self.job
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

        rx.await.unwrap_or_else(|_| {
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

pub struct SupervisedJobRunner;

impl SupervisedJobRunner {
    pub fn new() -> Self {
        Self
    }

    pub fn spawn(&self, config: SupervisedJobConfig) -> SupervisedJob {
        let command = Arc::new(Command {
            program: Program::Shell {
                shell: Shell::new("sh"),
                command: config.command.clone(),
                args: Vec::new(),
            },
            options: SpawnOptions {
                grouped: true,
                ..Default::default()
            },
        });
        let (job, job_handle) = start_job(command);
        let run_job = job.clone();
        let (shutdown_tx, shutdown_rx) = watch::channel(ShutdownRequest::None);
        let handle =
            tokio::spawn(
                async move { Self::run_job(config, run_job, job_handle, shutdown_rx).await },
            );

        SupervisedJob {
            job,
            handle,
            shutdown_tx,
        }
    }

    async fn setup_job(job: &Job, config: &SupervisedJobConfig) -> Option<JoinHandle<()>> {
        let logs_dir = config.logs_dir.as_ref()?;
        if let Err(err) = std::fs::create_dir_all(logs_dir) {
            eprintln!("[maestro]: failed to create logs dir: {err}");
            return None;
        }

        let log_path = logs_dir.join("logs.jsonl");
        let log_file = match OpenOptions::new().create(true).append(true).open(&log_path) {
            Ok(f) => Arc::new(StdMutex::new(f)),
            Err(err) => {
                eprintln!(
                    "[maestro]: failed to open log file {}: {err}",
                    log_path.display()
                );
                return None;
            }
        };

        let (pipe_tx, mut pipe_rx) =
            mpsc::unbounded_channel::<(os_pipe::PipeReader, os_pipe::PipeReader)>();

        job.set_spawn_hook(move |command, _| {
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

        let collector = tokio::spawn(async move {
            while let Some((stdout_reader, stderr_reader)) = pipe_rx.recv().await {
                tokio::spawn(read_pipe_to_jsonl(
                    stdout_reader,
                    "stdout",
                    log_file.clone(),
                ));
                tokio::spawn(read_pipe_to_jsonl(
                    stderr_reader,
                    "stderr",
                    log_file.clone(),
                ));
            }
        });

        Some(collector)
    }

    async fn run_job(
        config: SupervisedJobConfig,
        job: Job,
        mut job_handle: JoinHandle<()>,
        mut shutdown_rx: watch::Receiver<ShutdownRequest>,
    ) -> SupervisedJobStatus {
        let collector_handle = Self::setup_job(&job, &config).await;
        let name = config.name.clone();

        let shutdown_grace = Duration::from_millis(config.shutdown_grace_period_ms);
        let restart_delay = Duration::from_millis(config.restart_delay_ms);
        let shutdown_timeout = shutdown_grace.saturating_add(Duration::from_secs(3));

        let mut restart_count = 0_u32;
        let mut exit_status = SupervisedJobStatus::Completed;

        /// Handle a shutdown request; returns `true` if the job should stop, `false` to continue.
        async fn handle_shutdown(
            request: ShutdownRequest,
            job: &Job,
            grace: Duration,
            shutdown_timeout: Duration,
            docker_container: Option<&str>,
        ) -> bool {
            match request {
                ShutdownRequest::Force => force_stop_and_delete(job).await,
                ShutdownRequest::Graceful => {
                    graceful_stop_and_delete(job, grace, shutdown_timeout).await;
                }
                ShutdownRequest::None => return false,
            }
            if let Some(container) = docker_container {
                docker_kill(container).await;
            }
            true
        }

        loop {
            job.start().await;
            log_service_process_ids(&name, &job).await;

            let outcome = tokio::select! {
                biased;
                status = wait_for_job_exit(&job) => WorkerOutcome::Exited(status),
                _ = shutdown_rx.changed() => WorkerOutcome::Shutdown(*shutdown_rx.borrow()),
            };

            match outcome {
                WorkerOutcome::Shutdown(request) => {
                    if handle_shutdown(
                        request,
                        &job,
                        shutdown_grace,
                        shutdown_timeout,
                        config.docker_container.as_deref(),
                    )
                    .await
                    {
                        exit_status = SupervisedJobStatus::Stopped;
                        break;
                    }
                }
                WorkerOutcome::Exited(Some(status)) if !matches!(status, ProcessEnd::Success) => {
                    if let Some(max) = config.max_restarts {
                        if restart_count >= max {
                            eprintln!(
                                "[maestro]: service '{name}' failed with {status:?} and hit maxRestarts={max} (stopping)"
                            );
                            exit_status = SupervisedJobStatus::Crashed;
                            break;
                        }
                    }

                    restart_count += 1;
                    match config.max_restarts {
                        Some(max) => eprintln!(
                            "[maestro]: service '{name}' failed with {status:?}; restart {restart_count}/{max} in {}ms",
                            config.restart_delay_ms
                        ),
                        None => eprintln!(
                            "[maestro]: service '{name}' failed with {status:?}; restart {restart_count} in {}ms",
                            config.restart_delay_ms
                        ),
                    }

                    let delay_outcome = tokio::select! {
                        _ = shutdown_rx.changed() => WorkerOutcome::Shutdown(*shutdown_rx.borrow()),
                        _ = sleep(restart_delay) => WorkerOutcome::DelayElapsed,
                    };

                    if let WorkerOutcome::Shutdown(request) = delay_outcome {
                        if handle_shutdown(
                            request,
                            &job,
                            shutdown_grace,
                            shutdown_timeout,
                            config.docker_container.as_deref(),
                        )
                        .await
                        {
                            exit_status = SupervisedJobStatus::Stopped;
                            break;
                        }
                    }
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

        let _ = timeout(Duration::from_secs(2), job.delete_now()).await;
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

async fn graceful_stop_and_delete(job: &Job, grace: Duration, shutdown_timeout: Duration) {
    if !is_job_running(job).await {
        let _ = timeout(Duration::from_secs(2), job.delete_now()).await;
        return;
    }

    let graceful_completed = tokio::select! {
        _ = wait_for_job_exit(job) => true,
        result = timeout(
            shutdown_timeout,
            job.stop_with_signal(Signal::Terminate, grace),
        ) => result.is_ok(),
    };

    if !graceful_completed && is_job_running(job).await {
        eprintln!("[maestro]: graceful stop timed out; forcing stop");
        let _ = timeout(Duration::from_secs(2), job.signal(Signal::ForceStop)).await;
        let _ = timeout(Duration::from_secs(2), job.stop()).await;
    }

    let _ = timeout(Duration::from_secs(2), job.delete_now()).await;
}

async fn force_stop_and_delete(job: &Job) {
    if !is_job_running(job).await {
        let _ = timeout(Duration::from_secs(2), job.delete_now()).await;
        return;
    }

    let _ = timeout(Duration::from_secs(1), job.signal(Signal::ForceStop)).await;
    let _ = timeout(Duration::from_secs(2), job.stop()).await;
    let _ = timeout(Duration::from_secs(2), job.delete_now()).await;
}

async fn docker_kill(container: &str) {
    let _ = tokio::process::Command::new("docker")
        .args(["kill", container])
        .output()
        .await;
}

async fn is_job_running(job: &Job) -> bool {
    let (tx, rx) = tokio::sync::oneshot::channel();
    job.run(move |ctx| {
        let running = matches!(ctx.current, CommandState::Running { .. });
        let _ = tx.send(running);
    })
    .await;

    rx.await.unwrap_or(false)
}

async fn wait_for_job_exit(job: &Job) -> Option<ProcessEnd> {
    job.to_wait().await;

    let (tx, rx) = tokio::sync::oneshot::channel();
    job.run(move |ctx| {
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

async fn log_service_process_ids(name: &str, job: &Job) {
    let (tx, rx) = tokio::sync::oneshot::channel();
    job.run(move |ctx| {
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
        eprintln!("[maestro]: service '{name}' started: pid={pid} pgid={pgid_str}");
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
