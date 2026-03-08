use std::{sync::Arc, time::Duration};

use tokio::{
    sync::watch,
    task::JoinHandle,
    time::{sleep, timeout},
};
use watchexec_supervisor::{
    ProcessEnd, Signal,
    command::{Command, Program, Shell, SpawnOptions},
    job::{CommandState, Job, start_job},
};

use super::model::ServiceRuntimeConfig;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerExitStatus {
    Stopped,
    Completed,
    Crashed,
}

pub async fn run_managed_service(
    name: String,
    config: ServiceRuntimeConfig,
    mut shutdown_rx: watch::Receiver<bool>,
) -> WorkerExitStatus {
    let force_stop_on_shutdown = is_docker_run_command(&config.command);
    let command = Arc::new(Command {
        program: shell_program(config.command.clone()),
        options: SpawnOptions {
            grouped: true,
            ..Default::default()
        },
    });

    let shutdown_grace = Duration::from_millis(config.shutdown_grace_period_ms);
    let restart_delay = Duration::from_millis(config.restart_delay_ms);
    let shutdown_timeout = shutdown_grace.saturating_add(Duration::from_secs(3));

    let (job, supervisor_task) = start_job(command);
    let mut supervisor_task = supervisor_task;
    let mut restart_count = 0_u32;
    let mut exit_status = WorkerExitStatus::Completed;

    loop {
        job.start().await;
        log_service_process_ids(&name, &job).await;

        let outcome = tokio::select! {
            _ = shutdown_rx.changed() => WorkerOutcome::Shutdown,
            status = wait_for_job_exit(&job) => WorkerOutcome::Exited(status),
        };

        match outcome {
            WorkerOutcome::Shutdown => {
                if force_stop_on_shutdown {
                    stop_docker_run_and_delete(&job).await;
                } else {
                    graceful_stop_and_delete(&job, shutdown_grace, shutdown_timeout).await;
                }
                exit_status = WorkerExitStatus::Stopped;
                break;
            }
            WorkerOutcome::Exited(Some(status)) => {
                if is_failure(status) {
                    if restart_count >= config.max_restarts {
                        eprintln!(
                            "[maestro]: service '{name}' failed with {status:?} and hit maxRestarts={} (stopping)",
                            config.max_restarts
                        );
                        exit_status = WorkerExitStatus::Crashed;
                        break;
                    }

                    restart_count += 1;
                    eprintln!(
                        "[maestro]: service '{name}' failed with {status:?}; restart {restart_count}/{} in {}ms",
                        config.max_restarts, config.restart_delay_ms
                    );

                    let delay_outcome = tokio::select! {
                        _ = shutdown_rx.changed() => WorkerOutcome::Shutdown,
                        _ = sleep(restart_delay) => WorkerOutcome::DelayElapsed,
                    };

                    if let WorkerOutcome::Shutdown = delay_outcome {
                        if force_stop_on_shutdown {
                            stop_docker_run_and_delete(&job).await;
                        } else {
                            graceful_stop_and_delete(&job, shutdown_grace, shutdown_timeout).await;
                        }
                        break;
                    }

                    continue;
                }

                eprintln!("[maestro]: service '{name}' exited successfully (not restarting)");
                break;
            }
            WorkerOutcome::Exited(None) => {
                eprintln!("[maestro]: service '{name}' ended without an exit status");
                exit_status = WorkerExitStatus::Crashed;
                break;
            }
            WorkerOutcome::DelayElapsed => {
                continue;
            }
        }
    }

    let _ = timeout(Duration::from_secs(2), job.delete_now()).await;
    await_supervisor_shutdown(&mut supervisor_task).await;
    exit_status
}

enum WorkerOutcome {
    Shutdown,
    Exited(Option<ProcessEnd>),
    DelayElapsed,
}

async fn graceful_stop_and_delete(job: &Job, grace: Duration, shutdown_timeout: Duration) {
    if timeout(
        shutdown_timeout,
        job.stop_with_signal(Signal::Terminate, grace),
    )
    .await
    .is_err()
    {
        eprintln!("[maestro]: graceful stop timed out; forcing stop");
        let _ = timeout(Duration::from_secs(2), job.signal(Signal::ForceStop)).await;
        let _ = timeout(Duration::from_secs(2), job.stop()).await;
    }

    let _ = timeout(Duration::from_secs(2), job.delete_now()).await;
}

async fn stop_docker_run_and_delete(job: &Job) {
    // `docker run` usually handles SIGINT/SIGTERM and stops the container.
    let interrupt_stopped = timeout(
        Duration::from_secs(3),
        job.stop_with_signal(Signal::Interrupt, Duration::from_millis(500)),
    )
    .await
    .is_ok();
    if interrupt_stopped {
        let _ = timeout(Duration::from_secs(2), job.delete_now()).await;
        return;
    }

    let terminate_stopped = timeout(
        Duration::from_secs(3),
        job.stop_with_signal(Signal::Terminate, Duration::from_millis(500)),
    )
    .await
    .is_ok();
    if terminate_stopped {
        let _ = timeout(Duration::from_secs(2), job.delete_now()).await;
        return;
    }

    let _ = timeout(Duration::from_secs(1), job.signal(Signal::ForceStop)).await;
    let _ = timeout(Duration::from_secs(2), job.stop()).await;
    let _ = timeout(Duration::from_secs(2), job.delete_now()).await;
}

async fn await_supervisor_shutdown(supervisor_task: &mut JoinHandle<()>) {
    if timeout(Duration::from_secs(3), &mut *supervisor_task)
        .await
        .is_err()
    {
        eprintln!("[maestro]: supervisor task did not stop in time; waiting for cleanup");
        let _ = supervisor_task.await;
    }
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

fn is_failure(status: ProcessEnd) -> bool {
    !matches!(status, ProcessEnd::Success)
}

fn is_docker_run_command(command: &str) -> bool {
    let cmd = command.trim_start();
    cmd.starts_with("docker run ")
        || cmd.starts_with("exec docker run ")
        || cmd.starts_with("docker run\t")
        || cmd.starts_with("exec docker run\t")
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
        if let Some(pgid) = pgid {
            eprintln!("[maestro]: service '{name}' started: pid={pid} pgid={pgid}");
        } else {
            eprintln!("[maestro]: service '{name}' started: pid={pid} pgid=unknown");
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

fn shell_program(command: String) -> Program {
    Program::Shell {
        shell: Shell::new("sh"),
        command,
        args: Vec::new(),
    }
}
