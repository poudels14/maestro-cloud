use std::collections::BTreeMap;
use std::process::Child;
use std::sync::{Arc, Mutex, MutexGuard};

use crate::linux::{inspect_process, process_exit, signal_process, spawn_process};
use crate::{
    ProcessExit, ProcessHandle, ProcessSignal, ProcessSpec, ProcessStatus, SupervisorError,
};

/// Cloneable owner of child handles and detached-process identity operations.
///
/// Dropping the supervisor never kills children. Runtime manifests retain `ProcessHandle`, so a
/// replacement agent can inspect and signal exact processes after the original child handles are
/// gone.
#[derive(Clone, Default)]
pub struct ProcessSupervisor {
    state: Arc<Mutex<BTreeMap<ProcessHandle, ManagedProcess>>>,
}

impl ProcessSupervisor {
    /// Constructs an empty supervisor instance.
    pub fn new() -> Self {
        Self::default()
    }

    /// Spawns a new process group on Tokio's blocking pool and retains its child handle for reaping.
    pub async fn spawn(&self, spec: ProcessSpec) -> Result<ProcessHandle, SupervisorError> {
        spec.validate()?;
        let state = self.state.clone();
        tokio::task::spawn_blocking(move || {
            let (handle, child) = spawn_process(&spec)?;
            lock(&state)?.insert(
                handle,
                ManagedProcess {
                    child: Some(child),
                    exit: None,
                },
            );
            Ok(handle)
        })
        .await
        .map_err(task_error)?
    }

    /// Inspects an owned or adopted handle using cached wait status and exact procfs identity.
    pub async fn status(&self, handle: ProcessHandle) -> Result<ProcessStatus, SupervisorError> {
        let state = self.state.clone();
        tokio::task::spawn_blocking(move || status(&state, handle))
            .await
            .map_err(task_error)?
    }

    /// Sends a signal to the exact process group after validating PID start time.
    pub async fn signal(
        &self,
        handle: ProcessHandle,
        signal: ProcessSignal,
    ) -> Result<(), SupervisorError> {
        tokio::task::spawn_blocking(move || signal_process(handle, signal))
            .await
            .map_err(task_error)?
    }

    /// Waits and reaps a process spawned by this supervisor instance.
    ///
    /// Adopted processes cannot be waited because Linux permits only the original parent to reap
    /// them; callers should inspect those handles until procfs reports exit or disappearance.
    pub async fn wait(&self, handle: ProcessHandle) -> Result<ProcessExit, SupervisorError> {
        let state = self.state.clone();
        tokio::task::spawn_blocking(move || {
            let mut child = {
                let mut processes = lock(&state)?;
                let Some(process) = processes.get_mut(&handle) else {
                    return match inspect_process(handle)? {
                        ProcessStatus::Exited(exit) => Ok(exit),
                        ProcessStatus::Gone => Ok(ProcessExit {
                            code: None,
                            signal: None,
                        }),
                        ProcessStatus::Running => Err(SupervisorError::NotOwned { handle }),
                    };
                };
                if let Some(exit) = process.exit {
                    return Ok(exit);
                }
                process
                    .child
                    .take()
                    .ok_or(SupervisorError::NotOwned { handle })?
            };
            let status = child.wait().map_err(|error| SupervisorError::Io {
                operation: "wait",
                path: format!("/proc/{}/stat", handle.pid()).into(),
                message: error.to_string(),
            })?;
            let exit = process_exit(status);
            if let Some(process) = lock(&state)?.get_mut(&handle) {
                process.exit = Some(exit);
            }
            Ok(exit)
        })
        .await
        .map_err(task_error)?
    }

    /// Forgets cached child and exit state without signaling the detached process.
    pub async fn forget(&self, handle: ProcessHandle) -> Result<(), SupervisorError> {
        let state = self.state.clone();
        tokio::task::spawn_blocking(move || {
            lock(&state)?.remove(&handle);
            Ok(())
        })
        .await
        .map_err(task_error)?
    }
}

struct ManagedProcess {
    child: Option<Child>,
    exit: Option<ProcessExit>,
}

fn status(
    state: &Mutex<BTreeMap<ProcessHandle, ManagedProcess>>,
    handle: ProcessHandle,
) -> Result<ProcessStatus, SupervisorError> {
    let mut processes = lock(state)?;
    if let Some(process) = processes.get_mut(&handle) {
        if let Some(exit) = process.exit {
            return Ok(ProcessStatus::Exited(exit));
        }
        if let Some(child) = &mut process.child
            && let Some(status) = child.try_wait().map_err(|error| SupervisorError::Io {
                operation: "poll",
                path: format!("/proc/{}/stat", handle.pid()).into(),
                message: error.to_string(),
            })?
        {
            let exit = process_exit(status);
            process.exit = Some(exit);
            process.child = None;
            return Ok(ProcessStatus::Exited(exit));
        }
    }
    drop(processes);
    inspect_process(handle)
}

fn lock<T>(mutex: &Mutex<T>) -> Result<MutexGuard<'_, T>, SupervisorError> {
    mutex.lock().map_err(|_| SupervisorError::StateUnavailable)
}

fn task_error(error: tokio::task::JoinError) -> SupervisorError {
    SupervisorError::Task {
        message: error.to_string(),
    }
}
