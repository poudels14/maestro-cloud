use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::time::Duration;

use kernel_api::WorkloadId;
use supervisor::{ProcessExit, ProcessHandle, ProcessStatus, ProcessSupervisor, SupervisorError};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::process_manifest::{
    ManifestState, ProcessManifest, load_optional_manifest, write_manifest,
};
use crate::process_settings::ProcessRuntimeSettings;
use crate::process_stream::ProcessEventJournal;
use crate::process_support::{blocking, runtime_supervisor_error};
use crate::{
    ProcessWorkload, RuntimeClock, RuntimeError, RuntimeEventKind, TokioRuntimeClock,
    WorkloadState, WorkloadStatus,
};

mod operations;

/// Detached host-process implementation of `WorkloadRuntime`.
///
/// Manifests persist ownership, an opaque spec fingerprint, log paths, and PID start-time identity;
/// plaintext workload specifications and secret environment values are never persisted.
#[derive(Clone)]
pub struct ProcessRuntime {
    root: Arc<PathBuf>,
    supervisor: ProcessSupervisor,
    clock: Arc<dyn RuntimeClock>,
    settings: ProcessRuntimeSettings,
    cached_specs: Arc<Mutex<BTreeMap<WorkloadId, ProcessWorkload>>>,
    operation_gates: Arc<tokio::sync::Mutex<BTreeMap<WorkloadId, Weak<Semaphore>>>>,
    events: ProcessEventJournal,
}

impl ProcessRuntime {
    /// Constructs a process backend rooted in one absolute node-owned state directory.
    pub fn new(
        root: PathBuf,
        supervisor: ProcessSupervisor,
        clock: Arc<dyn RuntimeClock>,
        settings: ProcessRuntimeSettings,
    ) -> Result<Self, RuntimeError> {
        if !root.is_absolute() {
            return Err(RuntimeError::InvalidSpec {
                message: "process runtime root must be absolute".to_owned(),
            });
        }
        Ok(Self {
            root: Arc::new(root),
            supervisor,
            clock,
            settings: settings.validate()?,
            cached_specs: Arc::new(Mutex::new(BTreeMap::new())),
            operation_gates: Arc::new(tokio::sync::Mutex::new(BTreeMap::new())),
            events: ProcessEventJournal::new(),
        })
    }

    /// Constructs a production process backend with Linux supervision and Tokio time.
    pub fn production(root: PathBuf) -> Result<Self, RuntimeError> {
        Self::new(
            root,
            ProcessSupervisor::new(),
            Arc::new(TokioRuntimeClock::new()),
            ProcessRuntimeSettings::default(),
        )
    }

    async fn observe(
        &self,
        manifest: &mut ProcessManifest,
    ) -> Result<WorkloadStatus, RuntimeError> {
        let Some(process) = manifest.process else {
            let state = if manifest.state == ManifestState::Stopped {
                WorkloadState::Stopped
            } else {
                WorkloadState::Created
            };
            return Ok(WorkloadStatus {
                state,
                exit_code: None,
                detail: None,
            });
        };
        match self
            .supervisor
            .status(process)
            .await
            .map_err(runtime_supervisor_error)?
        {
            ProcessStatus::Running => Ok(WorkloadStatus {
                state: WorkloadState::Running,
                exit_code: None,
                detail: None,
            }),
            ProcessStatus::Exited(exit) => self.record_exit(manifest, exit.code, exit.signal).await,
            ProcessStatus::Gone => self.record_exit(manifest, None, None).await,
        }
    }

    async fn record_exit(
        &self,
        manifest: &mut ProcessManifest,
        exit_code: Option<i32>,
        signal: Option<i32>,
    ) -> Result<WorkloadStatus, RuntimeError> {
        if manifest.state != ManifestState::Stopped {
            manifest.state = ManifestState::Stopped;
            self.write(manifest.clone()).await?;
            self.events
                .emit(&manifest.metadata, RuntimeEventKind::Exited, exit_code)?;
        }
        Ok(WorkloadStatus {
            state: WorkloadState::Stopped,
            exit_code,
            detail: signal.map(|signal| format!("process exited from signal {signal}")),
        })
    }

    async fn await_exit(
        &self,
        workload_id: &WorkloadId,
        process: ProcessHandle,
        operation: &'static str,
        timeout: Duration,
    ) -> Result<ProcessExit, RuntimeError> {
        let deadline = self.clock.now().saturating_add(timeout);
        let wait = self.supervisor.wait(process);
        tokio::pin!(wait);
        let wait_result = tokio::select! {
            result = &mut wait => Some(result),
            () = self.clock.sleep_until(deadline) => None,
        };
        match wait_result {
            Some(Ok(exit)) => Ok(exit),
            Some(Err(SupervisorError::NotOwned { .. })) => {
                self.poll_adopted_exit(workload_id, process, operation, timeout, deadline)
                    .await
            }
            Some(Err(error)) => Err(runtime_supervisor_error(error)),
            None => Err(RuntimeError::Timeout {
                operation,
                workload_id: workload_id.clone(),
                timeout,
            }),
        }
    }

    async fn poll_adopted_exit(
        &self,
        workload_id: &WorkloadId,
        process: ProcessHandle,
        operation: &'static str,
        timeout: Duration,
        deadline: crate::MonotonicTime,
    ) -> Result<ProcessExit, RuntimeError> {
        loop {
            match self
                .supervisor
                .status(process)
                .await
                .map_err(runtime_supervisor_error)?
            {
                ProcessStatus::Exited(exit) => return Ok(exit),
                ProcessStatus::Gone => {
                    return Ok(ProcessExit {
                        code: None,
                        signal: None,
                    });
                }
                ProcessStatus::Running => {}
            }
            let now = self.clock.now();
            if now >= deadline {
                return Err(RuntimeError::Timeout {
                    operation,
                    workload_id: workload_id.clone(),
                    timeout,
                });
            }
            self.clock
                .sleep_until(std::cmp::min(
                    now.saturating_add(self.settings.poll_interval),
                    deadline,
                ))
                .await;
        }
    }

    async fn load_optional(
        &self,
        workload_id: WorkloadId,
    ) -> Result<Option<ProcessManifest>, RuntimeError> {
        let root = self.root.clone();
        blocking(move || {
            load_optional_manifest(&root, &workload_id).map_err(|error| error.into_runtime())
        })
        .await
    }

    async fn load(&self, workload_id: WorkloadId) -> Result<ProcessManifest, RuntimeError> {
        self.load_optional(workload_id.clone())
            .await?
            .ok_or(RuntimeError::NotFound { workload_id })
    }

    async fn write(&self, manifest: ProcessManifest) -> Result<(), RuntimeError> {
        let root = self.root.clone();
        blocking(move || write_manifest(&root, &manifest).map_err(|error| error.into_runtime()))
            .await
    }

    fn cached_specs(
        &self,
    ) -> Result<MutexGuard<'_, BTreeMap<WorkloadId, ProcessWorkload>>, RuntimeError> {
        self.cached_specs
            .lock()
            .map_err(|_| RuntimeError::Unavailable {
                message: "process runtime spec cache lock was poisoned".to_owned(),
            })
    }

    pub(crate) async fn operation(
        &self,
        workload_id: &WorkloadId,
    ) -> Result<OwnedSemaphorePermit, RuntimeError> {
        let gate = {
            let mut gates = self.operation_gates.lock().await;
            gates.retain(|_, gate| gate.strong_count() > 0);
            match gates.get(workload_id).and_then(Weak::upgrade) {
                Some(gate) => gate,
                None => {
                    let gate = Arc::new(Semaphore::new(1));
                    gates.insert(workload_id.clone(), Arc::downgrade(&gate));
                    gate
                }
            }
        };
        gate.acquire_owned()
            .await
            .map_err(|error| RuntimeError::Unavailable {
                message: format!("process operation gate closed unexpectedly: {error}"),
            })
    }
}
