use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use async_trait::async_trait;
use kernel_api::{ClusterId, NodeId, WorkloadId};
use supervisor::{
    ProcessExit, ProcessHandle, ProcessSignal, ProcessStatus, ProcessSupervisor, SupervisorError,
};

use crate::file_log::FileLogStream;
use crate::process_manifest::{
    ManifestState, ProcessManifest, commit_process_start, load_manifests, load_optional_manifest,
    remove_manifest, write_manifest,
};
use crate::process_settings::ProcessRuntimeSettings;
use crate::process_stream::ProcessEventJournal;
use crate::process_support::{
    blocking, build_supervised_spec, process_handle, process_paths, read_cgroup_path,
    runtime_supervisor_error, spec_fingerprint, validate_process_handle,
};
use crate::{
    Capabilities, CgroupPath, EventRequest, ExecRequest, ExecSession, LogRequest, LogStream,
    ObservedWorkload, ProcessWorkload, RuntimeClock, RuntimeError, RuntimeEventKind,
    RuntimeEventStream, ShutdownRequest, TokioRuntimeClock, WorkloadHandle, WorkloadRuntime,
    WorkloadSpec, WorkloadState, WorkloadStatus,
};

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
    mutation: Arc<tokio::sync::Mutex<()>>,
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
            mutation: Arc::new(tokio::sync::Mutex::new(())),
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
}

#[async_trait]
impl WorkloadRuntime for ProcessRuntime {
    fn capabilities(&self) -> Capabilities {
        Capabilities::none()
    }

    async fn create(&self, spec: &WorkloadSpec) -> Result<WorkloadHandle, RuntimeError> {
        let WorkloadSpec::Process(process) = spec else {
            return Err(RuntimeError::InvalidSpec {
                message: "process runtime accepts only process workloads".to_owned(),
            });
        };
        let fingerprint = spec_fingerprint(spec)?;
        let workload_id = process.configuration.metadata.workload_id.clone();
        let paths = process_paths(&self.root, &workload_id);
        build_supervised_spec(process, &paths)?;
        let _guard = self.mutation.lock().await;
        if let Some(manifest) = self.load_optional(workload_id.clone()).await? {
            if manifest.fingerprint != fingerprint {
                return Err(RuntimeError::Conflict {
                    workload_id,
                    message: "process manifest exists for a different workload specification"
                        .to_owned(),
                });
            }
            self.cached_specs()?
                .insert(workload_id.clone(), process.clone());
            return process_handle(workload_id);
        }
        let manifest = ProcessManifest::created(
            process.configuration.metadata.clone(),
            fingerprint,
            paths.stdout,
            paths.stderr,
        );
        self.write(manifest).await?;
        self.cached_specs()?
            .insert(workload_id.clone(), process.clone());
        self.events.emit(
            &process.configuration.metadata,
            RuntimeEventKind::Created,
            None,
        )?;
        process_handle(workload_id)
    }

    async fn start(&self, handle: &WorkloadHandle) -> Result<(), RuntimeError> {
        validate_process_handle(handle)?;
        let _guard = self.mutation.lock().await;
        let mut manifest = self.load(handle.workload_id().clone()).await?;
        if let Some(process) = manifest.process {
            let status = self
                .supervisor
                .status(process)
                .await
                .map_err(runtime_supervisor_error)?;
            if status == ProcessStatus::Running {
                return Ok(());
            }
            self.supervisor
                .forget(process)
                .await
                .map_err(runtime_supervisor_error)?;
        }
        let spec = self
            .cached_specs()?
            .get(handle.workload_id())
            .cloned()
            .ok_or_else(|| RuntimeError::Conflict {
                workload_id: handle.workload_id().clone(),
                message: "process spec must be re-established with create before start".to_owned(),
            })?;
        let paths = process_paths(&self.root, handle.workload_id());
        let supervised = build_supervised_spec(&spec, &paths)?;
        manifest.operation_generation = manifest.operation_generation.saturating_add(1);
        manifest.state = ManifestState::Starting;
        manifest.process = None;
        self.write(manifest.clone()).await?;
        let root = self.root.clone();
        let workload_id = handle.workload_id().clone();
        let operation_generation = manifest.operation_generation;
        self.supervisor
            .spawn_committed(supervised, move |process| {
                commit_process_start(&root, &workload_id, operation_generation, process)
                    .map_err(|error| error.into_supervisor())
            })
            .await
            .map_err(runtime_supervisor_error)?;
        self.events
            .emit(&manifest.metadata, RuntimeEventKind::Started, None)?;
        Ok(())
    }

    async fn stop(
        &self,
        handle: &WorkloadHandle,
        request: ShutdownRequest,
    ) -> Result<(), RuntimeError> {
        validate_process_handle(handle)?;
        let _guard = self.mutation.lock().await;
        let mut manifest = self.load(handle.workload_id().clone()).await?;
        let Some(process) = manifest.process else {
            manifest.state = ManifestState::Stopped;
            self.write(manifest).await?;
            return Ok(());
        };
        if self.observe(&mut manifest).await?.state == WorkloadState::Stopped {
            return Ok(());
        }
        self.supervisor
            .signal(process, ProcessSignal::Terminate)
            .await
            .map_err(runtime_supervisor_error)?;
        let exit = self
            .await_exit(handle.workload_id(), process, "stop", request.timeout)
            .await?;
        self.record_exit(&mut manifest, exit.code, exit.signal)
            .await?;
        Ok(())
    }

    async fn kill(&self, handle: &WorkloadHandle) -> Result<(), RuntimeError> {
        validate_process_handle(handle)?;
        let _guard = self.mutation.lock().await;
        let mut manifest = self.load(handle.workload_id().clone()).await?;
        let Some(process) = manifest.process else {
            manifest.state = ManifestState::Stopped;
            self.write(manifest).await?;
            return Ok(());
        };
        if self.observe(&mut manifest).await?.state == WorkloadState::Stopped {
            return Ok(());
        }
        self.supervisor
            .signal(process, ProcessSignal::Kill)
            .await
            .map_err(runtime_supervisor_error)?;
        let exit = self
            .await_exit(
                handle.workload_id(),
                process,
                "kill",
                self.settings.kill_timeout,
            )
            .await?;
        self.record_exit(&mut manifest, exit.code, exit.signal)
            .await?;
        Ok(())
    }

    async fn remove(&self, handle: &WorkloadHandle) -> Result<(), RuntimeError> {
        validate_process_handle(handle)?;
        let _guard = self.mutation.lock().await;
        let Some(mut manifest) = self.load_optional(handle.workload_id().clone()).await? else {
            return Ok(());
        };
        if self.observe(&mut manifest).await?.state == WorkloadState::Running {
            return Err(RuntimeError::Conflict {
                workload_id: handle.workload_id().clone(),
                message: "running process workload cannot be removed".to_owned(),
            });
        }
        let root = self.root.clone();
        let workload_id = handle.workload_id().clone();
        blocking(move || {
            remove_manifest(&root, &workload_id).map_err(|error| error.into_runtime())
        })
        .await?;
        self.cached_specs()?.remove(handle.workload_id());
        if let Some(process) = manifest.process {
            self.supervisor
                .forget(process)
                .await
                .map_err(runtime_supervisor_error)?;
        }
        self.events
            .emit(&manifest.metadata, RuntimeEventKind::Removed, None)?;
        Ok(())
    }

    async fn status(&self, handle: &WorkloadHandle) -> Result<WorkloadStatus, RuntimeError> {
        validate_process_handle(handle)?;
        let _guard = self.mutation.lock().await;
        let mut manifest = self.load(handle.workload_id().clone()).await?;
        self.observe(&mut manifest).await
    }

    async fn list(
        &self,
        cluster_id: &ClusterId,
        node_id: &NodeId,
    ) -> Result<Vec<ObservedWorkload>, RuntimeError> {
        let _guard = self.mutation.lock().await;
        let root = self.root.clone();
        let manifests =
            blocking(move || load_manifests(&root).map_err(|error| error.into_runtime())).await?;
        let mut observed = Vec::new();
        for mut manifest in manifests.into_iter().filter(|manifest| {
            &manifest.metadata.cluster_id == cluster_id && &manifest.metadata.node_id == node_id
        }) {
            let status = self.observe(&mut manifest).await?;
            observed.push(ObservedWorkload {
                handle: process_handle(manifest.metadata.workload_id.clone())?,
                metadata: manifest.metadata,
                status,
            });
        }
        Ok(observed)
    }

    async fn events(
        &self,
        request: EventRequest,
    ) -> Result<Box<dyn RuntimeEventStream>, RuntimeError> {
        self.events
            .stream(request, self.clock.clone(), self.settings.poll_interval)
    }

    async fn logs(
        &self,
        handle: &WorkloadHandle,
        request: LogRequest,
    ) -> Result<Box<dyn LogStream>, RuntimeError> {
        validate_process_handle(handle)?;
        let manifest = self.load(handle.workload_id().clone()).await?;
        FileLogStream::open(
            manifest.stdout_path,
            manifest.stderr_path,
            request.after.as_ref(),
            request.mode,
            self.clock.clone(),
            self.settings.poll_interval,
        )
    }

    async fn exec(
        &self,
        _handle: &WorkloadHandle,
        _request: ExecRequest,
    ) -> Result<Box<dyn ExecSession>, RuntimeError> {
        Err(RuntimeError::Unsupported {
            capability: crate::RuntimeCapability::Exec,
        })
    }

    async fn stats_handle(&self, handle: &WorkloadHandle) -> Result<CgroupPath, RuntimeError> {
        validate_process_handle(handle)?;
        let manifest = self.load(handle.workload_id().clone()).await?;
        let process = manifest.process.ok_or_else(|| RuntimeError::Conflict {
            workload_id: handle.workload_id().clone(),
            message: "created process workload does not have a cgroup yet".to_owned(),
        })?;
        let path = blocking(move || read_cgroup_path(process)).await?;
        CgroupPath::new(path).map_err(|error| RuntimeError::Rejected {
            message: error.to_string(),
        })
    }
}
