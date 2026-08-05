use async_trait::async_trait;
use kernel_api::{ClusterId, NodeId};
use supervisor::{ProcessSignal, ProcessStatus};

use super::ProcessRuntime;
use crate::file_log::FileLogStream;
use crate::process_manifest::{
    ManifestState, ProcessManifest, commit_process_start, list_workload_ids, remove_manifest,
};
use crate::process_support::{
    blocking, build_supervised_spec, process_handle, process_paths, read_cgroup_path,
    runtime_supervisor_error, spec_fingerprint, validate_process_handle,
};
use crate::{
    Capabilities, CgroupPath, EventRequest, ExecRequest, ExecSession, LogRequest, LogStream,
    ObservedWorkload, RuntimeError, RuntimeEventKind, RuntimeEventStream, ShutdownRequest,
    WorkloadHandle, WorkloadRuntime, WorkloadSpec, WorkloadState, WorkloadStatsReading,
    WorkloadStatus,
};

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
        if process.configuration.user_namespace.is_some() {
            return Err(RuntimeError::InvalidSpec {
                message: "host-process workloads cannot request a user namespace".to_owned(),
            });
        }
        let fingerprint = spec_fingerprint(spec)?;
        let workload_id = process.configuration.metadata.workload_id.clone();
        let paths = process_paths(&self.root, &workload_id);
        build_supervised_spec(process, &paths)?;
        let _operation = self.operation(&workload_id).await?;
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
        let _operation = self.operation(handle.workload_id()).await?;
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
        let _operation = self.operation(handle.workload_id()).await?;
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
        let _operation = self.operation(handle.workload_id()).await?;
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
        let _operation = self.operation(handle.workload_id()).await?;
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
        let _operation = self.operation(handle.workload_id()).await?;
        let mut manifest = self.load(handle.workload_id().clone()).await?;
        self.observe(&mut manifest).await
    }

    async fn list(
        &self,
        cluster_id: &ClusterId,
        node_id: &NodeId,
    ) -> Result<Vec<ObservedWorkload>, RuntimeError> {
        let root = self.root.clone();
        let workload_ids =
            blocking(move || list_workload_ids(&root).map_err(|error| error.into_runtime()))
                .await?;
        let mut observed = Vec::new();
        for workload_id in workload_ids {
            let _operation = self.operation(&workload_id).await?;
            let Some(mut manifest) = self.load_optional(workload_id).await? else {
                continue;
            };
            if &manifest.metadata.cluster_id != cluster_id || &manifest.metadata.node_id != node_id
            {
                continue;
            }
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

    async fn stats(&self, handle: &WorkloadHandle) -> Result<WorkloadStatsReading, RuntimeError> {
        validate_process_handle(handle)?;
        let manifest = self.load(handle.workload_id().clone()).await?;
        let process = manifest.process.ok_or_else(|| RuntimeError::Conflict {
            workload_id: handle.workload_id().clone(),
            message: "created process workload does not have a cgroup yet".to_owned(),
        })?;
        let path = blocking(move || read_cgroup_path(process)).await?;
        CgroupPath::new(path)
            .map(WorkloadStatsReading::CgroupV2)
            .map_err(|error| RuntimeError::Rejected {
                message: error.to_string(),
            })
    }
}
