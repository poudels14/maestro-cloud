use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use async_trait::async_trait;
use containerd::services::v1::snapshots::{
    PrepareSnapshotRequest, RemoveSnapshotRequest, StatSnapshotRequest,
};
use containerd::services::v1::{
    CreateContainerRequest, DeleteContainerRequest, GetContainerRequest, ListContainersRequest,
    ListTasksRequest,
};
use containerd::tonic::transport::Channel;
use kernel_api::{ClusterId, NodeId, WorkloadId};

use crate::cgroup;
use crate::containerd_build::{BuildctlRunner, ProcessBuildctlRunner};
use crate::containerd_config::{container_record, fingerprint, validate_runtime_features};
use crate::containerd_event::ContainerdEventStream;
use crate::containerd_exec::start_exec;
use crate::containerd_image::load_image;
use crate::containerd_io::task_paths;
use crate::containerd_network::ContainerdNetworkState;
use crate::containerd_resolver::prepare_resolver_file;
use crate::containerd_settings::ContainerdRuntimeSettings;
use crate::containerd_support::{
    CLUSTER_LABEL, NODE_LABEL, container_id, container_name, is_already_exists, is_not_found,
    namespaced_timeout, observed_workload, runtime_status, task_container_id, task_status,
    validate_existing,
};
use crate::containerd_volume::prepare_managed_volumes;
use crate::file_log::FileLogStream;
use crate::{
    ArtifactStore, Capabilities, CgroupPath, EventRequest, ExecRequest, ExecSession, LogRequest,
    LogStream, ObservedWorkload, RuntimeCapability, RuntimeClock, RuntimeError, RuntimeEventStream,
    ShutdownRequest, WorkloadHandle, WorkloadRuntime, WorkloadSpec, WorkloadState,
    WorkloadStatsReading, WorkloadStatus,
};

/// Native containerd workload backend scoped to one explicit containerd namespace.
#[derive(Clone)]
pub struct ContainerdRuntime {
    pub(crate) channel: Channel,
    pub(crate) settings: Arc<ContainerdRuntimeSettings>,
    pub(crate) clock: Arc<dyn RuntimeClock>,
    pub(crate) build_runner: Arc<dyn BuildctlRunner>,
    next_exec: Arc<AtomicU64>,
    pub(crate) network_state: Arc<tokio::sync::Mutex<ContainerdNetworkState>>,
}

impl ContainerdRuntime {
    /// Connects to the configured containerd Unix socket and validates node-local settings.
    pub async fn connect(
        settings: ContainerdRuntimeSettings,
        clock: Arc<dyn RuntimeClock>,
    ) -> Result<Self, RuntimeError> {
        settings.validate()?;
        let channel = containerd::connect(&settings.socket)
            .await
            .map_err(|error| RuntimeError::Unavailable {
                message: format!(
                    "failed to connect to containerd at `{}`: {error}",
                    settings.socket.display()
                ),
            })?;
        Self::new(channel, settings, clock)
    }

    /// Wraps an established containerd channel with validated runtime settings.
    pub fn new(
        channel: Channel,
        settings: ContainerdRuntimeSettings,
        clock: Arc<dyn RuntimeClock>,
    ) -> Result<Self, RuntimeError> {
        settings.validate()?;
        let build_runner = Arc::new(ProcessBuildctlRunner);
        Ok(Self {
            channel,
            settings: Arc::new(settings),
            clock,
            build_runner,
            next_exec: Arc::new(AtomicU64::new(1)),
            network_state: Arc::new(tokio::sync::Mutex::new(ContainerdNetworkState::default())),
        })
    }

    async fn container(
        &self,
        container_id: &str,
        workload_id: &WorkloadId,
    ) -> Result<containerd::services::v1::Container, RuntimeError> {
        let response = containerd::services::v1::containers_client::ContainersClient::new(
            self.channel.clone(),
        )
        .get(namespaced_timeout(
            GetContainerRequest {
                id: container_id.to_owned(),
            },
            &self.settings.namespace,
            self.settings.rpc_timeout,
        )?)
        .await
        .map_err(|error| runtime_status(error, workload_id))?;
        response
            .into_inner()
            .container
            .ok_or_else(|| RuntimeError::Unavailable {
                message: format!("containerd omitted metadata for container `{container_id}`"),
            })
    }

    async fn snapshot_exists(
        &self,
        key: &str,
        workload_id: &WorkloadId,
    ) -> Result<bool, RuntimeError> {
        let result = containerd::services::v1::snapshots::snapshots_client::SnapshotsClient::new(
            self.channel.clone(),
        )
        .stat(namespaced_timeout(
            StatSnapshotRequest {
                snapshotter: self.settings.snapshotter.clone(),
                key: key.to_owned(),
            },
            &self.settings.namespace,
            self.settings.rpc_timeout,
        )?)
        .await;
        match result {
            Ok(_) => Ok(true),
            Err(error) if is_not_found(&error) => Ok(false),
            Err(error) => Err(runtime_status(error, workload_id)),
        }
    }
}

#[async_trait]
impl WorkloadRuntime for ContainerdRuntime {
    fn capabilities(&self) -> Capabilities {
        Capabilities::new([
            RuntimeCapability::Exec,
            RuntimeCapability::InteractiveExec,
            RuntimeCapability::KillExec,
            RuntimeCapability::DynamicNetwork,
            RuntimeCapability::BuildArtifact,
            RuntimeCapability::PushArtifact,
            RuntimeCapability::TransferArtifact,
        ])
    }

    async fn create(&self, spec: &WorkloadSpec) -> Result<WorkloadHandle, RuntimeError> {
        let WorkloadSpec::Container(workload) = spec else {
            return Err(RuntimeError::InvalidSpec {
                message: "containerd runtime accepts only container workloads".to_owned(),
            });
        };
        validate_runtime_features(workload)?;
        let workload_id = &workload.configuration.metadata.workload_id;
        let fingerprint = fingerprint(spec)?;
        let container_id = container_name(workload_id);
        let snapshot_key = snapshot_key(&container_id);
        let existing = self.container(&container_id, workload_id).await;
        match existing {
            Ok(container) => {
                let handle = validate_existing(
                    &container,
                    workload_id,
                    &fingerprint,
                    &self.settings.namespace,
                )?;
                if self.snapshot_exists(&snapshot_key, workload_id).await? {
                    prepare_managed_volumes(&self.settings.state_root, &workload.configuration)
                        .await?;
                    if let Some(dns_server) = workload.configuration.dns_server {
                        prepare_resolver_file(
                            &self.settings.state_root,
                            workload_id,
                            dns_server,
                            &workload.configuration.metadata.cluster_id,
                        )
                        .await?;
                    }
                    return Ok(handle);
                }
                self.remove(&handle).await?;
            }
            Err(RuntimeError::NotFound { .. }) => {}
            Err(error) => return Err(error),
        }
        self.ensure_local(&workload.image)
            .await
            .map_err(|error| crate::artifact::workload_artifact_error(&workload.image, error))?;
        let image = load_image(
            self.channel.clone(),
            &self.settings.namespace,
            workload.image.as_str(),
            workload_id,
        )
        .await?;
        let snapshot_result =
            containerd::services::v1::snapshots::snapshots_client::SnapshotsClient::new(
                self.channel.clone(),
            )
            .prepare(namespaced_timeout(
                PrepareSnapshotRequest {
                    snapshotter: self.settings.snapshotter.clone(),
                    key: snapshot_key.clone(),
                    parent: image.snapshot_parent,
                    labels: HashMap::from([(
                        "com.maestro.workload-id".to_owned(),
                        workload_id.to_string(),
                    )]),
                },
                &self.settings.namespace,
                self.settings.rpc_timeout,
            )?)
            .await;
        if let Err(error) = snapshot_result
            && !is_already_exists(&error)
        {
            return Err(runtime_status(error, workload_id));
        }
        let record = container_record(
            spec,
            &image.configuration,
            &self.settings,
            snapshot_key,
            fingerprint.clone(),
        )?;
        prepare_managed_volumes(&self.settings.state_root, &workload.configuration).await?;
        if let Some(dns_server) = workload.configuration.dns_server {
            prepare_resolver_file(
                &self.settings.state_root,
                workload_id,
                dns_server,
                &workload.configuration.metadata.cluster_id,
            )
            .await?;
        }
        let result = containerd::services::v1::containers_client::ContainersClient::new(
            self.channel.clone(),
        )
        .create(namespaced_timeout(
            CreateContainerRequest {
                container: Some(record),
            },
            &self.settings.namespace,
            self.settings.rpc_timeout,
        )?)
        .await;
        match result {
            Ok(response) => {
                let container =
                    response
                        .into_inner()
                        .container
                        .ok_or_else(|| RuntimeError::Unavailable {
                            message: "containerd create omitted container metadata".to_owned(),
                        })?;
                validate_existing(
                    &container,
                    workload_id,
                    &fingerprint,
                    &self.settings.namespace,
                )
            }
            Err(error) if is_already_exists(&error) => {
                let container = self.container(&container_id, workload_id).await?;
                validate_existing(
                    &container,
                    workload_id,
                    &fingerprint,
                    &self.settings.namespace,
                )
            }
            Err(error) => Err(runtime_status(error, workload_id)),
        }
    }

    async fn start(&self, handle: &WorkloadHandle) -> Result<(), RuntimeError> {
        let container_id = container_id(handle, &self.settings.namespace)?.to_owned();
        let process = self.task(&container_id, handle.workload_id()).await?;
        match task_status(process.as_ref()).state {
            WorkloadState::Running | WorkloadState::Paused => Ok(()),
            WorkloadState::Created => {
                if process.is_none() {
                    self.create_task(handle, &container_id).await?;
                }
                self.start_task(handle, &container_id).await
            }
            WorkloadState::Stopped => {
                self.delete_task(&container_id, handle.workload_id())
                    .await?;
                self.create_task(handle, &container_id).await?;
                self.start_task(handle, &container_id).await
            }
            WorkloadState::Failed => Err(RuntimeError::Conflict {
                workload_id: handle.workload_id().clone(),
                message: "containerd task is in an unknown state".to_owned(),
            }),
        }
    }

    async fn stop(
        &self,
        handle: &WorkloadHandle,
        request: ShutdownRequest,
    ) -> Result<(), RuntimeError> {
        let container_id = container_id(handle, &self.settings.namespace)?;
        let process = self.task(container_id, handle.workload_id()).await?;
        match task_status(process.as_ref()).state {
            WorkloadState::Running | WorkloadState::Paused => {
                self.signal_and_wait(handle, 15, request.timeout, "stop")
                    .await
            }
            WorkloadState::Created if process.is_some() => {
                self.delete_task(container_id, handle.workload_id()).await
            }
            _ => Ok(()),
        }
    }

    async fn kill(&self, handle: &WorkloadHandle) -> Result<(), RuntimeError> {
        let container_id = container_id(handle, &self.settings.namespace)?;
        let process = self.task(container_id, handle.workload_id()).await?;
        match task_status(process.as_ref()).state {
            WorkloadState::Running | WorkloadState::Paused => {
                self.signal_and_wait(handle, 9, self.settings.kill_timeout, "kill")
                    .await
            }
            WorkloadState::Created if process.is_some() => {
                self.delete_task(container_id, handle.workload_id()).await
            }
            _ => Ok(()),
        }
    }

    async fn remove(&self, handle: &WorkloadHandle) -> Result<(), RuntimeError> {
        let container_id = container_id(handle, &self.settings.namespace)?.to_owned();
        let process = self.task(&container_id, handle.workload_id()).await?;
        if matches!(
            task_status(process.as_ref()).state,
            WorkloadState::Running | WorkloadState::Paused
        ) {
            return Err(RuntimeError::Conflict {
                workload_id: handle.workload_id().clone(),
                message: "running containerd workload cannot be removed".to_owned(),
            });
        }
        if process.is_some() {
            self.delete_task(&container_id, handle.workload_id())
                .await?;
        }
        let delete = containerd::services::v1::containers_client::ContainersClient::new(
            self.channel.clone(),
        )
        .delete(namespaced_timeout(
            DeleteContainerRequest {
                id: container_id.clone(),
            },
            &self.settings.namespace,
            self.settings.rpc_timeout,
        )?)
        .await;
        if let Err(error) = delete
            && !is_not_found(&error)
        {
            return Err(runtime_status(error, handle.workload_id()));
        }
        let remove = containerd::services::v1::snapshots::snapshots_client::SnapshotsClient::new(
            self.channel.clone(),
        )
        .remove(namespaced_timeout(
            RemoveSnapshotRequest {
                snapshotter: self.settings.snapshotter.clone(),
                key: snapshot_key(&container_id),
            },
            &self.settings.namespace,
            self.settings.rpc_timeout,
        )?)
        .await;
        if let Err(error) = remove
            && !is_not_found(&error)
        {
            return Err(runtime_status(error, handle.workload_id()));
        }
        let paths = task_paths(&self.settings.state_root, handle.workload_id());
        match tokio::fs::remove_dir_all(&paths.directory).await {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(RuntimeError::Unavailable {
                message: format!(
                    "failed to remove containerd IO directory `{}`: {error}",
                    paths.directory.display()
                ),
            }),
        }
    }

    async fn status(&self, handle: &WorkloadHandle) -> Result<WorkloadStatus, RuntimeError> {
        let container_id = container_id(handle, &self.settings.namespace)?;
        self.container(container_id, handle.workload_id()).await?;
        let process = self.task(container_id, handle.workload_id()).await?;
        Ok(task_status(process.as_ref()))
    }

    async fn list(
        &self,
        cluster_id: &ClusterId,
        node_id: &NodeId,
    ) -> Result<Vec<ObservedWorkload>, RuntimeError> {
        let containers = containerd::services::v1::containers_client::ContainersClient::new(
            self.channel.clone(),
        )
        .list(namespaced_timeout(
            ListContainersRequest::default(),
            &self.settings.namespace,
            self.settings.rpc_timeout,
        )?)
        .await
        .map_err(|error| RuntimeError::Unavailable {
            message: format!("failed to list containerd containers: {error}"),
        })?
        .into_inner()
        .containers;
        let tasks = containerd::services::v1::tasks_client::TasksClient::new(self.channel.clone())
            .list(namespaced_timeout(
                ListTasksRequest::default(),
                &self.settings.namespace,
                self.settings.rpc_timeout,
            )?)
            .await
            .map_err(|error| RuntimeError::Unavailable {
                message: format!("failed to list containerd tasks: {error}"),
            })?
            .into_inner()
            .tasks
            .into_iter()
            .map(|process| (task_container_id(&process).to_owned(), process))
            .collect::<HashMap<_, _>>();
        let cluster_id = cluster_id.to_string();
        let node_id = node_id.to_string();
        let mut observed = containers
            .iter()
            .filter(|container| {
                container.labels.get(CLUSTER_LABEL) == Some(&cluster_id)
                    && container.labels.get(NODE_LABEL) == Some(&node_id)
            })
            .map(|container| {
                observed_workload(
                    container,
                    &self.settings.namespace,
                    task_status(tasks.get(&container.id)),
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        observed.sort_by(|left, right| left.handle.workload_id().cmp(right.handle.workload_id()));
        Ok(observed)
    }

    async fn events(
        &self,
        request: EventRequest,
    ) -> Result<Box<dyn RuntimeEventStream>, RuntimeError> {
        ContainerdEventStream::subscribe(
            self.channel.clone(),
            self.settings.namespace.clone(),
            request,
        )
    }

    async fn logs(
        &self,
        handle: &WorkloadHandle,
        request: LogRequest,
    ) -> Result<Box<dyn LogStream>, RuntimeError> {
        let container_id = container_id(handle, &self.settings.namespace)?;
        self.container(container_id, handle.workload_id()).await?;
        let paths = task_paths(&self.settings.state_root, handle.workload_id());
        FileLogStream::open(
            paths.stdout,
            paths.stderr,
            request.after.as_ref(),
            request.mode,
            self.clock.clone(),
            self.settings.log_poll_interval,
        )
    }

    async fn exec(
        &self,
        handle: &WorkloadHandle,
        request: ExecRequest,
    ) -> Result<Box<dyn ExecSession>, RuntimeError> {
        let container_id = container_id(handle, &self.settings.namespace)?.to_owned();
        self.container(&container_id, handle.workload_id()).await?;
        start_exec(
            self.channel.clone(),
            &self.settings,
            self.next_exec.clone(),
            container_id,
            request,
        )
        .await
    }

    async fn stats(&self, handle: &WorkloadHandle) -> Result<WorkloadStatsReading, RuntimeError> {
        let container_id = container_id(handle, &self.settings.namespace)?;
        let process = self
            .task(container_id, handle.workload_id())
            .await?
            .ok_or_else(|| RuntimeError::Conflict {
                workload_id: handle.workload_id().clone(),
                message: "containerd workload has no running task".to_owned(),
            })?;
        if !matches!(
            task_status(Some(&process)).state,
            WorkloadState::Running | WorkloadState::Paused
        ) {
            return Err(RuntimeError::Conflict {
                workload_id: handle.workload_id().clone(),
                message: "containerd workload has no running task".to_owned(),
            });
        }
        let path = tokio::task::spawn_blocking(move || cgroup::read_cgroup_path(process.pid))
            .await
            .map_err(|error| RuntimeError::Unavailable {
                message: format!("containerd cgroup inspection task failed: {error}"),
            })??;
        CgroupPath::new(path)
            .map(WorkloadStatsReading::CgroupV2)
            .map_err(|error| RuntimeError::Rejected {
                message: error.to_string(),
            })
    }
}

pub(crate) fn snapshot_key(container_id: &str) -> String {
    format!("{container_id}-rootfs")
}
