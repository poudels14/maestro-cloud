use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use docker::Docker;
use docker::exec::{CreateExecOptions, StartExecResults};
use docker::query_parameters::{
    CreateContainerOptionsBuilder, EventsOptionsBuilder, InspectContainerOptions,
    ListContainersOptionsBuilder, LogsOptionsBuilder, RemoveContainerOptionsBuilder,
    StopContainerOptionsBuilder,
};
use kernel_api::{ClusterId, NodeId};

use crate::cgroup;
use crate::docker_config::{CLUSTER_LABEL, MANAGED_LABEL, NODE_LABEL, container_config};
use crate::docker_network_ipam::DockerNetworkState;
use crate::docker_stream::{DockerEventStream, DockerExecSession, DockerLogStream, log_since};
use crate::docker_support::{
    container_id, docker_handle, is_conflict, is_not_found, is_not_modified, observed_workload,
    process_id, runtime_error, validate_existing, workload_status,
};
use crate::{
    Capabilities, CgroupPath, EventRequest, ExecMode, ExecRequest, ExecSession, LogMode,
    LogRequest, LogStream, ObservedWorkload, RuntimeCapability, RuntimeError, RuntimeEventStream,
    ShutdownRequest, WorkloadHandle, WorkloadRuntime, WorkloadSpec, WorkloadState, WorkloadStatus,
};

/// Docker Engine workload backend using Bollard's native daemon API.
#[derive(Clone)]
pub struct DockerRuntime {
    pub(crate) client: Docker,
    pub(crate) network_state: Arc<tokio::sync::Mutex<DockerNetworkState>>,
}

impl DockerRuntime {
    /// Connects using `DOCKER_HOST` or the platform's standard local Docker socket.
    pub fn connect_with_defaults() -> Result<Self, RuntimeError> {
        let client =
            Docker::connect_with_defaults().map_err(|error| RuntimeError::Unavailable {
                message: format!("failed to connect to the docker API: {error}"),
            })?;
        Ok(Self::new(client))
    }

    /// Wraps an existing Bollard client while preserving its connection configuration.
    pub fn new(client: Docker) -> Self {
        Self {
            client,
            network_state: Arc::new(tokio::sync::Mutex::new(DockerNetworkState::default())),
        }
    }

    pub(crate) async fn inspect_container(
        &self,
        handle: &WorkloadHandle,
    ) -> Result<docker::models::ContainerInspectResponse, RuntimeError> {
        let container_id = container_id(handle)?;
        self.client
            .inspect_container(container_id, None::<InspectContainerOptions>)
            .await
            .map_err(|error| runtime_error(error, handle.workload_id()))
    }
}

#[async_trait]
impl WorkloadRuntime for DockerRuntime {
    fn capabilities(&self) -> Capabilities {
        Capabilities::new([
            RuntimeCapability::Exec,
            RuntimeCapability::InteractiveExec,
            RuntimeCapability::DynamicNetwork,
        ])
    }

    async fn create(&self, spec: &WorkloadSpec) -> Result<WorkloadHandle, RuntimeError> {
        let config = container_config(spec)?;
        let workload_id = &spec.configuration().metadata.workload_id;
        match self
            .client
            .inspect_container(&config.name, None::<InspectContainerOptions>)
            .await
        {
            Ok(inspect) => validate_existing(&inspect, workload_id, &config.fingerprint),
            Err(error) if is_not_found(&error) => {
                let options = CreateContainerOptionsBuilder::default()
                    .name(&config.name)
                    .build();
                match self
                    .client
                    .create_container(Some(options), config.body)
                    .await
                {
                    Ok(created) => docker_handle(workload_id.clone(), created.id),
                    Err(error) if is_conflict(&error) => {
                        let inspect = self
                            .client
                            .inspect_container(&config.name, None::<InspectContainerOptions>)
                            .await
                            .map_err(|error| runtime_error(error, workload_id))?;
                        validate_existing(&inspect, workload_id, &config.fingerprint)
                    }
                    Err(error) => Err(runtime_error(error, workload_id)),
                }
            }
            Err(error) => Err(runtime_error(error, workload_id)),
        }
    }

    async fn start(&self, handle: &WorkloadHandle) -> Result<(), RuntimeError> {
        let status = self.status(handle).await?;
        if matches!(status.state, WorkloadState::Running | WorkloadState::Paused) {
            Ok(())
        } else {
            let result = self
                .client
                .start_container(container_id(handle)?, None)
                .await;
            match result {
                Ok(()) => Ok(()),
                Err(error) if is_not_modified(&error) => Ok(()),
                Err(error) => Err(runtime_error(error, handle.workload_id())),
            }
        }
    }

    async fn stop(
        &self,
        handle: &WorkloadHandle,
        request: ShutdownRequest,
    ) -> Result<(), RuntimeError> {
        let status = self.status(handle).await?;
        if matches!(status.state, WorkloadState::Running | WorkloadState::Paused) {
            let options = StopContainerOptionsBuilder::default()
                .t(stop_timeout_seconds(request.timeout))
                .build();
            let result = self
                .client
                .stop_container(container_id(handle)?, Some(options))
                .await;
            match result {
                Ok(()) => Ok(()),
                Err(error) if is_not_modified(&error) => Ok(()),
                Err(error) => Err(runtime_error(error, handle.workload_id())),
            }
        } else {
            Ok(())
        }
    }

    async fn kill(&self, handle: &WorkloadHandle) -> Result<(), RuntimeError> {
        let status = self.status(handle).await?;
        if matches!(status.state, WorkloadState::Running | WorkloadState::Paused) {
            self.client
                .kill_container(container_id(handle)?, None)
                .await
                .map_err(|error| runtime_error(error, handle.workload_id()))
        } else {
            Ok(())
        }
    }

    async fn remove(&self, handle: &WorkloadHandle) -> Result<(), RuntimeError> {
        let container_id = container_id(handle)?;
        let options = RemoveContainerOptionsBuilder::default().v(true).build();
        match self
            .client
            .remove_container(container_id, Some(options))
            .await
        {
            Ok(()) => Ok(()),
            Err(error) if is_not_found(&error) => Ok(()),
            Err(error) => Err(runtime_error(error, handle.workload_id())),
        }
    }

    async fn status(&self, handle: &WorkloadHandle) -> Result<WorkloadStatus, RuntimeError> {
        Ok(workload_status(&self.inspect_container(handle).await?))
    }

    async fn list(
        &self,
        cluster_id: &ClusterId,
        node_id: &NodeId,
    ) -> Result<Vec<ObservedWorkload>, RuntimeError> {
        let filters = ownership_filters(cluster_id, node_id);
        let options = ListContainersOptionsBuilder::default()
            .all(true)
            .filters(&filters)
            .build();
        let summaries = self
            .client
            .list_containers(Some(options))
            .await
            .map_err(|error| RuntimeError::Unavailable {
                message: error.to_string(),
            })?;
        let mut observed = Vec::with_capacity(summaries.len());
        for summary in summaries {
            let container_id = summary.id.ok_or_else(|| RuntimeError::Unavailable {
                message: "docker list response omitted a container ID".to_owned(),
            })?;
            match self
                .client
                .inspect_container(&container_id, None::<InspectContainerOptions>)
                .await
            {
                Ok(inspect) => observed.push(observed_workload(&inspect)?),
                Err(error) if is_not_found(&error) => {}
                Err(error) => {
                    return Err(RuntimeError::Unavailable {
                        message: error.to_string(),
                    });
                }
            }
        }
        observed.sort_by(|left, right| left.handle.workload_id().cmp(right.handle.workload_id()));
        Ok(observed)
    }

    async fn events(
        &self,
        request: EventRequest,
    ) -> Result<Box<dyn RuntimeEventStream>, RuntimeError> {
        let filters = ownership_filters(&request.cluster_id, &request.node_id);
        let mut options = EventsOptionsBuilder::default().filters(&filters);
        if let Some(cursor) = request.after {
            options = options.since(cursor.as_str());
        }
        let events = Box::pin(self.client.events(Some(options.build())));
        Ok(Box::new(DockerEventStream::new(events)))
    }

    async fn logs(
        &self,
        handle: &WorkloadHandle,
        request: LogRequest,
    ) -> Result<Box<dyn LogStream>, RuntimeError> {
        let container_id = container_id(handle)?;
        let since = log_since(request.after.as_ref())?;
        let options = LogsOptionsBuilder::default()
            .follow(request.mode == LogMode::Follow)
            .stdout(true)
            .stderr(true)
            .timestamps(true)
            .since(since)
            .build();
        let output = Box::pin(self.client.logs(container_id, Some(options)));
        Ok(Box::new(DockerLogStream::new(output)))
    }

    async fn exec(
        &self,
        handle: &WorkloadHandle,
        request: ExecRequest,
    ) -> Result<Box<dyn ExecSession>, RuntimeError> {
        let container_id = container_id(handle)?;
        let terminal = matches!(request.mode, ExecMode::Terminal { .. });
        let mut command = Vec::with_capacity(request.command.arguments.len() + 1);
        command.push(request.command.executable);
        command.extend(request.command.arguments);
        let environment = request
            .environment
            .into_iter()
            .map(|(name, value)| format!("{name}={value}"))
            .collect();
        let created = self
            .client
            .create_exec(
                container_id,
                CreateExecOptions {
                    attach_stdin: Some(true),
                    attach_stdout: Some(true),
                    attach_stderr: Some(true),
                    tty: Some(terminal),
                    env: Some(environment),
                    cmd: Some(command),
                    ..Default::default()
                },
            )
            .await
            .map_err(|error| runtime_error(error, handle.workload_id()))?;
        let started = self
            .client
            .start_exec(&created.id, None)
            .await
            .map_err(|error| runtime_error(error, handle.workload_id()))?;
        match started {
            StartExecResults::Attached { output, input } => {
                if let ExecMode::Terminal { columns, rows } = request.mode {
                    self.client
                        .resize_exec(
                            &created.id,
                            docker::exec::ResizeExecOptions {
                                height: rows,
                                width: columns,
                            },
                        )
                        .await
                        .map_err(|error| runtime_error(error, handle.workload_id()))?;
                }
                Ok(Box::new(DockerExecSession::new(
                    self.client.clone(),
                    created.id,
                    terminal,
                    input,
                    output,
                )))
            }
            StartExecResults::Detached => Err(RuntimeError::Rejected {
                message: "docker unexpectedly detached an attached exec session".to_owned(),
            }),
        }
    }

    async fn stats_handle(&self, handle: &WorkloadHandle) -> Result<CgroupPath, RuntimeError> {
        let inspect = self.inspect_container(handle).await?;
        let process_id = process_id(&inspect, handle.workload_id())?;
        let path = tokio::task::spawn_blocking(move || cgroup::read_cgroup_path(process_id))
            .await
            .map_err(|error| RuntimeError::Unavailable {
                message: format!("docker cgroup inspection task failed: {error}"),
            })??;
        CgroupPath::new(path).map_err(|error| RuntimeError::Rejected {
            message: error.to_string(),
        })
    }
}

fn ownership_filters(cluster_id: &ClusterId, node_id: &NodeId) -> HashMap<String, Vec<String>> {
    HashMap::from([
        ("type".to_owned(), vec!["container".to_owned()]),
        (
            "label".to_owned(),
            vec![
                format!("{MANAGED_LABEL}=true"),
                format!("{CLUSTER_LABEL}={cluster_id}"),
                format!("{NODE_LABEL}={node_id}"),
            ],
        ),
    ])
}

fn stop_timeout_seconds(timeout: Duration) -> i32 {
    let rounded = timeout
        .as_secs()
        .saturating_add(u64::from(timeout.subsec_nanos() > 0));
    i32::try_from(rounded).unwrap_or(i32::MAX)
}
