use std::time::Duration;

use containerd::services::v1::snapshots::MountsRequest;
use containerd::services::v1::{
    CreateTaskRequest, DeleteTaskRequest, GetRequest, KillRequest, StartRequest, WaitRequest,
};
use kernel_api::WorkloadId;

use crate::containerd::{ContainerdRuntime, snapshot_key};
use crate::containerd_io::{path_text, prepare_task_files};
use crate::containerd_support::{
    container_id, is_not_found, namespaced, namespaced_timeout, runtime_status,
};
use crate::{RuntimeError, WorkloadHandle};

const TASK_CLEANUP_POLL_INTERVAL: Duration = Duration::from_millis(25);

impl ContainerdRuntime {
    pub(crate) async fn task(
        &self,
        container_id: &str,
        workload_id: &WorkloadId,
    ) -> Result<Option<containerd::types::v1::Process>, RuntimeError> {
        let response =
            containerd::services::v1::tasks_client::TasksClient::new(self.channel.clone())
                .get(namespaced_timeout(
                    GetRequest {
                        container_id: container_id.to_owned(),
                        exec_id: String::new(),
                    },
                    &self.settings.namespace,
                    self.settings.rpc_timeout,
                )?)
                .await;
        match response {
            Ok(response) => Ok(response.into_inner().process),
            Err(error) if is_not_found(&error) => Ok(None),
            Err(error) => Err(runtime_status(error, workload_id)),
        }
    }

    pub(crate) async fn create_task(
        &self,
        handle: &WorkloadHandle,
        container_id: &str,
    ) -> Result<u32, RuntimeError> {
        let mounts = containerd::services::v1::snapshots::snapshots_client::SnapshotsClient::new(
            self.channel.clone(),
        )
        .mounts(namespaced_timeout(
            MountsRequest {
                snapshotter: self.settings.snapshotter.clone(),
                key: snapshot_key(container_id),
            },
            &self.settings.namespace,
            self.settings.rpc_timeout,
        )?)
        .await
        .map_err(|error| runtime_status(error, handle.workload_id()))?
        .into_inner()
        .mounts;
        let paths = prepare_task_files(&self.settings.state_root, handle.workload_id()).await?;
        let response =
            containerd::services::v1::tasks_client::TasksClient::new(self.channel.clone())
                .create(namespaced_timeout(
                    CreateTaskRequest {
                        container_id: container_id.to_owned(),
                        rootfs: mounts,
                        stdout: path_text(&paths.stdout)?,
                        stderr: path_text(&paths.stderr)?,
                        ..Default::default()
                    },
                    &self.settings.namespace,
                    self.settings.rpc_timeout,
                )?)
                .await
                .map_err(|error| runtime_status(error, handle.workload_id()))?
                .into_inner();
        if response.pid == 0 {
            Err(RuntimeError::Unavailable {
                message: format!(
                    "containerd created task for workload `{}` without a process id",
                    handle.workload_id()
                ),
            })
        } else {
            Ok(response.pid)
        }
    }

    pub(crate) async fn start_task(
        &self,
        handle: &WorkloadHandle,
        container_id: &str,
    ) -> Result<(), RuntimeError> {
        containerd::services::v1::tasks_client::TasksClient::new(self.channel.clone())
            .start(namespaced_timeout(
                StartRequest {
                    container_id: container_id.to_owned(),
                    exec_id: String::new(),
                },
                &self.settings.namespace,
                self.settings.rpc_timeout,
            )?)
            .await
            .map_err(|error| runtime_status(error, handle.workload_id()))?;
        Ok(())
    }

    pub(crate) async fn delete_task(
        &self,
        container_id: &str,
        workload_id: &WorkloadId,
    ) -> Result<(), RuntimeError> {
        let result = containerd::services::v1::tasks_client::TasksClient::new(self.channel.clone())
            .delete(namespaced_timeout(
                DeleteTaskRequest {
                    container_id: container_id.to_owned(),
                },
                &self.settings.namespace,
                self.settings.rpc_timeout,
            )?)
            .await;
        match result {
            Ok(_) => self.await_task_cgroup_cleanup(workload_id).await,
            Err(error) if is_not_found(&error) => self.await_task_cgroup_cleanup(workload_id).await,
            Err(error) => Err(runtime_status(error, workload_id)),
        }
    }

    pub(crate) async fn await_task_cgroup_cleanup(
        &self,
        workload_id: &WorkloadId,
    ) -> Result<(), RuntimeError> {
        let path = std::path::Path::new("/sys/fs/cgroup")
            .join(&self.settings.namespace)
            .join(workload_id.as_str());
        let deadline = self.clock.now().saturating_add(self.settings.kill_timeout);
        loop {
            match tokio::fs::try_exists(&path).await {
                Ok(false) => return Ok(()),
                Ok(true) if self.clock.now() < deadline => {
                    let retry_at = self.clock.now().saturating_add(TASK_CLEANUP_POLL_INTERVAL);
                    self.clock
                        .sleep_until(std::cmp::min(retry_at, deadline))
                        .await;
                }
                Ok(true) => {
                    return Err(RuntimeError::Timeout {
                        operation: "delete containerd task cgroup",
                        workload_id: workload_id.clone(),
                        timeout: self.settings.kill_timeout,
                    });
                }
                Err(error) => {
                    return Err(RuntimeError::Unavailable {
                        message: format!(
                            "failed to inspect deleted task cgroup `{}`: {error}",
                            path.display()
                        ),
                    });
                }
            }
        }
    }

    pub(crate) async fn signal_and_wait(
        &self,
        handle: &WorkloadHandle,
        signal: u32,
        timeout: Duration,
        operation: &'static str,
    ) -> Result<(), RuntimeError> {
        let container_id = container_id(handle, &self.settings.namespace)?;
        let mut tasks =
            containerd::services::v1::tasks_client::TasksClient::new(self.channel.clone());
        tasks
            .kill(namespaced_timeout(
                KillRequest {
                    container_id: container_id.to_owned(),
                    exec_id: String::new(),
                    signal,
                    all: true,
                },
                &self.settings.namespace,
                self.settings.rpc_timeout,
            )?)
            .await
            .map_err(|error| runtime_status(error, handle.workload_id()))?;
        let deadline = self.clock.now().saturating_add(timeout);
        let wait = tasks.wait(namespaced(
            WaitRequest {
                container_id: container_id.to_owned(),
                exec_id: String::new(),
            },
            &self.settings.namespace,
        )?);
        tokio::pin!(wait);
        tokio::select! {
            response = &mut wait => {
                response.map_err(|error| runtime_status(error, handle.workload_id()))?;
                Ok(())
            }
            () = self.clock.sleep_until(deadline) => Err(RuntimeError::Timeout {
                operation,
                workload_id: handle.workload_id().clone(),
                timeout,
            }),
        }
    }
}
