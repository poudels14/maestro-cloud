use std::collections::BTreeMap;
use std::process::Command;
use std::sync::Arc;

use clustertest::ClusterSetupCluster;
use kernel_api::{
    Assignment, AssignmentPhase, CommandSpec, DeploymentPhase, ReplicaState, ResourceKind,
    ServiceId, WorkloadId,
};
use kernel_store::Store;
use logs::{LogBody, LogQueryScope, LogQueryStore, LogReadOrder, LogReadQuery};
use logstore::{DuckLogStoreRuntime, DuckStoreSettings};
use runtime::{
    ContainerdRuntime, ContainerdRuntimeSettings, ExecMode, ExecOutput, ExecRequest,
    TokioRuntimeClock, WorkloadHandle, WorkloadRuntime, WorkloadState,
};

use super::workload_fixture::{LOG_STDERR_MARKER, LOG_STDOUT_MARKER, put_service};
use super::*;

const WORKLOAD_TIMEOUT: Duration = Duration::from_secs(90);

#[tokio::test]
#[ignore = "requires root, containerd, etcd, iproute2, nftables, WireGuard, and registry access"]
async fn real_process_workload_adopts_and_recovers_after_runtime_loss()
-> Result<(), Box<dyn std::error::Error>> {
    let mut cluster = RealProcessCluster::new(1).await?;
    cluster.bootstrap_seed().await?;

    let store = cluster.wait_store().await?;
    put_service(&store, &cluster.cluster.cluster_id).await?;
    drop(store);

    let initial = cluster.await_ready_workload(Some(0), None).await?;
    cluster.await_log_checkpoint(&initial.workload_id).await?;

    cluster.stop_daemon(0).await?;
    cluster.assert_shipped_logs().await?;
    let first_boot = cluster.read_boot_marker().await?;

    cluster.launch_node(0, StoreLaunchMode::Restart).await?;
    cluster.wait_store().await?;
    cluster.ensure_workload_ready(0).await?;
    let adopted = cluster.await_ready_workload(Some(0), None).await?;
    let adopted_boot = cluster.read_boot_marker().await?;
    if adopted_boot != first_boot {
        return Err("agent restart replaced a live containerd workload".into());
    }

    cluster.stop_process(0).await?;
    cluster.kill_runtime_workload().await?;
    cluster.launch_node(0, StoreLaunchMode::Restart).await?;
    cluster.wait_store().await?;
    cluster.ensure_workload_ready(0).await?;
    let recovered = cluster
        .await_ready_workload(None, Some(&first_boot))
        .await?;
    let recovered_boot = cluster.read_boot_marker().await?;
    if recovered_boot == first_boot {
        return Err("lost runtime task was not recreated".into());
    }

    cluster.stop_process(0).await?;
    cluster.remove_runtime_workload().await?;
    Ok(())
}

struct ReadyWorkload {
    workload_id: WorkloadId,
}

impl RealProcessCluster {
    pub(super) fn runtime_namespace(&self) -> String {
        format!("maestro-{}", self.cluster.cluster_id)
    }

    async fn stop_daemon(&mut self, index: usize) -> Result<(), RealClusterError> {
        let Some(mut child) = self.node_mut(index)?.child.take() else {
            return Err(RealClusterError::new("node process is not running"));
        };
        let status = Command::new("kill")
            .args(["-TERM", &child.id().to_string()])
            .status()
            .map_err(RealClusterError::from_display)?;
        if !status.success() {
            return Err(RealClusterError::new(format!(
                "failed to terminate daemon process {}",
                child.id()
            )));
        }
        let deadline = tokio::time::Instant::now() + SETUP_TIMEOUT;
        loop {
            if child
                .try_wait()
                .map_err(RealClusterError::from_display)?
                .is_some()
            {
                kill_process_group(child.id());
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                child.kill().map_err(RealClusterError::from_display)?;
                child.wait().map_err(RealClusterError::from_display)?;
                kill_process_group(child.id());
                return Err(RealClusterError::new(
                    "daemon did not stop gracefully before its deadline",
                ));
            }
            tokio::time::sleep(RETRY_DELAY).await;
        }
    }

    async fn await_ready_workload(
        &mut self,
        _expected_restarts: Option<u32>,
        replaced_boot: Option<&str>,
    ) -> Result<ReadyWorkload, RealClusterError> {
        let deadline = tokio::time::Instant::now() + WORKLOAD_TIMEOUT;
        let mut last_observation = "store was unavailable".to_owned();
        let mut last_boot_observation = "boot marker was not inspected".to_owned();
        loop {
            self.ensure_children_running()?;
            if let Ok(store) = self.connect_store().await {
                let assignments =
                    list_resources::<Assignment>(&store, &self.cluster.cluster_id, "Assignment")
                        .await
                        .map_err(RealClusterError::from_display)?;
                let replicas = list_resources::<ReplicaState>(
                    &store,
                    &self.cluster.cluster_id,
                    "ReplicaState",
                )
                .await
                .map_err(RealClusterError::from_display)?;
                last_observation = format!(
                    "assignments={:?}; replicas={:?}",
                    assignments
                        .iter()
                        .map(|assignment| (
                            assignment.meta.id.as_str(),
                            assignment.status.phase,
                            assignment
                                .status
                                .conditions
                                .iter()
                                .map(|condition| {
                                    (
                                        condition.reason.0.as_str(),
                                        condition.message.as_str(),
                                        condition.last_transition_time.0,
                                    )
                                })
                                .collect::<Vec<_>>(),
                        ))
                        .collect::<Vec<_>>(),
                    replicas
                        .iter()
                        .map(|replica| (
                            replica.spec.assignment_id.as_str(),
                            replica.status.phase,
                            replica
                                .status
                                .conditions
                                .iter()
                                .map(|condition| {
                                    (
                                        condition.condition_type,
                                        condition.reason.0.as_str(),
                                        condition.message.as_str(),
                                    )
                                })
                                .collect::<Vec<_>>(),
                        ))
                        .collect::<Vec<_>>(),
                );
                if let Some(assignment) = assignments.iter().find(|assignment| {
                    assignment.spec.service_id.as_str() == "m3-runtime"
                        && assignment.status.phase == AssignmentPhase::Running
                }) && let Some(workload_id) = assignment.status.workload_id.clone()
                    && let Some(replica) = replicas.into_iter().find(|replica| {
                        replica.spec.assignment_id == assignment.meta.id
                            && replica.status.phase == DeploymentPhase::Ready
                    })
                {
                    let ready = ReadyWorkload { workload_id };
                    if let Some(previous) = replaced_boot {
                        match self.read_boot_marker().await {
                            Ok(current) if current != previous => return Ok(ready),
                            Ok(current) => {
                                last_boot_observation = format!("boot marker remained `{current}`");
                            }
                            Err(error) => last_boot_observation = error.to_string(),
                        }
                    } else {
                        return Ok(ready);
                    }
                }
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(RealClusterError::new(format!(
                    "real workload did not converge with replacement marker {replaced_boot:?} at {}; {last_observation}; {last_boot_observation}; log: {}",
                    OffsetDateTime::now_utc(),
                    read_log(&self.node(0)?.log_path),
                )));
            }
            tokio::time::sleep(RETRY_DELAY).await;
        }
    }

    async fn await_log_checkpoint(
        &mut self,
        workload_id: &WorkloadId,
    ) -> Result<(), RealClusterError> {
        let checkpoint = self
            .node(0)?
            .data_directory
            .join("agent")
            .join("log-checkpoints")
            .join(format!("{}.cursor", workload_id.as_str()));
        let deadline = tokio::time::Instant::now() + WORKLOAD_TIMEOUT;
        loop {
            self.ensure_children_running()?;
            if std::fs::metadata(&checkpoint).is_ok_and(|metadata| metadata.len() > 0) {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(RealClusterError::new(format!(
                    "runtime log delivery did not commit checkpoint `{}`",
                    checkpoint.display()
                )));
            }
            tokio::time::sleep(RETRY_DELAY).await;
        }
    }

    async fn assert_shipped_logs(&self) -> Result<(), RealClusterError> {
        let database = self
            .node(0)?
            .data_directory
            .join("agent")
            .join("logs.duckdb");
        let runtime = DuckLogStoreRuntime::open(
            DuckStoreSettings::new(database, 1_024).map_err(RealClusterError::from_display)?,
        )
        .await
        .map_err(RealClusterError::from_display)?;
        let entries = runtime
            .store()
            .query_logs(
                &LogReadQuery::new(
                    LogQueryScope::Service(
                        ServiceId::new("m3-runtime").map_err(RealClusterError::from_display)?,
                    ),
                    LogReadOrder::OldestFirst,
                    100,
                )
                .map_err(RealClusterError::from_display)?,
            )
            .await
            .map_err(RealClusterError::from_display)?;
        let bodies = entries
            .iter()
            .filter_map(|entry| match &entry.entry.body {
                LogBody::Text(body) => Some(body.as_str()),
                LogBody::Bytes(_) => None,
            })
            .collect::<Vec<_>>();
        let complete = [LOG_STDOUT_MARKER, LOG_STDERR_MARKER]
            .into_iter()
            .all(|marker| bodies.iter().any(|body| body.contains(marker)));
        runtime
            .shutdown()
            .await
            .map_err(RealClusterError::from_display)?;
        if complete {
            Ok(())
        } else {
            Err(RealClusterError::new(format!(
                "shipped logs omitted runtime markers: {bodies:?}"
            )))
        }
    }

    async fn inspection_runtime(&self) -> Result<ContainerdRuntime, RealClusterError> {
        let state_root = self
            .node(0)?
            .data_directory
            .join("runtime")
            .join("containerd");
        ContainerdRuntime::connect(
            ContainerdRuntimeSettings {
                socket: self.containerd_socket.clone(),
                namespace: self.runtime_namespace(),
                state_root,
                ..ContainerdRuntimeSettings::default()
            },
            Arc::new(TokioRuntimeClock::new()),
        )
        .await
        .map_err(RealClusterError::from_display)
    }

    async fn runtime_workload(
        &self,
        runtime: &ContainerdRuntime,
    ) -> Result<WorkloadHandle, RealClusterError> {
        let workloads = runtime
            .list(&self.cluster.cluster_id, &self.node(0)?.node_id)
            .await
            .map_err(RealClusterError::from_display)?;
        let matching = workloads
            .iter()
            .filter(|workload| workload.metadata.service_id.as_str() == "m3-runtime")
            .collect::<Vec<_>>();
        let [workload] = matching.as_slice() else {
            return Err(RealClusterError::new(format!(
                "expected one m3-runtime workload, observed {} across {} owned workloads",
                matching.len(),
                workloads.len(),
            )));
        };
        if workload.status.state != WorkloadState::Running {
            return Err(RealClusterError::new(format!(
                "owned runtime workload was {:?} with exit code {:?} and detail {:?}",
                workload.status.state, workload.status.exit_code, workload.status.detail,
            )));
        }
        Ok(workload.handle.clone())
    }

    async fn read_boot_marker(&self) -> Result<String, RealClusterError> {
        let runtime = self.inspection_runtime().await?;
        let handle = self.runtime_workload(&runtime).await?;
        exec_stdout(&runtime, &handle, "cat /tmp/maestro-boot-id").await
    }

    async fn kill_runtime_workload(&self) -> Result<(), RealClusterError> {
        let runtime = self.inspection_runtime().await?;
        let handle = self.runtime_workload(&runtime).await?;
        runtime
            .kill(&handle)
            .await
            .map_err(RealClusterError::from_display)
    }

    async fn remove_runtime_workload(&self) -> Result<(), RealClusterError> {
        let runtime = self.inspection_runtime().await?;
        let workloads = runtime
            .list(&self.cluster.cluster_id, &self.node(0)?.node_id)
            .await
            .map_err(RealClusterError::from_display)?;
        for workload in workloads {
            runtime
                .kill(&workload.handle)
                .await
                .map_err(RealClusterError::from_display)?;
            runtime
                .remove(&workload.handle)
                .await
                .map_err(RealClusterError::from_display)?;
        }
        Ok(())
    }
}

pub(super) async fn list_resources<Resource>(
    store: &dyn Store,
    cluster_id: &ClusterId,
    kind: &str,
) -> Result<Vec<Resource>, Box<dyn std::error::Error>>
where
    Resource: serde::de::DeserializeOwned,
{
    let prefix = Keyspace::new(cluster_id).resource_kind(&ResourceKind::new(kind)?);
    store
        .list(&prefix)
        .await?
        .values
        .into_iter()
        .map(|stored| serde_json::from_slice(&stored.value).map_err(Into::into))
        .collect()
}

async fn exec_stdout(
    runtime: &ContainerdRuntime,
    handle: &WorkloadHandle,
    command: &str,
) -> Result<String, RealClusterError> {
    let mut session = runtime
        .exec(
            handle,
            ExecRequest {
                command: CommandSpec {
                    executable: "/bin/sh".to_owned(),
                    arguments: vec!["-c".to_owned(), command.to_owned()],
                },
                environment: BTreeMap::new(),
                mode: ExecMode::Pipes,
            },
        )
        .await
        .map_err(RealClusterError::from_display)?;
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    loop {
        match session
            .next()
            .await
            .map_err(RealClusterError::from_display)?
        {
            Some(ExecOutput::Stdout(payload)) => stdout.extend(payload),
            Some(ExecOutput::Stderr(payload)) => stderr.extend(payload),
            Some(ExecOutput::Exited { code: Some(0) }) => {
                return String::from_utf8(stdout)
                    .map(|value| value.trim().to_owned())
                    .map_err(RealClusterError::from_display);
            }
            Some(ExecOutput::Exited { code }) => {
                return Err(RealClusterError::new(format!(
                    "boot marker exec exited with {code:?}: {}",
                    String::from_utf8_lossy(&stderr).trim()
                )));
            }
            None => {
                return Err(RealClusterError::new(
                    "boot marker exec ended without status",
                ));
            }
        }
    }
}
