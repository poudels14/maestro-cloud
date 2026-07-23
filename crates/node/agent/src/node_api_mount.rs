use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use kernel_api::WorkloadId;
use node_fabric::{WorkloadAuthorization, WorkloadClaims};
use runtime::WorkloadMount;
use tokio::sync::{Mutex, oneshot};
use tokio::task::JoinHandle;

use crate::node_api::{
    BoundWorkloadNodeApi, NodeApiServices, NodeApiSocketOwner, WorkloadControlAccess,
};
use crate::node_api_files::{
    NodeApiMountError, cleanup_node_api_directory, list_workload_directories,
    prepare_node_api_files, remove_stale_socket,
};

pub(crate) struct NodeApiMountManager {
    root: PathBuf,
    services: Option<NodeApiServices>,
    running: Mutex<BTreeMap<WorkloadId, RunningNodeApi>>,
}

impl NodeApiMountManager {
    pub(crate) fn new(
        root: PathBuf,
        services: Option<NodeApiServices>,
    ) -> Result<Self, NodeApiMountError> {
        crate::node_api_files::validate_root(&root)?;
        Ok(Self {
            root,
            services,
            running: Mutex::new(BTreeMap::new()),
        })
    }

    pub(crate) async fn ensure(
        &self,
        workload_id: &WorkloadId,
        owner: NodeApiSocketOwner,
        claims: WorkloadClaims,
        control_access: WorkloadControlAccess,
    ) -> Result<WorkloadMount, NodeApiMountError> {
        let mut running = self.running.lock().await;
        let services = self
            .services
            .clone()
            .ok_or(NodeApiMountError::ServicesUnavailable)?;
        let root = self.root.clone();
        let prepared_workload_id = workload_id.clone();
        let prepared = tokio::task::spawn_blocking(move || {
            prepare_node_api_files(&root, &prepared_workload_id, owner)
        })
        .await
        .map_err(task_error)??;
        let mount = prepared.workload_mount();
        let binding = NodeApiBinding {
            owner,
            claims: claims.clone(),
            control_access,
        };
        if let Some(existing) = running.get(workload_id) {
            if existing.binding != binding {
                return Err(NodeApiMountError::BindingConflict {
                    workload_id: workload_id.clone(),
                });
            }
            if !existing.task.is_finished() {
                return Ok(mount);
            }
        }
        if let Some(finished) = running.remove(workload_id) {
            let _finished_result = finished.task.await;
        }
        remove_stale_socket(&prepared.socket_path)?;
        let authorization =
            WorkloadAuthorization::new(prepared.token, owner.user_id, claims.clone());
        let server = BoundWorkloadNodeApi::bind(
            &prepared.socket_path,
            authorization,
            owner,
            control_access,
            services,
        )?;
        let (shutdown, shutdown_receiver) = oneshot::channel();
        let task = tokio::spawn(server.serve_with_shutdown(async move {
            let _ = shutdown_receiver.await;
        }));
        running.insert(
            workload_id.clone(),
            RunningNodeApi {
                binding,
                shutdown,
                task,
            },
        );
        Ok(mount)
    }

    pub(crate) async fn cleanup(&self, workload_id: &WorkloadId) -> Result<(), NodeApiMountError> {
        let running = self.running.lock().await.remove(workload_id);
        let server_result = match running {
            Some(running) => {
                let _ = running.shutdown.send(());
                Some(running.task.await.map_err(task_error)?)
            }
            None => None,
        };
        let directory = self.root.join(workload_id.as_str());
        tokio::task::spawn_blocking(move || cleanup_node_api_directory(&directory))
            .await
            .map_err(task_error)??;
        if let Some(result) = server_result {
            result?;
        }
        Ok(())
    }

    pub(crate) async fn cleanup_stale(
        &self,
        active_workloads: &BTreeSet<String>,
    ) -> Result<usize, NodeApiMountError> {
        let root = self.root.clone();
        let workloads = tokio::task::spawn_blocking(move || list_workload_directories(&root))
            .await
            .map_err(task_error)??;
        let stale = workloads
            .into_iter()
            .filter(|workload_id| !active_workloads.contains(workload_id.as_str()))
            .collect::<Vec<_>>();
        for workload_id in &stale {
            self.cleanup(workload_id).await?;
        }
        Ok(stale.len())
    }

    pub(crate) async fn shutdown_all(&self) -> Result<(), NodeApiMountError> {
        let running = {
            let mut guard = self.running.lock().await;
            std::mem::take(&mut *guard)
                .into_values()
                .collect::<Vec<_>>()
        };
        for server in running {
            let _ = server.shutdown.send(());
            server.task.await.map_err(task_error)??;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct NodeApiBinding {
    owner: NodeApiSocketOwner,
    claims: WorkloadClaims,
    control_access: WorkloadControlAccess,
}

struct RunningNodeApi {
    binding: NodeApiBinding,
    shutdown: oneshot::Sender<()>,
    task: JoinHandle<Result<(), crate::NodeApiServerError>>,
}

fn task_error(error: tokio::task::JoinError) -> NodeApiMountError {
    NodeApiMountError::Task {
        message: error.to_string(),
    }
}
