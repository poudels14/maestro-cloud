use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::{Arc, Weak};

use kernel_api::WorkloadId;
use node_fabric::{WorkloadAuthorization, WorkloadClaims};
use runtime::WorkloadMount;
use tokio::sync::{Mutex, OwnedSemaphorePermit, RwLock, Semaphore, oneshot};
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
    files: Arc<dyn NodeApiFileSystem>,
    running: Mutex<BTreeMap<WorkloadId, RunningNodeApi>>,
    lifecycle: RwLock<()>,
    operation_gates: Mutex<BTreeMap<WorkloadId, Weak<Semaphore>>>,
}

impl NodeApiMountManager {
    pub(crate) fn new(
        root: PathBuf,
        services: Option<NodeApiServices>,
    ) -> Result<Self, NodeApiMountError> {
        Self::with_file_system(root, services, Arc::new(HostNodeApiFileSystem))
    }

    pub(crate) fn with_file_system(
        root: PathBuf,
        services: Option<NodeApiServices>,
        files: Arc<dyn NodeApiFileSystem>,
    ) -> Result<Self, NodeApiMountError> {
        crate::node_api_files::validate_root(&root)?;
        Ok(Self {
            root,
            services,
            files,
            running: Mutex::new(BTreeMap::new()),
            lifecycle: RwLock::new(()),
            operation_gates: Mutex::new(BTreeMap::new()),
        })
    }

    pub(crate) async fn ensure(
        &self,
        workload_id: &WorkloadId,
        owner: NodeApiSocketOwner,
        claims: WorkloadClaims,
        control_access: WorkloadControlAccess,
    ) -> Result<WorkloadMount, NodeApiMountError> {
        let _lifecycle = self.lifecycle.read().await;
        let _operation = self.operation(workload_id).await?;
        let services = self
            .services
            .clone()
            .ok_or(NodeApiMountError::ServicesUnavailable)?;
        let files = self.files.clone();
        let root = self.root.clone();
        let prepared_workload_id = workload_id.clone();
        let prepared =
            tokio::task::spawn_blocking(move || files.prepare(&root, &prepared_workload_id, owner))
                .await
                .map_err(task_error)??;
        let mount = prepared.workload_mount();
        let binding = NodeApiBinding {
            owner,
            claims: claims.clone(),
            control_access,
        };
        let finished = {
            let mut running = self.running.lock().await;
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
            running.remove(workload_id)
        };
        if let Some(finished) = finished {
            let _finished_result = finished.task.await;
        }
        let files = self.files.clone();
        let socket_path = prepared.socket_path.clone();
        tokio::task::spawn_blocking(move || files.remove_stale_socket(&socket_path))
            .await
            .map_err(task_error)??;
        let authorization =
            WorkloadAuthorization::new(prepared.token, owner.user_id, claims.clone());
        let mut running = self.running.lock().await;
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
        let _lifecycle = self.lifecycle.read().await;
        let _operation = self.operation(workload_id).await?;
        let running = self.running.lock().await.remove(workload_id);
        let server_result = match running {
            Some(running) => {
                let _ = running.shutdown.send(());
                Some(running.task.await.map_err(task_error)?)
            }
            None => None,
        };
        let files = self.files.clone();
        let directory = self.root.join(workload_id.as_str());
        tokio::task::spawn_blocking(move || files.cleanup(&directory))
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
        let files = self.files.clone();
        let root = self.root.clone();
        let workloads = tokio::task::spawn_blocking(move || files.list_workloads(&root))
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
        let _lifecycle = self.lifecycle.write().await;
        let running = {
            let mut running = self.running.lock().await;
            std::mem::take(&mut *running)
                .into_values()
                .collect::<Vec<_>>()
        };
        for server in running {
            let _ = server.shutdown.send(());
            server.task.await.map_err(task_error)??;
        }
        Ok(())
    }

    async fn operation(
        &self,
        workload_id: &WorkloadId,
    ) -> Result<OwnedSemaphorePermit, NodeApiMountError> {
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
            .map_err(|error| NodeApiMountError::Task {
                message: format!("node API operation gate closed unexpectedly: {error}"),
            })
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

pub(crate) trait NodeApiFileSystem: Send + Sync {
    fn prepare(
        &self,
        root: &std::path::Path,
        workload_id: &WorkloadId,
        owner: NodeApiSocketOwner,
    ) -> Result<crate::node_api_files::PreparedNodeApiFiles, NodeApiMountError>;

    fn remove_stale_socket(&self, socket_path: &std::path::Path) -> Result<(), NodeApiMountError>;

    fn cleanup(&self, directory: &std::path::Path) -> Result<(), NodeApiMountError>;

    fn list_workloads(&self, root: &std::path::Path) -> Result<Vec<WorkloadId>, NodeApiMountError>;
}

struct HostNodeApiFileSystem;

impl NodeApiFileSystem for HostNodeApiFileSystem {
    fn prepare(
        &self,
        root: &std::path::Path,
        workload_id: &WorkloadId,
        owner: NodeApiSocketOwner,
    ) -> Result<crate::node_api_files::PreparedNodeApiFiles, NodeApiMountError> {
        prepare_node_api_files(root, workload_id, owner)
    }

    fn remove_stale_socket(&self, socket_path: &std::path::Path) -> Result<(), NodeApiMountError> {
        remove_stale_socket(socket_path)
    }

    fn cleanup(&self, directory: &std::path::Path) -> Result<(), NodeApiMountError> {
        cleanup_node_api_directory(directory)
    }

    fn list_workloads(&self, root: &std::path::Path) -> Result<Vec<WorkloadId>, NodeApiMountError> {
        list_workload_directories(root)
    }
}

fn task_error(error: tokio::task::JoinError) -> NodeApiMountError {
    NodeApiMountError::Task {
        message: error.to_string(),
    }
}
