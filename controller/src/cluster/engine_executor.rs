//! [`ReplicaExecutor`] impl that drives the existing [`Engine`].
//!
//! The executor keeps a small in-memory table of `(service_id, replica_index)
//! → ReplicaHandle` so it can stop the correct replica later. It depends on a
//! pluggable [`DeploymentLookup`] callback so it works against any source of
//! truth (cluster store, in-memory map, etc).

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use tokio::sync::Mutex;

use super::assignment_store::ReplicaExecutor;
use super::scheduling::Assignment;
use crate::deployment::provider::DeployOutput;
use crate::deployment::store::ClusterStore;
use crate::deployment::types::{DeploymentStatus, ReplicaState, ServiceDeployment};
use crate::engine::{Engine, ReplicaHandle, ReplicaSpec};
use crate::logs::LogConfig;
use crate::runtime::RuntimeProvider;
use crate::supervisor::ShutdownRequest;

#[async_trait]
pub trait DeploymentLookup: Send + Sync {
    /// Return the [`ServiceDeployment`] that the assignment refers to, plus the
    /// [`DeployOutput`] used to start a replica of it. Returning `None` causes
    /// the executor to log + skip the start.
    async fn resolve(&self, assignment: &Assignment) -> Result<Option<DeploymentInputs>>;
}

pub struct DeploymentInputs {
    pub deployment: ServiceDeployment,
    pub deploy_output: DeployOutput,
    pub max_restarts: Option<u32>,
    pub restart_delay_ms: u64,
    pub shutdown_grace_period_ms: u64,
    pub container_hostname: String,
    pub runtime_cli: String,
    pub log_config: Option<LogConfig>,
}

#[derive(derive_builder::Builder)]
#[builder(pattern = "owned")]
pub struct EngineReplicaExecutor {
    engine: Arc<Engine>,
    lookup: Arc<dyn DeploymentLookup>,
    /// In-memory cache of (service_id, replica_index) → ReplicaHandle. Survives
    /// only until the controller restarts; the durable state lives in etcd via
    /// [`ClusterStore`] (assignments + replica_states) and is recovered on
    /// startup by the reconciliation loop.
    #[builder(setter(skip), default)]
    handles: Mutex<HashMap<(String, u32), ReplicaHandle>>,
    /// When set, the executor persists replica lifecycle transitions to etcd
    /// so the cluster has a durable view of "what's running where".
    #[builder(default)]
    store: Option<Arc<dyn ClusterStore>>,
    /// Used to evict orphan containers on first start of a (service, replica)
    /// pair after a controller restart. If a previous controller process left
    /// a container with the same name running, the supervisor would fail with
    /// "name already in use"; we remove first, then start.
    #[builder(default)]
    runtime: Option<Arc<dyn RuntimeProvider>>,
}

impl EngineReplicaExecutor {
    pub async fn running_replica_count(&self) -> usize {
        self.handles.lock().await.len()
    }
}

#[async_trait]
impl ReplicaExecutor for EngineReplicaExecutor {
    async fn start(&self, assignment: &Assignment) -> Result<()> {
        let inputs = match self.lookup.resolve(assignment).await? {
            Some(inputs) => inputs,
            None => {
                return Err(anyhow!(
                    "no deployment data for {}/{} (deployment {})",
                    assignment.service_id,
                    assignment.replica_index,
                    assignment.deployment_id
                ));
            }
        };
        let key = (assignment.service_id.clone(), assignment.replica_index);
        let previous_handle = self.handles.lock().await.remove(&key);
        if let Some(previous_handle) = previous_handle {
            self.engine
                .stop_replica(&previous_handle, ShutdownRequest::Graceful)
                .await;
        } else if let Some(runtime) = self.runtime.as_ref() {
            let _ = runtime.remove_container(&inputs.container_hostname).await;
        }
        let spec = ReplicaSpec {
            deployment: &inputs.deployment,
            replica_index: assignment.replica_index,
            deploy_output: inputs.deploy_output,
            max_restarts: inputs.max_restarts,
            restart_delay_ms: inputs.restart_delay_ms,
            shutdown_grace_period_ms: inputs.shutdown_grace_period_ms,
            container_hostname: inputs.container_hostname,
            runtime_cli: inputs.runtime_cli,
            log_config: inputs.log_config,
        };
        match self.engine.start_replica(spec).await? {
            Some(handle) => {
                self.handles.lock().await.insert(key, handle);
                if let Some(store) = self.store.as_ref() {
                    let state = ReplicaState {
                        replica_index: assignment.replica_index,
                        status: DeploymentStatus::PendingReady,
                        healthcheck_failures: 0,
                        restart_attempts: 0,
                    };
                    if let Err(err) = store
                        .upsert_replica_state(
                            &assignment.service_id,
                            &assignment.deployment_id,
                            state,
                        )
                        .await
                    {
                        eprintln!(
                            "executor: failed to persist replica state for {}/{}: {err}",
                            assignment.service_id, assignment.replica_index
                        );
                    }
                }
                Ok(())
            }
            None => Err(anyhow!(
                "engine declined to start {}/{}",
                assignment.service_id,
                assignment.replica_index
            )),
        }
    }

    async fn stop(&self, service_id: &str, replica_index: u32) -> Result<()> {
        let key = (service_id.to_string(), replica_index);
        let removed_handle = self.handles.lock().await.remove(&key);
        if let Some(handle) = removed_handle {
            self.engine
                .stop_replica(&handle, ShutdownRequest::Graceful)
                .await;
            if let Some(store) = self.store.as_ref() {
                if let Err(err) = store
                    .delete_replica_state(service_id, &handle.deployment_id, replica_index)
                    .await
                {
                    eprintln!(
                        "executor: failed to delete replica state for {service_id}/{replica_index}: {err}"
                    );
                }
            }
        }
        Ok(())
    }
}
