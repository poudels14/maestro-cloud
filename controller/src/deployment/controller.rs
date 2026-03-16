use std::{collections::HashMap, sync::Arc, time::Duration};

use anyhow::Result;
use tokio::{sync::broadcast, time::sleep};

use crate::deployment::store::ClusterStore;
use crate::deployment::types::{
    Deployment, DeploymentConfig, DeploymentStatus, QueuedDeployment, ServiceDeployment,
    ServiceProvider,
};
use crate::signal::ShutdownEvent;
use crate::supervisor::controller::{FinishedJob, JobSupervisor};
use crate::{
    deployment::provider::{
        DockerDeploymentProvider, ServiceCommandPlanner, ShellDeploymentProvider,
    },
    supervisor::{ShutdownRequest, SupervisedJobConfig, SupervisedJobStatus},
};

const DEFAULT_RESTART_DELAY_MS: u64 = 1_000;
const DEFAULT_MAX_RESTARTS: Option<u32> = Some(5);
#[cfg(not(test))]
const DEFAULT_SHUTDOWN_GRACE_PERIOD_MS: u64 = 15_000;
#[cfg(test)]
const DEFAULT_SHUTDOWN_GRACE_PERIOD_MS: u64 = 200;
const POLL_INTERVAL: Duration = Duration::from_secs(1);

pub struct DeploymentController {
    config: DeploymentConfig,
    store: Arc<dyn ClusterStore>,
    signal_rx: broadcast::Receiver<ShutdownEvent>,
    supervisor: JobSupervisor,
    docker_provider: DockerDeploymentProvider,
    shell_provider: ShellDeploymentProvider,
    deployments: HashMap<String, Deployment>,
    shutdown_in_progress: bool,
}

impl DeploymentController {
    pub fn new(
        config: DeploymentConfig,
        store: Arc<dyn ClusterStore>,
        supervisor: JobSupervisor,
        signal_rx: broadcast::Receiver<ShutdownEvent>,
    ) -> Self {
        let docker_provider = DockerDeploymentProvider {
            network: config.network.clone(),
        };
        Self {
            config,
            store,
            signal_rx,
            supervisor,
            docker_provider,
            shell_provider: ShellDeploymentProvider,
            deployments: HashMap::new(),
            shutdown_in_progress: false,
        }
    }

    pub(crate) async fn run(&mut self) -> Result<()> {
        let mut shutdown_started = false;
        let mut signal_rx = self.signal_rx.resubscribe();

        if let Err(err) = self.queue_terminated_active_deployments().await {
            eprintln!("[maestro]: failed to queue terminated deployments on startup: {err}");
        }

        loop {
            tokio::select! {
                signal = signal_rx.recv() => {
                    match signal {
                        Ok(ShutdownEvent::Graceful) => {
                            if !shutdown_started {
                                self.shutdown_all(ShutdownRequest::Graceful).await;
                                shutdown_started = true;
                            }
                        }
                        Ok(ShutdownEvent::Force) | Err(broadcast::error::RecvError::Closed)=> {
                            self.shutdown_all(ShutdownRequest::Force).await;
                            return Ok(());
                        }
                        Err(broadcast::error::RecvError::Lagged(_)) => {}
                    }
                }
                _ = sleep(POLL_INTERVAL) => {
                    self.reap_finished_tasks().await;
                    if shutdown_started {
                        if !self.has_running_services() {
                            return Ok(());
                        }
                    } else if let Err(err) = self.reconcile_deployments().await {
                        eprintln!("[maestro]: controller queue scan error: {err}");
                    }
                }
            }
        }
    }

    pub(crate) async fn reconcile_deployments(&mut self) -> Result<()> {
        self.stop_removed_deployments().await;
        self.remove_old_deployments().await;
        self.reconcile_replicas().await;
        let queued = self.store.list_queued_deployments().await?;
        for queued_deployment in queued {
            self.process_queued_deployment(queued_deployment).await?;
        }
        Ok(())
    }

    async fn queue_terminated_active_deployments(&self) -> Result<()> {
        let service_ids = self.store.list_service_ids().await?;
        for service_id in service_ids {
            let status = self.store.get_service_status(&service_id).await?;
            if status != Some(DeploymentStatus::Terminated) {
                continue;
            }
            let Some(info) = self.store.read_service_info(&service_id).await? else {
                continue;
            };
            let deployment = ServiceDeployment::new(info.config)?;
            let _ = self.store.queue_deployment(deployment).await?;
        }

        Ok(())
    }

    pub(crate) async fn reap_finished_tasks(&mut self) {
        let finished = self.supervisor.reap_finished_jobs().await;
        self.update_jobs_status(finished).await;
    }

    async fn shutdown_all(&mut self, request: ShutdownRequest) {
        if !self.shutdown_in_progress {
            self.shutdown_in_progress = true;
            self.mark_deployments_terminated().await;
        }
        let _ = self.supervisor.shutdown_all(request).await;
    }

    pub(crate) fn has_running_services(&self) -> bool {
        self.supervisor.has_jobs()
    }

    pub(crate) fn into_supervisor(self) -> JobSupervisor {
        self.supervisor
    }

    async fn process_queued_deployment(
        &mut self,
        queued_deployment: QueuedDeployment,
    ) -> Result<()> {
        let _build_command = match queued_deployment.deployment.config.provider {
            ServiceProvider::Docker => self.docker_provider.build(&queued_deployment.deployment),
            ServiceProvider::Shell => self.shell_provider.build(&queued_deployment.deployment),
        };

        let claimed = self
            .store
            .claim_deployment_building(&queued_deployment)
            .await?;
        if !claimed {
            return Ok(());
        }

        let service_id = queued_deployment.service_id;
        let deployment_id = queued_deployment.deployment.id.clone();
        let replicas = queued_deployment.deployment.config.deploy.replicas;
        let has_healthcheck = queued_deployment
            .deployment
            .config
            .deploy
            .healthcheck_path
            .as_ref()
            .is_some_and(|p| !p.trim().is_empty());

        let replica_status = if has_healthcheck {
            DeploymentStatus::PendingReady
        } else {
            DeploymentStatus::Ready
        };

        for replica_index in 0..replicas {
            let deploy_command = match queued_deployment.deployment.config.provider {
                ServiceProvider::Docker => self
                    .docker_provider
                    .deploy(&queued_deployment.deployment, replica_index),
                ServiceProvider::Shell => self
                    .shell_provider
                    .deploy(&queued_deployment.deployment, replica_index),
            };
            let Some(deploy_command) = deploy_command else {
                eprintln!(
                    "[maestro]: skipping replica{replica_index} of deployment `{deployment_id}` for service `{service_id}`: no deploy command",
                );
                continue;
            };

            let replica_job_id = replica_job_id(&deployment_id, replica_index);
            let job = SupervisedJobConfig {
                id: replica_job_id.clone(),
                name: format!("{service_id}/{deployment_id}/replica{replica_index}"),
                command: deploy_command,
                restart_delay_ms: DEFAULT_RESTART_DELAY_MS,
                max_restarts: DEFAULT_MAX_RESTARTS,
                shutdown_grace_period_ms: DEFAULT_SHUTDOWN_GRACE_PERIOD_MS,
                logs_dir: Some(
                    self.config
                        .deployment_logs_dir()
                        .join("services")
                        .join(&service_id)
                        .join("deployments")
                        .join(&deployment_id)
                        .join(format!("replica{replica_index}")),
                ),
            };

            if let Some(task_id) = self.supervisor.start_job(job) {
                self.deployments.insert(
                    task_id,
                    Deployment {
                        service_id: service_id.clone(),
                        id: deployment_id.clone(),
                        replica_index,
                    },
                );
            } else {
                eprintln!(
                    "[maestro]: failed to start replica{replica_index} of deployment `{deployment_id}` for service `{service_id}`: job already exists"
                );
            }

            if let Err(err) = self
                .store
                .update_replica_status(
                    &service_id,
                    &deployment_id,
                    replica_index,
                    replica_status.clone(),
                )
                .await
            {
                eprintln!(
                    "[maestro]: failed to update replica{replica_index} of deployment `{deployment_id}` status: {err}",
                );
            }
        }

        let deployment_ref = Deployment {
            service_id,
            id: deployment_id,
            replica_index: 0,
        };
        if let Err(err) = self
            .store
            .update_deployment_status(&deployment_ref, DeploymentStatus::Ready)
            .await
        {
            eprintln!(
                "[maestro]: failed to update deployment `{}` to Ready: {err}",
                deployment_ref.id
            );
        }

        Ok(())
    }

    async fn update_jobs_status(&mut self, finished: Vec<FinishedJob>) {
        for finished_task in finished {
            let job_id = finished_task.id;
            let Some(deployment) = self.deployments.remove(&job_id) else {
                continue;
            };

            let remaining_replicas = self
                .deployments
                .values()
                .filter(|d| d.id == deployment.id)
                .count();

            eprintln!(
                "[maestro]: service `{}` deployment `{}` replica{} finished ({} replicas still running)",
                deployment.service_id, deployment.id, deployment.replica_index, remaining_replicas
            );

            if self.shutdown_in_progress {
                continue;
            }

            let replica_states = self
                .store
                .list_replica_states(&deployment.service_id, &deployment.id)
                .await
                .unwrap_or_default();
            let current_replica_status = replica_states
                .iter()
                .find(|r| r.replica_index == deployment.replica_index)
                .map(|r| &r.status);

            let new_status = match current_replica_status {
                Some(
                    DeploymentStatus::Ready
                    | DeploymentStatus::PendingReady
                    | DeploymentStatus::Building,
                ) => match finished_task.status {
                    SupervisedJobStatus::Crashed => Some(DeploymentStatus::Crashed),
                    SupervisedJobStatus::Stopped => Some(DeploymentStatus::Terminated),
                    _ => None,
                },
                _ => None,
            };

            if let Some(new_status) = new_status {
                if let Err(err) = self
                    .store
                    .update_replica_status(
                        &deployment.service_id,
                        &deployment.id,
                        deployment.replica_index,
                        new_status.clone(),
                    )
                    .await
                {
                    eprintln!(
                        "[maestro]: failed to update replica{} of deployment `{}` to {new_status:?}: {err}",
                        deployment.replica_index, deployment.id
                    );
                }
            }
        }
    }

    async fn mark_deployments_terminated(&self) {
        let mut seen_deployment_ids = std::collections::HashSet::new();
        for deployment in self.deployments.values() {
            if !seen_deployment_ids.insert(deployment.id.clone()) {
                continue;
            }
            if let Err(err) = self
                .store
                .update_deployment_status(deployment, DeploymentStatus::Terminated)
                .await
            {
                eprintln!(
                    "[maestro]: failed to pre-mark deployment `{}` terminated during shutdown: {err}",
                    deployment.id
                );
            }
        }
    }

    async fn stop_removed_deployments(&mut self) {
        let running_job_ids = self.deployments.keys().cloned().collect::<Vec<_>>();
        for job_id in running_job_ids {
            let Some(deployment) = self.deployments.get(&job_id) else {
                continue;
            };

            let should_stop = match self.store.read_service_deployment(deployment).await {
                Ok(Some(d)) => d.status == DeploymentStatus::Removed,
                Ok(None) => false,
                Err(err) => {
                    eprintln!(
                        "[maestro]: failed to read deployment `{}` stop state: {err}",
                        deployment.id
                    );
                    false
                }
            };
            if should_stop {
                let _ = self
                    .store
                    .delete_replica_state(
                        &deployment.service_id,
                        &deployment.id,
                        deployment.replica_index,
                    )
                    .await;
                let _ = self
                    .supervisor
                    .shutdown_job(&job_id, ShutdownRequest::Graceful);
            }
        }
    }

    async fn remove_old_deployments(&mut self) {
        let mut services: HashMap<String, Vec<String>> = HashMap::new();
        for deployment in self.deployments.values() {
            let ids = services.entry(deployment.service_id.clone()).or_default();
            if !ids.contains(&deployment.id) {
                ids.push(deployment.id.clone());
            }
        }

        for (service_id, deployment_ids) in &services {
            if deployment_ids.len() <= 1 {
                continue;
            }

            let store_deployments = match self.store.list_service_deployments(service_id).await {
                Ok(d) => d,
                Err(_) => continue,
            };

            let latest_active = store_deployments.iter().find(|d| {
                matches!(
                    d.status,
                    DeploymentStatus::Ready
                        | DeploymentStatus::PendingReady
                        | DeploymentStatus::Building
                )
            });
            let Some(latest) = latest_active else {
                continue;
            };

            let has_ready_replica = self
                .store
                .list_replica_states(service_id, &latest.id)
                .await
                .unwrap_or_default()
                .iter()
                .any(|r| r.status == DeploymentStatus::Ready);

            if !has_ready_replica {
                continue;
            }

            for old in &store_deployments {
                if old.id == latest.id {
                    continue;
                }
                if !matches!(
                    old.status,
                    DeploymentStatus::Ready
                        | DeploymentStatus::PendingReady
                        | DeploymentStatus::Building
                ) {
                    continue;
                }

                let deployment_ref = Deployment {
                    service_id: service_id.clone(),
                    id: old.id.clone(),
                    replica_index: 0,
                };

                eprintln!(
                    "[maestro]: removing old deployment `{}` for service `{service_id}` (new deployment `{}` has ready replica)",
                    old.id, latest.id
                );

                if let Err(err) = self
                    .store
                    .update_deployment_status(&deployment_ref, DeploymentStatus::Removed)
                    .await
                {
                    eprintln!(
                        "[maestro]: failed to mark old deployment `{}` as removed: {err}",
                        old.id
                    );
                }
            }
        }
    }

    async fn reconcile_replicas(&mut self) {
        let mut seen_deployments: HashMap<String, String> = HashMap::new();
        for deployment in self.deployments.values() {
            seen_deployments
                .entry(deployment.id.clone())
                .or_insert_with(|| deployment.service_id.clone());
        }

        if seen_deployments.is_empty() {
            return;
        }

        for (deployment_id, service_id) in &seen_deployments {
            let desired = match self.store.read_service_info(service_id).await {
                Ok(Some(info)) => info.config.deploy.replicas,
                _ => continue,
            };

            let running_indices: Vec<u32> = self
                .deployments
                .values()
                .filter(|d| d.id == *deployment_id)
                .map(|d| d.replica_index)
                .collect();
            let running_count = running_indices.len() as u32;

            if desired != running_count {
                eprintln!(
                    "[maestro]: reconcile_replicas for `{service_id}/{deployment_id}`: desired={desired}, running={running_count}, running_indices={running_indices:?}"
                );
            }

            if desired > running_count {
                let first = self.deployments.values().find(|d| d.id == *deployment_id);
                let Some(first) = first else { continue };
                let deployment_record = match self.store.read_service_deployment(first).await {
                    Ok(Some(d)) => d,
                    _ => continue,
                };
                let max_existing = running_indices.iter().copied().max().unwrap_or(0);
                for replica_index in (max_existing + 1)..=(max_existing + (desired - running_count))
                {
                    self.start_replica(
                        service_id,
                        deployment_id,
                        replica_index,
                        &deployment_record,
                    )
                    .await;
                }
            } else if desired < running_count {
                let mut sorted = running_indices;
                sorted.sort();
                sorted.reverse();
                let excess = (running_count - desired) as usize;
                for &replica_index in sorted.iter().take(excess) {
                    let job_id = replica_job_id(deployment_id, replica_index);
                    let _ = self
                        .supervisor
                        .shutdown_job(&job_id, ShutdownRequest::Graceful);
                    let _ = self
                        .store
                        .delete_replica_state(service_id, deployment_id, replica_index)
                        .await;
                }
            }

            let replica_states = self
                .store
                .list_replica_states(service_id, deployment_id)
                .await
                .unwrap_or_default();
            for state in &replica_states {
                if state.replica_index >= desired {
                    let _ = self
                        .store
                        .delete_replica_state(service_id, deployment_id, state.replica_index)
                        .await;
                }
            }
        }
    }

    async fn start_replica(
        &mut self,
        service_id: &str,
        deployment_id: &str,
        replica_index: u32,
        deployment_record: &ServiceDeployment,
    ) {
        let deploy_command = match deployment_record.config.provider {
            ServiceProvider::Docker => self
                .docker_provider
                .deploy(deployment_record, replica_index),
            ServiceProvider::Shell => self.shell_provider.deploy(deployment_record, replica_index),
        };
        let Some(deploy_command) = deploy_command else {
            return;
        };

        let job_id = replica_job_id(deployment_id, replica_index);
        let job = SupervisedJobConfig {
            id: job_id.clone(),
            name: format!("{service_id}/{deployment_id}/replica{replica_index}"),
            command: deploy_command,
            restart_delay_ms: DEFAULT_RESTART_DELAY_MS,
            max_restarts: DEFAULT_MAX_RESTARTS,
            shutdown_grace_period_ms: DEFAULT_SHUTDOWN_GRACE_PERIOD_MS,
            logs_dir: Some(
                self.config
                    .deployment_logs_dir()
                    .join("services")
                    .join(service_id)
                    .join("deployments")
                    .join(deployment_id)
                    .join(format!("replica{replica_index}")),
            ),
        };

        if let Some(task_id) = self.supervisor.start_job(job) {
            self.deployments.insert(
                task_id,
                Deployment {
                    service_id: service_id.to_string(),
                    id: deployment_id.to_string(),
                    replica_index,
                },
            );

            let has_healthcheck = deployment_record
                .config
                .deploy
                .healthcheck_path
                .as_ref()
                .is_some_and(|p| !p.trim().is_empty());
            let status = if has_healthcheck {
                DeploymentStatus::PendingReady
            } else {
                DeploymentStatus::Ready
            };
            let _ = self
                .store
                .update_replica_status(service_id, deployment_id, replica_index, status)
                .await;
        }
    }
}

fn replica_job_id(deployment_id: &str, replica_index: u32) -> String {
    format!("{deployment_id}-replica-{replica_index}")
}
