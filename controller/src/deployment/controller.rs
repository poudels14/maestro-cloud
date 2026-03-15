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
        Self {
            config,
            store,
            signal_rx,
            supervisor,
            docker_provider: DockerDeploymentProvider,
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
        let queued = self.store.list_queued_deployments().await?;
        for queued_deployment in queued {
            self.process_queued_deployment(queued_deployment).await?;
        }
        Ok(())
    }

    async fn queue_terminated_active_deployments(&self) -> Result<()> {
        let service_ids = self.store.list_service_ids().await?;
        for service_id in service_ids {
            let Some(info) = self.store.read_service_info(&service_id).await? else {
                continue;
            };
            if info.status != Some(DeploymentStatus::Terminated) {
                continue;
            }
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
        let deploy_command = match queued_deployment.deployment.config.provider {
            ServiceProvider::Docker => self.docker_provider.deploy(&queued_deployment.deployment),
            ServiceProvider::Shell => self.shell_provider.deploy(&queued_deployment.deployment),
        };
        let Some(deploy_command) = deploy_command else {
            eprintln!(
                "[maestro]: skipping queued deployment `{}` for service `{}`: no image or deploy command",
                queued_deployment.deployment.id, queued_deployment.service_id
            );
            return Ok(());
        };

        let claimed = self
            .store
            .claim_deployment_building(&queued_deployment)
            .await?;
        if !claimed {
            return Ok(());
        }

        let service_id = queued_deployment.service_id;
        let deployment_id = queued_deployment.deployment.id;
        let job = SupervisedJobConfig {
            id: deployment_id.clone(),
            name: format!("{service_id}/{deployment_id}"),
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
                    .join(&deployment_id),
            ),
        };

        let Some(task_id) = self.supervisor.start_job(job) else {
            eprintln!(
                "[maestro]: failed to start deployment `{deployment_id}` for service `{service_id}`: job already exists"
            );
            return Ok(());
        };

        let deployment = Deployment {
            service_id,
            id: deployment_id,
        };

        let has_healthcheck = queued_deployment
            .deployment
            .config
            .deploy
            .healthcheck_path
            .as_ref()
            .is_some_and(|p| !p.is_empty());

        let next_status = if has_healthcheck {
            DeploymentStatus::PendingReady
        } else {
            DeploymentStatus::Ready
        };

        if let Err(err) = self
            .store
            .update_deployment_status(&deployment, next_status)
            .await
        {
            eprintln!(
                "[maestro]: failed to update deployment `{}` status: {err}",
                deployment.id
            );
        }

        self.deployments.insert(task_id, deployment);

        Ok(())
    }

    async fn update_jobs_status(&mut self, finished: Vec<FinishedJob>) {
        for finished_task in finished {
            let job_id = finished_task.id;
            let Some(deployment) = self.deployments.remove(&job_id) else {
                continue;
            };

            eprintln!(
                "[maestro]: service `{}` deployment `{}` worker finished",
                deployment.service_id, deployment.id
            );

            if self.shutdown_in_progress {
                continue;
            }

            let deployment_status = match self.store.read_service_deployment(&deployment).await {
                Ok(Some(d)) => Some(d.status),
                Ok(None) => None,
                Err(err) => {
                    eprintln!(
                        "[maestro]: failed to read deployment `{}` remove state: {err}",
                        deployment.id
                    );
                    None
                }
            };

            let new_status = match deployment_status {
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
                    .update_deployment_status(&deployment, new_status.clone())
                    .await
                {
                    eprintln!(
                        "[maestro]: failed to update deployment `{}` to {new_status:?}: {err}",
                        deployment.id
                    );
                }
            }
        }
    }

    async fn mark_deployments_terminated(&self) {
        for deployment in self.deployments.values() {
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
                    .supervisor
                    .shutdown_job(&job_id, ShutdownRequest::Graceful);
            }
        }
    }
}
