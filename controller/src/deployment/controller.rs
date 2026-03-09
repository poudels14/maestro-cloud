use std::{collections::HashMap, sync::Arc, time::Duration};

use anyhow::Result;
use tokio::{sync::broadcast, time::sleep};

use crate::deployment::DeploymentConfig;
use crate::signal::ShutdownEvent;
use crate::supervisor::controller::{FinishedJob, JobSupervisor};
use crate::{
    deployment::{
        provider::{DockerDeploymentProvider, ServiceCommandPlanner, ShellDeploymentProvider},
        store::{ClusterStore, QueuedDeployment},
        types::{DeploymentStatus, ServiceDeployment, ServiceProvider},
    },
    supervisor::{ShutdownRequest, SupervisedJobConfig, SupervisedJobStatus},
};

const DEFAULT_RESTART_DELAY_MS: u64 = 1_000;
const DEFAULT_MAX_RESTARTS: u32 = 5;
const DEFAULT_SHUTDOWN_GRACE_PERIOD_MS: u64 = 15_000;
const POLL_INTERVAL: Duration = Duration::from_secs(1);

struct Deployment {
    id: String,
    service_id: String,
}

pub struct DeploymentController {
    config: DeploymentConfig,
    store: Arc<dyn ClusterStore>,
    signal_rx: broadcast::Receiver<ShutdownEvent>,
    supervisor: JobSupervisor,
    docker_provider: DockerDeploymentProvider,
    shell_provider: ShellDeploymentProvider,
    deployments: HashMap<String, Deployment>,
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
                        Ok(ShutdownEvent::Force) |Err(broadcast::error::RecvError::Closed)=> {
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
                        continue;
                    }
                    if let Err(err) = self.process_queued_deployments().await {
                        eprintln!("[maestro]: controller queue scan error: {err}");
                    }
                }
            }
        }
    }

    pub(crate) async fn process_queued_deployments(&mut self) -> Result<()> {
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
        self.apply_finished_jobs(finished).await;
    }

    async fn shutdown_all(&mut self, request: ShutdownRequest) {
        let finished = self.supervisor.shutdown_all(request).await;
        self.apply_finished_jobs(finished).await;
    }

    pub(crate) fn has_running_services(&self) -> bool {
        self.supervisor.has_jobs()
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
        let deployment_key = queued_deployment.key;
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
                    .logs_dir()
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

        self.deployments.insert(
            task_id,
            Deployment {
                service_id: service_id.clone(),
                id: deployment_id.clone(),
            },
        );

        if let Err(err) = self
            .store
            .mark_deployment_ready(&service_id, &deployment_key, &deployment_id)
            .await
        {
            eprintln!("[maestro]: failed to mark deployment `{deployment_id}` ready: {err}");
        }

        Ok(())
    }

    async fn apply_finished_jobs(&mut self, finished: Vec<FinishedJob>) {
        for finished_task in finished {
            let job_id = finished_task.id;
            let Some(running) = self.deployments.remove(&job_id) else {
                continue;
            };
            let service_id = running.service_id;
            let deployment_id = running.id;

            eprintln!(
                "[maestro]: service `{service_id}` deployment `{deployment_id}` worker finished"
            );

            if finished_task.status == SupervisedJobStatus::Crashed {
                if let Err(err) = self
                    .store
                    .mark_deployment_crashed(&service_id, &deployment_id)
                    .await
                {
                    eprintln!(
                        "[maestro]: failed to mark deployment `{deployment_id}` crashed: {err}"
                    );
                }
            } else if finished_task.status == SupervisedJobStatus::Stopped {
                if let Err(err) = self
                    .store
                    .mark_deployment_terminated(&service_id, &deployment_id)
                    .await
                {
                    eprintln!(
                        "[maestro]: failed to mark deployment `{deployment_id}` terminated: {err}"
                    );
                }
            }
        }
    }
}
