use std::{collections::HashMap, path::PathBuf, sync::Arc, time::Duration};

use anyhow::Result;
use tokio::{sync::broadcast, task::JoinHandle, time::sleep};

use crate::deployment::dns::DnsManager;
use crate::deployment::provider::{
    BuildOutput, ContainerDeploymentProvider, ServiceCommandPlanner, ShellDeploymentProvider,
};
use crate::deployment::store::ClusterStore;
use crate::deployment::types::{
    Deployment, DeploymentBuildInfo, DeploymentConfig, DeploymentStatus, GitCommitInfo,
    QueuedDeployment, ServiceDeployment, ServiceProvider,
};
use crate::logs::{LogConfig, LogEntry, LogOrigin};
use crate::runtime::{BuildSpec, RuntimeProvider};
use crate::signal::ShutdownEvent;
use crate::supervisor::controller::{FinishedJob, JobSupervisor};
use crate::supervisor::{ContainerRef, ShutdownRequest, SupervisedJobConfig, SupervisedJobStatus};

const DEFAULT_RESTART_DELAY_MS: u64 = 5_000;
const DEFAULT_MAX_RESTARTS: Option<u32> = Some(10);
#[cfg(not(test))]
const DEFAULT_SHUTDOWN_GRACE_PERIOD_MS: u64 = 15_000;
#[cfg(test)]
const DEFAULT_SHUTDOWN_GRACE_PERIOD_MS: u64 = 200;
const POLL_INTERVAL: Duration = Duration::from_secs(1);
#[cfg(not(test))]
const INGRESS_DRAIN_GRACE_PERIOD_MS: u64 = 5_000;
#[cfg(test)]
const INGRESS_DRAIN_GRACE_PERIOD_MS: u64 = 50;
const BUILD_TIMEOUT: Duration = Duration::from_secs(30 * 60);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControllerExitReason {
    Shutdown,
    Restart,
}

struct PendingBuild {
    handle: JoinHandle<Result<BuildOutput>>,
    queued_deployment: QueuedDeployment,
    build_dir: PathBuf,
    started_at: std::time::Instant,
}

pub struct DeploymentController {
    config: DeploymentConfig,
    runtime: Arc<dyn RuntimeProvider>,
    dns_manager: Option<Arc<DnsManager>>,
    dns_domain: Option<String>,
    store: Arc<dyn ClusterStore>,
    signal_rx: broadcast::Receiver<ShutdownEvent>,
    supervisor: JobSupervisor,
    container_provider: ContainerDeploymentProvider,
    shell_provider: ShellDeploymentProvider,
    deployments: HashMap<String, Deployment>,
    pending_builds: HashMap<String, PendingBuild>,
    shutdown_in_progress: bool,
    logger: crate::logs::SystemLogger,
    log_sender: Option<flume::Sender<LogEntry>>,
}

impl DeploymentController {
    pub fn new(
        config: DeploymentConfig,
        store: Arc<dyn ClusterStore>,
        supervisor: JobSupervisor,
        signal_rx: broadcast::Receiver<ShutdownEvent>,
        log_sender: Option<flume::Sender<LogEntry>>,
        runtime: Arc<dyn RuntimeProvider>,
        dns_manager: Option<Arc<DnsManager>>,
        coredns_ip: Option<String>,
    ) -> Self {
        let dns_domain = config
            .tailscale_authkey
            .as_ref()
            .map(|_| format!("{}.maestro.internal", config.cluster_name));
        let dns_server = if runtime.requires_explicit_dns() {
            coredns_ip
        } else {
            None
        };
        let container_provider = ContainerDeploymentProvider {
            runtime: runtime.clone(),
            network: config.network.clone(),
            dns_domain: dns_domain.clone(),
            dns_server,
            secrets_dir: std::fs::canonicalize(&config.data_dir)
                .unwrap_or_else(|_| config.data_dir.clone())
                .join("secrets"),
        };
        Self {
            config,
            runtime,
            dns_manager,
            dns_domain,
            store,
            signal_rx,
            supervisor,
            container_provider,
            shell_provider: ShellDeploymentProvider,
            deployments: HashMap::new(),
            pending_builds: HashMap::new(),
            shutdown_in_progress: false,
            logger: crate::logs::SystemLogger::new(log_sender.clone()),
            log_sender,
        }
    }

    pub(crate) async fn run(&mut self) -> Result<ControllerExitReason> {
        let mut shutdown_started = false;
        let mut exit_reason = ControllerExitReason::Shutdown;
        let mut signal_rx = self.signal_rx.resubscribe();

        if let Err(err) = self.queue_terminated_active_deployments().await {
            self.logger.emit(
                "error",
                &format!("failed to queue terminated deployments on startup: {err}"),
            );
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
                            return Ok(exit_reason);
                        }
                        Err(broadcast::error::RecvError::Lagged(_)) => {}
                    }
                }
                _ = sleep(POLL_INTERVAL) => {
                    if let Some(reason) = self.check_system_upgrade().await {
                        exit_reason = reason;
                        if !shutdown_started {
                            self.shutdown_all(ShutdownRequest::Graceful).await;
                            shutdown_started = true;
                        }
                    }
                    self.reap_finished_tasks().await;
                    if shutdown_started {
                        if !self.has_running_services() {
                            return Ok(exit_reason);
                        }
                    } else if let Err(err) = self.reconcile_deployments().await {
                        self.logger.emit("error", &format!("controller queue scan error: {err}"));
                    }
                }
            }
        }
    }

    pub(crate) async fn reconcile_deployments(&mut self) -> Result<()> {
        self.stop_removed_deployments().await;
        self.drain_old_deployments().await;
        self.check_pending_builds().await;
        self.reconcile_replicas().await;
        let queued = self.store.list_queued_deployments().await?;
        for queued_deployment in queued {
            self.process_queued_deployment(queued_deployment).await?;
        }
        Ok(())
    }

    async fn check_system_upgrade(&self) -> Option<ControllerExitReason> {
        let request = self.store.read_system_upgrade_request().await;
        let Some(system_type) = request.ok().flatten() else {
            return None;
        };
        let _ = self.store.delete_system_upgrade_request().await;

        if system_type == "nixos" {
            self.logger.emit("info", "starting NixOS system upgrade");
            let flake_result = tokio::process::Command::new("nix")
                .args(["flake", "update", "--flake", "/etc/maestro"])
                .output()
                .await;
            if let Err(err) = &flake_result {
                self.logger
                    .emit("error", &format!("nix flake update failed: {err}"));
                return None;
            }
            let flake_output = flake_result.unwrap();
            if !flake_output.status.success() {
                let stderr = String::from_utf8_lossy(&flake_output.stderr);
                self.logger
                    .emit("error", &format!("nix flake update failed: {stderr}"));
                return None;
            }

            self.logger.emit("info", "running nixos-rebuild boot");
            let rebuild_result = tokio::process::Command::new("nixos-rebuild")
                .args(["boot", "--flake", "/etc/maestro#default"])
                .output()
                .await;
            if let Err(err) = &rebuild_result {
                self.logger
                    .emit("error", &format!("nixos-rebuild failed: {err}"));
                return None;
            }
            let rebuild_output = rebuild_result.unwrap();
            if !rebuild_output.status.success() {
                let stderr = String::from_utf8_lossy(&rebuild_output.stderr);
                self.logger
                    .emit("error", &format!("nixos-rebuild failed: {stderr}"));
                return None;
            }

            self.logger
                .emit("info", "NixOS rebuild complete, pre-building system images");
            let images = [
                ("maestro-admin", Some("Dockerfile.admin")),
                ("maestro-probe", Some("Dockerfile.probe")),
            ];
            for (tag, dockerfile) in &images {
                let result = self
                    .runtime
                    .build_image(
                        &BuildSpec {
                            context_dir: self.config.project_dir.clone(),
                            tag: tag.to_string(),
                            dockerfile: dockerfile.map(String::from),
                            build_args: Default::default(),
                        },
                        None,
                        None,
                    )
                    .await;
                if let Err(err) = result {
                    self.logger.emit(
                        "warn",
                        &format!("failed to pre-build {tag}: {err} (will rebuild on next start)"),
                    );
                }
            }

            self.logger
                .emit("info", "NixOS upgrade complete, rebooting");
            let _ = tokio::process::Command::new("reboot").output().await;
            None
        } else {
            self.logger
                .emit("info", "upgrade requested, rebuilding system images");
            let images = [
                ("maestro-admin", Some("Dockerfile.admin")),
                ("maestro-probe", Some("Dockerfile.probe")),
            ];
            for (tag, dockerfile) in images {
                let result = self
                    .runtime
                    .build_image(
                        &BuildSpec {
                            context_dir: self.config.project_dir.clone(),
                            tag: tag.to_string(),
                            dockerfile: dockerfile.map(String::from),
                            build_args: Default::default(),
                        },
                        None,
                        None,
                    )
                    .await;
                if let Err(err) = result {
                    self.logger
                        .emit("error", &format!("failed to rebuild {tag}: {err}"));
                    return None;
                }
            }
            self.logger
                .emit("info", "system images rebuilt, draining and restarting");
            Some(ControllerExitReason::Restart)
        }
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
        for (_, pending) in self.pending_builds.drain() {
            pending.handle.abort();
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
        mut queued_deployment: QueuedDeployment,
    ) -> Result<()> {
        let deployment_id = queued_deployment.deployment.id.clone();

        if self.pending_builds.contains_key(&deployment_id) {
            return Ok(());
        }

        let claimed = self
            .store
            .claim_deployment_building(&queued_deployment)
            .await?;
        if !claimed {
            return Ok(());
        }

        if let Err(err) = queued_deployment.deployment.resolve_secrets().await {
            let error_msg = format!("failed to resolve secrets: {err}");
            self.logger.emit(
                "error",
                &format!(
                    "{}/{}: {error_msg}",
                    queued_deployment.service_id, deployment_id
                ),
            );
            if let Some(sender) = &self.log_sender {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as i64;
                let _ = sender.try_send(LogEntry {
                    seq: 0,
                    ts: now,
                    level: Arc::from("error"),
                    stream: Arc::from("stderr"),
                    text: error_msg,
                    source: Arc::from(
                        format!("{}/{}/", queued_deployment.service_id, deployment_id).as_str(),
                    ),
                    origin: LogOrigin::Service,
                    tags: Arc::new(serde_json::Value::Array(vec![])),
                    attrs: vec![],
                });
            }
            let _ = self
                .store
                .update_deployment_status(
                    &Deployment {
                        id: deployment_id.clone(),
                        service_id: queued_deployment.service_id.clone(),
                        replica_index: 0,
                    },
                    DeploymentStatus::Crashed,
                )
                .await;
            return Ok(());
        }

        if queued_deployment.deployment.config.build.is_some() {
            let short_id: String = deployment_id.chars().take(6).collect();
            let image_tag = format!("{}:{short_id}", queued_deployment.deployment.config.id);
            let build_dir = self
                .config
                .data_dir
                .join("builds")
                .join(&queued_deployment.service_id)
                .join(&short_id);

            let provider = self.container_provider.clone();
            let deployment = queued_deployment.deployment.clone();
            let log_sender = self.log_sender.clone();
            let build_dir_clone = build_dir.clone();

            let handle = tokio::spawn(async move {
                provider
                    .build(&deployment, &build_dir_clone, &image_tag, log_sender)
                    .await
            });

            self.pending_builds.insert(
                deployment_id,
                PendingBuild {
                    handle,
                    queued_deployment,
                    build_dir,
                    started_at: std::time::Instant::now(),
                },
            );
            return Ok(());
        }

        self.start_deployment_replicas(&queued_deployment).await;

        let has_healthcheck = queued_deployment
            .deployment
            .config
            .deploy
            .healthcheck_path
            .as_ref()
            .is_some_and(|p| !p.trim().is_empty());
        let deployment_status = if has_healthcheck {
            DeploymentStatus::PendingReady
        } else {
            DeploymentStatus::Ready
        };
        let deployment_ref = Deployment {
            service_id: queued_deployment.service_id.clone(),
            id: queued_deployment.deployment.id.clone(),
            replica_index: 0,
        };
        if let Err(err) = self
            .store
            .update_deployment_status(&deployment_ref, deployment_status)
            .await
        {
            self.logger.emit(
                "error",
                &format!(
                    "failed to update deployment `{}` status: {err}",
                    queued_deployment.deployment.id
                ),
            );
        }

        Ok(())
    }

    async fn check_pending_builds(&mut self) {
        let deployment_ids: Vec<String> = self.pending_builds.keys().cloned().collect();

        for deployment_id in deployment_ids {
            let timed_out = self
                .pending_builds
                .get(&deployment_id)
                .is_some_and(|p| p.started_at.elapsed() > BUILD_TIMEOUT);

            if timed_out {
                let pending = self.pending_builds.remove(&deployment_id).unwrap();
                pending.handle.abort();
                let _ = std::fs::remove_dir_all(&pending.build_dir);
                self.logger.emit(
                    "error",
                    &format!(
                        "build timed out for deployment `{deployment_id}` ({}s limit)",
                        BUILD_TIMEOUT.as_secs()
                    ),
                );
                let deployment_ref = Deployment {
                    service_id: pending.queued_deployment.service_id,
                    id: deployment_id,
                    replica_index: 0,
                };
                let _ = self
                    .store
                    .update_deployment_status(&deployment_ref, DeploymentStatus::Crashed)
                    .await;
                continue;
            }

            let finished = self
                .pending_builds
                .get(&deployment_id)
                .is_some_and(|p| p.handle.is_finished());

            if !finished {
                continue;
            }

            let pending = self.pending_builds.remove(&deployment_id).unwrap();
            let service_id = pending.queued_deployment.service_id.clone();
            let result = pending.handle.await;

            let build_result = match result {
                Ok(inner) => inner,
                Err(err) => Err(anyhow::anyhow!("build task panicked: {err}")),
            };

            match build_result {
                Ok(output) => {
                    let _ = std::fs::remove_dir_all(&pending.build_dir);

                    let mut queued = pending.queued_deployment;
                    queued.deployment.build = Some(DeploymentBuildInfo {
                        docker_image_id: output.image_tag,
                    });
                    queued.deployment.git_commit = Some(GitCommitInfo {
                        reference: output.commit_sha,
                        message: output.commit_message,
                    });
                    let deployment_ref = Deployment {
                        service_id: service_id.clone(),
                        id: deployment_id.clone(),
                        replica_index: 0,
                    };
                    if let Err(err) = self
                        .store
                        .update_deployment_build_info(&deployment_ref, &queued.deployment)
                        .await
                    {
                        self.logger.emit(
                            "error",
                            &format!(
                                "failed to store build info for deployment `{deployment_id}`: {err}"
                            ),
                        );
                    }
                    self.start_deployment_replicas(&queued).await;
                }
                Err(err) => {
                    self.logger.emit(
                        "error",
                        &format!(
                            "build failed for service `{service_id}` deployment `{deployment_id}`: {err}"
                        ),
                    );
                    let deployment_ref = Deployment {
                        service_id,
                        id: deployment_id,
                        replica_index: 0,
                    };
                    let _ = self
                        .store
                        .update_deployment_status(&deployment_ref, DeploymentStatus::Crashed)
                        .await;
                }
            }
        }
    }

    async fn start_deployment_replicas(&mut self, queued_deployment: &QueuedDeployment) {
        let service_id = &queued_deployment.service_id;
        let deployment_id = &queued_deployment.deployment.id;
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
            let deploy_output = match queued_deployment.deployment.config.provider {
                ServiceProvider::Docker => self
                    .container_provider
                    .deploy(&queued_deployment.deployment, replica_index),
                ServiceProvider::Shell => self
                    .shell_provider
                    .deploy(&queued_deployment.deployment, replica_index),
            };
            let Some(deploy_output) = deploy_output else {
                self.logger.emit(
                    "error",
                    &format!(
                        "skipping replica{replica_index} of deployment `{deployment_id}` for service `{service_id}`: no deploy command"
                    ),
                );
                continue;
            };

            let replica_job_id = replica_job_id(deployment_id, replica_index);
            let container_hostname = queued_deployment
                .deployment
                .hostname_for_replica(replica_index);
            let max_restarts = queued_deployment
                .deployment
                .config
                .deploy
                .max_restarts
                .or(DEFAULT_MAX_RESTARTS);
            let job = SupervisedJobConfig {
                id: replica_job_id.clone(),
                name: format!("{service_id}/{deployment_id}/replica{replica_index}"),
                command: deploy_output.command,
                restart_delay_ms: DEFAULT_RESTART_DELAY_MS,
                max_restart_delay_ms: None,
                max_restarts,
                shutdown_grace_period_ms: DEFAULT_SHUTDOWN_GRACE_PERIOD_MS,
                container: Some(ContainerRef {
                    name: container_hostname.clone(),
                    runtime_cli: self.runtime.cli_name().to_string(),
                }),
                secrets_mount: deploy_output.secrets_mount,
                log_config: self.log_sender.clone().map(|sender| {
                    let mut tags = self.config.tags.clone();
                    tags.push(format!("service:{service_id}"));
                    tags.push(format!("hostname:{container_hostname}"));
                    tags.push(format!("deployment_id:{deployment_id}"));
                    tags.push(format!("replica:{replica_index}"));
                    tags.push(format!("cluster:{}", self.config.cluster_name));
                    LogConfig {
                        sender,
                        tags,
                        origin: LogOrigin::Service,
                    }
                }),
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
                register_container_dns(
                    &container_hostname,
                    &self.dns_domain,
                    &self.dns_manager,
                    &self.runtime,
                );
            } else {
                self.logger.emit(
                    "error",
                    &format!(
                        "failed to start replica{replica_index} of deployment `{deployment_id}` for service `{service_id}`: job already exists"
                    ),
                );
            }

            if let Err(err) = self
                .store
                .update_replica_status(
                    service_id,
                    deployment_id,
                    replica_index,
                    replica_status.clone(),
                )
                .await
            {
                self.logger.emit(
                    "error",
                    &format!(
                        "failed to update replica{replica_index} of deployment `{deployment_id}` status: {err}"
                    ),
                );
            }
        }

        if replica_status == DeploymentStatus::Ready {
            let deployment_ref = Deployment {
                service_id: service_id.clone(),
                id: deployment_id.clone(),
                replica_index: 0,
            };
            let _ = self
                .store
                .update_deployment_status(&deployment_ref, DeploymentStatus::Ready)
                .await;
        }
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

            self.logger.emit(
                "info",
                &format!(
                    "service `{}` deployment `{}` replica{} finished ({} replicas still running)",
                    deployment.service_id,
                    deployment.id,
                    deployment.replica_index,
                    remaining_replicas
                ),
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
                    self.logger.emit(
                        "error",
                        &format!(
                            "failed to update replica{} of deployment `{}` to {new_status:?}: {err}",
                            deployment.replica_index, deployment.id
                        ),
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
                self.logger.emit(
                    "error",
                    &format!(
                        "failed to pre-mark deployment `{}` terminated during shutdown: {err}",
                        deployment.id
                    ),
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

            let store_deployment = match self.store.read_service_deployment(deployment).await {
                Ok(Some(d)) => d,
                Ok(None) => {
                    self.logger.emit(
                        "info",
                        &format!(
                            "deployment `{}` for service `{}` no longer exists in store, stopping",
                            deployment.id, deployment.service_id
                        ),
                    );
                    let _ = self
                        .supervisor
                        .shutdown_job(&job_id, ShutdownRequest::Graceful);
                    continue;
                }
                Err(err) => {
                    self.logger.emit(
                        "error",
                        &format!(
                            "failed to read deployment `{}` stop state: {err}",
                            deployment.id
                        ),
                    );
                    continue;
                }
            };

            if store_deployment.status == DeploymentStatus::Draining {
                let _ = self
                    .store
                    .delete_replica_state(
                        &deployment.service_id,
                        &deployment.id,
                        deployment.replica_index,
                    )
                    .await;

                let drain_elapsed = store_deployment.drained_at.is_some_and(|drained_at| {
                    crate::utils::time::current_time_millis()
                        .map(|now| now.saturating_sub(drained_at) >= INGRESS_DRAIN_GRACE_PERIOD_MS)
                        .unwrap_or(false)
                });
                if drain_elapsed {
                    let deployment_ref = Deployment {
                        service_id: deployment.service_id.clone(),
                        id: deployment.id.clone(),
                        replica_index: deployment.replica_index,
                    };
                    let _ = self
                        .store
                        .update_deployment_status(&deployment_ref, DeploymentStatus::Removed)
                        .await;
                    let _ = self
                        .supervisor
                        .shutdown_job(&job_id, ShutdownRequest::Graceful);
                    let hostname = store_deployment.hostname_for_replica(deployment.replica_index);
                    deregister_container_dns(&hostname, &self.dns_domain, &self.dns_manager);
                    cleanup_deployment_image(
                        &store_deployment,
                        &self.config.data_dir,
                        &self.runtime,
                    );
                }
            } else if store_deployment.status == DeploymentStatus::Removed {
                let _ = self
                    .supervisor
                    .shutdown_job(&job_id, ShutdownRequest::Graceful);
                let hostname = store_deployment.hostname_for_replica(deployment.replica_index);
                deregister_container_dns(&hostname, &self.dns_domain, &self.dns_manager);
                cleanup_deployment_image(&store_deployment, &self.config.data_dir, &self.runtime);
            }
        }
    }

    async fn drain_old_deployments(&mut self) {
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

                self.logger.emit(
                    "info",
                    &format!(
                        "draining old deployment `{}` for service `{service_id}` (new deployment `{}` has ready replica)",
                        old.id, latest.id
                    ),
                );

                if let Err(err) = self
                    .store
                    .update_deployment_status(&deployment_ref, DeploymentStatus::Draining)
                    .await
                {
                    self.logger.emit(
                        "error",
                        &format!(
                            "failed to mark old deployment `{}` as draining: {err}",
                            old.id
                        ),
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
                self.logger.emit(
                    "info",
                    &format!(
                        "reconcile_replicas for `{service_id}/{deployment_id}`: desired={desired}, running={running_count}, running_indices={running_indices:?}"
                    ),
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
        let deploy_output = match deployment_record.config.provider {
            ServiceProvider::Docker => self
                .container_provider
                .deploy(deployment_record, replica_index),
            ServiceProvider::Shell => self.shell_provider.deploy(deployment_record, replica_index),
        };
        let Some(deploy_output) = deploy_output else {
            return;
        };

        let job_id = replica_job_id(deployment_id, replica_index);
        let container_hostname = deployment_record.hostname_for_replica(replica_index);
        let max_restarts = deployment_record
            .config
            .deploy
            .max_restarts
            .or(DEFAULT_MAX_RESTARTS);
        let job = SupervisedJobConfig {
            id: job_id.clone(),
            name: format!("{service_id}/{deployment_id}/replica{replica_index}"),
            command: deploy_output.command,
            restart_delay_ms: DEFAULT_RESTART_DELAY_MS,
            max_restart_delay_ms: None,
            max_restarts,
            shutdown_grace_period_ms: DEFAULT_SHUTDOWN_GRACE_PERIOD_MS,
            container: Some(ContainerRef {
                name: container_hostname.clone(),
                runtime_cli: self.runtime.cli_name().to_string(),
            }),
            secrets_mount: deploy_output.secrets_mount,
            log_config: self.log_sender.clone().map(|sender| {
                let mut tags = self.config.tags.clone();
                tags.push(format!("service:{service_id}"));
                tags.push(format!("hostname:{container_hostname}"));
                tags.push(format!("deployment_id:{deployment_id}"));
                tags.push(format!("replica:{replica_index}"));
                tags.push(format!("cluster:{}", self.config.cluster_name));
                LogConfig {
                    sender,
                    tags,
                    origin: LogOrigin::Service,
                }
            }),
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
            register_container_dns(
                &container_hostname,
                &self.dns_domain,
                &self.dns_manager,
                &self.runtime,
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

fn cleanup_deployment_image(
    deployment: &ServiceDeployment,
    data_dir: &std::path::Path,
    runtime: &Arc<dyn RuntimeProvider>,
) {
    if let Some(build_info) = &deployment.build {
        let image_id = build_info.docker_image_id.clone();
        let runtime = runtime.clone();
        tokio::spawn(async move {
            let _ = runtime.remove_image(&image_id).await;
        });
    }
    let short_id: String = deployment.id.chars().take(6).collect();
    let build_dir = data_dir
        .join("builds")
        .join(&deployment.config.id)
        .join(&short_id);
    if build_dir.exists() {
        let _ = std::fs::remove_dir_all(&build_dir);
    }
}

fn replica_job_id(deployment_id: &str, replica_index: u32) -> String {
    format!("{deployment_id}-replica-{replica_index}")
}

fn register_container_dns(
    hostname: &str,
    dns_domain: &Option<String>,
    dns_manager: &Option<Arc<DnsManager>>,
    runtime: &Arc<dyn RuntimeProvider>,
) {
    if let (Some(domain), Some(dns)) = (dns_domain.as_deref(), dns_manager.as_ref()) {
        let hostname = hostname.to_string();
        let domain = domain.to_string();
        let runtime = runtime.clone();
        let dns = dns.clone();
        tokio::spawn(async move {
            if let Some(ip) = runtime.inspect_container_ip(&hostname).await {
                dns.set_record(&hostname, &domain, &ip);
                let _ = dns.flush();
            }
        });
    }
}

fn deregister_container_dns(
    hostname: &str,
    dns_domain: &Option<String>,
    dns_manager: &Option<Arc<DnsManager>>,
) {
    if let (Some(domain), Some(dns)) = (dns_domain.as_deref(), dns_manager.as_ref()) {
        dns.remove_records_for_hostname(hostname, domain);
        let _ = dns.flush();
    }
}
