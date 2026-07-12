use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};

use anyhow::Result;
use tokio::{sync::broadcast, task::JoinHandle, time::sleep};

use crate::config::BuilderType;
use crate::deployment::dns::DnsManager;
use crate::deployment::provider::{BuildOutput, ContainerDeploymentProvider};
use crate::deployment::store::ClusterStore;
use crate::deployment::types::{
    ControllerConfig, Deployment, DeploymentBuildInfo, DeploymentStatus, QueuedDeployment,
    ReplicaState, ServiceDeployment,
};
use crate::engine::provider::DeploymentProvider;
use crate::engine::replica_supervisor::{JobReplicaSupervisor, ReplicaSupervisor};
use crate::engine::{Engine, LogSink, ReplicaHandle, ReplicaSpec};
use crate::health::MAX_REPLICA_RESTART_ATTEMPTS;
use crate::logs::{LogConfig, LogEntry, LogOrigin, Logger};
use crate::runtime::{BuildSpec, RuntimeProvider};
use crate::signal::ShutdownEvent;
use crate::supervisor::controller::{FinishedJob, JobSupervisor};
use crate::supervisor::{ShutdownRequest, SupervisedJobStatus};
use crate::utils::clock::{self, Clock};

use super::{ADMIN_IMAGE_TAG, PROBE_IMAGE_TAG, TAILSCALE_IMAGE_TAG};

const DEFAULT_RESTART_DELAY_MS: u64 = 5_000;
const DEFAULT_MAX_RESTARTS: Option<u32> = Some(10);
#[cfg(not(test))]
const DEFAULT_SHUTDOWN_GRACE_PERIOD_MS: u64 = 60_000;
#[cfg(test)]
const DEFAULT_SHUTDOWN_GRACE_PERIOD_MS: u64 = 200;
const POLL_INTERVAL: Duration = Duration::from_secs(1);
const IMAGE_PRUNE_INTERVAL: Duration = Duration::from_secs(15 * 60);
#[cfg(not(test))]
const INGRESS_DRAIN_GRACE_PERIOD_MS: u64 = 5_000;
#[cfg(test)]
const INGRESS_DRAIN_GRACE_PERIOD_MS: u64 = 50;
const BUILD_TIMEOUT_MS: u64 = 30 * 60 * 1000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControllerExitReason {
    Shutdown,
    Restart,
}

struct PendingBuild {
    handle: JoinHandle<Result<BuildOutput>>,
    queued_deployment: QueuedDeployment,
    build_dir: PathBuf,
    started_at_ms: u64,
}

fn system_image_build_specs() -> [(&'static str, Option<&'static str>); 3] {
    [
        (ADMIN_IMAGE_TAG, Some("Dockerfile.admin")),
        (PROBE_IMAGE_TAG, Some("Dockerfile.probe")),
        (TAILSCALE_IMAGE_TAG, Some("dns/Dockerfile.tailscale")),
    ]
}

pub struct DeploymentController {
    config: ControllerConfig,
    runtime: Arc<dyn RuntimeProvider>,
    dns_manager: Option<Arc<DnsManager>>,
    dns_domain: Option<String>,
    store: Arc<dyn ClusterStore>,
    signal_rx: broadcast::Receiver<ShutdownEvent>,
    supervisor: JobSupervisor,
    container_engine: Arc<Engine>,
    clock: Arc<dyn Clock>,
    deployments: HashMap<String, Deployment>,
    pending_builds: HashMap<String, PendingBuild>,
    shutdown_in_progress: bool,
    logger: Logger,
    log_sender: Option<flume::Sender<LogEntry>>,
    slack: crate::slack::SlackNotifier,
    notified_ready: HashSet<String>,
    notified_crashed: HashSet<String>,
}

impl DeploymentController {
    pub fn new(
        config: ControllerConfig,
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
            coredns_ip.clone()
        } else {
            None
        };
        let container_provider = ContainerDeploymentProvider {
            runtime: runtime.clone(),
            build_command_env: config.build_command_env.clone(),
            network: config.network.clone(),
            dns_domain: dns_domain.clone(),
            dns_server,
            secrets_dir: std::fs::canonicalize(&config.data_dir)
                .unwrap_or_else(|_| config.data_dir.clone())
                .join("secrets"),
            uploads_dir: config.probe_dir().join("data/uploads"),
        };
        let provider: Arc<dyn DeploymentProvider> = Arc::new(container_provider);
        let supervisor_handle: Arc<dyn ReplicaSupervisor> = Arc::new(JobReplicaSupervisor::new());
        let container_engine: Arc<Engine> = Arc::new(Engine::new(
            provider,
            supervisor_handle,
            config.data_dir.clone(),
        ));
        Self::with_engine(
            config,
            store,
            supervisor,
            signal_rx,
            log_sender,
            runtime,
            dns_manager,
            container_engine,
            dns_domain,
            clock::system(),
        )
    }

    pub fn with_engine(
        config: ControllerConfig,
        store: Arc<dyn ClusterStore>,
        supervisor: JobSupervisor,
        signal_rx: broadcast::Receiver<ShutdownEvent>,
        log_sender: Option<flume::Sender<LogEntry>>,
        runtime: Arc<dyn RuntimeProvider>,
        dns_manager: Option<Arc<DnsManager>>,
        container_engine: Arc<Engine>,
        dns_domain: Option<String>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        let logger = Logger::new(log_sender.clone());
        let slack = crate::slack::SlackNotifier::new(
            config.slack_webhook_url.clone(),
            Some(store.clone()),
            config.cluster_name.clone(),
            logger.clone(),
        );
        Self {
            config,
            runtime,
            dns_manager,
            dns_domain,
            store,
            signal_rx,
            supervisor,
            container_engine,
            clock,
            deployments: HashMap::new(),
            pending_builds: HashMap::new(),
            shutdown_in_progress: false,
            logger,
            log_sender,
            slack,
            notified_ready: HashSet::new(),
            notified_crashed: HashSet::new(),
        }
    }

    pub(crate) async fn run(&mut self) -> Result<ControllerExitReason> {
        let mut shutdown_started = false;
        let mut exit_reason = ControllerExitReason::Shutdown;
        let mut signal_rx = self.signal_rx.resubscribe();
        let mut image_prune_interval = tokio::time::interval(IMAGE_PRUNE_INTERVAL);
        image_prune_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        image_prune_interval.tick().await;

        let tmp_dir = self.config.data_dir.join("tmp");
        let _ = std::fs::remove_dir_all(&tmp_dir);
        let _ = self.store.delete_system_upgrade_request().await;
        let _ = self.store.delete_system_restart_request().await;

        if let Err(err) = self.queue_terminated_active_deployments().await {
            self.logger.emit(
                "error",
                &format!("failed to queue terminated deployments on startup: {err}"),
            );
        }
        self.prune_images().await;

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
                _ = image_prune_interval.tick() => {
                    self.prune_containers().await;
                    self.prune_images().await;
                }
                _ = sleep(POLL_INTERVAL) => {
                    if let Some(reason) = self.check_system_upgrade().await {
                        exit_reason = reason;
                        if !shutdown_started {
                            self.shutdown_all(ShutdownRequest::Graceful).await;
                            shutdown_started = true;
                        }
                    }
                    if self.check_system_restart().await {
                        exit_reason = ControllerExitReason::Restart;
                        if !shutdown_started {
                            self.shutdown_all(ShutdownRequest::Graceful).await;
                            shutdown_started = true;
                        }
                    }
                    self.reap_finished_tasks().await;
                    if shutdown_started {
                        if !self.has_running_services().await {
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
        self.abort_canceled_builds().await;
        self.check_pending_builds().await;
        self.reconcile_replicas().await;
        let queued = self.store.list_queued_deployments().await?;
        for queued_deployment in queued {
            self.process_queued_deployment(queued_deployment).await?;
        }
        self.cleanup_orphaned_deployments().await;
        self.reconcile_replica_dns().await;
        self.reconcile_stable_dns().await;
        Ok(())
    }

    async fn reconcile_replica_dns(&self) {
        let (Some(dns), Some(domain)) = (self.dns_manager.as_ref(), self.dns_domain.as_deref())
        else {
            return;
        };
        let service_ids = match self.store.list_service_ids().await {
            Ok(ids) => ids,
            Err(_) => return,
        };
        let mut touched = false;
        for service_id in &service_ids {
            let deployments = self
                .store
                .list_service_deployments(service_id)
                .await
                .unwrap_or_default();
            for deployment in &deployments {
                if !matches!(
                    deployment.status,
                    DeploymentStatus::Building
                        | DeploymentStatus::PendingReady
                        | DeploymentStatus::Ready
                ) {
                    continue;
                }
                let replicas = self
                    .store
                    .list_replica_states(service_id, &deployment.id)
                    .await
                    .unwrap_or_default();
                for replica in &replicas {
                    if !matches!(
                        replica.status,
                        DeploymentStatus::PendingReady | DeploymentStatus::Ready
                    ) {
                        continue;
                    }
                    let hostname = deployment.hostname_for_replica(replica.replica_index);
                    let Some(ip) = self.runtime.inspect_container_ip(&hostname).await else {
                        continue;
                    };
                    let existing = dns.lookup(&hostname, domain);
                    if existing.as_slice() == [ip.clone()] {
                        continue;
                    }
                    dns.set_record(&hostname, domain, &ip);
                    touched = true;
                    let from = if existing.is_empty() {
                        "missing".to_string()
                    } else {
                        existing.join(",")
                    };
                    self.logger.emit(
                        "info",
                        &format!("reconciled DNS for `{hostname}`: {from} -> {ip}"),
                    );
                }
            }
        }
        if touched {
            let _ = dns.flush();
        }
    }

    async fn reconcile_stable_dns(&self) {
        let (Some(dns), Some(domain)) = (self.dns_manager.as_ref(), self.dns_domain.as_deref())
        else {
            return;
        };
        let infos = match self.store.list_service_infos().await {
            Ok(infos) => infos,
            Err(_) => return,
        };
        let mut touched = false;
        for info in &infos {
            let service_id = &info.config.id;
            let deployments = self
                .store
                .list_service_deployments(service_id)
                .await
                .unwrap_or_default();
            let Some(latest_ready) = deployments
                .iter()
                .find(|d| d.status == DeploymentStatus::Ready)
            else {
                dns.remove_records_for_hostname(service_id, domain);
                touched = true;
                continue;
            };
            let replica_states = self
                .store
                .list_replica_states(service_id, &latest_ready.id)
                .await
                .unwrap_or_default();
            let mut ips: Vec<String> = replica_states
                .iter()
                .filter(|r| r.status == DeploymentStatus::Ready)
                .flat_map(|r| {
                    let hostname = latest_ready.hostname_for_replica(r.replica_index);
                    dns.lookup(&hostname, domain)
                })
                .collect();
            ips.sort();
            ips.dedup();
            dns.set_records(service_id, domain, &ips);
            touched = true;
        }
        if touched {
            let _ = dns.flush();
        }
    }

    async fn abort_canceled_builds(&mut self) {
        let deployment_ids: Vec<String> = self.pending_builds.keys().cloned().collect();

        for deployment_id in deployment_ids {
            let Some(pending) = self.pending_builds.get(&deployment_id) else {
                continue;
            };
            let deployment_ref = Deployment {
                service_id: pending.queued_deployment.service_id.clone(),
                id: deployment_id.clone(),
                replica_index: 0,
            };
            let is_canceled = self
                .store
                .read_service_deployment(&deployment_ref)
                .await
                .ok()
                .flatten()
                .is_some_and(|deployment| deployment.status == DeploymentStatus::Canceled);

            if !is_canceled {
                continue;
            }

            let pending = self.pending_builds.remove(&deployment_id).unwrap();
            pending.handle.abort();
            let _ = std::fs::remove_dir_all(&pending.build_dir);
            self.logger.emit(
                "info",
                &format!("build canceled for deployment `{deployment_id}`"),
            );
        }
    }

    async fn check_system_restart(&self) -> bool {
        let requested = self
            .store
            .read_system_restart_request()
            .await
            .ok()
            .unwrap_or(false);
        if requested {
            self.logger
                .emit("info", "restart requested, draining and restarting");
            self.mark_deployments_terminated().await;
        }
        requested
    }

    async fn check_system_upgrade(&self) -> Option<ControllerExitReason> {
        let request = self.store.read_system_upgrade_request().await;
        let system_type = request.ok().flatten()?;
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
            for (tag, dockerfile) in system_image_build_specs() {
                let result = self
                    .runtime
                    .build_image(
                        &BuildSpec {
                            context_dir: self.config.project_dir.clone(),
                            tag: tag.to_string(),
                            dockerfile: dockerfile.map(String::from),
                            labels: Default::default(),
                            build_args: Default::default(),
                            secrets: Default::default(),
                            command_env: Default::default(),
                            builder: BuilderType::Default,
                            depot_project: None,
                            push_to_registry: false,
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

            self.logger.emit(
                "info",
                "marking active deployments terminated before reboot",
            );
            self.mark_deployments_terminated().await;
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;

            self.logger
                .emit("info", "NixOS upgrade complete, rebooting");
            let _ = tokio::process::Command::new("reboot").output().await;
            None
        } else {
            self.logger
                .emit("info", "upgrade requested, rebuilding system images");
            for (tag, dockerfile) in system_image_build_specs() {
                let result = self
                    .runtime
                    .build_image(
                        &BuildSpec {
                            context_dir: self.config.project_dir.clone(),
                            tag: tag.to_string(),
                            dockerfile: dockerfile.map(String::from),
                            labels: Default::default(),
                            build_args: Default::default(),
                            secrets: Default::default(),
                            command_env: Default::default(),
                            builder: BuilderType::Default,
                            depot_project: None,
                            push_to_registry: false,
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
            let needs_recovery = matches!(
                status,
                Some(DeploymentStatus::Terminated)
                    | Some(DeploymentStatus::Ready)
                    | Some(DeploymentStatus::PendingReady)
                    | Some(DeploymentStatus::Building)
            );
            if !needs_recovery {
                continue;
            }
            let Some(info) = self.store.read_service_info(&service_id).await? else {
                continue;
            };
            if !matches!(status, Some(DeploymentStatus::Terminated)) {
                self.logger.emit(
                    "info",
                    &format!(
                        "recovering stale deployment for `{service_id}` (status was {status:?} on startup with no live container)"
                    ),
                );
                let stale_deployments = self
                    .store
                    .list_service_deployments(&service_id)
                    .await
                    .unwrap_or_default();
                for stale in &stale_deployments {
                    if matches!(
                        stale.status,
                        DeploymentStatus::Ready
                            | DeploymentStatus::PendingReady
                            | DeploymentStatus::Building
                            | DeploymentStatus::Draining
                    ) {
                        let deployment_ref = Deployment {
                            id: stale.id.clone(),
                            service_id: service_id.clone(),
                            replica_index: 0,
                        };
                        if let Err(err) = self
                            .store
                            .update_deployment_status(&deployment_ref, DeploymentStatus::Terminated)
                            .await
                        {
                            self.logger.emit(
                                "warn",
                                &format!(
                                    "failed to mark stale deployment `{}` terminated: {err}",
                                    stale.id
                                ),
                            );
                        }
                    }
                }
            }
            let deployment = ServiceDeployment::new(info.config)?;
            let _ = self.store.queue_deployment(deployment).await?;
        }

        Ok(())
    }

    pub(crate) async fn reap_finished_tasks(&mut self) {
        let mut finished = self.supervisor.reap_finished_jobs().await;
        finished.extend(self.container_engine.reap_finished_replicas().await);
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
        let _ = self.container_engine.shutdown_all_replicas(request).await;
        let _ = self.supervisor.shutdown_all(request).await;
    }

    pub(crate) async fn has_running_services(&self) -> bool {
        self.supervisor.has_jobs() || self.container_engine.has_running_replicas().await
    }

    async fn shutdown_deployment_replica(
        &self,
        service_id: &str,
        deployment_id: &str,
        replica_index: u32,
        request: ShutdownRequest,
    ) {
        let handle = ReplicaHandle {
            task_id: replica_job_id(deployment_id, replica_index),
            service_id: service_id.to_string(),
            deployment_id: deployment_id.to_string(),
            replica_index,
        };
        let _ = self.container_engine.stop_replica(&handle, request).await;
    }

    pub(crate) fn into_supervisor(self) -> JobSupervisor {
        self.supervisor
    }

    fn notify_deployment_crashed_once(
        &mut self,
        service_id: &str,
        deployment_id: &str,
        reason: &str,
    ) {
        if self.notified_crashed.insert(deployment_id.to_string()) {
            self.slack
                .notify_deployment_crashed(service_id, deployment_id, reason);
        }
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

        self.slack.notify_deployment_queued(
            &queued_deployment.service_id,
            &deployment_id,
            &queued_deployment.deployment.config.version,
        );

        let service_log_source = format!("{}/{}/", queued_deployment.service_id, deployment_id);

        if let Some(build_info) = queued_deployment.deployment.build.clone() {
            let should_pull = match &queued_deployment.deployment.config.build {
                None => true,
                Some(build) => build.registry.is_some(),
            };
            if should_pull {
                let log_source_str =
                    format!("{}/{}/restart", queued_deployment.service_id, deployment_id);
                if let Err(err) = self
                    .runtime
                    .pull_image(
                        &build_info.docker_image_id,
                        self.log_sender.as_ref(),
                        Some(&log_source_str),
                    )
                    .await
                {
                    let error_msg = format!(
                        "failed to pull image `{}`: {err}",
                        build_info.docker_image_id
                    );
                    self.logger.emit(
                        "error",
                        &format!(
                            "{}/{}: {error_msg}",
                            queued_deployment.service_id, deployment_id
                        ),
                    );
                    self.logger.emit_from_source(
                        "error",
                        &error_msg,
                        &service_log_source,
                        LogOrigin::Service,
                    );
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
                    self.notify_deployment_crashed_once(
                        &queued_deployment.service_id.clone(),
                        &deployment_id,
                        &error_msg,
                    );
                    return Ok(());
                }
            }
            self.deploy_service(&mut queued_deployment).await;
            return Ok(());
        }

        if queued_deployment.deployment.has_build_step() {
            self.prune_images().await;

            if queued_deployment.deployment.config.build.is_some() {
                if let Err(err) = queued_deployment
                    .deployment
                    .resolve_build_secrets(&self.logger)
                    .await
                {
                    let error_msg = format!("failed to resolve build secrets: {err}");
                    self.logger.emit(
                        "error",
                        &format!(
                            "{}/{}: {error_msg}",
                            queued_deployment.service_id, deployment_id
                        ),
                    );
                    self.logger.emit_from_source(
                        "error",
                        &error_msg,
                        &service_log_source,
                        LogOrigin::Service,
                    );
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
                    self.notify_deployment_crashed_once(
                        &queued_deployment.service_id.clone(),
                        &deployment_id,
                        &error_msg,
                    );
                    self.prune_service_images(&queued_deployment.service_id)
                        .await;
                    return Ok(());
                }

                if let Err(err) = self
                    .store
                    .save_build_data(&queued_deployment.service_id, &queued_deployment.deployment)
                    .await
                {
                    self.logger.emit(
                        "warn",
                        &format!(
                            "{}/{}: failed to save build data: {err}",
                            queued_deployment.service_id, deployment_id
                        ),
                    );
                }
            }

            let engine = self.container_engine.clone();
            let prepare_logs = LogSink::new(
                self.log_sender.clone(),
                format!("{}/{}/build", queued_deployment.service_id, deployment_id),
            );
            let prep = match engine
                .prepare(&queued_deployment.deployment, &prepare_logs)
                .await
            {
                Ok(prep) => {
                    if prep.git_commit.is_some() {
                        queued_deployment.deployment.git_commit = prep.git_commit.clone();
                        let deployment_ref = Deployment {
                            service_id: queued_deployment.service_id.clone(),
                            id: deployment_id.clone(),
                            replica_index: 0,
                        };
                        let _ = self
                            .store
                            .update_deployment_build_info(
                                &deployment_ref,
                                &queued_deployment.deployment,
                            )
                            .await;
                    }
                    prep
                }
                Err(err) => {
                    let reason = format!("source resolution failed: {err}");
                    self.logger.emit(
                        "error",
                        &format!(
                            "source resolution failed for service `{}` deployment `{deployment_id}`: {err}",
                            queued_deployment.service_id
                        ),
                    );
                    let deployment_ref = Deployment {
                        service_id: queued_deployment.service_id.clone(),
                        id: deployment_id.clone(),
                        replica_index: 0,
                    };
                    let _ = self
                        .store
                        .update_deployment_status(&deployment_ref, DeploymentStatus::Crashed)
                        .await;
                    self.notify_deployment_crashed_once(
                        &deployment_ref.service_id.clone(),
                        &deployment_ref.id.clone(),
                        &reason,
                    );
                    let _ = engine.cleanup(&queued_deployment.deployment, None).await;
                    self.prune_service_images(&queued_deployment.service_id)
                        .await;
                    return Ok(());
                }
            };

            let build_dir = prep
                .build_dir
                .clone()
                .unwrap_or_else(|| self.config.data_dir.join("tmp"));
            let engine_for_build = engine.clone();
            let prep_for_build = prep.clone();
            let build_logs = LogSink::new(
                self.log_sender.clone(),
                format!("{}/{}/build", queued_deployment.service_id, deployment_id),
            );

            let handle = tokio::spawn(async move {
                engine_for_build
                    .build(&prep_for_build, &build_logs)
                    .await
                    .map(|artifact| BuildOutput {
                        image_tag: artifact.image_tag().unwrap_or("").to_string(),
                    })
            });

            self.pending_builds.insert(
                deployment_id,
                PendingBuild {
                    handle,
                    queued_deployment,
                    build_dir,
                    started_at_ms: self.clock.now_ms(),
                },
            );
            return Ok(());
        }

        self.deploy_service(&mut queued_deployment).await;

        Ok(())
    }

    async fn check_pending_builds(&mut self) {
        let deployment_ids: Vec<String> = self.pending_builds.keys().cloned().collect();

        for deployment_id in deployment_ids {
            let timed_out = self.pending_builds.get(&deployment_id).is_some_and(|p| {
                self.clock.now_ms().saturating_sub(p.started_at_ms) > BUILD_TIMEOUT_MS
            });

            if timed_out {
                let pending = self.pending_builds.remove(&deployment_id).unwrap();
                pending.handle.abort();
                let _ = std::fs::remove_dir_all(&pending.build_dir);
                let reason = format!("build timed out ({}s limit)", BUILD_TIMEOUT_MS / 1000);
                self.logger.emit(
                    "error",
                    &format!(
                        "build timed out for deployment `{deployment_id}` ({}s limit)",
                        BUILD_TIMEOUT_MS / 1000
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
                self.notify_deployment_crashed_once(
                    &deployment_ref.service_id.clone(),
                    &deployment_ref.id.clone(),
                    &reason,
                );
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

            let _ = std::fs::remove_dir_all(&pending.build_dir);

            let current_status = self
                .store
                .read_service_deployment(&Deployment {
                    service_id: service_id.clone(),
                    id: deployment_id.clone(),
                    replica_index: 0,
                })
                .await
                .ok()
                .flatten()
                .map(|deployment| deployment.status);

            if current_status == Some(DeploymentStatus::Canceled) {
                self.logger.emit(
                    "info",
                    &format!(
                        "skipping canceled deployment `{deployment_id}` after build completion"
                    ),
                );
                continue;
            }

            match build_result {
                Ok(output) => {
                    let mut queued = pending.queued_deployment;
                    let cleanup_service_id = queued.service_id.clone();
                    queued.deployment.build = Some(DeploymentBuildInfo {
                        docker_image_id: output.image_tag,
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
                    self.deploy_service(&mut queued).await;
                    self.prune_service_images(&cleanup_service_id).await;
                }
                Err(build_err) => {
                    let reason = format!("build failed: {build_err}");
                    self.logger.emit(
                        "error",
                        &format!(
                            "build failed for service `{service_id}` deployment `{deployment_id}`: {build_err}"
                        ),
                    );
                    let deployment_ref = Deployment {
                        service_id: service_id.clone(),
                        id: deployment_id.clone(),
                        replica_index: 0,
                    };
                    let _ = self
                        .store
                        .update_deployment_status(&deployment_ref, DeploymentStatus::Crashed)
                        .await;
                    let _ = self
                        .container_engine
                        .cleanup(&pending.queued_deployment.deployment, None)
                        .await;
                    self.notify_deployment_crashed_once(&service_id, &deployment_id, &reason);
                    self.prune_service_images(&service_id).await;
                }
            }
        }
    }

    async fn deploy_service(&mut self, queued_deployment: &mut QueuedDeployment) {
        let service_id = &queued_deployment.service_id.clone();
        let deployment_id = &queued_deployment.deployment.id.clone();
        let service_log_source = format!("{service_id}/{deployment_id}/");

        match queued_deployment
            .deployment
            .resolve_deploy_secrets(&self.logger)
            .await
        {
            Ok(Some(resolved)) => {
                self.logger.emit_from_source(
                    "info",
                    &format!("loaded {} keys from `{}`", resolved.count, resolved.source),
                    &service_log_source,
                    LogOrigin::Service,
                );
            }
            Ok(None) => {}
            Err(err) => {
                let error_msg = format!("failed to resolve deploy secrets: {err}");
                self.logger.emit(
                    "error",
                    &format!("{service_id}/{deployment_id}: {error_msg}"),
                );
                self.logger.emit_from_source(
                    "error",
                    &error_msg,
                    &service_log_source,
                    LogOrigin::Service,
                );
                let _ = self
                    .store
                    .update_deployment_status(
                        &Deployment {
                            id: deployment_id.clone(),
                            service_id: service_id.clone(),
                            replica_index: 0,
                        },
                        DeploymentStatus::Crashed,
                    )
                    .await;
                self.notify_deployment_crashed_once(service_id, deployment_id, &error_msg);
                self.prune_service_images(service_id).await;
                return;
            }
        }

        if let Err(err) = self
            .store
            .save_deploy_data(service_id, &queued_deployment.deployment)
            .await
        {
            self.logger.emit(
                "warn",
                &format!("{service_id}/{deployment_id}: failed to save deploy data: {err}"),
            );
        }

        if let Err(err) = prepare_volumes(&queued_deployment.deployment) {
            let error_msg = format!("failed to prepare volumes: {err}");
            self.logger.emit(
                "error",
                &format!("{service_id}/{deployment_id}: {error_msg}"),
            );
            self.logger.emit_from_source(
                "error",
                &error_msg,
                &service_log_source,
                LogOrigin::Service,
            );
            let _ = self
                .store
                .update_deployment_status(
                    &Deployment {
                        id: deployment_id.clone(),
                        service_id: service_id.clone(),
                        replica_index: 0,
                    },
                    DeploymentStatus::Crashed,
                )
                .await;
            self.notify_deployment_crashed_once(service_id, deployment_id, &error_msg);
            return;
        }

        let replicas = queued_deployment.deployment.config.deploy.replicas;
        let replica_status = initial_replica_status_for_deployment(&queued_deployment.deployment);

        for replica_index in 0..replicas {
            let deploy_output = self
                .container_engine
                .deploy_command(&queued_deployment.deployment, replica_index);
            let Some(deploy_output) = deploy_output else {
                self.logger.emit(
                    "error",
                    &format!(
                        "skipping replica{replica_index} of deployment `{deployment_id}` for service `{service_id}`: no deploy command"
                    ),
                );
                let _ = self
                    .store
                    .update_replica_status(
                        service_id,
                        deployment_id,
                        replica_index,
                        DeploymentStatus::Crashed,
                    )
                    .await;
                continue;
            };

            let container_hostname = queued_deployment
                .deployment
                .hostname_for_replica(replica_index);
            let max_restarts = queued_deployment
                .deployment
                .config
                .deploy
                .max_restarts
                .or(DEFAULT_MAX_RESTARTS);
            let log_config = self.log_sender.clone().map(|sender| {
                let mut tags = self.config.tags.clone();
                tags.push(format!("service:{service_id}"));
                tags.push(format!("hostname:{container_hostname}"));
                tags.push(format!("deployment_id:{deployment_id}"));
                tags.push(format!("replica:{replica_index}"));
                tags.push(format!("cluster:{}", self.config.cluster_name));
                if let Some(path) = queued_deployment
                    .deployment
                    .config
                    .deploy
                    .healthcheck_path
                    .as_deref()
                    .map(str::trim)
                    .filter(|path| !path.is_empty())
                {
                    tags.push(crate::logs::healthcheck_path_tag(path));
                }
                LogConfig {
                    sender,
                    tags,
                    origin: LogOrigin::Service,
                }
            });
            let spec = ReplicaSpec {
                deployment: &queued_deployment.deployment,
                replica_index,
                deploy_output,
                max_restarts,
                restart_delay_ms: DEFAULT_RESTART_DELAY_MS,
                shutdown_grace_period_ms: DEFAULT_SHUTDOWN_GRACE_PERIOD_MS,
                container_hostname: container_hostname.clone(),
                runtime_cli: self.runtime.cli_name().to_string(),
                log_config,
            };
            let started = match self.container_engine.start_replica(spec).await {
                Ok(handle) => handle,
                Err(err) => {
                    self.logger.emit(
                        "error",
                        &format!(
                            "engine refused to start replica{replica_index} of `{deployment_id}`: {err}"
                        ),
                    );
                    None
                }
            };

            if let Some(handle) = started {
                self.deployments.insert(
                    handle.task_id.clone(),
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
                    &self.logger,
                );
            } else {
                self.logger.emit(
                    "error",
                    &format!(
                        "failed to start replica{replica_index} of deployment `{deployment_id}` for service `{service_id}`: job already exists"
                    ),
                );
                let _ = self
                    .store
                    .update_replica_status(
                        service_id,
                        deployment_id,
                        replica_index,
                        DeploymentStatus::Crashed,
                    )
                    .await;
                self.remove_replica_container(&queued_deployment.deployment, replica_index)
                    .await;
                continue;
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

        let deployment_ref = Deployment {
            service_id: service_id.clone(),
            id: deployment_id.clone(),
            replica_index: 0,
        };
        let _ = self
            .store
            .update_deployment_status(&deployment_ref, replica_status)
            .await;
        self.prune_service_images(service_id).await;
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

            if let Some(new_status) = new_status
                && let Err(err) = self
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

            if matches!(
                finished_task.status,
                SupervisedJobStatus::Completed
                    | SupervisedJobStatus::Stopped
                    | SupervisedJobStatus::Crashed
            ) && let Ok(Some(deployment_record)) =
                self.store.read_service_deployment(&deployment).await
            {
                self.remove_replica_container(&deployment_record, deployment.replica_index)
                    .await;
            }
        }
    }

    async fn remove_replica_container(&self, deployment: &ServiceDeployment, replica_index: u32) {
        let hostname = deployment.hostname_for_replica(replica_index);
        self.remove_container_by_hostname(&hostname).await;
        deregister_container_dns(&hostname, &self.dns_domain, &self.dns_manager);
    }

    async fn remove_deployment_containers(&self, deployment: &ServiceDeployment) {
        for replica_index in 0..deployment.config.deploy.replicas {
            self.remove_replica_container(deployment, replica_index)
                .await;
        }
    }

    async fn remove_container_by_hostname(&self, hostname: &str) {
        if let Err(err) = self.runtime.remove_container(hostname).await {
            self.logger.emit(
                "warn",
                &format!(
                    "failed to remove container `{hostname}` during deployment cleanup: {err}"
                ),
            );
        }
    }

    async fn prune_containers(&self) {
        if let Err(err) = self.runtime.prune_containers().await {
            self.logger.emit(
                "warn",
                &format!(
                    "failed to prune stopped {} containers: {err}",
                    self.runtime.cli_name()
                ),
            );
        }
    }

    async fn prune_images(&self) {
        let service_ids = match self.store.list_service_ids().await {
            Ok(ids) => ids,
            Err(err) => {
                self.logger.emit(
                    "warn",
                    &format!("failed to list services for image cleanup: {err}"),
                );
                return;
            }
        };

        for service_id in &service_ids {
            self.prune_service_images(service_id).await;
        }

        if !self.pending_builds.is_empty() {
            return;
        }

        if let Err(err) = self.runtime.prune_images().await {
            self.logger.emit(
                "warn",
                &format!(
                    "failed to prune unused {} images: {err}",
                    self.runtime.cli_name()
                ),
            );
        }
    }

    async fn prune_service_images(&self, service_id: &str) {
        let deployments = match self.store.list_service_deployments(service_id).await {
            Ok(deployments) => deployments,
            Err(err) => {
                self.logger.emit(
                    "warn",
                    &format!(
                        "failed to list deployments for service `{service_id}` during image cleanup: {err}"
                    ),
                );
                return;
            }
        };

        let latest_built_image = deployments
            .iter()
            .filter_map(|deployment| {
                deployment_image(deployment).map(|image| (deployment.created_at, image))
            })
            .max_by_key(|(created_at, _)| *created_at)
            .map(|(_, image)| image);

        let active_images = deployments
            .iter()
            .filter(|deployment| is_active(&deployment.status))
            .filter_map(deployment_image);

        let retained_images = active_images
            .chain(latest_built_image)
            .collect::<HashSet<_>>();

        for deployment in deployments
            .iter()
            .filter(|deployment| is_terminal(&deployment.status))
        {
            self.remove_deployment_containers(deployment).await;
            remove_build_dir(deployment, &self.config.data_dir);
            remove_upload_archive(deployment, &self.config.probe_dir().join("data/uploads"));
        }

        let stale_images = deployments
            .iter()
            .filter_map(deployment_image)
            .filter(|image| !retained_images.contains(image))
            .collect::<HashSet<_>>();

        for image in stale_images {
            if let Err(err) = self.runtime.remove_image(&image).await {
                self.logger.emit(
                    "warn",
                    &format!("failed to remove old image `{image}`: {err}"),
                );
            }
        }
    }

    async fn mark_deployments_terminated(&self) {
        let mut seen_deployment_ids = HashSet::new();
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

    async fn cleanup_orphaned_deployments(&mut self) {
        let tracked_job_ids: HashSet<String> = self.deployments.keys().cloned().collect();
        let pending_deployment_ids: HashSet<String> = self.pending_builds.keys().cloned().collect();

        let service_ids = match self.store.list_service_ids().await {
            Ok(ids) => ids,
            Err(_) => return,
        };

        for service_id in &service_ids {
            let deployments = match self.store.list_service_deployments(service_id).await {
                Ok(d) => d,
                Err(_) => continue,
            };

            for deployment in &deployments {
                if !matches!(
                    deployment.status,
                    DeploymentStatus::Draining
                        | DeploymentStatus::Ready
                        | DeploymentStatus::Building
                        | DeploymentStatus::PendingReady
                ) {
                    continue;
                }

                if pending_deployment_ids.contains(&deployment.id) {
                    continue;
                }

                self.reconcile_orphaned_deployment(service_id, deployment, &tracked_job_ids)
                    .await;
            }
        }
    }

    async fn reconcile_orphaned_deployment(
        &mut self,
        service_id: &str,
        deployment: &ServiceDeployment,
        tracked_job_ids: &HashSet<String>,
    ) {
        let replicas = deployment.config.deploy.replicas;
        let replica_states_snapshot = self
            .store
            .list_replica_states(service_id, &deployment.id)
            .await
            .unwrap_or_default();
        let exhausted_indices: HashSet<u32> = replica_states_snapshot
            .iter()
            .filter(|state| {
                state.replica_index < replicas
                    && state.status == DeploymentStatus::Crashed
                    && state.restart_attempts >= MAX_REPLICA_RESTART_ATTEMPTS
            })
            .map(|state| state.replica_index)
            .collect();
        let orphaned_replicas = (0..replicas)
            .filter(|replica_index| {
                let job_id = replica_job_id(&deployment.id, *replica_index);
                !tracked_job_ids.contains(&job_id) && !exhausted_indices.contains(replica_index)
            })
            .collect::<Vec<_>>();

        if orphaned_replicas.is_empty() {
            return;
        }

        if deployment.status == DeploymentStatus::Draining {
            self.logger.emit(
                "info",
                &format!(
                    "cleaning up orphaned draining deployment `{}` for service `{service_id}`",
                    deployment.id
                ),
            );
            for replica_index in 0..replicas {
                let hostname = deployment.hostname_for_replica(replica_index);
                let _ = self.runtime.remove_container(&hostname).await;
                deregister_container_dns(&hostname, &self.dns_domain, &self.dns_manager);
                let _ = self
                    .store
                    .delete_replica_state(service_id, &deployment.id, replica_index)
                    .await;
            }
            let deployment_ref = Deployment {
                service_id: service_id.to_string(),
                id: deployment.id.clone(),
                replica_index: 0,
            };
            let _ = self
                .store
                .update_deployment_status(&deployment_ref, DeploymentStatus::Removed)
                .await;
            remove_build_dir(deployment, &self.config.data_dir);
            remove_upload_archive(deployment, &self.config.probe_dir().join("data/uploads"));
            self.prune_service_images(service_id).await;
            return;
        }

        let replica_states = self
            .store
            .list_replica_states(service_id, &deployment.id)
            .await
            .unwrap_or_default();
        let mut any_orphaned_container_alive = false;

        for replica_index in &orphaned_replicas {
            let hostname = deployment.hostname_for_replica(*replica_index);
            let container_alive = self.runtime.inspect_container_ip(&hostname).await.is_some();
            if container_alive {
                any_orphaned_container_alive = true;
                register_container_dns(
                    &hostname,
                    &self.dns_domain,
                    &self.dns_manager,
                    &self.runtime,
                    &self.logger,
                );
                let replica_status = recovered_replica_status_for_deployment(deployment);
                let needs_replica_state = replica_states
                    .iter()
                    .find(|state| state.replica_index == *replica_index)
                    .is_none_or(|state| state.status != replica_status);
                if needs_replica_state {
                    self.logger.emit(
                        "info",
                        &format!(
                            "restoring orphaned replica{replica_index} of deployment `{}` for service `{service_id}` as {replica_status:?}",
                            deployment.id
                        ),
                    );
                    let _ = self
                        .store
                        .update_replica_status(
                            service_id,
                            &deployment.id,
                            *replica_index,
                            replica_status,
                        )
                        .await;
                }
            } else {
                deregister_container_dns(&hostname, &self.dns_domain, &self.dns_manager);
                let _ = self
                    .store
                    .delete_replica_state(service_id, &deployment.id, *replica_index)
                    .await;
            }
        }

        if any_orphaned_container_alive {
            let deployment_status = recovered_replica_status_for_deployment(deployment);
            if deployment.status != deployment_status {
                let deployment_ref = Deployment {
                    service_id: service_id.to_string(),
                    id: deployment.id.clone(),
                    replica_index: 0,
                };
                let _ = self
                    .store
                    .update_deployment_status(&deployment_ref, deployment_status)
                    .await;
            }
        } else if orphaned_replicas.len() == replicas as usize {
            self.logger.emit(
                "info",
                &format!(
                    "marking orphaned deployment `{}` for service `{service_id}` as terminated (container not running)",
                    deployment.id
                ),
            );
            for replica_index in 0..replicas {
                let hostname = deployment.hostname_for_replica(replica_index);
                deregister_container_dns(&hostname, &self.dns_domain, &self.dns_manager);
                let _ = self
                    .store
                    .delete_replica_state(service_id, &deployment.id, replica_index)
                    .await;
            }
            let deployment_ref = Deployment {
                service_id: service_id.to_string(),
                id: deployment.id.clone(),
                replica_index: 0,
            };
            let _ = self
                .store
                .update_deployment_status(&deployment_ref, DeploymentStatus::Terminated)
                .await;
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
                    self.shutdown_deployment_replica(
                        &deployment.service_id,
                        &deployment.id,
                        deployment.replica_index,
                        ShutdownRequest::Graceful,
                    )
                    .await;
                    let hostname = deployment_hostname(
                        &deployment.service_id,
                        &deployment.id,
                        deployment.replica_index,
                    );
                    self.remove_container_by_hostname(&hostname).await;
                    deregister_container_dns(&hostname, &self.dns_domain, &self.dns_manager);
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
                    self.clock.now_ms().saturating_sub(drained_at) >= INGRESS_DRAIN_GRACE_PERIOD_MS
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
                    self.shutdown_deployment_replica(
                        &deployment.service_id,
                        &deployment.id,
                        deployment.replica_index,
                        ShutdownRequest::Graceful,
                    )
                    .await;
                    self.remove_replica_container(&store_deployment, deployment.replica_index)
                        .await;
                    remove_build_dir(&store_deployment, &self.config.data_dir);
                    self.prune_service_images(&deployment.service_id).await;
                }
            } else if store_deployment.status == DeploymentStatus::Removed {
                self.shutdown_deployment_replica(
                    &deployment.service_id,
                    &deployment.id,
                    deployment.replica_index,
                    ShutdownRequest::Graceful,
                )
                .await;
                self.remove_replica_container(&store_deployment, deployment.replica_index)
                    .await;
                remove_build_dir(&store_deployment, &self.config.data_dir);
                self.prune_service_images(&deployment.service_id).await;
            }
        }
    }

    async fn drain_old_deployments(&mut self) {
        let service_ids = match self.store.list_service_ids().await {
            Ok(ids) => ids,
            Err(_) => return,
        };

        for service_id in &service_ids {
            let store_deployments = match self.store.list_service_deployments(service_id).await {
                Ok(d) => d,
                Err(_) => continue,
            };

            let active: Vec<&ServiceDeployment> = store_deployments
                .iter()
                .filter(|d| {
                    matches!(
                        d.status,
                        DeploymentStatus::Ready | DeploymentStatus::PendingReady
                    )
                })
                .collect();

            if active.len() <= 1 {
                continue;
            }

            let latest = active[0];

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

            for old in &active[1..] {
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
        let mut seen_deployments: HashMap<String, Deployment> = HashMap::new();
        for deployment in self.deployments.values() {
            seen_deployments
                .entry(deployment.id.clone())
                .or_insert_with(|| deployment.clone());
        }

        if seen_deployments.is_empty() {
            return;
        }

        for (deployment_id, first_deployment) in &seen_deployments {
            let service_id = &first_deployment.service_id;
            let deployment_record = match self.store.read_service_deployment(first_deployment).await
            {
                Ok(Some(deployment)) => deployment,
                _ => continue,
            };
            let desired = match self.store.read_service_info(service_id).await {
                Ok(Some(info)) => info.effective_replicas(),
                _ => continue,
            };

            let mut replica_states = self
                .store
                .list_replica_states(service_id, deployment_id)
                .await
                .unwrap_or_default();

            let exhausted_indices: HashSet<u32> = replica_states
                .iter()
                .filter(|state| {
                    state.replica_index < desired
                        && state.status == DeploymentStatus::Crashed
                        && state.restart_attempts >= MAX_REPLICA_RESTART_ATTEMPTS
                })
                .map(|state| state.replica_index)
                .collect();

            let intended_indices: HashSet<u32> = (0..desired)
                .filter(|index| !exhausted_indices.contains(index))
                .collect();

            let crashed_to_handle: Vec<(u32, u32)> = replica_states
                .iter()
                .filter(|state| {
                    state.replica_index < desired && state.status == DeploymentStatus::Crashed
                })
                .map(|state| (state.replica_index, state.restart_attempts))
                .collect();
            for (replica_index, attempts) in crashed_to_handle {
                let job_id = replica_job_id(deployment_id, replica_index);
                if !self.deployments.contains_key(&job_id) {
                    continue;
                }
                self.shutdown_deployment_replica(
                    service_id,
                    deployment_id,
                    replica_index,
                    ShutdownRequest::Graceful,
                )
                .await;
                self.deployments.remove(&job_id);
                if attempts < MAX_REPLICA_RESTART_ATTEMPTS {
                    let _ = self
                        .store
                        .upsert_replica_state(
                            service_id,
                            deployment_id,
                            ReplicaState {
                                replica_index,
                                status: initial_replica_status_for_deployment(&deployment_record),
                                healthcheck_failures: 0,
                                restart_attempts: attempts + 1,
                            },
                        )
                        .await;
                }
            }

            let running_indices: HashSet<u32> = self
                .deployments
                .values()
                .filter(|deployment| deployment.id == *deployment_id)
                .map(|deployment| deployment.replica_index)
                .collect();

            if running_indices != intended_indices {
                self.logger.emit(
                    "info",
                    &format!(
                        "reconcile_replicas for `{service_id}/{deployment_id}`: desired={desired}, running={running_indices:?}, intended={intended_indices:?}, exhausted={exhausted_indices:?}"
                    ),
                );
            }

            let mut missing: Vec<u32> = intended_indices
                .difference(&running_indices)
                .copied()
                .collect();
            missing.sort();
            for replica_index in missing {
                self.start_replica(service_id, deployment_id, replica_index, &deployment_record)
                    .await;
            }

            let mut excess: Vec<u32> = running_indices
                .iter()
                .filter(|index| **index >= desired)
                .copied()
                .collect();
            excess.sort();
            for replica_index in excess {
                let job_id = replica_job_id(deployment_id, replica_index);
                self.shutdown_deployment_replica(
                    service_id,
                    deployment_id,
                    replica_index,
                    ShutdownRequest::Graceful,
                )
                .await;
                self.deployments.remove(&job_id);
                let _ = self
                    .store
                    .delete_replica_state(service_id, deployment_id, replica_index)
                    .await;
            }

            replica_states = self
                .store
                .list_replica_states(service_id, deployment_id)
                .await
                .unwrap_or_default();

            let running_indices: HashSet<u32> = self
                .deployments
                .values()
                .filter(|deployment| deployment.id == *deployment_id)
                .map(|deployment| deployment.replica_index)
                .collect();

            let mut repaired_replica_state = false;
            for &replica_index in &running_indices {
                if replica_index >= desired {
                    continue;
                }
                let current_replica_status = replica_states
                    .iter()
                    .find(|state| state.replica_index == replica_index)
                    .map(|state| state.status.clone());
                let replica_status = self
                    .reconciled_replica_status(
                        &deployment_record,
                        replica_index,
                        current_replica_status.as_ref(),
                    )
                    .await;
                let needs_replica_state = current_replica_status.as_ref() != Some(&replica_status);
                if needs_replica_state {
                    self.logger.emit(
                        "info",
                        &format!(
                            "repairing replica{replica_index} state for running deployment `{deployment_id}` of service `{service_id}` as {replica_status:?}"
                        ),
                    );
                    let _ = self
                        .store
                        .update_replica_status(
                            service_id,
                            deployment_id,
                            replica_index,
                            replica_status,
                        )
                        .await;
                    repaired_replica_state = true;
                }
            }
            if repaired_replica_state {
                replica_states = self
                    .store
                    .list_replica_states(service_id, deployment_id)
                    .await
                    .unwrap_or_default();
            }
            for state in &replica_states {
                if state.replica_index >= desired {
                    let _ = self
                        .store
                        .delete_replica_state(service_id, deployment_id, state.replica_index)
                        .await;
                }
            }

            let any_ready = replica_states.iter().any(|state| {
                state.replica_index < desired && state.status == DeploymentStatus::Ready
            });
            if any_ready {
                let deployment_ref = Deployment {
                    service_id: service_id.clone(),
                    id: deployment_id.clone(),
                    replica_index: 0,
                };
                let _ = self
                    .store
                    .update_deployment_status(&deployment_ref, DeploymentStatus::Ready)
                    .await;
                if self.notified_ready.insert(deployment_id.clone()) {
                    self.slack
                        .notify_deployment_ready(service_id, deployment_id);
                }
            }

            let all_exhausted = desired > 0
                && (0..desired).all(|index| {
                    replica_states.iter().any(|state| {
                        state.replica_index == index
                            && state.status == DeploymentStatus::Crashed
                            && state.restart_attempts >= MAX_REPLICA_RESTART_ATTEMPTS
                    })
                });
            if all_exhausted && is_active(&deployment_record.status) {
                let deployment_ref = Deployment {
                    service_id: service_id.clone(),
                    id: deployment_id.clone(),
                    replica_index: 0,
                };
                let _ = self
                    .store
                    .update_deployment_status(&deployment_ref, DeploymentStatus::Crashed)
                    .await;
                let reason = format!(
                    "all {desired} replicas exhausted {MAX_REPLICA_RESTART_ATTEMPTS} restart attempts"
                );
                self.notify_deployment_crashed_once(service_id, deployment_id, &reason);
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
        let deploy_output = self
            .container_engine
            .deploy_command(deployment_record, replica_index);
        let Some(deploy_output) = deploy_output else {
            return;
        };

        let container_hostname = deployment_record.hostname_for_replica(replica_index);
        let max_restarts = deployment_record
            .config
            .deploy
            .max_restarts
            .or(DEFAULT_MAX_RESTARTS);
        let log_config = self.log_sender.clone().map(|sender| {
            let mut tags = self.config.tags.clone();
            tags.push(format!("service:{service_id}"));
            tags.push(format!("hostname:{container_hostname}"));
            tags.push(format!("deployment_id:{deployment_id}"));
            tags.push(format!("replica:{replica_index}"));
            tags.push(format!("cluster:{}", self.config.cluster_name));
            if let Some(path) = deployment_record
                .config
                .deploy
                .healthcheck_path
                .as_deref()
                .map(str::trim)
                .filter(|path| !path.is_empty())
            {
                tags.push(crate::logs::healthcheck_path_tag(path));
            }
            LogConfig {
                sender,
                tags,
                origin: LogOrigin::Service,
            }
        });
        let spec = ReplicaSpec {
            deployment: deployment_record,
            replica_index,
            deploy_output,
            max_restarts,
            restart_delay_ms: DEFAULT_RESTART_DELAY_MS,
            shutdown_grace_period_ms: DEFAULT_SHUTDOWN_GRACE_PERIOD_MS,
            container_hostname: container_hostname.clone(),
            runtime_cli: self.runtime.cli_name().to_string(),
            log_config,
        };
        let started = match self.container_engine.start_replica(spec).await {
            Ok(handle) => handle,
            Err(err) => {
                self.logger.emit(
                    "error",
                    &format!(
                        "engine refused to start replica{replica_index} of `{deployment_id}`: {err}"
                    ),
                );
                None
            }
        };

        if let Some(handle) = started {
            self.deployments.insert(
                handle.task_id.clone(),
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
                &self.logger,
            );

            let status = initial_replica_status_for_deployment(deployment_record);
            let _ = self
                .store
                .update_replica_status(service_id, deployment_id, replica_index, status)
                .await;
        }
    }

    async fn reconciled_replica_status(
        &self,
        deployment: &ServiceDeployment,
        replica_index: u32,
        current_status: Option<&DeploymentStatus>,
    ) -> DeploymentStatus {
        if current_status == Some(&DeploymentStatus::Ready) {
            DeploymentStatus::Ready
        } else if current_status == Some(&DeploymentStatus::Crashed) {
            DeploymentStatus::Crashed
        } else if !deployment_has_healthcheck(deployment) {
            let hostname = deployment.hostname_for_replica(replica_index);
            if self.runtime.inspect_container_ip(&hostname).await.is_some() {
                DeploymentStatus::Ready
            } else {
                DeploymentStatus::Building
            }
        } else {
            initial_replica_status_for_deployment(deployment)
        }
    }
}

fn remove_build_dir(deployment: &ServiceDeployment, data_dir: &std::path::Path) {
    let short_id: String = deployment.id.chars().take(6).collect();
    let build_dir = data_dir
        .join("builds")
        .join(&deployment.config.id)
        .join(&short_id);
    if build_dir.exists() {
        let _ = std::fs::remove_dir_all(&build_dir);
    }
}

fn remove_upload_archive(deployment: &ServiceDeployment, uploads_dir: &std::path::Path) {
    if let Some(filename) = deployment.upload_archive.as_deref() {
        let archive_path = uploads_dir.join(filename);
        if archive_path.exists() {
            let _ = std::fs::remove_file(&archive_path);
        }
    }
}

fn replica_job_id(deployment_id: &str, replica_index: u32) -> String {
    format!("{deployment_id}-replica-{replica_index}")
}

fn deployment_has_healthcheck(deployment: &ServiceDeployment) -> bool {
    deployment
        .config
        .deploy
        .healthcheck_path
        .as_ref()
        .is_some_and(|path| !path.trim().is_empty())
}

fn initial_replica_status_for_deployment(deployment: &ServiceDeployment) -> DeploymentStatus {
    if deployment_has_healthcheck(deployment) {
        DeploymentStatus::PendingReady
    } else {
        DeploymentStatus::Building
    }
}

fn recovered_replica_status_for_deployment(deployment: &ServiceDeployment) -> DeploymentStatus {
    if deployment.status == DeploymentStatus::Ready || !deployment_has_healthcheck(deployment) {
        DeploymentStatus::Ready
    } else {
        initial_replica_status_for_deployment(deployment)
    }
}

fn deployment_hostname(service_id: &str, deployment_id: &str, replica_index: u32) -> String {
    let short_id: String = deployment_id.chars().take(6).collect();
    if replica_index == 0 {
        format!("{service_id}-{short_id}")
    } else {
        format!("{service_id}-{short_id}-{replica_index}")
    }
}

fn is_terminal(status: &DeploymentStatus) -> bool {
    matches!(
        status,
        DeploymentStatus::Canceled
            | DeploymentStatus::Crashed
            | DeploymentStatus::Removed
            | DeploymentStatus::Terminated
    )
}

fn is_active(status: &DeploymentStatus) -> bool {
    matches!(
        status,
        DeploymentStatus::Queued
            | DeploymentStatus::Building
            | DeploymentStatus::PendingReady
            | DeploymentStatus::Ready
            | DeploymentStatus::Draining
    )
}

fn deployment_image(deployment: &ServiceDeployment) -> Option<String> {
    deployment
        .build
        .as_ref()
        .map(|build| build.docker_image_id.clone())
}

fn register_container_dns(
    hostname: &str,
    dns_domain: &Option<String>,
    dns_manager: &Option<Arc<DnsManager>>,
    runtime: &Arc<dyn RuntimeProvider>,
    logger: &Logger,
) {
    if let (Some(domain), Some(dns)) = (dns_domain.as_deref(), dns_manager.as_ref()) {
        let hostname = hostname.to_string();
        let domain = domain.to_string();
        let runtime = runtime.clone();
        let dns = dns.clone();
        let logger = logger.clone();
        tokio::spawn(async move {
            if let Some(ip) = runtime.inspect_container_ip(&hostname).await {
                dns.set_record(&hostname, &domain, &ip);
                let _ = dns.flush();
            } else {
                logger.emit(
                    "error",
                    &format!(
                        "failed to register DNS for `{hostname}`: container IP unavailable; will retry on next reconcile"
                    ),
                );
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

fn prepare_volumes(deployment: &ServiceDeployment) -> std::io::Result<()> {
    for volume in &deployment.config.deploy.volumes {
        let path = std::path::Path::new(&volume.host_path);
        if !path.exists() {
            std::fs::create_dir_all(path)?;
        }
        if let Some(owner) = volume.owner.as_ref() {
            std::os::unix::fs::chown(path, Some(owner.uid), Some(owner.resolved_gid()))?;
        }
    }
    Ok(())
}
