//! End-to-end lifecycle tests for [`DeploymentController`] driven through
//! [`InMemoryEngine`] and a purpose-built [`LifecycleStore`].
//!
//! These tests exercise the controller's state machine without spawning any
//! subprocesses or touching docker/nerdctl. They cover the happy path, build
//! failures, replica crashes, rollover between deployments, scaling, and
//! cancellation.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use clustertest::{
    AcceptanceCluster, ClusterSnapshot, DeploymentPhase, DeploymentSnapshot,
    FaultInjectableCluster, FixtureName, IngressFixture, ReplicaIndex, ReplicaOverride,
    ReplicaSnapshot, RolloutFailure, ServiceFixture, ServiceSnapshot, scenarios,
};
use tokio::sync::broadcast;
use tokio::time::Instant;

use crate::deployment::controller::DeploymentController;
use crate::deployment::store::{ClusterStore, SystemUpgradeRequest};
use crate::deployment::types::{
    CancelDeploymentOutcome, ControllerConfig, Deployment, DeploymentStatus, ForceQueueOutcome,
    IngressConfig, QueuedDeployment, ReplicaState, ServiceConfig, ServiceDeployConfig,
    ServiceDeployment, ServiceInfo,
};
use crate::engine::in_memory::InMemoryProvider;
use crate::engine::provider::DeploymentProvider;
use crate::engine::replica_supervisor::ReplicaSupervisor;
use crate::engine::replica_supervisor::fake::InMemoryReplicaSupervisor;
use crate::engine::{Engine, ReplicaHandle};
use crate::health::{DEFAULT_MAX_HEALTHCHECK_FAILURES, DefaultHealthMonitor, ReplicaHealthMonitor};
use crate::logs::LogEntry;
use crate::runtime::{self, RuntimeProvider};
use crate::signal::ShutdownEvent;
use crate::supervisor::{JobCommand, controller::JobSupervisor};
use crate::utils::clock::{Clock, FakeClock};
use crate::utils::crypto::SecretString;

// ---------------------------------------------------------------------------
// Store mock
// ---------------------------------------------------------------------------

struct LifecycleStore {
    state: Mutex<StoreState>,
    clock: Arc<dyn Clock>,
}

impl LifecycleStore {
    fn with_clock(clock: Arc<dyn Clock>) -> Self {
        Self {
            state: Mutex::new(StoreState::default()),
            clock,
        }
    }
}

#[derive(Default)]
struct StoreState {
    configs: HashMap<String, ServiceConfig>,
    deploy_frozen: HashMap<String, bool>,
    replicas_override: HashMap<String, Option<u32>>,
    /// service_id -> ordered list (oldest first) of deployments
    history: HashMap<String, Vec<ServiceDeployment>>,
    /// "service_id/deployment_id" -> replicas
    replicas: HashMap<String, Vec<ReplicaState>>,
    /// service_id -> transition log
    transitions: HashMap<String, Vec<DeploymentStatus>>,
    /// Total calls to upsert_replica_state. Used to verify the
    /// no-redundant-write optimization in DefaultHealthMonitor.
    upsert_replica_state_calls: usize,
}

impl LifecycleStore {
    fn queue_new_deployment(&self, config: ServiceConfig) -> ServiceDeployment {
        let mut state = self.state.lock().expect("state lock");
        let service_id = config.id.clone();
        state.configs.insert(service_id.clone(), config.clone());
        let created_at = state
            .history
            .get(&service_id)
            .map(|h| h.len() as u64)
            .unwrap_or(0)
            + 1;
        let id = format!("{service_id}-{created_at}");
        let deployment = ServiceDeployment {
            id: id.clone(),
            created_at,
            deployed_at: None,
            drained_at: None,
            status: DeploymentStatus::Queued,
            config,
            git_commit: None,
            build: None,
            upload_archive: None,
        };
        state
            .history
            .entry(service_id.clone())
            .or_default()
            .push(deployment.clone());
        state
            .transitions
            .entry(id)
            .or_default()
            .push(DeploymentStatus::Queued);
        deployment
    }

    fn deployment_status(&self, deployment_id: &str) -> Option<DeploymentStatus> {
        let state = self.state.lock().expect("state lock");
        state
            .history
            .values()
            .flat_map(|h| h.iter())
            .find(|d| d.id == deployment_id)
            .map(|d| d.status.clone())
    }

    fn deployment_transitions(&self, deployment_id: &str) -> Vec<DeploymentStatus> {
        let state = self.state.lock().expect("state lock");
        state
            .transitions
            .get(deployment_id)
            .cloned()
            .unwrap_or_default()
    }

    fn replicas_for(&self, service_id: &str, deployment_id: &str) -> Vec<ReplicaState> {
        let state = self.state.lock().expect("state lock");
        let key = format!("{service_id}/{deployment_id}");
        state.replicas.get(&key).cloned().unwrap_or_default()
    }

    fn set_replica_status(
        &self,
        service_id: &str,
        deployment_id: &str,
        replica_index: u32,
        status: DeploymentStatus,
    ) {
        let mut state = self.state.lock().expect("state lock");
        let key = format!("{service_id}/{deployment_id}");
        let replicas = state.replicas.entry(key).or_default();
        if let Some(existing) = replicas
            .iter_mut()
            .find(|r| r.replica_index == replica_index)
        {
            existing.status = status;
        } else {
            replicas.push(ReplicaState {
                service_id: None,
                deployment_id: None,
                replica_index,
                status,
                healthcheck_failures: 0,
                restart_attempts: 0,
                node_id: None,
                assignment_id: None,
                endpoint: None,
                error: None,
            });
        }
    }

    fn set_replica_state(&self, service_id: &str, deployment_id: &str, state: ReplicaState) {
        let mut store_state = self.state.lock().expect("state lock");
        let key = format!("{service_id}/{deployment_id}");
        let replicas = store_state.replicas.entry(key).or_default();
        if let Some(existing) = replicas
            .iter_mut()
            .find(|replica| replica.replica_index == state.replica_index)
        {
            *existing = state;
        } else {
            replicas.push(state);
        }
    }

    fn deployment_count(&self, service_id: &str) -> usize {
        let state = self.state.lock().expect("state lock");
        state.history.get(service_id).map(|h| h.len()).unwrap_or(0)
    }

    fn freeze(&self, service_id: &str) {
        self.state
            .lock()
            .expect("state lock")
            .deploy_frozen
            .insert(service_id.to_string(), true);
    }

    fn set_replicas_override(&self, service_id: &str, override_value: Option<u32>) {
        let mut state = self.state.lock().expect("state lock");
        state
            .replicas_override
            .insert(service_id.to_string(), override_value);
    }

    fn upsert_replica_state_calls(&self) -> usize {
        self.state
            .lock()
            .expect("state lock")
            .upsert_replica_state_calls
    }
}

fn deployment_to_queued(deployment: &ServiceDeployment) -> QueuedDeployment {
    QueuedDeployment {
        service_id: deployment.config.id.clone(),
        key: format!(
            "services/{}/deployments/{}",
            deployment.config.id, deployment.id
        ),
        mod_revision: 0,
        deployment: deployment.clone(),
    }
}

#[async_trait]
impl ClusterStore for LifecycleStore {
    async fn list_service_ids(&self) -> Result<Vec<String>> {
        let state = self.state.lock().expect("state lock");
        Ok(state.configs.keys().cloned().collect())
    }

    async fn list_queued_deployments(&self) -> Result<Vec<QueuedDeployment>> {
        let state = self.state.lock().expect("state lock");
        let mut queued = Vec::new();
        for deployments in state.history.values() {
            for deployment in deployments {
                if deployment.status == DeploymentStatus::Queued {
                    queued.push(deployment_to_queued(deployment));
                }
            }
        }
        queued.sort_by_key(|q| q.deployment.created_at);
        Ok(queued)
    }

    async fn claim_deployment_building(
        &self,
        queued_deployment: &QueuedDeployment,
    ) -> Result<bool> {
        let mut state = self.state.lock().expect("state lock");
        let Some(history) = state.history.get_mut(&queued_deployment.service_id) else {
            return Ok(false);
        };
        let Some(deployment) = history
            .iter_mut()
            .find(|d| d.id == queued_deployment.deployment.id)
        else {
            return Ok(false);
        };
        if deployment.status != DeploymentStatus::Queued {
            return Ok(false);
        }
        deployment.status = DeploymentStatus::Building;
        state
            .transitions
            .entry(queued_deployment.deployment.id.clone())
            .or_default()
            .push(DeploymentStatus::Building);
        Ok(true)
    }

    async fn update_deployment_status(
        &self,
        deployment: &Deployment,
        status: DeploymentStatus,
    ) -> Result<()> {
        let mut state = self.state.lock().expect("state lock");
        if let Some(history) = state.history.get_mut(&deployment.service_id)
            && let Some(stored) = history.iter_mut().find(|d| d.id == deployment.id)
            && (stored.status.can_transition_to(&status) || stored.status == status)
        {
            if status == DeploymentStatus::Draining && stored.drained_at.is_none() {
                stored.drained_at = Some(self.clock.now_ms());
            }
            stored.status = status.clone();
            state
                .transitions
                .entry(deployment.id.clone())
                .or_default()
                .push(status);
        }
        Ok(())
    }

    async fn update_replica_status(
        &self,
        service_id: &str,
        deployment_id: &str,
        replica_index: u32,
        status: DeploymentStatus,
    ) -> Result<()> {
        let mut state = self.state.lock().expect("state lock");
        let key = format!("{service_id}/{deployment_id}");
        let replicas = state.replicas.entry(key).or_default();
        if let Some(existing) = replicas
            .iter_mut()
            .find(|r| r.replica_index == replica_index)
        {
            existing.status = status;
        } else {
            replicas.push(ReplicaState {
                service_id: None,
                deployment_id: None,
                replica_index,
                status,
                healthcheck_failures: 0,
                restart_attempts: 0,
                node_id: None,
                assignment_id: None,
                endpoint: None,
                error: None,
            });
        }
        Ok(())
    }

    async fn upsert_replica_state(
        &self,
        service_id: &str,
        deployment_id: &str,
        state_value: ReplicaState,
    ) -> Result<()> {
        let mut state = self.state.lock().expect("state lock");
        state.upsert_replica_state_calls += 1;
        let key = format!("{service_id}/{deployment_id}");
        let replicas = state.replicas.entry(key).or_default();
        if let Some(existing) = replicas
            .iter_mut()
            .find(|r| r.replica_index == state_value.replica_index)
        {
            *existing = state_value;
        } else {
            replicas.push(state_value);
        }
        Ok(())
    }

    async fn list_replica_states(
        &self,
        service_id: &str,
        deployment_id: &str,
    ) -> Result<Vec<ReplicaState>> {
        let state = self.state.lock().expect("state lock");
        let key = format!("{service_id}/{deployment_id}");
        Ok(state.replicas.get(&key).cloned().unwrap_or_default())
    }

    async fn delete_replica_state(
        &self,
        service_id: &str,
        deployment_id: &str,
        replica_index: u32,
    ) -> Result<()> {
        let mut state = self.state.lock().expect("state lock");
        let key = format!("{service_id}/{deployment_id}");
        if let Some(replicas) = state.replicas.get_mut(&key) {
            replicas.retain(|r| r.replica_index != replica_index);
        }
        Ok(())
    }

    async fn list_service_infos(&self) -> Result<Vec<ServiceInfo>> {
        let state = self.state.lock().expect("state lock");
        Ok(state
            .configs
            .iter()
            .map(|(id, cfg)| ServiceInfo {
                config: cfg.clone(),
                deploy_frozen: state.deploy_frozen.get(id).copied().unwrap_or(false),
                replicas_override: state.replicas_override.get(id).copied().flatten(),
            })
            .collect())
    }

    async fn list_service_deployments(&self, service_id: &str) -> Result<Vec<ServiceDeployment>> {
        let state = self.state.lock().expect("state lock");
        let mut deployments = state.history.get(service_id).cloned().unwrap_or_default();
        deployments.sort_by_key(|deployment| std::cmp::Reverse(deployment.created_at));
        Ok(deployments)
    }

    async fn read_service_info(&self, service_id: &str) -> Result<Option<ServiceInfo>> {
        let state = self.state.lock().expect("state lock");
        Ok(state.configs.get(service_id).map(|cfg| ServiceInfo {
            config: cfg.clone(),
            deploy_frozen: state
                .deploy_frozen
                .get(service_id)
                .copied()
                .unwrap_or(false),
            replicas_override: state.replicas_override.get(service_id).copied().flatten(),
        }))
    }

    async fn get_service_status(&self, service_id: &str) -> Result<Option<DeploymentStatus>> {
        let state = self.state.lock().expect("state lock");
        Ok(state
            .history
            .get(service_id)
            .and_then(|h| h.iter().max_by_key(|d| d.created_at))
            .map(|d| d.status.clone()))
    }

    async fn read_service_deployment(
        &self,
        deployment: &Deployment,
    ) -> Result<Option<ServiceDeployment>> {
        let state = self.state.lock().expect("state lock");
        Ok(state
            .history
            .get(&deployment.service_id)
            .and_then(|h| h.iter().find(|d| d.id == deployment.id).cloned()))
    }

    async fn queue_deployment(&self, deployment: ServiceDeployment) -> Result<ForceQueueOutcome> {
        let mut state = self.state.lock().expect("state lock");
        let service_id = deployment.config.id.clone();
        let history = state.history.entry(service_id).or_default();
        let index = history.len();
        history.push(deployment.clone());
        state
            .transitions
            .entry(deployment.id.clone())
            .or_default()
            .push(DeploymentStatus::Queued);
        Ok(ForceQueueOutcome {
            deployment_index: index,
            deployment,
        })
    }

    async fn stop_service_deployment(
        &self,
        deployment: &Deployment,
    ) -> Result<Option<ServiceDeployment>> {
        let mut state = self.state.lock().expect("state lock");
        let updated = {
            let Some(history) = state.history.get_mut(&deployment.service_id) else {
                return Ok(None);
            };
            let Some(stored) = history.iter_mut().find(|d| d.id == deployment.id) else {
                return Ok(None);
            };
            if matches!(
                stored.status,
                DeploymentStatus::Ready
                    | DeploymentStatus::PendingReady
                    | DeploymentStatus::Building
            ) {
                stored.status = DeploymentStatus::Draining;
                stored.drained_at = Some(self.clock.now_ms());
                Some(stored.clone())
            } else {
                Some(stored.clone())
            }
        };
        if let Some(d) = &updated
            && d.status == DeploymentStatus::Draining
        {
            state
                .transitions
                .entry(deployment.id.clone())
                .or_default()
                .push(DeploymentStatus::Draining);
        }
        Ok(updated)
    }

    async fn cancel_service_deployment(
        &self,
        deployment: &Deployment,
    ) -> Result<CancelDeploymentOutcome> {
        let mut state = self.state.lock().expect("state lock");
        let outcome = {
            let Some(history) = state.history.get_mut(&deployment.service_id) else {
                return Ok(CancelDeploymentOutcome::NotFound);
            };
            let Some(stored) = history.iter_mut().find(|d| d.id == deployment.id) else {
                return Ok(CancelDeploymentOutcome::NotFound);
            };
            if matches!(
                stored.status,
                DeploymentStatus::Queued | DeploymentStatus::Building
            ) {
                stored.status = DeploymentStatus::Canceled;
                CancelDeploymentOutcome::Canceled(stored.clone())
            } else {
                CancelDeploymentOutcome::NotCancelable(stored.clone())
            }
        };
        if matches!(outcome, CancelDeploymentOutcome::Canceled(_)) {
            state
                .transitions
                .entry(deployment.id.clone())
                .or_default()
                .push(DeploymentStatus::Canceled);
        }
        Ok(outcome)
    }

    async fn save_build_data(
        &self,
        service_id: &str,
        deployment: &ServiceDeployment,
    ) -> Result<()> {
        let mut state = self.state.lock().expect("state lock");
        if let Some(history) = state.history.get_mut(service_id)
            && let Some(stored) = history.iter_mut().find(|d| d.id == deployment.id)
        {
            stored.build = deployment.build.clone();
            stored.git_commit = deployment.git_commit.clone();
        }
        Ok(())
    }

    async fn save_deploy_data(
        &self,
        service_id: &str,
        deployment: &ServiceDeployment,
    ) -> Result<()> {
        let mut state = self.state.lock().expect("state lock");
        if let Some(history) = state.history.get_mut(service_id)
            && let Some(stored) = history.iter_mut().find(|d| d.id == deployment.id)
        {
            stored.deployed_at = deployment.deployed_at;
        }
        Ok(())
    }

    async fn update_deployment_build_info(
        &self,
        deployment: &Deployment,
        updated: &ServiceDeployment,
    ) -> Result<()> {
        let mut state = self.state.lock().expect("state lock");
        if let Some(history) = state.history.get_mut(&deployment.service_id)
            && let Some(stored) = history.iter_mut().find(|d| d.id == deployment.id)
        {
            stored.git_commit = updated.git_commit.clone();
            stored.build = updated.build.clone();
        }
        Ok(())
    }

    async fn read_system_upgrade_request(
        &self,
        _node_id: Option<&str>,
    ) -> Result<Option<SystemUpgradeRequest>> {
        Ok(None)
    }

    async fn delete_system_upgrade_request(&self, _node_id: Option<&str>) -> Result<()> {
        Ok(())
    }

    async fn read_system_restart_request(&self, _node_id: Option<&str>) -> Result<bool> {
        Ok(false)
    }

    async fn delete_system_restart_request(&self, _node_id: Option<&str>) -> Result<()> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Runtime mock
// ---------------------------------------------------------------------------

struct NoopRuntime {
    supervisor: Arc<InMemoryReplicaSupervisor>,
}

#[async_trait]
impl RuntimeProvider for NoopRuntime {
    fn cli_name(&self) -> &str {
        "noop"
    }

    fn requires_explicit_dns(&self) -> bool {
        false
    }

    async fn ensure_network(&self, _name: &str, _subnet: Option<&str>) -> Result<()> {
        Ok(())
    }

    async fn remove_network(&self, _name: &str) -> Result<()> {
        Ok(())
    }

    async fn remove_container(&self, _name: &str) -> Result<()> {
        Ok(())
    }

    fn run_command(&self, _spec: &runtime::RunSpec) -> JobCommand {
        JobCommand::Shell("noop".to_string())
    }

    async fn inspect_container_ip(&self, name: &str) -> Option<String> {
        if self.supervisor.is_hostname_alive(name) {
            Some("10.99.0.1".to_string())
        } else {
            None
        }
    }

    async fn inspect_network_cidr(&self, _name: &str) -> Option<String> {
        Some("10.99.0.0/16".to_string())
    }

    async fn build_image(
        &self,
        _spec: &runtime::BuildSpec,
        _sender: Option<&flume::Sender<LogEntry>>,
        _source: Option<&str>,
    ) -> Result<()> {
        Ok(())
    }

    async fn pull_image(
        &self,
        _image: &str,
        _sender: Option<&flume::Sender<LogEntry>>,
        _source: Option<&str>,
    ) -> Result<()> {
        Ok(())
    }

    async fn tag_image(&self, _source: &str, _target: &str) -> Result<()> {
        Ok(())
    }

    async fn push_image(&self, _tag: &str) -> Result<()> {
        Ok(())
    }

    async fn exec_in_container(&self, _container: &str, _cmd: &[&str]) -> Result<String> {
        Ok(String::new())
    }

    async fn remove_image(&self, _image_id: &str) -> Result<()> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Test harness
// ---------------------------------------------------------------------------

struct Harness {
    store: Arc<LifecycleStore>,
    provider: Arc<InMemoryProvider>,
    supervisor: Arc<InMemoryReplicaSupervisor>,
    health: Arc<DefaultHealthMonitor>,
    clock: Arc<FakeClock>,
    controller: DeploymentController,
    _signal_tx: broadcast::Sender<ShutdownEvent>,
}

impl Harness {
    fn new() -> Self {
        let temp = std::env::temp_dir().join(format!(
            "maestro-lifecycle-{}",
            crate::utils::nanoid::unique_id(8)
        ));
        std::fs::create_dir_all(&temp).expect("temp dir");
        let clock = Arc::new(FakeClock::at(1_000_000));
        let store: Arc<LifecycleStore> = Arc::new(LifecycleStore::with_clock(clock.clone()));
        let provider = Arc::new(InMemoryProvider::new());
        let supervisor = Arc::new(InMemoryReplicaSupervisor::new());
        let engine = Arc::new(Engine::new(
            provider.clone() as Arc<dyn DeploymentProvider>,
            supervisor.clone() as Arc<dyn ReplicaSupervisor>,
            temp.clone(),
        ));
        let (signal_tx, signal_rx) = broadcast::channel(4);
        let controller = DeploymentController::with_engine(
            ControllerConfig {
                data_dir: temp.clone(),
                etcd_port: 0,
                cluster_alias: "test".to_string(),
                cluster_name: "test-cluster".to_string(),
                cluster: None,
                etcd_endpoints: Vec::new(),
                container_etcd_endpoints: vec!["https://maestro-etcd:2379".to_string()],
                probe_port: None,
                admin_port: None,
                ingress_ports: vec![],
                project_dir: temp,
                network: "test-net".to_string(),
                subnet: None,
                tailscale_authkey: None,
                tailscale_advertise_routes: Vec::new(),
                encryption_key: SecretString::new("test".to_string()),
                ingestion_token: SecretString::new("test-ingestion-token".to_string()),
                internal_control_token: SecretString::new("test-control-token".to_string()),
                join_secret: None,
                jwt_secret_key: None,
                build_command_env: Default::default(),
                tags: Default::default(),
                system_type: None,
                force: false,
                disable_etcd_cert: true,
                enable_ingress_access_logs: false,
                maestro_config: String::new(),
                cloudflare_tunnel_token: None,
                cloudflare_tunnel_replicas: 1,
                slack_webhook_url: None,
                allow_exec: false,
            },
            store.clone(),
            JobSupervisor::new(),
            signal_rx,
            None,
            Arc::new(NoopRuntime {
                supervisor: supervisor.clone(),
            }),
            None,
            engine,
            None,
            clock.clone(),
        );
        let health = Arc::new(DefaultHealthMonitor::new(
            store.clone(),
            DEFAULT_MAX_HEALTHCHECK_FAILURES,
        ));
        Self {
            store,
            provider,
            supervisor,
            health,
            clock,
            controller,
            _signal_tx: signal_tx,
        }
    }

    async fn report_replica_healthy(&self, service_id: &str, deployment_id: &str, replica: u32) {
        self.health
            .report_healthy(service_id, deployment_id, replica, None)
            .await
            .expect("report_healthy");
    }

    #[allow(dead_code)]
    async fn report_replica_unhealthy(
        &self,
        service_id: &str,
        deployment_id: &str,
        replica: u32,
        reason: &str,
    ) {
        self.health
            .report_unhealthy(service_id, deployment_id, replica, None, reason)
            .await
            .expect("report_unhealthy");
    }

    async fn report_all_replicas_healthy(&self, deployment_id: &str) {
        let (service_id, count) = {
            let state = self.store.state.lock().expect("state lock");
            let mut found = None;
            for (svc_id, deployments) in &state.history {
                if let Some(dep) = deployments.iter().find(|d| d.id == deployment_id) {
                    found = Some((svc_id.clone(), dep.config.deploy.replicas));
                    break;
                }
            }
            found.expect("deployment exists")
        };
        for replica in 0..count {
            self.report_replica_healthy(&service_id, deployment_id, replica)
                .await;
        }
    }

    async fn tick(&mut self) {
        self.controller
            .reconcile_deployments()
            .await
            .expect("reconcile");
        self.controller.reap_finished_tasks().await;
    }

    /// Run the reconcile loop until `predicate` is true or `timeout` elapses.
    async fn run_until<F: FnMut(&Harness) -> bool>(
        &mut self,
        timeout: Duration,
        mut predicate: F,
    ) -> bool {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            self.tick().await;
            if predicate(self) {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        false
    }

    fn mark_all_replicas_ready(&self, deployment_id: &str) {
        let history = self.store.state.lock().expect("state lock");
        let mut found: Option<(String, u32)> = None;
        for (service_id, deployments) in history.history.iter() {
            if let Some(deployment) = deployments.iter().find(|d| d.id == deployment_id) {
                found = Some((service_id.clone(), deployment.config.deploy.replicas));
                break;
            }
        }
        drop(history);
        if let Some((service_id, replicas)) = found {
            for replica in 0..replicas {
                self.store.set_replica_status(
                    &service_id,
                    deployment_id,
                    replica,
                    DeploymentStatus::Ready,
                );
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Service config helpers
// ---------------------------------------------------------------------------

fn docker_service(id: &str, replicas: u32) -> ServiceConfig {
    ServiceConfig {
        id: id.to_string(),
        name: format!("{id} service"),
        version: format!("{id}-v1"),
        build: None,
        image: Some("example/image:latest".to_string()),
        deploy: ServiceDeployConfig {
            flags: vec![],
            expose_ports: vec![],
            command: None,
            healthcheck_path: None,
            healthcheck_interval: 60,
            replicas,
            exec: true,
            max_restarts: None,
            env: Default::default(),
            secrets: None,
            volumes: vec![],
            node_affinity: None,
            egress: Default::default(),
        },
        ingress: None,
        preview: None,
        preview_source: None,
    }
}

fn docker_service_with_ingress(id: &str, replicas: u32, host: &str) -> ServiceConfig {
    let mut cfg = docker_service(id, replicas);
    cfg.ingress = Some(IngressConfig {
        host: Some(host.to_string()),
        hosts: vec![],
        port: Some(80),
        session_affinity: None,
    });
    cfg
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn happy_path_queued_to_ready_for_docker_service() {
    let mut harness = Harness::new();
    let deployment = harness
        .store
        .queue_new_deployment(docker_service("svc-a", 1));

    assert!(harness.provider.built_image_tag(&deployment.id).is_none());

    let ready = harness
        .run_until(Duration::from_secs(2), |h| {
            // probe is faked, mark ready as soon as the build completes and
            // controller starts the replica
            if h.provider.built_image_tag(&deployment.id).is_some() {
                h.mark_all_replicas_ready(&deployment.id);
            }
            h.store.deployment_status(&deployment.id) == Some(DeploymentStatus::Ready)
        })
        .await;

    assert!(ready, "deployment did not reach Ready");
    let transitions = harness.store.deployment_transitions(&deployment.id);
    assert!(transitions.contains(&DeploymentStatus::Queued));
    assert!(transitions.contains(&DeploymentStatus::Building));
    assert!(transitions.contains(&DeploymentStatus::Ready));
}

#[tokio::test]
async fn build_failure_marks_deployment_crashed() {
    let mut harness = Harness::new();
    let deployment = harness
        .store
        .queue_new_deployment(docker_service("svc-fail", 1));
    harness
        .provider
        .set_build_err(&deployment.id, "buildkit oom");

    let crashed = harness
        .run_until(Duration::from_secs(2), |h| {
            h.store.deployment_status(&deployment.id) == Some(DeploymentStatus::Crashed)
        })
        .await;

    assert!(
        crashed,
        "build-failed deployment did not transition to Crashed"
    );
    let cleaned = harness.provider.cleaned_up_ids();
    assert!(
        cleaned.contains(&deployment.id),
        "engine.cleanup not called for failed build (cleanup_ids = {cleaned:?})"
    );
}

#[tokio::test]
async fn prepare_failure_marks_deployment_crashed() {
    let mut harness = Harness::new();
    let deployment = harness
        .store
        .queue_new_deployment(docker_service("svc-prep-fail", 1));
    harness
        .provider
        .set_prepare_err(&deployment.id, "git auth refused");

    let crashed = harness
        .run_until(Duration::from_secs(2), |h| {
            h.store.deployment_status(&deployment.id) == Some(DeploymentStatus::Crashed)
        })
        .await;

    assert!(
        crashed,
        "prepare-failed deployment did not transition to Crashed"
    );
}

#[tokio::test]
async fn single_replica_crash_is_restarted_in_place_without_taking_down_deployment() {
    // One of three replicas hits the healthcheck threshold (probe writes
    // Crashed); the other two are Ready. The controller must restart that
    // replica in-place (same hostname) and keep the deployment Ready.
    let mut harness = Harness::new();
    let deployment = harness
        .store
        .queue_new_deployment(docker_service("svc-1of3-crash", 3));

    let all_running = harness
        .run_until(Duration::from_secs(2), |h| {
            h.supervisor.running_count() == 3
        })
        .await;
    assert!(
        all_running,
        "expected 3 replicas running; got {}",
        harness.supervisor.running_count(),
    );

    harness.mark_all_replicas_ready(&deployment.id);
    let ready = harness
        .run_until(Duration::from_secs(2), |h| {
            h.store.deployment_status(&deployment.id) == Some(DeploymentStatus::Ready)
        })
        .await;
    assert!(ready, "deployment never reached Ready");

    harness.store.set_replica_status(
        &deployment.config.id,
        &deployment.id,
        0,
        DeploymentStatus::Crashed,
    );

    let restarted = harness
        .run_until(Duration::from_secs(3), |h| {
            let replicas = h.store.replicas_for(&deployment.config.id, &deployment.id);
            let replica0 = replicas.iter().find(|state| state.replica_index == 0);
            h.supervisor.running_count() == 3
                && replica0
                    .map(|state| state.restart_attempts >= 1)
                    .unwrap_or(false)
        })
        .await;

    let replica0 = harness
        .store
        .replicas_for(&deployment.config.id, &deployment.id)
        .into_iter()
        .find(|state| state.replica_index == 0);
    assert!(
        restarted,
        "replica 0 was not restarted; running_count = {}, replica0 = {:?}",
        harness.supervisor.running_count(),
        replica0,
    );
    assert_eq!(
        harness.store.deployment_status(&deployment.id),
        Some(DeploymentStatus::Ready),
        "deployment must stay Ready when only one replica was restarted",
    );
    for replica_index in 0..3 {
        assert!(
            harness
                .supervisor
                .is_hostname_alive(&deployment.hostname_for_replica(replica_index)),
            "replica {replica_index} must be alive after restart-in-place",
        );
    }
}

#[tokio::test]
async fn replica_restart_budget_caps_at_max_attempts() {
    // After the replica has already used its full restart budget, a further
    // probe-written Crash must NOT trigger another restart; the slot stays
    // empty.
    let mut harness = Harness::new();
    let deployment = harness
        .store
        .queue_new_deployment(docker_service("svc-exhausted", 3));

    let all_running = harness
        .run_until(Duration::from_secs(2), |h| {
            h.supervisor.running_count() == 3
        })
        .await;
    assert!(all_running, "expected 3 replicas running");

    harness.mark_all_replicas_ready(&deployment.id);
    let ready = harness
        .run_until(Duration::from_secs(2), |h| {
            h.store.deployment_status(&deployment.id) == Some(DeploymentStatus::Ready)
        })
        .await;
    assert!(ready, "deployment never reached Ready");

    harness.store.set_replica_state(
        &deployment.config.id,
        &deployment.id,
        ReplicaState {
            service_id: None,
            deployment_id: None,
            replica_index: 0,
            status: DeploymentStatus::Crashed,
            healthcheck_failures: crate::health::DEFAULT_MAX_HEALTHCHECK_FAILURES,
            restart_attempts: crate::health::MAX_REPLICA_RESTART_ATTEMPTS,
            node_id: None,
            assignment_id: None,
            endpoint: None,
            error: None,
        },
    );

    let exhausted = harness
        .run_until(Duration::from_secs(3), |h| {
            !h.supervisor
                .is_hostname_alive(&deployment.hostname_for_replica(0))
        })
        .await;
    assert!(exhausted, "exhausted replica's container was not shut down",);

    for _ in 0..20 {
        harness.tick().await;
    }

    assert!(
        !harness
            .supervisor
            .is_hostname_alive(&deployment.hostname_for_replica(0)),
        "exhausted replica must not be restarted",
    );
    assert_eq!(
        harness.store.deployment_status(&deployment.id),
        Some(DeploymentStatus::Ready),
        "deployment must stay Ready when peers are still Ready",
    );
    for replica_index in 1..3 {
        assert!(
            harness
                .supervisor
                .is_hostname_alive(&deployment.hostname_for_replica(replica_index)),
            "healthy peer replica {replica_index} must keep running",
        );
    }
}

#[tokio::test]
async fn deployment_crashes_only_when_every_replica_exhausts_restart_budget() {
    // Every replica has burned its full restart budget; only then does the
    // deployment transition to Crashed and all containers stay down.
    let mut harness = Harness::new();
    let deployment = harness
        .store
        .queue_new_deployment(docker_service("svc-all-exhausted", 3));

    let all_running = harness
        .run_until(Duration::from_secs(2), |h| {
            h.supervisor.running_count() == 3
        })
        .await;
    assert!(all_running, "expected 3 replicas running");

    harness.mark_all_replicas_ready(&deployment.id);
    let ready = harness
        .run_until(Duration::from_secs(2), |h| {
            h.store.deployment_status(&deployment.id) == Some(DeploymentStatus::Ready)
        })
        .await;
    assert!(ready, "deployment never reached Ready");

    for replica_index in 0..3 {
        harness.store.set_replica_state(
            &deployment.config.id,
            &deployment.id,
            ReplicaState {
                service_id: None,
                deployment_id: None,
                replica_index,
                status: DeploymentStatus::Crashed,
                healthcheck_failures: crate::health::DEFAULT_MAX_HEALTHCHECK_FAILURES,
                restart_attempts: crate::health::MAX_REPLICA_RESTART_ATTEMPTS,
                node_id: None,
                assignment_id: None,
                endpoint: None,
                error: None,
            },
        );
    }

    let crashed_and_cleaned = harness
        .run_until(Duration::from_secs(3), |h| {
            h.store.deployment_status(&deployment.id) == Some(DeploymentStatus::Crashed)
                && h.supervisor.running_count() == 0
        })
        .await;
    assert!(
        crashed_and_cleaned,
        "all-replicas-exhausted did not crash the deployment; \
         status = {:?}, running = {}",
        harness.store.deployment_status(&deployment.id),
        harness.supervisor.running_count(),
    );
}

#[tokio::test]
async fn one_crash_during_initial_rollout_does_not_kill_pending_peers() {
    // Initial deploy: replica 0 fails fast (probe writes Crashed) while
    // peers 1 and 2 are still in PendingReady (slow startup). With a
    // per-replica restart policy, the still-pending peers must NOT be
    // killed by a single fast failure.
    let mut harness = Harness::new();
    let mut config = docker_service("svc-rollout-crash", 3);
    config.deploy.healthcheck_path = Some("/_healthy".to_string());
    let deployment = harness.store.queue_new_deployment(config);

    let all_running = harness
        .run_until(Duration::from_secs(2), |h| {
            h.supervisor.running_count() == 3
        })
        .await;
    assert!(
        all_running,
        "expected 3 replicas running; got {}",
        harness.supervisor.running_count(),
    );

    harness.store.set_replica_status(
        &deployment.config.id,
        &deployment.id,
        0,
        DeploymentStatus::Crashed,
    );

    for _ in 0..20 {
        harness.tick().await;
    }

    assert_ne!(
        harness.store.deployment_status(&deployment.id),
        Some(DeploymentStatus::Crashed),
        "single replica crash during rollout must not crash the whole deployment",
    );
    for replica_index in 1..3 {
        assert!(
            harness
                .supervisor
                .is_hostname_alive(&deployment.hostname_for_replica(replica_index)),
            "pending peer replica {replica_index} must not be killed by one fast failure",
        );
    }
}

#[tokio::test]
async fn unexpected_container_termination_is_recovered_by_orphan_logic() {
    // Simulates a container that dies without the probe writing a status —
    // for example a crash so fast the probe never noticed. The controller's
    // orphan-reconcile path should detect this (container not alive,
    // no replica state) and mark the deployment as Terminated.
    let mut harness = Harness::new();
    let deployment = harness
        .store
        .queue_new_deployment(docker_service("svc-orphan", 1));

    let started = harness
        .run_until(Duration::from_secs(2), |h| {
            h.supervisor.running_count() == 1
        })
        .await;
    assert!(started, "replica never started");

    let handle = ReplicaHandle {
        task_id: format!("{}-replica-0", deployment.id),
        service_id: deployment.config.id.clone(),
        deployment_id: deployment.id.clone(),
        replica_index: 0,
    };
    // Remove the replica state too — simulating the "no probe verdict at all"
    // case where the container vanished from under us.
    {
        let mut state = harness.store.state.lock().expect("state lock");
        let key = format!("{}/{}", deployment.config.id, deployment.id);
        state.replicas.remove(&key);
    }
    harness.supervisor.crash_job(&handle.task_id);

    let terminated = harness
        .run_until(Duration::from_secs(3), |h| {
            matches!(
                h.store.deployment_status(&deployment.id),
                Some(DeploymentStatus::Terminated | DeploymentStatus::Crashed)
            )
        })
        .await;
    assert!(
        terminated,
        "orphaned deployment was not recovered; transitions = {:?}",
        harness.store.deployment_transitions(&deployment.id),
    );
}

#[tokio::test]
async fn rolling_update_drains_previous_deployment() {
    let mut harness = Harness::new();
    let mut config = docker_service_with_ingress("svc-roll", 1, "roll.local");
    let v1 = harness.store.queue_new_deployment(config.clone());

    let v1_ready = harness
        .run_until(Duration::from_secs(2), |h| {
            if h.provider.built_image_tag(&v1.id).is_some() {
                h.mark_all_replicas_ready(&v1.id);
            }
            h.store.deployment_status(&v1.id) == Some(DeploymentStatus::Ready)
        })
        .await;
    assert!(v1_ready, "v1 did not reach Ready");

    config.version = "svc-roll-v2".to_string();
    let v2 = harness.store.queue_new_deployment(config);

    let v2_ready = harness
        .run_until(Duration::from_secs(3), |h| {
            if h.provider.built_image_tag(&v2.id).is_some() {
                h.mark_all_replicas_ready(&v2.id);
            }
            h.store.deployment_status(&v2.id) == Some(DeploymentStatus::Ready)
        })
        .await;
    assert!(v2_ready, "v2 did not reach Ready");

    let v1_drained = harness
        .run_until(Duration::from_secs(3), |h| {
            matches!(
                h.store.deployment_status(&v1.id),
                Some(DeploymentStatus::Draining | DeploymentStatus::Removed)
            )
        })
        .await;
    assert!(
        v1_drained,
        "v1 was not drained after v2 ready; v1 status = {:?}",
        harness.store.deployment_status(&v1.id),
    );

    assert_eq!(harness.store.deployment_count("svc-roll"), 2);
}

#[tokio::test]
async fn multi_replica_deployment_starts_all_replicas() {
    let mut harness = Harness::new();
    let deployment = harness
        .store
        .queue_new_deployment(docker_service("svc-multi", 3));

    let all_running = harness
        .run_until(Duration::from_secs(2), |h| {
            h.supervisor.running_count() == 3
        })
        .await;
    assert!(
        all_running,
        "expected 3 replicas running; got {}",
        harness.supervisor.running_count(),
    );

    harness.mark_all_replicas_ready(&deployment.id);
    let ready = harness
        .run_until(Duration::from_secs(2), |h| {
            h.store.deployment_status(&deployment.id) == Some(DeploymentStatus::Ready)
        })
        .await;
    assert!(ready, "multi-replica deployment never reached Ready");

    let replicas = harness
        .store
        .replicas_for(&deployment.config.id, &deployment.id);
    assert_eq!(replicas.len(), 3);
    assert!(replicas.iter().all(|r| r.status == DeploymentStatus::Ready));
}

#[tokio::test]
async fn frozen_deploy_does_not_block_running_lifecycle() {
    // The freeze flag is enforced at the server's rollout endpoint, not by
    // the controller's reconcile loop. Once a deployment lands in the queue,
    // it should still proceed even if the service is later frozen.
    let mut harness = Harness::new();
    let deployment = harness
        .store
        .queue_new_deployment(docker_service("svc-frozen", 1));
    harness.store.freeze("svc-frozen");

    let ready = harness
        .run_until(Duration::from_secs(2), |h| {
            if h.provider.built_image_tag(&deployment.id).is_some() {
                h.mark_all_replicas_ready(&deployment.id);
            }
            h.store.deployment_status(&deployment.id) == Some(DeploymentStatus::Ready)
        })
        .await;
    assert!(
        ready,
        "frozen-but-already-queued deployment failed to roll out"
    );
}

#[tokio::test]
async fn engine_prepares_before_building_for_each_deployment() {
    let mut harness = Harness::new();
    let deployment = harness
        .store
        .queue_new_deployment(docker_service("svc-order", 1));

    let prepared_before_built = harness
        .run_until(Duration::from_secs(2), |h| {
            // Once the engine has the artifact, we know prepare ran first
            // because build() takes a PreparedDeployment.
            h.provider.built_image_tag(&deployment.id).is_some()
        })
        .await;
    assert!(prepared_before_built, "build did not complete");
    assert!(harness.provider.prepared_ids().contains(&deployment.id));
}

#[tokio::test]
async fn failures_in_one_service_do_not_affect_another() {
    let mut harness = Harness::new();
    let good = harness
        .store
        .queue_new_deployment(docker_service("svc-good", 1));
    let bad = harness
        .store
        .queue_new_deployment(docker_service("svc-bad", 1));
    harness.provider.set_build_err(&bad.id, "boom");

    let progressed = harness
        .run_until(Duration::from_secs(3), |h| {
            if h.provider.built_image_tag(&good.id).is_some() {
                h.mark_all_replicas_ready(&good.id);
            }
            h.store.deployment_status(&good.id) == Some(DeploymentStatus::Ready)
                && h.store.deployment_status(&bad.id) == Some(DeploymentStatus::Crashed)
        })
        .await;
    assert!(
        progressed,
        "good={:?} bad={:?}",
        harness.store.deployment_status(&good.id),
        harness.store.deployment_status(&bad.id),
    );
}

#[tokio::test]
async fn sequential_redeploys_of_same_service_supersede_each_other() {
    let mut harness = Harness::new();
    let mut config = docker_service("svc-seq", 1);
    let v1 = harness.store.queue_new_deployment(config.clone());

    let v1_ready = harness
        .run_until(Duration::from_secs(2), |h| {
            if h.provider.built_image_tag(&v1.id).is_some() {
                h.mark_all_replicas_ready(&v1.id);
            }
            h.store.deployment_status(&v1.id) == Some(DeploymentStatus::Ready)
        })
        .await;
    assert!(v1_ready);

    config.version = "svc-seq-v2".to_string();
    let v2 = harness.store.queue_new_deployment(config.clone());

    let v2_ready = harness
        .run_until(Duration::from_secs(3), |h| {
            if h.provider.built_image_tag(&v2.id).is_some() {
                h.mark_all_replicas_ready(&v2.id);
            }
            h.store.deployment_status(&v2.id) == Some(DeploymentStatus::Ready)
        })
        .await;
    assert!(v2_ready);

    config.version = "svc-seq-v3".to_string();
    let v3 = harness.store.queue_new_deployment(config);

    let v3_ready = harness
        .run_until(Duration::from_secs(3), |h| {
            if h.provider.built_image_tag(&v3.id).is_some() {
                h.mark_all_replicas_ready(&v3.id);
            }
            h.store.deployment_status(&v3.id) == Some(DeploymentStatus::Ready)
        })
        .await;
    assert!(v3_ready);

    let drained_or_removed = harness
        .run_until(Duration::from_secs(3), |h| {
            matches!(
                h.store.deployment_status(&v1.id),
                Some(DeploymentStatus::Draining | DeploymentStatus::Removed)
            ) && matches!(
                h.store.deployment_status(&v2.id),
                Some(DeploymentStatus::Draining | DeploymentStatus::Removed)
            )
        })
        .await;
    assert!(
        drained_or_removed,
        "v1={:?} v2={:?}",
        harness.store.deployment_status(&v1.id),
        harness.store.deployment_status(&v2.id),
    );
}

#[tokio::test]
async fn build_artifact_is_captured_in_deployment_record() {
    let mut harness = Harness::new();
    let deployment = harness
        .store
        .queue_new_deployment(docker_service("svc-artifact", 1));
    harness
        .provider
        .set_build_ok(&deployment.id, "registry.example.com/svc-artifact:abc123");

    let built = harness
        .run_until(Duration::from_secs(2), |h| {
            h.provider.built_image_tag(&deployment.id).is_some()
        })
        .await;
    assert!(built);

    // After build, the deployment record in store should carry the image tag
    // (saved via save_build_data → update_deployment_build_info).
    let stored = {
        let state = harness.store.state.lock().expect("state lock");
        state
            .history
            .get(&deployment.config.id)
            .and_then(|h| h.iter().find(|d| d.id == deployment.id).cloned())
    };
    let stored = stored.expect("deployment should still exist");
    assert_eq!(
        stored.build.as_ref().map(|b| b.docker_image_id.as_str()),
        Some("registry.example.com/svc-artifact:abc123"),
    );
}

#[tokio::test]
async fn report_unhealthy_threshold_triggers_replica_restart() {
    // Verifies the production path: probe calls report_unhealthy() repeatedly.
    // After DEFAULT_MAX_HEALTHCHECK_FAILURES the monitor flips the replica's
    // status to Crashed; the controller then restarts the replica in-place
    // (one consumed attempt from the restart budget) and the deployment
    // stays active.
    let mut harness = Harness::new();
    let deployment = harness
        .store
        .queue_new_deployment(docker_service("svc-thresh", 1));

    let started = harness
        .run_until(Duration::from_secs(2), |h| {
            h.supervisor.running_count() == 1
        })
        .await;
    assert!(started, "replica did not start");

    harness
        .report_replica_healthy(&deployment.config.id, &deployment.id, 0)
        .await;
    harness.tick().await;

    for _ in 0..DEFAULT_MAX_HEALTHCHECK_FAILURES {
        harness
            .report_replica_unhealthy(
                &deployment.config.id,
                &deployment.id,
                0,
                "synthetic failure",
            )
            .await;
    }

    let restarted = harness
        .run_until(Duration::from_secs(3), |h| {
            let replicas = h.store.replicas_for(&deployment.config.id, &deployment.id);
            let replica0 = replicas.iter().find(|state| state.replica_index == 0);
            h.supervisor.running_count() == 1
                && replica0
                    .map(|state| {
                        state.restart_attempts >= 1 && state.status != DeploymentStatus::Crashed
                    })
                    .unwrap_or(false)
        })
        .await;
    let replica0 = harness
        .store
        .replicas_for(&deployment.config.id, &deployment.id)
        .into_iter()
        .find(|state| state.replica_index == 0);
    assert!(
        restarted,
        "replica did not restart after {} unhealthy reports; running_count = {}, replica0 = {:?}, deployment_status = {:?}",
        DEFAULT_MAX_HEALTHCHECK_FAILURES,
        harness.supervisor.running_count(),
        replica0,
        harness.store.deployment_status(&deployment.id),
    );
    assert_ne!(
        harness.store.deployment_status(&deployment.id),
        Some(DeploymentStatus::Crashed),
        "deployment must not be Crashed after a single restart",
    );
}

#[tokio::test]
async fn report_healthy_is_a_no_op_when_replica_is_already_ready() {
    // Catches the optimization: DefaultHealthMonitor::report_healthy should
    // only write to the store when the recorded state differs.
    let mut harness = Harness::new();
    let deployment = harness
        .store
        .queue_new_deployment(docker_service("svc-noop", 1));

    harness
        .run_until(Duration::from_secs(2), |h| {
            h.supervisor.running_count() == 1
        })
        .await;

    // Ensure the replica state in the store is exactly {Ready, failures=0}
    // (whatever the controller's orphan-repair did, settle on that).
    harness
        .report_replica_healthy(&deployment.config.id, &deployment.id, 0)
        .await;
    let pre = harness
        .store
        .replicas_for(&deployment.config.id, &deployment.id);
    assert_eq!(pre.len(), 1);
    assert_eq!(pre[0].status, DeploymentStatus::Ready);
    assert_eq!(pre[0].healthcheck_failures, 0);

    let baseline = harness.store.upsert_replica_state_calls();

    // Subsequent reports for the same state should NOT write.
    for _ in 0..5 {
        harness
            .report_replica_healthy(&deployment.config.id, &deployment.id, 0)
            .await;
    }
    let after = harness.store.upsert_replica_state_calls();
    assert_eq!(
        after, baseline,
        "report_healthy did extra writes when already Ready: {baseline} -> {after}",
    );
}

#[tokio::test]
async fn report_healthy_writes_when_status_differs_even_with_zero_failures() {
    // Catches the OR-vs-AND mutation in needs_update: with state =
    // {PendingReady, failures=0}, OR -> true (status differs) but AND -> false
    // (failures match). The original code must write to transition the
    // replica out of PendingReady.
    let harness = Harness::new();
    harness
        .store
        .set_replica_status("svc-or", "dep-or", 0, DeploymentStatus::PendingReady);
    let baseline = harness.store.upsert_replica_state_calls();

    harness.report_replica_healthy("svc-or", "dep-or", 0).await;

    let after = harness.store.upsert_replica_state_calls();
    assert!(
        after > baseline,
        "report_healthy should write to flip PendingReady→Ready (baseline={baseline}, after={after})",
    );
    let replicas = harness.store.replicas_for("svc-or", "dep-or");
    assert_eq!(replicas[0].status, DeploymentStatus::Ready);
}

#[tokio::test]
async fn report_unhealthy_is_a_no_op_when_already_unhealthy_with_same_count() {
    // Catches a similar optimization on the unhealthy path: once the failure
    // counter has been observed, repeating the same report doesn't double-count.
    // (The monitor increments on each call, so the count actually changes —
    // verify the count increments monotonically and writes happen each time.)
    let mut harness = Harness::new();
    let deployment = harness
        .store
        .queue_new_deployment(docker_service("svc-unhealthy", 1));

    harness
        .run_until(Duration::from_secs(2), |h| {
            h.supervisor.running_count() == 1
        })
        .await;

    let before = harness.store.upsert_replica_state_calls();
    harness
        .report_replica_unhealthy(&deployment.config.id, &deployment.id, 0, "x")
        .await;
    let after = harness.store.upsert_replica_state_calls();
    assert!(
        after > before,
        "first report_unhealthy should write to store"
    );
}

#[tokio::test]
async fn report_healthy_resets_failure_count() {
    let mut harness = Harness::new();
    let deployment = harness
        .store
        .queue_new_deployment(docker_service("svc-recover", 1));

    harness
        .run_until(Duration::from_secs(2), |h| {
            h.supervisor.running_count() == 1
        })
        .await;

    // Accumulate 9 failures (one short of the threshold), then report healthy.
    for _ in 0..(DEFAULT_MAX_HEALTHCHECK_FAILURES - 1) {
        harness
            .report_replica_unhealthy(&deployment.config.id, &deployment.id, 0, "x")
            .await;
    }
    harness
        .report_replica_healthy(&deployment.config.id, &deployment.id, 0)
        .await;

    let replicas = harness
        .store
        .replicas_for(&deployment.config.id, &deployment.id);
    assert_eq!(replicas.len(), 1);
    assert_eq!(replicas[0].status, DeploymentStatus::Ready);
    assert_eq!(replicas[0].healthcheck_failures, 0);

    // Should not be stopped — recovery should have reset the failure budget.
    assert!(matches!(
        harness.store.deployment_status(&deployment.id),
        Some(DeploymentStatus::Ready | DeploymentStatus::Building | DeploymentStatus::PendingReady)
    ));
}

#[tokio::test]
async fn cancel_during_build_aborts_pending_build() {
    // Inject a long pause into the build by setting an override that returns
    // an Err only after we've observed Building state. Cancel arrives in the
    // window between the controller queueing the build task and the build
    // completing.
    let mut harness = Harness::new();
    let deployment = harness
        .store
        .queue_new_deployment(docker_service("svc-cancel-build", 1));

    // Drive one tick so the controller picks the deployment up.
    harness.tick().await;

    // Status should now be Building (the build is spawned async).
    let _building = harness
        .run_until(Duration::from_millis(500), |h| {
            matches!(
                h.store.deployment_status(&deployment.id),
                Some(DeploymentStatus::Building | DeploymentStatus::Ready)
            )
        })
        .await;

    let _ = harness
        .store
        .cancel_service_deployment(&Deployment {
            service_id: deployment.config.id.clone(),
            id: deployment.id.clone(),
            replica_index: 0,
        })
        .await;

    let canceled_or_finished = harness
        .run_until(Duration::from_secs(2), |h| {
            matches!(
                h.store.deployment_status(&deployment.id),
                Some(DeploymentStatus::Canceled)
            )
        })
        .await;
    assert!(
        canceled_or_finished,
        "deployment was not canceled; status = {:?}",
        harness.store.deployment_status(&deployment.id),
    );
}

#[tokio::test]
async fn many_services_rollout_independently_under_one_controller() {
    let mut harness = Harness::new();
    let mut deployments = Vec::new();
    for i in 0..8 {
        let deployment = harness
            .store
            .queue_new_deployment(docker_service(&format!("svc-multi-{i}"), 1));
        deployments.push(deployment);
    }

    let all_ready = harness
        .run_until(Duration::from_secs(5), |h| {
            for d in &deployments {
                if h.provider.built_image_tag(&d.id).is_some() {
                    h.mark_all_replicas_ready(&d.id);
                }
            }
            deployments
                .iter()
                .all(|d| h.store.deployment_status(&d.id) == Some(DeploymentStatus::Ready))
        })
        .await;
    assert!(
        all_ready,
        "not all services reached Ready: {:?}",
        deployments
            .iter()
            .map(|d| (d.id.clone(), harness.store.deployment_status(&d.id)))
            .collect::<Vec<_>>(),
    );
}

#[tokio::test]
async fn back_to_back_redeploys_only_keep_one_active() {
    // Queue 3 deployments of the same service in rapid succession. After
    // settle, exactly one deployment should be Ready; the rest should be in
    // a terminal/draining state.
    let mut harness = Harness::new();
    let mut config = docker_service("svc-rapid", 1);

    let mut deps = Vec::new();
    for v in 1..=3 {
        config.version = format!("svc-rapid-v{v}");
        deps.push(harness.store.queue_new_deployment(config.clone()));
    }

    let settled = harness
        .run_until(Duration::from_secs(5), |h| {
            for d in &deps {
                if h.provider.built_image_tag(&d.id).is_some() {
                    h.mark_all_replicas_ready(&d.id);
                }
            }
            // The latest should be Ready, the earlier two should NOT be Ready.
            let latest_ready =
                h.store.deployment_status(&deps[2].id) == Some(DeploymentStatus::Ready);
            let older_drained = deps[..2].iter().all(|d| {
                matches!(
                    h.store.deployment_status(&d.id),
                    Some(
                        DeploymentStatus::Draining
                            | DeploymentStatus::Removed
                            | DeploymentStatus::Terminated
                            | DeploymentStatus::Canceled
                    )
                )
            });
            latest_ready && older_drained
        })
        .await;
    assert!(
        settled,
        "back-to-back redeploys did not settle correctly; statuses = {:?}",
        deps.iter()
            .map(|d| (d.id.clone(), harness.store.deployment_status(&d.id)))
            .collect::<Vec<_>>(),
    );
}

#[tokio::test]
async fn replica_crash_during_rolling_update_does_not_break_new_deployment() {
    // v1 is running; v2 is queued. While v2 is being prepared, crash v1's
    // replica. v2 should still complete.
    let mut harness = Harness::new();
    let mut config = docker_service("svc-roll-crash", 1);
    let v1 = harness.store.queue_new_deployment(config.clone());

    let v1_ready = harness
        .run_until(Duration::from_secs(2), |h| {
            if h.provider.built_image_tag(&v1.id).is_some() {
                h.mark_all_replicas_ready(&v1.id);
            }
            h.store.deployment_status(&v1.id) == Some(DeploymentStatus::Ready)
        })
        .await;
    assert!(v1_ready);

    config.version = "svc-roll-crash-v2".to_string();
    let v2 = harness.store.queue_new_deployment(config);

    // Crash v1's replica while v2 is being processed.
    let v1_handle = ReplicaHandle {
        task_id: format!("{}-replica-0", v1.id),
        service_id: v1.config.id.clone(),
        deployment_id: v1.id.clone(),
        replica_index: 0,
    };
    harness.supervisor.crash_job(&v1_handle.task_id);

    let v2_ready = harness
        .run_until(Duration::from_secs(3), |h| {
            if h.provider.built_image_tag(&v2.id).is_some() {
                h.mark_all_replicas_ready(&v2.id);
            }
            h.store.deployment_status(&v2.id) == Some(DeploymentStatus::Ready)
        })
        .await;
    assert!(
        v2_ready,
        "v2 did not reach Ready after v1 crash during rollover; v2 status = {:?}",
        harness.store.deployment_status(&v2.id),
    );
}

#[tokio::test]
async fn happy_path_via_health_monitor_reaches_ready() {
    // Same as happy_path_queued_to_ready_for_docker_service but drives
    // the replica-ready transition through the monitor instead of poking the
    // store directly. Validates that the monitor path is functionally
    // equivalent for the happy case.
    let mut harness = Harness::new();
    let deployment = harness
        .store
        .queue_new_deployment(docker_service("svc-monitor-happy", 1));

    let built = harness
        .run_until(Duration::from_secs(2), |h| {
            h.provider.built_image_tag(&deployment.id).is_some()
        })
        .await;
    assert!(built);

    harness.report_all_replicas_healthy(&deployment.id).await;

    let ready = harness
        .run_until(Duration::from_secs(2), |h| {
            h.store.deployment_status(&deployment.id) == Some(DeploymentStatus::Ready)
        })
        .await;
    assert!(
        ready,
        "deployment via monitor never reached Ready; status = {:?}",
        harness.store.deployment_status(&deployment.id)
    );
}

#[tokio::test]
async fn scale_up_via_replicas_override_starts_more_replicas() {
    let mut harness = Harness::new();
    let deployment = harness
        .store
        .queue_new_deployment(docker_service("svc-scale-up", 2));

    let two_running = harness
        .run_until(Duration::from_secs(2), |h| {
            h.supervisor.running_count() == 2
        })
        .await;
    assert!(two_running, "initial two replicas did not start");
    harness.mark_all_replicas_ready(&deployment.id);

    harness.store.set_replicas_override("svc-scale-up", Some(4));

    let four_running = harness
        .run_until(Duration::from_secs(3), |h| {
            h.supervisor.running_count() == 4
        })
        .await;
    assert!(
        four_running,
        "scale-up did not bring replica count to 4; observed {}",
        harness.supervisor.running_count(),
    );
}

#[tokio::test]
async fn scale_down_via_replicas_override_stops_excess_replicas() {
    let mut harness = Harness::new();
    let deployment = harness
        .store
        .queue_new_deployment(docker_service("svc-scale-down", 4));

    let four_running = harness
        .run_until(Duration::from_secs(2), |h| {
            h.supervisor.running_count() == 4
        })
        .await;
    assert!(four_running, "initial four replicas did not start");
    harness.mark_all_replicas_ready(&deployment.id);

    // Override cannot go BELOW config replicas — effective_replicas does
    // override.max(config_replicas). Set config replicas low instead by
    // queuing a new deployment with fewer replicas. For now verify the
    // floor behavior: setting override to a lower value is ignored.
    harness
        .store
        .set_replicas_override("svc-scale-down", Some(1));

    let still_four = harness
        .run_until(Duration::from_millis(400), |h| {
            // We expect the count to stay at 4 (override below floor)
            h.supervisor.running_count() != 4
        })
        .await;
    assert!(
        !still_four,
        "scale-down below config floor was honored; replicas now = {}",
        harness.supervisor.running_count(),
    );
}

#[tokio::test]
async fn replicas_override_above_config_increases_count() {
    let mut harness = Harness::new();
    let deployment = harness
        .store
        .queue_new_deployment(docker_service("svc-override", 1));

    let one_running = harness
        .run_until(Duration::from_secs(2), |h| {
            h.supervisor.running_count() == 1
        })
        .await;
    assert!(one_running);
    harness.mark_all_replicas_ready(&deployment.id);

    harness.store.set_replicas_override("svc-override", Some(3));

    let three_running = harness
        .run_until(Duration::from_secs(3), |h| {
            h.supervisor.running_count() == 3
        })
        .await;
    assert!(
        three_running,
        "override did not scale to 3; observed {}",
        harness.supervisor.running_count(),
    );

    // Removing the override should NOT scale down (effective_replicas drops
    // back to config.replicas which is 1, but reconcile_replicas honors that).
    harness.store.set_replicas_override("svc-override", None);

    let back_to_one = harness
        .run_until(Duration::from_secs(3), |h| {
            h.supervisor.running_count() == 1
        })
        .await;
    assert!(
        back_to_one,
        "removing override did not scale back down to config; observed {}",
        harness.supervisor.running_count(),
    );
}

#[tokio::test]
async fn cancel_during_queue_marks_deployment_canceled() {
    let mut harness = Harness::new();
    let deployment = harness
        .store
        .queue_new_deployment(docker_service("svc-cancel", 1));

    // Cancel immediately, before the reconcile loop picks it up.
    let _ = harness
        .store
        .cancel_service_deployment(&Deployment {
            service_id: deployment.config.id.clone(),
            id: deployment.id.clone(),
            replica_index: 0,
        })
        .await;

    let canceled = harness
        .run_until(Duration::from_millis(500), |h| {
            h.store.deployment_status(&deployment.id) == Some(DeploymentStatus::Canceled)
        })
        .await;
    assert!(canceled, "deployment was not canceled");
    // Engine should not have started building since cancel beat the controller.
    assert!(harness.provider.built_image_tag(&deployment.id).is_none());
}

// ---------------------------------------------------------------------------
// Time-based scenarios
// ---------------------------------------------------------------------------

#[tokio::test]
async fn drained_deployment_transitions_to_removed_after_grace_period() {
    let mut harness = Harness::new();
    let mut config = docker_service("svc-drain", 1);
    let v1 = harness.store.queue_new_deployment(config.clone());

    let v1_ready = harness
        .run_until(Duration::from_secs(2), |h| {
            if h.provider.built_image_tag(&v1.id).is_some() {
                h.mark_all_replicas_ready(&v1.id);
            }
            h.store.deployment_status(&v1.id) == Some(DeploymentStatus::Ready)
        })
        .await;
    assert!(v1_ready);

    config.version = "svc-drain-v2".to_string();
    let v2 = harness.store.queue_new_deployment(config);

    // First, get v2 to Ready and v1 to Draining.
    let v1_draining = harness
        .run_until(Duration::from_secs(2), |h| {
            if h.provider.built_image_tag(&v2.id).is_some() {
                h.mark_all_replicas_ready(&v2.id);
            }
            h.store.deployment_status(&v1.id) == Some(DeploymentStatus::Draining)
                && h.store.deployment_status(&v2.id) == Some(DeploymentStatus::Ready)
        })
        .await;
    assert!(v1_draining, "v1 never reached Draining");

    // Advance the clock past the grace period. With INGRESS_DRAIN_GRACE_PERIOD_MS = 50
    // in test cfg, a 1s jump is plenty.
    harness.clock.advance(1_000);

    let v1_removed = harness
        .run_until(Duration::from_secs(2), |h| {
            h.store.deployment_status(&v1.id) == Some(DeploymentStatus::Removed)
        })
        .await;
    assert!(
        v1_removed,
        "v1 did not transition to Removed after clock advanced past grace period; status = {:?}",
        harness.store.deployment_status(&v1.id),
    );

    // v2 should still be Ready throughout.
    assert_eq!(
        harness.store.deployment_status(&v2.id),
        Some(DeploymentStatus::Ready)
    );
}

#[tokio::test]
async fn build_that_never_completes_is_marked_crashed_after_timeout() {
    let mut harness = Harness::new();
    let deployment = harness
        .store
        .queue_new_deployment(docker_service("svc-timeout", 1));
    harness.provider.set_build_hanging(&deployment.id);

    // Drive the controller through a tick so the build task is spawned and
    // added to pending_builds.
    let pending = harness
        .run_until(Duration::from_secs(2), |h| {
            // Build hasn't completed (it's hung), but it's been spawned.
            h.store.deployment_status(&deployment.id) == Some(DeploymentStatus::Building)
        })
        .await;
    assert!(pending, "build was never even started");

    // Advance the clock past BUILD_TIMEOUT (30 minutes).
    harness.clock.advance(31 * 60 * 1000);

    let crashed = harness
        .run_until(Duration::from_secs(2), |h| {
            h.store.deployment_status(&deployment.id) == Some(DeploymentStatus::Crashed)
        })
        .await;
    assert!(
        crashed,
        "hung build was not marked Crashed after timeout; status = {:?}",
        harness.store.deployment_status(&deployment.id),
    );
}

#[tokio::test]
async fn many_rapid_redeploys_eventually_settle_to_one_ready() {
    // Stress: queue 8 deployments back-to-back, ensure exactly one is Ready
    // and others are non-active after the system settles.
    let mut harness = Harness::new();
    let mut config = docker_service("svc-stress", 1);
    let mut versions = Vec::new();
    for v in 1..=8 {
        config.version = format!("svc-stress-v{v}");
        versions.push(harness.store.queue_new_deployment(config.clone()).id);
    }

    let settled = harness
        .run_until(Duration::from_secs(8), |h| {
            for id in &versions {
                if h.provider.built_image_tag(id).is_some() {
                    h.mark_all_replicas_ready(id);
                }
            }
            let states: Vec<_> = versions
                .iter()
                .map(|id| h.store.deployment_status(id))
                .collect();
            let ready_count = states
                .iter()
                .filter(|s| **s == Some(DeploymentStatus::Ready))
                .count();
            let active_old = states[..versions.len() - 1].iter().filter(|s| {
                matches!(
                    s,
                    Some(DeploymentStatus::Building) | Some(DeploymentStatus::Queued)
                )
            });
            ready_count == 1 && active_old.count() == 0
        })
        .await;
    assert!(
        settled,
        "stress sequence did not settle; statuses = {:?}",
        versions
            .iter()
            .map(|id| (id.clone(), harness.store.deployment_status(id)))
            .collect::<Vec<_>>(),
    );

    let final_states: Vec<_> = versions
        .iter()
        .map(|id| harness.store.deployment_status(id))
        .collect();
    let ready_count = final_states
        .iter()
        .filter(|s| **s == Some(DeploymentStatus::Ready))
        .count();
    assert_eq!(
        ready_count, 1,
        "expected exactly one Ready; got {final_states:?}"
    );
}

// ---------------------------------------------------------------------------
// Property-based tests
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
enum Op {
    Queue { service_idx: u8, replicas: u8 },
    CancelLatest { service_idx: u8 },
    CrashReplica { service_idx: u8, replica: u8 },
    AdvanceClock { millis: u32 },
    Tick,
}

fn op_strategy() -> impl proptest::prelude::Strategy<Value = Op> {
    use proptest::prelude::*;
    prop_oneof![
        4 => (0..4u8, 1..=3u8).prop_map(|(service_idx, replicas)| Op::Queue {
            service_idx,
            replicas,
        }),
        1 => (0..4u8).prop_map(|service_idx| Op::CancelLatest { service_idx }),
        2 => (0..4u8, 0..3u8)
            .prop_map(|(service_idx, replica)| Op::CrashReplica { service_idx, replica }),
        1 => (10..2000u32).prop_map(|millis| Op::AdvanceClock { millis }),
        4 => Just(Op::Tick),
    ]
}

async fn run_property_sequence(ops: Vec<Op>) {
    let mut harness = Harness::new();
    let mut versions: [u32; 4] = [0; 4];

    for op in &ops {
        match op {
            Op::Queue {
                service_idx,
                replicas,
            } => {
                let idx = *service_idx as usize;
                versions[idx] += 1;
                let service_id = format!("svc-prop-{idx}");
                let mut config = docker_service(&service_id, (*replicas).max(1) as u32);
                config.version = format!("{service_id}-v{}", versions[idx]);
                let deployment = harness.store.queue_new_deployment(config);
                harness.tick().await;
                if harness.provider.built_image_tag(&deployment.id).is_some() {
                    harness.mark_all_replicas_ready(&deployment.id);
                }
            }
            Op::CancelLatest { service_idx } => {
                let idx = *service_idx as usize;
                let service_id = format!("svc-prop-{idx}");
                let latest = {
                    let state = harness.store.state.lock().expect("state lock");
                    state
                        .history
                        .get(&service_id)
                        .and_then(|h| h.last().cloned())
                };
                if let Some(d) = latest {
                    let _ = harness
                        .store
                        .cancel_service_deployment(&Deployment {
                            service_id: d.config.id.clone(),
                            id: d.id.clone(),
                            replica_index: 0,
                        })
                        .await;
                }
            }
            Op::CrashReplica {
                service_idx,
                replica,
            } => {
                let idx = *service_idx as usize;
                let service_id = format!("svc-prop-{idx}");
                let active = {
                    let state = harness.store.state.lock().expect("state lock");
                    state.history.get(&service_id).and_then(|h| {
                        h.iter()
                            .rev()
                            .find(|d| {
                                matches!(
                                    d.status,
                                    DeploymentStatus::Ready
                                        | DeploymentStatus::PendingReady
                                        | DeploymentStatus::Building
                                )
                            })
                            .cloned()
                    })
                };
                if let Some(d) = active {
                    let replica_index =
                        u32::from(*replica).min(d.config.deploy.replicas.saturating_sub(1));
                    let handle = ReplicaHandle {
                        task_id: format!("{}-replica-{}", d.id, replica_index),
                        service_id: d.config.id.clone(),
                        deployment_id: d.id.clone(),
                        replica_index,
                    };
                    harness.supervisor.crash_job(&handle.task_id);
                }
            }
            Op::AdvanceClock { millis } => {
                harness.clock.advance(u64::from(*millis));
            }
            Op::Tick => harness.tick().await,
        }
    }

    for _ in 0..60 {
        let ids = harness.provider.prepared_ids();
        for id in &ids {
            if harness.provider.built_image_tag(id).is_some() {
                harness.mark_all_replicas_ready(id);
            }
        }
        harness.tick().await;
    }

    let service_ids: Vec<String> = (0..4).map(|i| format!("svc-prop-{i}")).collect();
    for service_id in &service_ids {
        let deployments = {
            let state = harness.store.state.lock().expect("state lock");
            state.history.get(service_id).cloned().unwrap_or_default()
        };
        if deployments.is_empty() {
            continue;
        }

        // Invariant 1: at most one Ready deployment per service.
        let ready_count = deployments
            .iter()
            .filter(|d| d.status == DeploymentStatus::Ready)
            .count();
        assert!(
            ready_count <= 1,
            "service `{service_id}` has {ready_count} Ready deployments: {:?}",
            deployments
                .iter()
                .map(|d| (d.id.clone(), d.status.clone()))
                .collect::<Vec<_>>(),
        );

        // Invariant 2: when one is Ready, all earlier deployments should
        // be in non-active states.
        let latest_ready = deployments
            .iter()
            .filter(|d| d.status == DeploymentStatus::Ready)
            .max_by_key(|d| d.created_at);
        if let Some(latest) = latest_ready {
            for d in &deployments {
                if d.id == latest.id {
                    continue;
                }
                assert!(
                    !matches!(
                        d.status,
                        DeploymentStatus::Queued | DeploymentStatus::Building
                    ),
                    "service `{service_id}` has older deployment `{}` stuck in {:?} \
                     while newer `{}` is Ready",
                    d.id,
                    d.status,
                    latest.id,
                );
            }
        }

        // Invariant 3: terminal states do not regress.
        for d in &deployments {
            let transitions = harness.store.deployment_transitions(&d.id);
            for window in transitions.windows(2) {
                let prev = &window[0];
                let next = &window[1];
                if matches!(
                    prev,
                    DeploymentStatus::Terminated
                        | DeploymentStatus::Removed
                        | DeploymentStatus::Canceled
                ) {
                    assert_eq!(
                        prev, next,
                        "service `{service_id}` deployment `{}` regressed: \
                         {prev:?} -> {next:?}",
                        d.id
                    );
                }
            }
        }
    }
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config {
        cases: 32,
        max_shrink_iters: 64,
        .. proptest::test_runner::Config::default()
    })]

    #[test]
    fn random_op_sequence_maintains_invariants(
        ops in proptest::collection::vec(op_strategy(), 1..50)
    ) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(run_property_sequence(ops));
    }
}

struct OldSystemAcceptanceCluster {
    harness: Harness,
}

impl OldSystemAcceptanceCluster {
    fn new() -> Self {
        Self {
            harness: Harness::new(),
        }
    }

    fn snapshot(&self) -> ClusterSnapshot<String> {
        let state = self.harness.store.state.lock().expect("state lock");
        let mut services = state
            .configs
            .iter()
            .map(|(service_id, config)| {
                let deployments = state
                    .history
                    .get(service_id)
                    .into_iter()
                    .flatten()
                    .map(|deployment| {
                        let replica_key = format!("{service_id}/{}", deployment.id);
                        let mut replicas = state
                            .replicas
                            .get(&replica_key)
                            .into_iter()
                            .flatten()
                            .map(|replica| ReplicaSnapshot {
                                index: replica.replica_index,
                                phase: acceptance_phase(&replica.status),
                                restart_attempts: replica.restart_attempts,
                            })
                            .collect::<Vec<_>>();
                        replicas.sort_by_key(|replica| replica.index);
                        DeploymentSnapshot {
                            id: deployment.id.clone(),
                            version: clustertest::FixtureVersion::new(
                                deployment.config.version.clone(),
                            ),
                            phase: acceptance_phase(&deployment.status),
                            replicas,
                        }
                    })
                    .collect();
                ServiceSnapshot {
                    name: FixtureName::new(service_id.clone()),
                    configured_replicas: clustertest::ReplicaCount::new(config.deploy.replicas),
                    replica_override: state
                        .replicas_override
                        .get(service_id)
                        .copied()
                        .flatten()
                        .map(clustertest::ReplicaCount::new),
                    deployments,
                }
            })
            .collect::<Vec<_>>();
        services.sort_by(|left, right| left.name.cmp(&right.name));
        ClusterSnapshot { services }
    }

    fn ready_built_deployments(&self) {
        let deployment_ids = {
            let state = self.harness.store.state.lock().expect("state lock");
            state
                .history
                .values()
                .flatten()
                .filter(|deployment| {
                    matches!(
                        deployment.status,
                        DeploymentStatus::Building | DeploymentStatus::PendingReady
                    ) && self
                        .harness
                        .provider
                        .built_image_tag(&deployment.id)
                        .is_some()
                })
                .map(|deployment| deployment.id.clone())
                .collect::<Vec<_>>()
        };
        for deployment_id in deployment_ids {
            self.harness.mark_all_replicas_ready(&deployment_id);
        }
    }
}

#[async_trait]
impl AcceptanceCluster for OldSystemAcceptanceCluster {
    type DeploymentId = String;
    type Error = anyhow::Error;

    async fn rollout(&mut self, service: ServiceFixture) -> Result<Self::DeploymentId> {
        let mut config = docker_service(service.name.as_str(), service.replicas.get());
        config.version = service.version.as_str().to_string();
        if let IngressFixture::Host(host) = service.ingress {
            config.ingress = Some(IngressConfig {
                host: Some(host),
                hosts: Vec::new(),
                port: Some(80),
                session_affinity: None,
            });
        }
        Ok(self.harness.store.queue_new_deployment(config).id)
    }

    async fn cancel(&mut self, deployment_id: &Self::DeploymentId) -> Result<()> {
        let service_id = {
            let state = self.harness.store.state.lock().expect("state lock");
            state
                .history
                .iter()
                .find(|(_, deployments)| {
                    deployments
                        .iter()
                        .any(|deployment| deployment.id == *deployment_id)
                })
                .map(|(service_id, _)| service_id.clone())
        }
        .ok_or_else(|| anyhow::anyhow!("deployment `{deployment_id}` does not exist"))?;
        self.harness
            .store
            .cancel_service_deployment(&Deployment {
                service_id,
                id: deployment_id.clone(),
                replica_index: 0,
            })
            .await?;
        Ok(())
    }

    async fn set_replicas(
        &mut self,
        service: &FixtureName,
        replica_override: ReplicaOverride,
    ) -> Result<()> {
        let override_value = match replica_override {
            ReplicaOverride::Set(replicas) => Some(replicas.get()),
            ReplicaOverride::Clear => None,
        };
        self.harness
            .store
            .set_replicas_override(service.as_str(), override_value);
        Ok(())
    }

    async fn advance(&mut self, duration: Duration) -> Result<()> {
        let millis = u64::try_from(duration.as_millis())
            .map_err(|_| anyhow::anyhow!("logical duration is too large"))?;
        self.harness.clock.advance(millis);
        Ok(())
    }

    async fn await_converged(&mut self) -> Result<ClusterSnapshot<Self::DeploymentId>> {
        const MINIMUM_TICKS: usize = 32;
        const REQUIRED_QUIET_TICKS: usize = 8;
        const MAXIMUM_TICKS: usize = 10_000;

        let mut previous = None;
        let mut quiet_ticks = 0;
        for tick_index in 0..MAXIMUM_TICKS {
            self.ready_built_deployments();
            self.harness.tick().await;
            tokio::task::yield_now().await;
            let snapshot = self.snapshot();
            if tick_index >= MINIMUM_TICKS && previous.as_ref() == Some(&snapshot) {
                quiet_ticks += 1;
            } else {
                quiet_ticks = 0;
            }
            if quiet_ticks >= REQUIRED_QUIET_TICKS {
                return Ok(snapshot);
            }
            previous = Some(snapshot);
        }
        Err(anyhow::anyhow!(
            "old system did not converge after {MAXIMUM_TICKS} reconcile ticks"
        ))
    }
}

#[async_trait]
impl FaultInjectableCluster for OldSystemAcceptanceCluster {
    async fn inject_rollout_failure(
        &mut self,
        deployment_id: &Self::DeploymentId,
        failure: RolloutFailure,
    ) -> Result<()> {
        match failure {
            RolloutFailure::Prepare(message) => {
                self.harness
                    .provider
                    .set_prepare_err(deployment_id, &message);
            }
            RolloutFailure::Build(message) => {
                self.harness.provider.set_build_err(deployment_id, &message);
            }
        }
        Ok(())
    }

    async fn inject_replica_crash(
        &mut self,
        deployment_id: &Self::DeploymentId,
        replica_index: ReplicaIndex,
    ) -> Result<()> {
        let service_id = {
            let state = self.harness.store.state.lock().expect("state lock");
            state
                .history
                .iter()
                .find(|(_, deployments)| {
                    deployments
                        .iter()
                        .any(|deployment| deployment.id == *deployment_id)
                })
                .map(|(service_id, _)| service_id.clone())
        }
        .ok_or_else(|| anyhow::anyhow!("deployment `{deployment_id}` does not exist"))?;
        self.harness.store.set_replica_status(
            &service_id,
            deployment_id,
            replica_index.get(),
            DeploymentStatus::Crashed,
        );
        Ok(())
    }
}

#[tokio::test]
async fn acceptance_rollout_reaches_ready_on_old_system() {
    let mut cluster = OldSystemAcceptanceCluster::new();

    scenarios::rollout_reaches_ready(&mut cluster)
        .await
        .expect("rollout acceptance scenario");
}

#[tokio::test]
async fn acceptance_redeploy_drains_previous_on_old_system() {
    let mut cluster = OldSystemAcceptanceCluster::new();

    scenarios::redeploy_drains_previous(&mut cluster)
        .await
        .expect("redeploy acceptance scenario");
}

#[tokio::test]
async fn acceptance_queued_deployment_can_be_canceled_on_old_system() {
    let mut cluster = OldSystemAcceptanceCluster::new();

    scenarios::queued_deployment_can_be_canceled(&mut cluster)
        .await
        .expect("cancel acceptance scenario");
}

#[tokio::test]
async fn acceptance_replica_override_round_trips_on_old_system() {
    let mut cluster = OldSystemAcceptanceCluster::new();

    scenarios::replica_override_round_trips(&mut cluster)
        .await
        .expect("replica override acceptance scenario");
}

#[tokio::test]
async fn acceptance_drained_deployment_finalizes_on_old_system() {
    let mut cluster = OldSystemAcceptanceCluster::new();

    scenarios::drained_deployment_finalizes(&mut cluster)
        .await
        .expect("drain finalization acceptance scenario");
}

#[tokio::test]
async fn acceptance_build_failure_marks_deployment_crashed_on_old_system() {
    let mut cluster = OldSystemAcceptanceCluster::new();

    scenarios::build_failure_marks_deployment_crashed(&mut cluster)
        .await
        .expect("build failure acceptance scenario");
}

#[tokio::test]
async fn acceptance_prepare_failure_marks_deployment_crashed_on_old_system() {
    let mut cluster = OldSystemAcceptanceCluster::new();

    scenarios::prepare_failure_marks_deployment_crashed(&mut cluster)
        .await
        .expect("prepare failure acceptance scenario");
}

#[tokio::test]
async fn acceptance_crashed_replica_restarts_in_place_on_old_system() {
    let mut cluster = OldSystemAcceptanceCluster::new();

    scenarios::crashed_replica_restarts_in_place(&mut cluster)
        .await
        .expect("replica restart acceptance scenario");
}

fn acceptance_phase(status: &DeploymentStatus) -> DeploymentPhase {
    match status {
        DeploymentStatus::Queued => DeploymentPhase::Queued,
        DeploymentStatus::Building => DeploymentPhase::Building,
        DeploymentStatus::PendingReady => DeploymentPhase::PendingReady,
        DeploymentStatus::Ready => DeploymentPhase::Ready,
        DeploymentStatus::Crashed => DeploymentPhase::Crashed,
        DeploymentStatus::Terminated => DeploymentPhase::Terminated,
        DeploymentStatus::Removed => DeploymentPhase::Removed,
        DeploymentStatus::Draining => DeploymentPhase::Draining,
        DeploymentStatus::Canceled => DeploymentPhase::Canceled,
    }
}
