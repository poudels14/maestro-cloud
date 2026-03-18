use crate::deployment::controller::DeploymentController;
use crate::deployment::keys::{service_deployment_history_key, service_id_from_history_key};
use crate::deployment::provider::{
    DockerDeploymentProvider, ServiceCommandPlanner, ShellDeploymentProvider,
};
use crate::deployment::store::ClusterStore;
use crate::deployment::types::{
    Command, Deployment, DeploymentConfig, DeploymentStatus, IngressConfig, QueuedDeployment,
    ReplicaState, ServiceBuildConfig, ServiceConfig, ServiceDeployConfig, ServiceDeployment,
    ServiceInfo, ServiceProvider,
};
use crate::supervisor::controller::JobSupervisor;
use crate::utils::crypto::SecretString;
use anyhow::Result;
use async_trait::async_trait;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    sync::broadcast,
    time::{Instant, sleep},
};

fn deployment_with_source(
    build: Option<ServiceBuildConfig>,
    image: Option<&str>,
    deploy_command: Option<Command>,
) -> ServiceDeployment {
    ServiceDeployment {
        id: "A1B2C3D4E5".to_string(),
        created_at: 1,
        deployed_at: None,
        drained_at: None,
        status: DeploymentStatus::Queued,

        config: ServiceConfig {
            id: "svc-1".to_string(),
            name: "service-1".to_string(),
            version: "cfg-1".to_string(),
            provider: ServiceProvider::Docker,
            build,
            image: image.map(str::to_string),
            deploy: ServiceDeployConfig {
                flags: vec![],
                expose_ports: vec![],
                command: deploy_command,
                healthcheck_path: Some("/_healthy".to_string()),
                replicas: 1,
                max_restarts: None,
                env: Default::default(),
                secrets: None,
            },
            ingress: None,
        },
        git_commit: None,
        build: None,
    }
}

#[test]
fn command_planner_uses_image_for_deploy_when_present() {
    let deployment = deployment_with_source(
        None,
        Some("traefik/whoami"),
        Some(Command {
            command: "arc-deploy".to_string(),
            args: vec!["--prod".to_string()],
        }),
    );

    let planner = DockerDeploymentProvider {
        network: "test-net".to_string(),
        dns_domain: None,
        secrets_dir: std::env::temp_dir().join("maestro-test-secrets"),
    };
    let deploy = planner
        .deploy(&deployment, 0)
        .expect("deploy command should exist");
    assert_eq!(
        deploy.command,
        "exec docker run --rm --name svc-1-A1B2C3 --hostname svc-1-A1B2C3 --network test-net traefik/whoami"
    );
}

#[test]
fn command_planner_appends_deploy_flags_to_docker_run() {
    let mut deployment = deployment_with_source(None, Some("traefik/whoami"), None);
    deployment.id = "ABCDEF123456".to_string();
    deployment.config.deploy.expose_ports = vec![80, 443];
    deployment.config.deploy.flags = vec![
        "--network=host".to_string(),
        "--label".to_string(),
        "env=test".to_string(),
    ];

    let planner = DockerDeploymentProvider {
        network: "test-net".to_string(),
        dns_domain: None,
        secrets_dir: std::env::temp_dir().join("maestro-test-secrets"),
    };
    let deploy = planner
        .deploy(&deployment, 0)
        .expect("deploy command should exist");

    assert_eq!(
        deploy.command,
        "exec docker run --rm --name svc-1-ABCDEF --hostname svc-1-ABCDEF --network test-net -p 0:80 -p 0:443 traefik/whoami --network=host --label env=test",
    );
}

#[test]
fn command_planner_falls_back_to_explicit_deploy_command() {
    let deployment = deployment_with_source(
        Some(ServiceBuildConfig {
            repo: "https://example.com/repo.git".to_string(),
            dockerfile_path: "Dockerfile".to_string(),
        }),
        None,
        Some(Command {
            command: "arc deploy".to_string(),
            args: vec!["--service".to_string(), "svc-1".to_string()],
        }),
    );

    let planner = DockerDeploymentProvider {
        network: "test-net".to_string(),
        dns_domain: None,
        secrets_dir: std::env::temp_dir().join("maestro-test-secrets"),
    };
    let deploy = planner
        .deploy(&deployment, 0)
        .expect("deploy command should exist");
    assert_eq!(deploy.command, "arc deploy --service svc-1");
}

#[test]
fn shell_command_planner_uses_explicit_deploy_command() {
    let deployment = ServiceDeployment {
        id: "ZXCVBN1234".to_string(),
        created_at: 1,
        deployed_at: None,
        drained_at: None,
        status: DeploymentStatus::Queued,

        config: ServiceConfig {
            id: "svc-shell".to_string(),
            name: "service-shell".to_string(),
            version: "cfg-shell".to_string(),
            provider: ServiceProvider::Shell,
            build: None,
            image: None,
            deploy: ServiceDeployConfig {
                flags: vec![],
                expose_ports: vec![],
                command: Some(Command {
                    command: "echo".to_string(),
                    args: vec!["ok".to_string()],
                }),
                healthcheck_path: Some("/_healthy".to_string()),
                replicas: 1,
                max_restarts: None,
                env: Default::default(),
                secrets: None,
            },
            ingress: None,
        },
        git_commit: None,
        build: None,
    };

    let planner = ShellDeploymentProvider;
    let deploy = planner
        .deploy(&deployment, 0)
        .expect("deploy command should exist");
    assert_eq!(deploy.command, "echo ok");
}

#[derive(Default)]
struct InMemoryStore {
    state: Mutex<InMemoryStoreState>,
}

#[derive(Default)]
struct InMemoryStoreState {
    configs: HashMap<String, ServiceConfig>,
    history: HashMap<String, Vec<ServiceDeployment>>,
    transitions: HashMap<String, Vec<DeploymentStatus>>,
    replica_states: HashMap<String, Vec<ReplicaState>>,
    ingress_backends: HashMap<String, Vec<String>>,
}

fn sync_ingress(state: &mut InMemoryStoreState, service_id: &str) {
    let Some(deployments) = state.history.get(service_id) else {
        return;
    };
    let has_ingress = deployments.iter().rev().any(|d| {
        matches!(
            d.status,
            DeploymentStatus::Ready | DeploymentStatus::PendingReady | DeploymentStatus::Building
        ) && d.config.ingress.is_some()
    });
    if !has_ingress {
        state.ingress_backends.remove(service_id);
        return;
    }

    let mut backends = Vec::new();
    for deployment in deployments.iter().rev() {
        if !matches!(
            deployment.status,
            DeploymentStatus::Ready | DeploymentStatus::PendingReady | DeploymentStatus::Building
        ) {
            continue;
        }
        let key = format!("{service_id}/{}", deployment.id);
        if let Some(replicas) = state.replica_states.get(&key) {
            for replica in replicas {
                if replica.status == DeploymentStatus::Ready {
                    backends.push(deployment.hostname_for_replica(replica.replica_index));
                }
            }
        }
    }
    state
        .ingress_backends
        .insert(service_id.to_string(), backends);
}

impl InMemoryStore {
    fn seed_queued_deployments(&self, services: usize, deployments_per_service: usize) {
        self.seed_queued_deployments_with_command(
            services,
            deployments_per_service,
            Command {
                command: "true".to_string(),
                args: vec![],
            },
        );
    }

    fn seed_queued_deployments_with_command(
        &self,
        services: usize,
        deployments_per_service: usize,
        deploy_command: Command,
    ) {
        let mut state = self.state.lock().expect("state lock");
        let mut created_at = 1_u64;
        for svc_idx in 0..services {
            let service_id = format!("svc-{svc_idx}");
            let config = ServiceConfig {
                id: service_id.clone(),
                name: format!("service-{svc_idx}"),
                version: format!("cfg-{svc_idx}"),
                provider: ServiceProvider::Shell,
                build: None,
                image: None,
                deploy: ServiceDeployConfig {
                    flags: vec![],
                    expose_ports: vec![],
                    command: Some(deploy_command.clone()),
                    healthcheck_path: Some("/_healthy".to_string()),
                    replicas: 1,
                    max_restarts: None,
                    env: Default::default(),
                    secrets: None,
                },
                ingress: None,
            };
            state.configs.insert(service_id.clone(), config.clone());

            let mut deployments = Vec::with_capacity(deployments_per_service);
            for dep_idx in 0..deployments_per_service {
                let deployment = ServiceDeployment {
                    id: format!("{service_id}-{dep_idx}"),
                    created_at,
                    deployed_at: None,
                    drained_at: None,
                    status: DeploymentStatus::Queued,

                    config: config.clone(),
                    git_commit: None,
                    build: None,
                };
                created_at += 1;
                state
                    .transitions
                    .insert(deployment.id.clone(), vec![DeploymentStatus::Queued]);
                deployments.push(deployment);
            }
            state.history.insert(service_id, deployments);
        }
    }

    fn seed_service_with_deploy_commands(&self, service_id: &str, deploy_commands: Vec<Command>) {
        let mut state = self.state.lock().expect("state lock");
        let mut created_at = 1_u64;
        let mut deployments = Vec::with_capacity(deploy_commands.len());

        for (dep_idx, deploy_command) in deploy_commands.into_iter().enumerate() {
            let config = ServiceConfig {
                id: service_id.to_string(),
                name: format!("service-{service_id}"),
                version: format!("cfg-{service_id}-{dep_idx}"),
                provider: ServiceProvider::Shell,
                build: None,
                image: None,
                deploy: ServiceDeployConfig {
                    flags: vec![],
                    expose_ports: vec![],
                    command: Some(deploy_command),
                    healthcheck_path: Some("/_healthy".to_string()),
                    replicas: 1,
                    max_restarts: None,
                    env: Default::default(),
                    secrets: None,
                },
                ingress: None,
            };

            if dep_idx == 0 {
                state.configs.insert(service_id.to_string(), config.clone());
            }

            let deployment = ServiceDeployment {
                id: format!("{service_id}-{dep_idx}"),
                created_at,
                deployed_at: None,
                drained_at: None,
                status: DeploymentStatus::Queued,

                config,
                git_commit: None,
                build: None,
            };
            created_at += 1;
            state
                .transitions
                .insert(deployment.id.clone(), vec![DeploymentStatus::Queued]);
            deployments.push(deployment);
        }

        state.history.insert(service_id.to_string(), deployments);
    }

    fn queued_count(&self) -> usize {
        let state = self.state.lock().expect("state lock");
        state
            .history
            .values()
            .flat_map(|items| items.iter())
            .filter(|deployment| deployment.status == DeploymentStatus::Queued)
            .count()
    }

    fn all_deployments(&self) -> Vec<ServiceDeployment> {
        let state = self.state.lock().expect("state lock");
        state
            .history
            .values()
            .flat_map(|items| items.iter().cloned())
            .collect()
    }

    fn transition_history(&self) -> HashMap<String, Vec<DeploymentStatus>> {
        let state = self.state.lock().expect("state lock");
        state.transitions.clone()
    }

    fn seed_service_config(&self, service_id: &str, ingress: Option<IngressConfig>) {
        let mut state = self.state.lock().expect("state lock");
        let config = ServiceConfig {
            id: service_id.to_string(),
            name: format!("service-{service_id}"),
            version: "cfg-0".to_string(),
            provider: ServiceProvider::Shell,
            build: None,
            image: None,
            deploy: ServiceDeployConfig {
                flags: vec![],
                expose_ports: vec![],
                command: None,
                healthcheck_path: None,
                replicas: 1,
                max_restarts: None,
                env: Default::default(),
                secrets: None,
            },
            ingress,
        };
        state.configs.insert(service_id.to_string(), config);
    }

    fn add_queued_deployment(&self, service_id: &str, deploy_command: Command) -> String {
        let mut state = self.state.lock().expect("state lock");
        let mut config = state
            .configs
            .get(service_id)
            .cloned()
            .expect("service config should exist");
        let dep_idx = state.history.get(service_id).map(|d| d.len()).unwrap_or(0);
        config.version = format!("cfg-{service_id}-{dep_idx}");
        config.deploy.command = Some(deploy_command);

        let deployment = ServiceDeployment {
            id: format!("{service_id}-{dep_idx}"),
            created_at: dep_idx as u64 + 1,
            deployed_at: None,
            drained_at: None,
            status: DeploymentStatus::Queued,
            config,
            git_commit: None,
            build: None,
        };
        let id = deployment.id.clone();
        state
            .transitions
            .insert(id.clone(), vec![DeploymentStatus::Queued]);
        state
            .history
            .entry(service_id.to_string())
            .or_default()
            .push(deployment);
        id
    }

    fn ingress_backends(&self, service_id: &str) -> Vec<String> {
        let state = self.state.lock().expect("state lock");
        state
            .ingress_backends
            .get(service_id)
            .cloned()
            .unwrap_or_default()
    }
}

#[async_trait]
impl ClusterStore for InMemoryStore {
    async fn list_service_ids(&self) -> Result<Vec<String>> {
        let state = self.state.lock().expect("state lock");
        let mut ids = state.configs.keys().cloned().collect::<Vec<_>>();
        ids.sort();
        Ok(ids)
    }

    async fn list_queued_deployments(&self) -> Result<Vec<QueuedDeployment>> {
        let state = self.state.lock().expect("state lock");
        let mut queued = Vec::new();
        for (service_id, deployments) in &state.history {
            for (index, deployment) in deployments.iter().enumerate() {
                if deployment.status != DeploymentStatus::Queued {
                    continue;
                }
                queued.push(QueuedDeployment {
                    service_id: service_id.clone(),
                    key: service_deployment_history_key(service_id, index),
                    mod_revision: u64::try_from(index).unwrap_or(0) + 1,
                    deployment: deployment.clone(),
                });
            }
        }
        queued.sort_by(|a, b| {
            a.deployment
                .created_at
                .cmp(&b.deployment.created_at)
                .then_with(|| a.key.cmp(&b.key))
        });
        Ok(queued)
    }

    async fn claim_deployment_building(
        &self,
        queued_deployment: &QueuedDeployment,
    ) -> Result<bool> {
        let Some((service_id, index)) = parse_history_key(&queued_deployment.key) else {
            return Ok(false);
        };
        let mut state = self.state.lock().expect("state lock");
        let deployment_id = {
            let Some(deployments) = state.history.get_mut(&service_id) else {
                return Ok(false);
            };
            let Some(deployment) = deployments.get_mut(index) else {
                return Ok(false);
            };
            if deployment.id != queued_deployment.deployment.id {
                return Ok(false);
            }
            if deployment.status != DeploymentStatus::Queued {
                return Ok(false);
            }
            deployment.status = DeploymentStatus::Building;
            deployment.id.clone()
        };
        state
            .transitions
            .entry(deployment_id.clone())
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
        let updated_id = {
            let Some(deployments) = state.history.get_mut(&deployment.service_id) else {
                return Ok(());
            };
            let Some(d) = deployments.iter_mut().find(|item| item.id == deployment.id) else {
                return Ok(());
            };
            if !d.status.can_transition_to(&status) {
                return Ok(());
            }
            d.status = status.clone();
            if status == DeploymentStatus::Ready {
                d.deployed_at = Some(1);
            }
            d.id.clone()
        };
        state
            .transitions
            .entry(updated_id)
            .or_default()
            .push(status);
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
        let replicas = state.replica_states.entry(key).or_default();
        if let Some(existing) = replicas
            .iter_mut()
            .find(|r| r.replica_index == replica_index)
        {
            existing.status = status;
        } else {
            replicas.push(ReplicaState {
                replica_index,
                status,
            });
        }
        sync_ingress(&mut state, service_id);
        Ok(())
    }

    async fn list_replica_states(
        &self,
        service_id: &str,
        deployment_id: &str,
    ) -> Result<Vec<ReplicaState>> {
        let state = self.state.lock().expect("state lock");
        let key = format!("{service_id}/{deployment_id}");
        Ok(state.replica_states.get(&key).cloned().unwrap_or_default())
    }

    async fn delete_replica_state(
        &self,
        service_id: &str,
        deployment_id: &str,
        replica_index: u32,
    ) -> Result<()> {
        let mut state = self.state.lock().expect("state lock");
        let key = format!("{service_id}/{deployment_id}");
        if let Some(replicas) = state.replica_states.get_mut(&key) {
            replicas.retain(|r| r.replica_index != replica_index);
        }
        sync_ingress(&mut state, service_id);
        Ok(())
    }

    async fn list_service_deployments(&self, service_id: &str) -> Result<Vec<ServiceDeployment>> {
        let state = self.state.lock().expect("state lock");
        let mut deployments = state.history.get(service_id).cloned().unwrap_or_default();
        deployments.reverse();
        Ok(deployments)
    }

    async fn get_service_status(&self, service_id: &str) -> Result<Option<DeploymentStatus>> {
        let state = self.state.lock().expect("state lock");
        let status = state
            .history
            .get(service_id)
            .and_then(|deps| deps.last())
            .map(|d| d.status.clone());
        Ok(status)
    }

    async fn read_service_info(&self, service_id: &str) -> Result<Option<ServiceInfo>> {
        let state = self.state.lock().expect("state lock");
        Ok(state.configs.get(service_id).map(|config| ServiceInfo {
            config: config.clone(),
        }))
    }

    async fn read_service_deployment(
        &self,
        deployment: &Deployment,
    ) -> Result<Option<ServiceDeployment>> {
        let state = self.state.lock().expect("state lock");
        let found = state
            .history
            .get(&deployment.service_id)
            .and_then(|deployments| deployments.iter().find(|item| item.id == deployment.id))
            .cloned();
        Ok(found)
    }

    async fn stop_service_deployment(
        &self,
        deployment: &Deployment,
    ) -> Result<Option<ServiceDeployment>> {
        let mut state = self.state.lock().expect("state lock");
        let updated = {
            let Some(deployments) = state.history.get_mut(&deployment.service_id) else {
                return Ok(None);
            };
            let Some(d) = deployments.iter_mut().find(|item| item.id == deployment.id) else {
                return Ok(None);
            };

            match d.status {
                DeploymentStatus::Ready
                | DeploymentStatus::PendingReady
                | DeploymentStatus::Building => {}
                DeploymentStatus::Draining | DeploymentStatus::Removed => {
                    return Ok(Some(d.clone()));
                }
                _ => return Ok(Some(d.clone())),
            }

            d.status = DeploymentStatus::Draining;
            d.drained_at = Some(1);
            d.clone()
        };

        state
            .transitions
            .entry(updated.id.clone())
            .or_default()
            .push(DeploymentStatus::Draining);
        Ok(Some(updated))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stress_supervisor_updates_deployment_statuses() {
    let now_millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should be after epoch")
        .as_millis();
    let data_dir = std::env::temp_dir().join(format!("maestro-test-{now_millis}"));

    let store = Arc::new(InMemoryStore::default());
    store.seed_queued_deployments(12, 15);
    let (signal_tx, _) = broadcast::channel(4);
    let signal_rx = signal_tx.subscribe();

    let mut controller = DeploymentController::new(
        DeploymentConfig {
            data_dir: data_dir.clone(),
            etcd_port: 0,
            cluster_name: "test".to_string(),
            probe_port: None,
            web_port: 0,
            project_dir: data_dir.clone(),
            network: "test-net".to_string(),
            subnet: None,
            tailscale_authkey: None,
            secret_key: SecretString::new("test".to_string()),
            tags: Default::default(),
        },
        store.clone(),
        JobSupervisor::new(),
        signal_rx,
        None,
    );

    let deadline = Instant::now() + Duration::from_secs(10);
    while store.queued_count() > 0 || controller.has_running_services() {
        controller
            .reconcile_deployments()
            .await
            .expect("queue processing should succeed");
        sleep(Duration::from_millis(3)).await;
        controller.reap_finished_tasks().await;
        assert!(Instant::now() < deadline, "stress run timed out");
    }

    let deployments = store.all_deployments();
    assert_eq!(deployments.len(), 180);
    assert!(
        deployments
            .iter()
            .all(|deployment| deployment.status == DeploymentStatus::Ready)
    );

    for history in store.transition_history().values() {
        assert_eq!(
            history.as_slice(),
            &[
                DeploymentStatus::Queued,
                DeploymentStatus::Building,
                DeploymentStatus::Ready,
            ]
        );
    }

    let _ = std::fs::remove_dir_all(&data_dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn queued_deployment_starts_even_with_running_job_for_same_service() {
    let now_millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should be after epoch")
        .as_millis();
    let data_dir = std::env::temp_dir().join(format!("maestro-test-concurrent-{now_millis}"));

    let store = Arc::new(InMemoryStore::default());
    store.seed_service_with_deploy_commands(
        "svc-0",
        vec![
            Command {
                command: "sleep".to_string(),
                args: vec!["3".to_string()],
            },
            Command {
                command: "true".to_string(),
                args: vec![],
            },
        ],
    );

    let (signal_tx, _) = broadcast::channel(4);
    let signal_rx = signal_tx.subscribe();

    let mut controller = DeploymentController::new(
        DeploymentConfig {
            data_dir: data_dir.clone(),
            etcd_port: 0,
            cluster_name: "test".to_string(),
            probe_port: None,
            web_port: 0,
            project_dir: data_dir.clone(),
            network: "test-net".to_string(),
            subnet: None,
            tailscale_authkey: None,
            secret_key: SecretString::new("test".to_string()),
            tags: Default::default(),
        },
        store.clone(),
        JobSupervisor::new(),
        signal_rx,
        None,
    );

    let start_deadline = Instant::now() + Duration::from_secs(2);
    let mut second_ready = false;
    while Instant::now() < start_deadline {
        controller
            .reconcile_deployments()
            .await
            .expect("queue processing should succeed");
        sleep(Duration::from_millis(25)).await;
        controller.reap_finished_tasks().await;

        let deployments = store.all_deployments();
        let first = deployments
            .iter()
            .find(|deployment| deployment.id == "svc-0-0")
            .expect("first deployment should exist");
        let second = deployments
            .iter()
            .find(|deployment| deployment.id == "svc-0-1")
            .expect("second deployment should exist");

        if second.status == DeploymentStatus::Ready {
            assert_ne!(first.status, DeploymentStatus::Terminated);
            second_ready = true;
            break;
        }
    }
    assert!(
        second_ready,
        "queued deployment was not started while another deployment for the service was still running"
    );

    let drain_deadline = Instant::now() + Duration::from_secs(5);
    while controller.has_running_services() {
        sleep(Duration::from_millis(25)).await;
        controller.reap_finished_tasks().await;
        assert!(
            Instant::now() < drain_deadline,
            "running jobs did not drain in time"
        );
    }

    let transitions = store.transition_history();
    assert_eq!(
        transitions.get("svc-0-0").map(Vec::as_slice),
        Some(
            &[
                DeploymentStatus::Queued,
                DeploymentStatus::Building,
                DeploymentStatus::Ready,
            ][..]
        )
    );
    assert_eq!(
        transitions.get("svc-0-1").map(Vec::as_slice),
        Some(
            &[
                DeploymentStatus::Queued,
                DeploymentStatus::Building,
                DeploymentStatus::Ready,
            ][..]
        )
    );

    let _ = std::fs::remove_dir_all(&data_dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stop_requested_active_deployment_is_marked_removed() {
    let now_millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should be after epoch")
        .as_millis();
    let data_dir = std::env::temp_dir().join(format!("maestro-test-stop-{now_millis}"));

    let store = Arc::new(InMemoryStore::default());
    store.seed_service_with_deploy_commands(
        "svc-0",
        vec![Command {
            command: "sleep".to_string(),
            args: vec!["3".to_string()],
        }],
    );

    let (signal_tx, _) = broadcast::channel(4);
    let signal_rx = signal_tx.subscribe();

    let mut controller = DeploymentController::new(
        DeploymentConfig {
            data_dir: data_dir.clone(),
            etcd_port: 0,
            cluster_name: "test".to_string(),
            probe_port: None,
            web_port: 0,
            project_dir: data_dir.clone(),
            network: "test-net".to_string(),
            subnet: None,
            tailscale_authkey: None,
            secret_key: SecretString::new("test".to_string()),
            tags: Default::default(),
        },
        store.clone(),
        JobSupervisor::new(),
        signal_rx,
        None,
    );

    let ready_deadline = Instant::now() + Duration::from_secs(2);
    loop {
        controller
            .reconcile_deployments()
            .await
            .expect("queue processing should succeed");
        sleep(Duration::from_millis(25)).await;
        controller.reap_finished_tasks().await;

        let deployment = store
            .all_deployments()
            .into_iter()
            .find(|item| item.id == "svc-0-0")
            .expect("deployment should exist");
        if deployment.status == DeploymentStatus::Ready {
            break;
        }

        assert!(
            Instant::now() < ready_deadline,
            "deployment did not reach ready in time"
        );
    }

    let outcome = store
        .stop_service_deployment(&Deployment {
            service_id: "svc-0".to_string(),
            id: "svc-0-0".to_string(),
            replica_index: 0,
        })
        .await
        .expect("stop request should succeed");
    assert!(matches!(outcome, Some(_)));

    let removed_deadline = Instant::now() + Duration::from_secs(22);
    loop {
        controller
            .reconcile_deployments()
            .await
            .expect("queue processing should succeed");
        sleep(Duration::from_millis(25)).await;
        controller.reap_finished_tasks().await;

        let deployment = store
            .all_deployments()
            .into_iter()
            .find(|item| item.id == "svc-0-0")
            .expect("deployment should exist");
        if deployment.status == DeploymentStatus::Removed && !controller.has_running_services() {
            break;
        }

        assert!(
            Instant::now() < removed_deadline,
            "deployment stop request was not applied in time"
        );
    }

    assert_eq!(
        store.transition_history().get("svc-0-0").map(Vec::as_slice),
        Some(
            &[
                DeploymentStatus::Queued,
                DeploymentStatus::Building,
                DeploymentStatus::Ready,
                DeploymentStatus::Draining,
                DeploymentStatus::Removed,
            ][..]
        )
    );

    let _ = std::fs::remove_dir_all(&data_dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn continuous_redeploy_maintains_ingress_backends() {
    let now_millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should be after epoch")
        .as_millis();
    let data_dir = std::env::temp_dir().join(format!("maestro-test-redeploy-{now_millis}"));

    let store = Arc::new(InMemoryStore::default());
    let service_id = "svc-ingress";
    store.seed_service_config(
        service_id,
        Some(IngressConfig {
            host: "test.local".to_string(),
            port: Some(80),
        }),
    );

    let deploy_cmd = Command {
        command: "sleep".to_string(),
        args: vec!["30".to_string()],
    };

    let (signal_tx, _) = broadcast::channel(4);
    let signal_rx = signal_tx.subscribe();

    let mut controller = DeploymentController::new(
        DeploymentConfig {
            data_dir: data_dir.clone(),
            etcd_port: 0,
            cluster_name: "test".to_string(),
            probe_port: None,
            web_port: 0,
            project_dir: data_dir.clone(),
            network: "test-net".to_string(),
            subnet: None,
            tailscale_authkey: None,
            secret_key: SecretString::new("test".to_string()),
            tags: Default::default(),
        },
        store.clone(),
        JobSupervisor::new(),
        signal_rx,
        None,
    );

    let mut ingress_was_empty_after_first_ready = false;
    let mut first_deployment_ready = false;
    let mut deployment_ids = Vec::new();

    for deploy_round in 0..4 {
        let dep_id = store.add_queued_deployment(service_id, deploy_cmd.clone());
        deployment_ids.push(dep_id.clone());

        let ready_deadline = Instant::now() + Duration::from_secs(5);
        loop {
            controller
                .reconcile_deployments()
                .await
                .expect("reconcile should succeed");
            sleep(Duration::from_millis(10)).await;
            controller.reap_finished_tasks().await;

            if first_deployment_ready {
                let backends = store.ingress_backends(service_id);
                if backends.is_empty() {
                    ingress_was_empty_after_first_ready = true;
                }
            }

            let deployments = store.all_deployments();
            let current = deployments.iter().find(|d| d.id == dep_id);
            if current.is_some_and(|d| d.status == DeploymentStatus::Ready) {
                first_deployment_ready = true;
                break;
            }

            assert!(
                Instant::now() < ready_deadline,
                "deployment {dep_id} (round {deploy_round}) did not reach Ready in time"
            );
        }

        for _ in 0..10 {
            controller
                .reconcile_deployments()
                .await
                .expect("reconcile should succeed");
            sleep(Duration::from_millis(10)).await;
            controller.reap_finished_tasks().await;

            let backends = store.ingress_backends(service_id);
            if backends.is_empty() {
                ingress_was_empty_after_first_ready = true;
            }
        }
    }

    assert!(
        !ingress_was_empty_after_first_ready,
        "ingress backends were empty after the first deployment was ready (downtime detected)"
    );

    let deployments = store.all_deployments();
    let service_deployments: Vec<_> = deployments
        .iter()
        .filter(|d| d.config.id == service_id)
        .collect();
    let ready_count = service_deployments
        .iter()
        .filter(|d| d.status == DeploymentStatus::Ready)
        .count();
    assert_eq!(ready_count, 1, "exactly one deployment should be Ready");

    let last_dep_id = deployment_ids.last().unwrap();
    let last_dep = service_deployments
        .iter()
        .find(|d| d.id == *last_dep_id)
        .expect("last deployment should exist");
    assert_eq!(
        last_dep.status,
        DeploymentStatus::Ready,
        "the latest deployment should be the one that is Ready"
    );

    let backends = store.ingress_backends(service_id);
    assert_eq!(backends.len(), 1, "ingress should have exactly one backend");
    let expected_hostname = last_dep.hostname_for_replica(0);
    assert_eq!(
        backends[0], expected_hostname,
        "ingress should point to the latest deployment's container"
    );

    let _ = std::fs::remove_dir_all(&data_dir);
}

fn parse_history_key(key: &str) -> Option<(String, usize)> {
    let service_id = service_id_from_history_key(key)?;
    let index = key.rsplit('/').next()?.parse::<usize>().ok()?;
    Some((service_id, index))
}
