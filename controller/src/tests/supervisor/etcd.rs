use super::*;
use crate::service::{
    ArcCommand, ServiceBuildConfig, ServiceConfig, ServiceDeployConfig, ServiceDeployment,
};
use std::sync::Mutex;
use tokio::time::Instant;

fn deployment_with_source(
    build: Option<ServiceBuildConfig>,
    image: Option<&str>,
    deploy_command: Option<ArcCommand>,
) -> ServiceDeployment {
    ServiceDeployment {
        id: "A1B2C3D4E5".to_string(),
        created_at: 1,
        deployed_at: None,
        status: DeploymentStatus::Queued,
        config: ServiceConfig {
            id: "svc-1".to_string(),
            name: "service-1".to_string(),
            version: "cfg-1".to_string(),
            build,
            image: image.map(str::to_string),
            deploy: ServiceDeployConfig {
                flags: vec![],
                ports: vec![],
                command: deploy_command,
                healthcheck_path: "/_healthy".to_string(),
            },
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
        Some(ArcCommand {
            command: "arc-deploy".to_string(),
            args: vec!["--prod".to_string()],
        }),
    );

    let planner = DefaultServiceCommandPlanner;
    let deploy = planner
        .deploy_command(&deployment)
        .expect("deploy command should exist");
    assert_eq!(
        deploy,
        "exec docker run --rm --name svc-1-A1B2C3 traefik/whoami"
    );
}

#[test]
fn command_planner_appends_deploy_flags_to_docker_run() {
    let mut deployment = deployment_with_source(None, Some("traefik/whoami"), None);
    deployment.id = "ABCDEF123456".to_string();
    deployment.config.deploy.ports = vec!["8080:80".to_string(), "8443:443".to_string()];
    deployment.config.deploy.flags = vec![
        "--network=host".to_string(),
        "--label".to_string(),
        "env=test".to_string(),
    ];

    let planner = DefaultServiceCommandPlanner;
    let deploy = planner
        .deploy_command(&deployment)
        .expect("deploy command should exist");

    assert_eq!(
        deploy,
        "exec docker run --rm --name svc-1-ABCDEF -p 8080:80 -p 8443:443 traefik/whoami --network=host --label env=test",
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
        Some(ArcCommand {
            command: "arc deploy".to_string(),
            args: vec!["--service".to_string(), "svc-1".to_string()],
        }),
    );

    let planner = DefaultServiceCommandPlanner;
    let deploy = planner
        .deploy_command(&deployment)
        .expect("deploy command should exist");
    assert_eq!(deploy, "arc deploy --service svc-1");
}

#[derive(Default)]
struct InMemoryStore {
    state: Mutex<InMemoryStoreState>,
}

#[derive(Default)]
struct InMemoryStoreState {
    configs: HashMap<String, ServiceConfig>,
    history: HashMap<String, Vec<ServiceDeployment>>,
    active: HashMap<String, ActiveDeployment>,
    transitions: HashMap<String, Vec<DeploymentStatus>>,
}

impl InMemoryStore {
    fn seed_queued_deployments(&self, services: usize, deployments_per_service: usize) {
        let mut state = self.state.lock().expect("state lock");
        let mut created_at = 1_u64;
        for svc_idx in 0..services {
            let service_id = format!("svc-{svc_idx}");
            let config = ServiceConfig {
                id: service_id.clone(),
                name: format!("service-{svc_idx}"),
                version: format!("cfg-{svc_idx}"),
                build: None,
                image: Some("traefik/whoami".to_string()),
                deploy: ServiceDeployConfig {
                    flags: vec![],
                    ports: vec![],
                    command: None,
                    healthcheck_path: "/_healthy".to_string(),
                },
            };
            state.configs.insert(service_id.clone(), config.clone());

            let mut deployments = Vec::with_capacity(deployments_per_service);
            for dep_idx in 0..deployments_per_service {
                let deployment = ServiceDeployment {
                    id: format!("{service_id}-{dep_idx}"),
                    created_at,
                    deployed_at: None,
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
}

#[async_trait]
impl DeploymentStore for InMemoryStore {
    async fn list_service_ids(&self) -> Result<Vec<String>, String> {
        let state = self.state.lock().expect("state lock");
        let mut ids = state.configs.keys().cloned().collect::<Vec<_>>();
        ids.sort();
        Ok(ids)
    }

    async fn queue_terminated_deployment(&self, _service_id: &str) -> Result<bool, String> {
        Ok(false)
    }

    async fn list_queued_deployments(&self) -> Result<Vec<QueuedDeployment>, String> {
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
    ) -> Result<bool, String> {
        let Some((service_id, index)) = parse_history_key(&queued_deployment.key) else {
            return Ok(false);
        };
        let mut state = self.state.lock().expect("state lock");
        let (deployment_id, deployment_version) = {
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
            (deployment.id.clone(), deployment.config.version.clone())
        };
        state
            .transitions
            .entry(deployment_id.clone())
            .or_default()
            .push(DeploymentStatus::Building);
        state.active.insert(
            service_id,
            ActiveDeployment {
                deployment_id,
                version: Some(deployment_version),
            },
        );
        Ok(true)
    }

    async fn mark_deployment_ready(
        &self,
        _service_id: &str,
        deployment_key: &str,
        deployment_id: &str,
    ) -> Result<(), String> {
        let Some((service_id, index)) = parse_history_key(deployment_key) else {
            return Ok(());
        };
        let mut state = self.state.lock().expect("state lock");
        let updated_id = {
            let Some(deployments) = state.history.get_mut(&service_id) else {
                return Ok(());
            };
            let Some(deployment) = deployments.get_mut(index) else {
                return Ok(());
            };
            if deployment.id != deployment_id {
                return Ok(());
            }
            if deployment.status != DeploymentStatus::Building {
                return Ok(());
            }
            deployment.status = DeploymentStatus::Ready;
            deployment.deployed_at = Some(1);
            deployment.id.clone()
        };
        state
            .transitions
            .entry(updated_id)
            .or_default()
            .push(DeploymentStatus::Ready);
        Ok(())
    }

    async fn mark_deployment_crashed(
        &self,
        service_id: &str,
        deployment_id: &str,
    ) -> Result<(), String> {
        let mut state = self.state.lock().expect("state lock");
        let updated_id = {
            let Some(deployments) = state.history.get_mut(service_id) else {
                return Ok(());
            };
            let Some(deployment) = deployments.iter_mut().find(|item| item.id == deployment_id)
            else {
                return Ok(());
            };
            if deployment.status == DeploymentStatus::Crashed {
                return Ok(());
            }
            deployment.status = DeploymentStatus::Crashed;
            deployment.id.clone()
        };
        state
            .transitions
            .entry(updated_id)
            .or_default()
            .push(DeploymentStatus::Crashed);
        Ok(())
    }

    async fn mark_deployment_terminated(
        &self,
        service_id: &str,
        deployment_id: &str,
    ) -> Result<(), String> {
        let mut state = self.state.lock().expect("state lock");
        let updated_id = {
            let Some(deployments) = state.history.get_mut(service_id) else {
                return Ok(());
            };
            let Some(deployment) = deployments.iter_mut().find(|item| item.id == deployment_id)
            else {
                return Ok(());
            };
            if deployment.status == DeploymentStatus::Terminated {
                return Ok(());
            }
            deployment.status = DeploymentStatus::Terminated;
            deployment.id.clone()
        };
        state
            .transitions
            .entry(updated_id)
            .or_default()
            .push(DeploymentStatus::Terminated);
        Ok(())
    }
}

#[derive(Clone, Copy)]
struct DummyWorkerRunner {
    delay: Duration,
    outcome: WorkerExitStatus,
}

impl WorkerRunner for DummyWorkerRunner {
    fn spawn(
        &self,
        _service_name: String,
        _config: ServiceRuntimeConfig,
        _shutdown_rx: watch::Receiver<bool>,
    ) -> JoinHandle<WorkerExitStatus> {
        let delay = self.delay;
        let outcome = self.outcome;
        tokio::spawn(async move {
            sleep(delay).await;
            outcome
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stress_supervisor_updates_deployment_statuses() {
    let store = Arc::new(InMemoryStore::default());
    store.seed_queued_deployments(12, 15);

    let mut supervisor = QueueSupervisor::with_parts(
        store.clone(),
        Arc::new(DefaultServiceCommandPlanner),
        Arc::new(DummyWorkerRunner {
            delay: Duration::from_millis(2),
            outcome: WorkerExitStatus::Crashed,
        }),
    );

    let deadline = Instant::now() + Duration::from_secs(10);
    while store.queued_count() > 0 || !supervisor.running.is_empty() {
        supervisor
            .process_queued_deployments()
            .await
            .expect("queue processing should succeed");
        sleep(Duration::from_millis(3)).await;
        supervisor.reap_finished_tasks().await;
        assert!(Instant::now() < deadline, "stress run timed out");
    }

    let deployments = store.all_deployments();
    assert_eq!(deployments.len(), 180);
    assert!(
        deployments
            .iter()
            .all(|deployment| deployment.status == DeploymentStatus::Crashed)
    );

    for history in store.transition_history().values() {
        assert_eq!(
            history.as_slice(),
            &[
                DeploymentStatus::Queued,
                DeploymentStatus::Building,
                DeploymentStatus::Ready,
                DeploymentStatus::Crashed,
            ]
        );
    }
}

fn parse_history_key(key: &str) -> Option<(String, usize)> {
    let service_id = service_id_from_history_key(key)?;
    let index = key.rsplit('/').next()?.parse::<usize>().ok()?;
    Some((service_id, index))
}
