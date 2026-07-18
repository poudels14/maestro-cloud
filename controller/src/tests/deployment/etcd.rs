use super::{is_missing_election_leader_message, is_terminal_replica_failure};
use crate::deployment::controller::DeploymentController;
use crate::deployment::keys::{service_deployment_history_key, service_id_from_history_key};
use crate::deployment::provider::{ContainerDeploymentProvider, ReplicaRuntimeIdentity};
use crate::deployment::store::ClusterStore;
use crate::deployment::types::{
    Command, ControllerConfig, Deployment, DeploymentBuildInfo, DeploymentStatus, QueuedDeployment,
    ReplicaState, SecretsConfig, ServiceBuildConfig, ServiceConfig, ServiceDeployConfig,
    ServiceDeployment, ServiceInfo,
};
use crate::runtime::{self, BuildSpec, RunSpec, RuntimeProvider};
use crate::supervisor::{JobCommand, controller::JobSupervisor};
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

#[test]
fn missing_election_leader_is_an_empty_optional_value() {
    assert!(is_missing_election_leader_message("election: no leader"));
}

#[test]
fn other_etcd_errors_are_not_hidden_as_a_missing_leader() {
    assert!(!is_missing_election_leader_message("etcd unavailable"));
}

#[test]
fn only_exhausted_crashes_are_retained_as_terminal_replica_failures() {
    let mut state = ReplicaState {
        service_id: Some("svc".to_string()),
        deployment_id: Some("deployment".to_string()),
        replica_index: 0,
        status: DeploymentStatus::Crashed,
        healthcheck_failures: 0,
        restart_attempts: crate::health::MAX_REPLICA_RESTART_ATTEMPTS - 1,
        node_id: Some("node-a".to_string()),
        assignment_id: Some("assignment-a".to_string()),
        endpoint: None,
        error: Some("image pull failed".to_string()),
    };

    assert!(!is_terminal_replica_failure(&state));
    state.restart_attempts = crate::health::MAX_REPLICA_RESTART_ATTEMPTS;
    assert!(is_terminal_replica_failure(&state));
    state.status = DeploymentStatus::Ready;
    assert!(!is_terminal_replica_failure(&state));
}

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
            build,
            image: image.map(str::to_string),
            deploy: ServiceDeployConfig {
                flags: vec![],
                expose_ports: vec![],
                command: deploy_command,
                healthcheck_path: Some("/_healthy".to_string()),
                replicas: 1,
                exec: true,
                max_restarts: None,
                env: Default::default(),
                secrets: None,
                volumes: vec![],
                node_affinity: None,
                egress: Default::default(),
                healthcheck_interval: 60,
            },
            ingress: None,
            preview: None,
            preview_source: None,
        },
        git_commit: None,
        build: None,
        upload_archive: None,
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

    let planner = ContainerDeploymentProvider {
        runtime: runtime::create_provider(crate::config::RuntimeType::Docker),
        build_command_env: Default::default(),
        network: "test-net".to_string(),
        dns_domain: None,
        dns_server: None,
        secrets_dir: std::env::temp_dir().join("maestro-test-secrets"),
        uploads_dir: std::env::temp_dir().join("maestro-test-uploads"),
    };
    let deploy = planner
        .deploy(&deployment, 0)
        .expect("deploy command should exist");
    assert_eq!(
        deploy.command,
        crate::supervisor::JobCommand::Exec {
            program: "docker".to_string(),
            args: vec![
                "run",
                "--rm",
                "--name",
                "svc-1-A1B2C3",
                "--hostname",
                "svc-1-A1B2C3",
                "--network",
                "test-net",
                "traefik/whoami",
            ]
            .into_iter()
            .map(String::from)
            .collect(),
        }
    );
}

#[test]
fn clustered_deploy_command_uses_the_reserved_assignment_address() {
    let deployment = deployment_with_source(None, Some("traefik/whoami"), None);
    let planner = ContainerDeploymentProvider {
        runtime: runtime::create_provider(crate::config::RuntimeType::Nerdctl),
        build_command_env: Default::default(),
        network: "test-net".to_string(),
        dns_domain: None,
        dns_server: None,
        secrets_dir: std::env::temp_dir().join("maestro-test-secrets"),
        uploads_dir: std::env::temp_dir().join("maestro-test-uploads"),
    };
    let identity = ReplicaRuntimeIdentity {
        node_id: "node-a".to_string(),
        service_id: "svc-1".to_string(),
        deployment_id: deployment.id.clone(),
        replica_index: 0,
        assignment_id: "assignment-a".to_string(),
        container_ip: "172.22.1.2".parse().unwrap(),
        runtime_suffix: Some("node-3001".to_string()),
    };

    let deploy = planner
        .deploy_with_identity(&deployment, 0, Some(&identity))
        .expect("clustered deploy command");
    let JobCommand::Exec { args, .. } = deploy.command else {
        panic!("container deployment must use an exec command");
    };
    assert!(args.windows(2).any(|pair| pair == ["--ip", "172.22.1.2"]));
}

#[test]
fn command_planner_disables_pull_for_prepared_images() {
    let mut deployment = deployment_with_source(None, Some("traefik/whoami"), None);
    deployment.build = Some(DeploymentBuildInfo {
        docker_image_id: "traefik/whoami".to_string(),
    });

    let planner = ContainerDeploymentProvider {
        runtime: runtime::create_provider(crate::config::RuntimeType::Docker),
        build_command_env: Default::default(),
        network: "test-net".to_string(),
        dns_domain: None,
        dns_server: None,
        secrets_dir: std::env::temp_dir().join("maestro-test-secrets"),
        uploads_dir: std::env::temp_dir().join("maestro-test-uploads"),
    };
    let deploy = planner
        .deploy(&deployment, 0)
        .expect("deploy command should exist");

    let args = match deploy.command {
        crate::supervisor::JobCommand::Exec { args, .. } => args,
        _ => panic!("expected exec command"),
    };
    let pull_index = args
        .iter()
        .position(|arg| arg == "--pull=never")
        .expect("pull should be disabled");
    let image_index = args
        .iter()
        .position(|arg| arg == "traefik/whoami")
        .expect("image should be present");
    assert!(pull_index < image_index);
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

    let planner = ContainerDeploymentProvider {
        runtime: runtime::create_provider(crate::config::RuntimeType::Docker),
        build_command_env: Default::default(),
        network: "test-net".to_string(),
        dns_domain: None,
        dns_server: None,
        secrets_dir: std::env::temp_dir().join("maestro-test-secrets"),
        uploads_dir: std::env::temp_dir().join("maestro-test-uploads"),
    };
    let deploy = planner
        .deploy(&deployment, 0)
        .expect("deploy command should exist");

    assert_eq!(
        deploy.command,
        crate::supervisor::JobCommand::Exec {
            program: "docker".to_string(),
            args: vec![
                "run",
                "--rm",
                "--name",
                "svc-1-ABCDEF",
                "--hostname",
                "svc-1-ABCDEF",
                "--network",
                "test-net",
                "-p",
                "0:80",
                "-p",
                "0:443",
                "traefik/whoami",
                "--network=host",
                "--label",
                "env=test",
            ]
            .into_iter()
            .map(String::from)
            .collect(),
        }
    );
}

#[test]
fn secrets_mount_content_quotes_values() {
    let mut deployment = deployment_with_source(None, Some("my-app:latest"), None);
    deployment.config.deploy.secrets = Some(SecretsConfig {
        mount_path: "/app/.env".to_string(),
        source: None,
        items: HashMap::from([
            ("SIMPLE".to_string(), "hello".to_string()),
            ("WITH_QUOTES".to_string(), "say \"hi\"".to_string()),
            ("MULTILINE".to_string(), "line1\nline2".to_string()),
            ("WITH_BACKSLASH".to_string(), "path\\to\\file".to_string()),
        ]),
        keys: HashMap::new(),
    });

    let planner = ContainerDeploymentProvider {
        runtime: runtime::create_provider(crate::config::RuntimeType::Docker),
        build_command_env: Default::default(),
        network: "test-net".to_string(),
        dns_domain: None,
        dns_server: None,
        secrets_dir: std::env::temp_dir().join("maestro-test-secrets-quote"),
        uploads_dir: std::env::temp_dir().join("maestro-test-uploads"),
    };
    let deploy = planner
        .deploy(&deployment, 0)
        .expect("should produce deploy output");
    let content = deploy
        .secrets_mount
        .expect("should have secrets mount")
        .content;

    let parsed: HashMap<String, String> = dotenvy::from_read_iter(content.as_bytes())
        .filter_map(|item| item.ok())
        .collect();

    assert_eq!(parsed.get("SIMPLE").unwrap(), "hello");
    assert_eq!(parsed.get("WITH_QUOTES").unwrap(), "say \"hi\"");
    assert_eq!(parsed.get("MULTILINE").unwrap(), "line1\nline2");
    assert_eq!(parsed.get("WITH_BACKSLASH").unwrap(), "path\\to\\file");
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
    fn all_deployments(&self) -> Vec<ServiceDeployment> {
        let state = self.state.lock().expect("state lock");
        state
            .history
            .values()
            .flat_map(|items| items.iter().cloned())
            .collect()
    }

    fn seed_active_deployment(
        &self,
        service_id: &str,
        deployment_id: &str,
        status: DeploymentStatus,
        healthcheck_path: Option<&str>,
        replicas: u32,
    ) -> ServiceDeployment {
        let mut state = self.state.lock().expect("state lock");
        let config = ServiceConfig {
            id: service_id.to_string(),
            name: format!("service-{service_id}"),
            version: "cfg-0".to_string(),
            build: None,
            image: None,
            deploy: ServiceDeployConfig {
                flags: vec![],
                expose_ports: vec![],
                command: Some(Command {
                    command: "sleep".to_string(),
                    args: vec!["30".to_string()],
                }),
                healthcheck_path: healthcheck_path.map(str::to_string),
                replicas,
                exec: true,
                max_restarts: None,
                env: Default::default(),
                secrets: None,
                volumes: vec![],
                node_affinity: None,
                egress: Default::default(),
                healthcheck_interval: 60,
            },
            ingress: None,
            preview: None,
            preview_source: None,
        };
        state.configs.insert(service_id.to_string(), config.clone());

        let deployment = ServiceDeployment {
            id: deployment_id.to_string(),
            created_at: 1,
            deployed_at: None,
            drained_at: None,
            status: status.clone(),
            config,
            git_commit: None,
            build: None,
            upload_archive: None,
        };
        state
            .transitions
            .insert(deployment.id.clone(), vec![status]);
        state
            .history
            .insert(service_id.to_string(), vec![deployment.clone()]);
        deployment
    }

    fn seed_queued_docker_deployment(
        &self,
        service_id: &str,
        deployment_id: &str,
        healthcheck_path: Option<&str>,
    ) -> ServiceDeployment {
        let mut state = self.state.lock().expect("state lock");
        let config = ServiceConfig {
            id: service_id.to_string(),
            name: format!("service-{service_id}"),
            version: "cfg-0".to_string(),
            build: None,
            image: Some("example/image:latest".to_string()),
            deploy: ServiceDeployConfig {
                flags: vec![],
                expose_ports: vec![],
                command: None,
                healthcheck_path: healthcheck_path.map(str::to_string),
                replicas: 1,
                exec: true,
                max_restarts: None,
                env: Default::default(),
                secrets: None,
                volumes: vec![],
                node_affinity: None,
                egress: Default::default(),
                healthcheck_interval: 60,
            },
            ingress: None,
            preview: None,
            preview_source: None,
        };
        state.configs.insert(service_id.to_string(), config.clone());

        let deployment = ServiceDeployment {
            id: deployment_id.to_string(),
            created_at: 1,
            deployed_at: None,
            drained_at: None,
            status: DeploymentStatus::Queued,
            config,
            git_commit: None,
            build: None,
            upload_archive: None,
        };
        state
            .transitions
            .insert(deployment.id.clone(), vec![DeploymentStatus::Queued]);
        state
            .history
            .insert(service_id.to_string(), vec![deployment.clone()]);
        deployment
    }

    fn replica_states(&self, service_id: &str, deployment_id: &str) -> Vec<ReplicaState> {
        let state = self.state.lock().expect("state lock");
        let key = format!("{service_id}/{deployment_id}");
        state.replica_states.get(&key).cloned().unwrap_or_default()
    }
}

#[derive(Default)]
struct TestRuntimeProvider {
    containers: Mutex<HashMap<String, String>>,
    pulled_images: Mutex<Vec<String>>,
}

impl TestRuntimeProvider {
    fn with_container(name: &str, ip: &str) -> Self {
        let mut containers = HashMap::new();
        containers.insert(name.to_string(), ip.to_string());
        Self {
            containers: Mutex::new(containers),
            pulled_images: Mutex::new(Vec::new()),
        }
    }

    fn add_container(&self, name: &str, ip: &str) {
        self.containers
            .lock()
            .expect("containers lock")
            .insert(name.to_string(), ip.to_string());
    }

    fn pulled_images(&self) -> Vec<String> {
        self.pulled_images
            .lock()
            .expect("pulled images lock")
            .clone()
    }
}

#[async_trait]
impl RuntimeProvider for TestRuntimeProvider {
    fn cli_name(&self) -> &str {
        "test-runtime"
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

    async fn remove_container(&self, name: &str) -> Result<()> {
        self.containers
            .lock()
            .expect("containers lock")
            .remove(name);
        Ok(())
    }

    fn run_command(&self, _spec: &RunSpec) -> JobCommand {
        JobCommand::Exec {
            program: "true".to_string(),
            args: vec![],
        }
    }

    async fn inspect_container_ip(&self, name: &str) -> Option<String> {
        self.containers
            .lock()
            .expect("containers lock")
            .get(name)
            .cloned()
    }

    async fn inspect_network_cidr(&self, _name: &str) -> Option<String> {
        None
    }

    async fn build_image(
        &self,
        _spec: &BuildSpec,
        _log_sender: Option<&flume::Sender<crate::logs::LogEntry>>,
        _log_source: Option<&str>,
    ) -> Result<()> {
        Ok(())
    }

    async fn pull_image(
        &self,
        image: &str,
        _log_sender: Option<&flume::Sender<crate::logs::LogEntry>>,
        _log_source: Option<&str>,
    ) -> Result<()> {
        self.pulled_images
            .lock()
            .expect("pulled images lock")
            .push(image.to_string());
        Ok(())
    }

    async fn resolve_immutable_image_reference(&self, image: &str) -> Result<String> {
        let repository = image.strip_suffix(":latest").unwrap_or(image);
        Ok(format!("{repository}@sha256:{}", "a".repeat(64)))
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

fn test_controller_config(data_dir: std::path::PathBuf) -> ControllerConfig {
    ControllerConfig {
        data_dir: data_dir.clone(),
        etcd_port: 0,
        cluster_alias: "test".to_string(),
        cluster_name: "test".to_string(),
        cluster: None,
        etcd_endpoints: Vec::new(),
        container_etcd_endpoints: vec!["https://maestro-etcd:2379".to_string()],
        probe_port: None,
        admin_port: None,
        ingress_ports: vec![],
        project_dir: data_dir,
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
        sync_ingress(&mut state, service_id);
        Ok(())
    }

    async fn upsert_replica_state(
        &self,
        service_id: &str,
        deployment_id: &str,
        replica_state: ReplicaState,
    ) -> Result<()> {
        let mut state = self.state.lock().expect("state lock");
        let key = format!("{service_id}/{deployment_id}");
        let replicas = state.replica_states.entry(key).or_default();
        if let Some(existing) = replicas
            .iter_mut()
            .find(|r| r.replica_index == replica_state.replica_index)
        {
            *existing = replica_state;
        } else {
            replicas.push(replica_state);
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

    async fn save_deploy_data(
        &self,
        _service_id: &str,
        _deployment: &ServiceDeployment,
    ) -> Result<()> {
        Ok(())
    }

    async fn update_deployment_build_info(
        &self,
        deployment: &Deployment,
        updated: &ServiceDeployment,
    ) -> Result<()> {
        let mut state = self.state.lock().expect("state lock");
        let Some(deployments) = state.history.get_mut(&deployment.service_id) else {
            return Ok(());
        };
        let Some(stored) = deployments.iter_mut().find(|item| item.id == deployment.id) else {
            return Ok(());
        };
        stored.build = updated.build.clone();
        stored.git_commit = updated.git_commit.clone();
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
            deploy_frozen: false,
            replicas_override: None,
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
async fn docker_deployment_pins_digest_and_stays_building_until_container_exists() {
    let now_millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should be after epoch")
        .as_millis();
    let data_dir = std::env::temp_dir().join(format!("maestro-test-docker-ready-{now_millis}"));

    let store = Arc::new(InMemoryStore::default());
    let deployment = store.seed_queued_docker_deployment("svc-docker", "ABCDEF1234", None);
    let runtime = Arc::new(TestRuntimeProvider::default());
    let (signal_tx, _) = broadcast::channel(4);
    let signal_rx = signal_tx.subscribe();
    let mut controller = DeploymentController::new(
        test_controller_config(data_dir.clone()),
        store.clone(),
        JobSupervisor::new(),
        signal_rx,
        None,
        runtime.clone(),
        None,
        None,
    );

    controller
        .reconcile_deployments()
        .await
        .expect("reconcile should succeed");

    assert!(
        store
            .replica_states("svc-docker", &deployment.id)
            .is_empty()
    );

    let build_deadline = Instant::now() + Duration::from_secs(2);
    loop {
        controller
            .reconcile_deployments()
            .await
            .expect("reconcile should succeed");
        let replicas = store.replica_states("svc-docker", &deployment.id);
        if !replicas.is_empty() {
            assert_eq!(replicas.len(), 1);
            assert_eq!(replicas[0].status, DeploymentStatus::Building);
            break;
        }
        assert!(
            Instant::now() < build_deadline,
            "image pull did not complete in time"
        );
        sleep(Duration::from_millis(10)).await;
    }

    assert_eq!(
        runtime.pulled_images(),
        vec!["example/image:latest".to_string()]
    );

    let building = store
        .all_deployments()
        .into_iter()
        .find(|item| item.id == deployment.id)
        .expect("deployment should exist");
    assert_eq!(building.status, DeploymentStatus::Building);
    assert_eq!(
        building
            .build
            .as_ref()
            .map(|build| build.docker_image_id.as_str()),
        Some(
            "example/image@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        )
    );

    runtime.add_container(&deployment.hostname_for_replica(0), "10.0.0.2");
    controller
        .reconcile_deployments()
        .await
        .expect("reconcile should succeed");

    let ready = store
        .all_deployments()
        .into_iter()
        .find(|item| item.id == deployment.id)
        .expect("deployment should exist");
    assert_eq!(ready.status, DeploymentStatus::Ready);
    let replicas = store.replica_states("svc-docker", &deployment.id);
    assert_eq!(replicas.len(), 1);
    assert_eq!(replicas[0].status, DeploymentStatus::Ready);

    let _ = std::fs::remove_dir_all(&data_dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn orphaned_building_deployment_with_running_container_restores_ready_state() {
    let now_millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should be after epoch")
        .as_millis();
    let data_dir = std::env::temp_dir().join(format!("maestro-test-orphan-ready-{now_millis}"));

    let store = Arc::new(InMemoryStore::default());
    let deployment = store.seed_active_deployment(
        "svc-orphan",
        "ABCDEF1234",
        DeploymentStatus::Building,
        None,
        1,
    );
    let runtime = Arc::new(TestRuntimeProvider::with_container(
        &deployment.hostname_for_replica(0),
        "10.0.0.2",
    ));
    let (signal_tx, _) = broadcast::channel(4);
    let signal_rx = signal_tx.subscribe();
    let mut controller = DeploymentController::new(
        test_controller_config(data_dir.clone()),
        store.clone(),
        JobSupervisor::new(),
        signal_rx,
        None,
        runtime,
        None,
        None,
    );

    controller
        .reconcile_deployments()
        .await
        .expect("reconcile should succeed");

    let updated = store
        .all_deployments()
        .into_iter()
        .find(|item| item.id == deployment.id)
        .expect("deployment should exist");
    assert_eq!(updated.status, DeploymentStatus::Ready);

    let replicas = store.replica_states("svc-orphan", &deployment.id);
    assert_eq!(replicas.len(), 1);
    assert_eq!(replicas[0].status, DeploymentStatus::Ready);

    let _ = std::fs::remove_dir_all(&data_dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn orphaned_building_deployment_with_healthcheck_restores_pending_ready_state() {
    let now_millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should be after epoch")
        .as_millis();
    let data_dir = std::env::temp_dir().join(format!("maestro-test-orphan-pending-{now_millis}"));

    let store = Arc::new(InMemoryStore::default());
    let deployment = store.seed_active_deployment(
        "svc-orphan",
        "ABCDEF1234",
        DeploymentStatus::Building,
        Some("/health"),
        1,
    );
    let runtime = Arc::new(TestRuntimeProvider::with_container(
        &deployment.hostname_for_replica(0),
        "10.0.0.2",
    ));
    let (signal_tx, _) = broadcast::channel(4);
    let signal_rx = signal_tx.subscribe();
    let mut controller = DeploymentController::new(
        test_controller_config(data_dir.clone()),
        store.clone(),
        JobSupervisor::new(),
        signal_rx,
        None,
        runtime,
        None,
        None,
    );

    controller
        .reconcile_deployments()
        .await
        .expect("reconcile should succeed");

    let updated = store
        .all_deployments()
        .into_iter()
        .find(|item| item.id == deployment.id)
        .expect("deployment should exist");
    assert_eq!(updated.status, DeploymentStatus::PendingReady);

    let replicas = store.replica_states("svc-orphan", &deployment.id);
    assert_eq!(replicas.len(), 1);
    assert_eq!(replicas[0].status, DeploymentStatus::PendingReady);

    let _ = std::fs::remove_dir_all(&data_dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn orphaned_building_deployment_without_container_is_terminated() {
    let now_millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should be after epoch")
        .as_millis();
    let data_dir =
        std::env::temp_dir().join(format!("maestro-test-orphan-terminated-{now_millis}"));

    let store = Arc::new(InMemoryStore::default());
    let deployment = store.seed_active_deployment(
        "svc-orphan",
        "ABCDEF1234",
        DeploymentStatus::Building,
        None,
        1,
    );
    let runtime = Arc::new(TestRuntimeProvider::default());
    let (signal_tx, _) = broadcast::channel(4);
    let signal_rx = signal_tx.subscribe();
    let mut controller = DeploymentController::new(
        test_controller_config(data_dir.clone()),
        store.clone(),
        JobSupervisor::new(),
        signal_rx,
        None,
        runtime,
        None,
        None,
    );

    controller
        .reconcile_deployments()
        .await
        .expect("reconcile should succeed");

    let updated = store
        .all_deployments()
        .into_iter()
        .find(|item| item.id == deployment.id)
        .expect("deployment should exist");
    assert_eq!(updated.status, DeploymentStatus::Terminated);
    assert!(
        store
            .replica_states("svc-orphan", &deployment.id)
            .is_empty()
    );

    let _ = std::fs::remove_dir_all(&data_dir);
}

fn parse_history_key(key: &str) -> Option<(String, usize)> {
    let service_id = service_id_from_history_key(key)?;
    let index = key.rsplit('/').next()?.parse::<usize>().ok()?;
    Some((service_id, index))
}
