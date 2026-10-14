#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use maestro_control::{ImageBuilder, ObservedReplicas, StoreWriter};
use maestro_core::{
    BuildSpec, ByteSize, DeploySpec, Deployment, Millicores, NodeCapacity, NodeId, NodeRecord,
    NodeRole, NodeState, ReplicaId, ReplicaPhase, ReplicaStatus, ServiceSpec,
};
use maestro_store::{ConsensusError, Mutation, StateMachine};
use tempfile::TempDir;

pub struct LocalWriter {
    _directory: TempDir,
    state: StateMachine,
}

#[derive(Default)]
pub struct FakeObserved {
    phases: Mutex<HashMap<ReplicaId, ReplicaPhase>>,
    lost_nodes: Mutex<Vec<NodeId>>,
}

pub struct FakeClock(pub AtomicU64);

impl LocalWriter {
    pub fn new() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let state = StateMachine::open(directory.path().join("state.redb")).unwrap();
        Self {
            _directory: directory,
            state,
        }
    }
}

impl StoreWriter for LocalWriter {
    async fn write(&self, mutations: Vec<Mutation>) -> Result<u64, ConsensusError> {
        self.state
            .apply(&mutations)
            .map_err(|error| match error.is_rejection() {
                true => ConsensusError::Rejected(error.to_string()),
                false => ConsensusError::Store(error),
            })
    }

    fn state(&self) -> &StateMachine {
        &self.state
    }
}

impl FakeObserved {
    pub fn lose_node(&self, node_id: NodeId) {
        self.lost_nodes.lock().unwrap().push(node_id);
    }

    pub fn set(&self, replica: &str, phase: ReplicaPhase) {
        self.phases
            .lock()
            .unwrap()
            .insert(ReplicaId::from(replica), phase);
    }
}

impl ObservedReplicas for FakeObserved {
    fn node_available(&self, node_id: NodeId) -> bool {
        !self.lost_nodes.lock().unwrap().contains(&node_id)
    }

    fn replicas(&self) -> Vec<ReplicaStatus> {
        let ids = self
            .phases
            .lock()
            .unwrap()
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        ids.iter().filter_map(|id| self.replica(id)).collect()
    }

    fn replica(&self, replica_id: &ReplicaId) -> Option<ReplicaStatus> {
        self.phases
            .lock()
            .unwrap()
            .get(replica_id)
            .map(|phase| ReplicaStatus {
                replica_id: replica_id.clone(),
                service_id: "unused".into(),
                deployment_id: "unused".into(),
                phase: *phase,
                ip: None,
                restarts: 0,
                consecutive_failures: 0,
                last_error: None,
            })
    }
}

impl FakeClock {
    pub fn advance(&self, millis: u64) {
        self.0.fetch_add(millis, Ordering::SeqCst);
    }

    pub fn now(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
}

pub fn clock() -> Arc<FakeClock> {
    Arc::new(FakeClock(AtomicU64::new(1_000_000)))
}

pub fn node(id: u64, cores: u32, memory_gib: u64) -> NodeRecord {
    NodeRecord {
        id: NodeId(id),
        name: format!("node{id}"),
        role: NodeRole::Voter,
        endpoint: String::new(),
        subnet: None,
        runs_workloads: true,
        capacity: NodeCapacity {
            cpu: Millicores::new(cores * 1000),
            memory: ByteSize::new(memory_gib << 30),
        },
        reserved: NodeCapacity {
            cpu: Millicores::new(1000),
            memory: ByteSize::new(2 << 30),
        },
        labels: Default::default(),
        state: NodeState::Active,
        wireguard: None,
    }
}

pub fn image_service(replicas: u32) -> ServiceSpec {
    ServiceSpec {
        name: "API".to_string(),
        image: Some("nginx:1.27".to_string()),
        build: None,
        deploy: DeploySpec {
            replicas,
            ..DeploySpec::default()
        },
        ingress: None,
        preview: None,
    }
}

pub struct FakeBuilder {
    pub started: Mutex<Vec<String>>,
    failures: Mutex<Vec<String>>,
    gate: tokio::sync::Semaphore,
}

impl Default for FakeBuilder {
    fn default() -> Self {
        Self {
            started: Mutex::default(),
            failures: Mutex::default(),
            gate: tokio::sync::Semaphore::new(0),
        }
    }
}

impl FakeBuilder {
    pub fn fail(&self, deployment: &str) {
        self.failures.lock().unwrap().push(deployment.to_string());
    }

    pub fn release(&self, builds: usize) {
        self.gate.add_permits(builds);
    }

    pub fn started(&self) -> Vec<String> {
        self.started.lock().unwrap().clone()
    }
}

impl ImageBuilder for FakeBuilder {
    async fn build(&self, deployment: Deployment) -> Result<String, String> {
        self.started.lock().unwrap().push(deployment.id.to_string());
        self.gate.acquire().await.unwrap().forget();
        match self
            .failures
            .lock()
            .unwrap()
            .contains(&deployment.id.to_string())
        {
            true => Err("dockerfile not found".to_string()),
            false => Ok(format!(
                "maestro.local/{}:{}",
                deployment.service_id, deployment.id
            )),
        }
    }
    async fn resolve_sources(&self, deployment: Deployment) -> Result<ServiceSpec, String> {
        let mut spec = deployment.spec;
        match spec.deploy.env.source.take().as_deref() {
            Some("aws-secret://missing") => Err("secret not found".to_string()),
            Some(_) => {
                spec.deploy
                    .env
                    .items
                    .entry("FROM_SOURCE".to_string())
                    .or_insert_with(|| "fetched".to_string());
                Ok(spec)
            }
            None => Ok(spec),
        }
    }

    async fn resolve(&self, image: String) -> Result<String, String> {
        match image.starts_with("missing") {
            true => Err("image or tag not found".to_string()),
            false => Ok(format!("{image}@sha256:feed")),
        }
    }
}

pub fn build_service(replicas: u32) -> ServiceSpec {
    ServiceSpec {
        name: "API".to_string(),
        image: None,
        build: Some(BuildSpec {
            repo: Some("https://github.com/acme/api.git".to_string()),
            branch: Some("main".to_string()),
            dockerfile: "Dockerfile".to_string(),
            watch: false,
            registry: None,
            depot: None,
            env: Default::default(),
            secrets: Default::default(),
        }),
        deploy: DeploySpec {
            replicas,
            ..DeploySpec::default()
        },
        ingress: None,
        preview: None,
    }
}
