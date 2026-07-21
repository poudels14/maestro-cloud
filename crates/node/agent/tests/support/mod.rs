use std::collections::{BTreeMap, BTreeSet};
use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use kernel_api::{
    ArtifactTemplate, Assignment, AssignmentId, AssignmentPhase, AssignmentSpec, AssignmentStatus,
    ClusterId, Deployment, DeploymentId, DeploymentPhase, DeploymentSpec, DeploymentStatus,
    ExecPolicy, Generation, HealthCheckSpec, HealthProbe, NodeId, ObjectMeta, PlacementConstraint,
    ReplicaState, ReplicaStateId, ReplicaStateSpec, ReplicaStateStatus, ResourceKind, ResourceName,
    ResourceRevision, ServiceId, ServiceSpec, Timestamp,
};
use kernel_store::{
    Clock, ExpectedVersion, InMemoryStore, Keyspace, MonotonicTime, PutRequest, Store,
};
use node_agent::{AssignmentAgent, AssignmentAgentSettings, StatusClock};
use runtime::{FakeRuntime, NetworkCidr, NetworkProvider, NetworkSpec, WorkloadRuntime};

pub use runtime::FakeNetworkProvider;

pub const WORKLOAD_ADDRESS: IpAddr = IpAddr::V4(Ipv4Addr::new(10, 42, 1, 8));

pub fn cluster_id() -> ClusterId {
    ClusterId::new("cluster-1").unwrap()
}

pub fn node_id() -> NodeId {
    NodeId::new("node-1").unwrap()
}

pub struct ExitWorld {
    pub store: Arc<InMemoryStore>,
    pub monotonic_clock: Arc<TestClock>,
    pub status_clock: Arc<TestStatusClock>,
    state_root: tempfile::TempDir,
}

impl ExitWorld {
    pub fn new() -> Self {
        let monotonic_clock = Arc::new(TestClock::default());
        Self {
            store: Arc::new(InMemoryStore::new(monotonic_clock.clone())),
            monotonic_clock,
            status_clock: Arc::new(TestStatusClock::new(1_750_000_000_000)),
            state_root: tempfile::tempdir().unwrap(),
        }
    }

    pub async fn seed(&self) -> Result<(), Box<dyn std::error::Error>> {
        let assignment = assignment();
        let resources = [
            (
                "Deployment",
                "deployment-1",
                serde_json::to_vec(&deployment())?,
            ),
            (
                "Assignment",
                "assignment-1",
                serde_json::to_vec(&assignment)?,
            ),
            (
                "ReplicaState",
                "replica-1",
                serde_json::to_vec(&replica(&assignment))?,
            ),
        ];
        for (kind, id, value) in resources {
            let key = Keyspace::new(&cluster_id())
                .resource(&ResourceKind::new(kind)?, &ResourceName::new(id)?);
            self.store
                .put_cas(PutRequest {
                    key,
                    value,
                    expected: ExpectedVersion::Missing,
                    session: None,
                })
                .await?;
        }
        Ok(())
    }

    pub fn assignment_agent(
        &self,
        runtime: Arc<FakeRuntime>,
        network: Arc<FakeNetworkProvider>,
    ) -> AssignmentAgent {
        let runtime: Arc<dyn WorkloadRuntime> = runtime;
        let network: Arc<dyn NetworkProvider> = network;
        AssignmentAgent::new(
            self.store.clone(),
            runtime,
            network,
            AssignmentAgentSettings {
                cluster_id: cluster_id(),
                node_id: node_id(),
                network: NetworkSpec {
                    name: "maestro-node-1".to_owned(),
                    range: NetworkCidr::new(IpAddr::V4(Ipv4Addr::new(10, 42, 1, 0)), 24).unwrap(),
                    gateway: IpAddr::V4(Ipv4Addr::new(10, 42, 1, 1)),
                    mtu_bytes: 1_420,
                },
                stop_timeout: Duration::from_secs(5),
                resync_interval: Duration::from_secs(30),
                restart_backoff_base: Duration::from_secs(5),
                restart_backoff_max: Duration::from_secs(60),
                secrets_root: self.state_root.path().join("secrets"),
                node_api_root: self.state_root.path().join("node-api"),
            },
            None,
            self.monotonic_clock.clone(),
            self.status_clock.clone(),
        )
        .unwrap()
    }

    pub fn checkpoint_root(&self) -> std::path::PathBuf {
        self.state_root.path().join("log-checkpoints")
    }

    pub async fn assignment(&self) -> Result<Assignment, Box<dyn std::error::Error>> {
        self.load("Assignment", "assignment-1").await
    }

    pub async fn replica(&self) -> Result<ReplicaState, Box<dyn std::error::Error>> {
        self.load("ReplicaState", "replica-1").await
    }

    async fn load<T: serde::de::DeserializeOwned>(
        &self,
        kind: &str,
        id: &str,
    ) -> Result<T, Box<dyn std::error::Error>> {
        let key = Keyspace::new(&cluster_id())
            .resource(&ResourceKind::new(kind)?, &ResourceName::new(id)?);
        let value = self.store.get(&key).await?.ok_or("resource missing")?;
        Ok(serde_json::from_slice(&value.value)?)
    }
}

pub fn assignment() -> Assignment {
    Assignment {
        meta: ObjectMeta {
            id: AssignmentId::new("assignment-1").unwrap(),
            labels: BTreeMap::new(),
            annotations: BTreeMap::new(),
            revision: ResourceRevision::default(),
            generation: Generation(1),
            owner_refs: Vec::new(),
            finalizers: BTreeSet::new(),
            deletion_timestamp: None,
        },
        spec: AssignmentSpec {
            service_id: ServiceId::new("api").unwrap(),
            deployment_id: DeploymentId::new("deployment-1").unwrap(),
            restart_generation: Generation(1),
            replica_index: 0,
            node_id: node_id(),
            placement_epoch: 1,
            workload_address: WORKLOAD_ADDRESS,
            replaces_assignment_id: None,
        },
        status: AssignmentStatus {
            phase: AssignmentPhase::Pending,
            workload_id: None,
            conditions: Vec::new(),
        },
    }
}

fn deployment() -> Deployment {
    Deployment {
        meta: ObjectMeta {
            id: DeploymentId::new("deployment-1").unwrap(),
            labels: BTreeMap::new(),
            annotations: BTreeMap::new(),
            revision: ResourceRevision::default(),
            generation: Generation(1),
            owner_refs: Vec::new(),
            finalizers: BTreeSet::new(),
            deletion_timestamp: None,
        },
        spec: DeploymentSpec {
            service_id: ServiceId::new("api").unwrap(),
            service_generation: Generation(1),
            restart_generation: Generation(1),
            goal: kernel_api::DeploymentGoal::Run,
            service: ServiceSpec {
                name: "API".to_owned(),
                version: "1.0.0".to_owned(),
                artifact: ArtifactTemplate::Image {
                    reference: "registry.test/api:latest".to_owned(),
                },
                command: None,
                replicas: 1,
                exposed_ports: vec![8080],
                health_check: Some(HealthCheckSpec {
                    probe: HealthProbe::Http {
                        port: 8080,
                        path: "/ready".to_owned(),
                    },
                    interval_secs: 30,
                    unhealthy_threshold: 3,
                }),
                max_restarts: Some(3),
                environment: BTreeMap::from([("MODE".to_owned(), "production".to_owned())]),
                user: None,
                node_api: kernel_api::NodeApiAccess::Disabled,
                secrets: None,
                volumes: Vec::new(),
                placement: PlacementConstraint::default(),
                exec: ExecPolicy::Allowed,
            },
            build_id: None,
        },
        status: DeploymentStatus {
            phase: DeploymentPhase::PendingReady,
            created_at: Timestamp(1_750_000_000_000),
            ready_at: None,
            draining_at: None,
            image_digest: Some("registry.test/api@sha256:abc".to_owned()),
            conditions: Vec::new(),
        },
    }
}

fn replica(assignment: &Assignment) -> ReplicaState {
    ReplicaState {
        meta: ObjectMeta {
            id: ReplicaStateId::new("replica-1").unwrap(),
            labels: BTreeMap::new(),
            annotations: BTreeMap::new(),
            revision: ResourceRevision::default(),
            generation: Generation(1),
            owner_refs: Vec::new(),
            finalizers: BTreeSet::new(),
            deletion_timestamp: None,
        },
        spec: ReplicaStateSpec {
            service_id: assignment.spec.service_id.clone(),
            deployment_id: assignment.spec.deployment_id.clone(),
            assignment_id: assignment.meta.id.clone(),
            replica_index: assignment.spec.replica_index,
        },
        status: ReplicaStateStatus {
            phase: DeploymentPhase::PendingReady,
            node_id: Some(assignment.spec.node_id.clone()),
            workload_id: None,
            healthcheck_failures: 0,
            restart_attempts: 0,
            restart_pending_attempt: None,
            restart_not_before: None,
            conditions: Vec::new(),
        },
    }
}

#[derive(Default)]
pub struct TestClock(AtomicU64);

#[async_trait]
impl Clock for TestClock {
    fn now(&self) -> MonotonicTime {
        MonotonicTime::from_duration(Duration::from_millis(self.0.load(Ordering::SeqCst)))
    }

    async fn sleep_until(&self, _deadline: MonotonicTime) {
        std::future::pending().await
    }
}

pub struct TestStatusClock(AtomicI64);

impl TestStatusClock {
    fn new(milliseconds: i64) -> Self {
        Self(AtomicI64::new(milliseconds))
    }
}

impl StatusClock for TestStatusClock {
    fn now(&self) -> Timestamp {
        Timestamp(self.0.load(Ordering::SeqCst))
    }
}
