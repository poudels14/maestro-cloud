use std::collections::{BTreeSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use kernel_api::{
    Assignment, AssignmentPhase, Deployment, DeploymentPhase, HealthCheckSpec, HealthProbe,
    ObjectMeta, ReplicaState, ReplicaStateId, ReplicaStateSpec, ReplicaStateStatus, ResourceKind,
    ResourceName, ResourceRevision, Timestamp, WorkloadId,
};
use kernel_store::{
    Clock, ExpectedVersion, InMemoryStore, Keyspace, MonotonicTime, PutRequest, Store,
};

use crate::health::healthy_stagger;
use crate::{
    HealthAgent, HealthAgentSettings, HealthProbeError, HealthProbeTarget, HealthProber,
    StatusClock,
};

use super::assignment::{assignment, cluster_id, deployment, node_id};

#[tokio::test]
async fn health_reconcile_thresholds_failures_and_resets_on_success()
-> Result<(), Box<dyn std::error::Error>> {
    let clock = Arc::new(TestClock::default());
    let store = Arc::new(InMemoryStore::new(clock.clone()));
    let prober = Arc::new(FakeHealthProber::new([
        Err(HealthProbeError::Unhealthy {
            message: "connection refused".to_owned(),
        }),
        Ok(()),
    ]));
    let (assignment, deployment, replica) = health_resources(3);
    seed(store.as_ref(), &assignment, &deployment, &replica).await?;
    let agent = agent(store.clone(), prober.clone(), clock.clone());

    let first = agent.reconcile_once().await?;
    assert_eq!(first.probed, 1);
    assert_eq!(first.unhealthy, 1);
    let observed = load_replica(store.as_ref()).await?;
    assert_eq!(observed.status.phase, DeploymentPhase::PendingReady);
    assert_eq!(observed.status.healthcheck_failures, 1);

    assert_eq!(agent.reconcile_once().await?.probed, 0);
    clock.advance(Duration::from_secs(5));
    let recovered = agent.reconcile_once().await?;
    assert_eq!(recovered.ready, 1);
    let observed = load_replica(store.as_ref()).await?;
    assert_eq!(observed.status.phase, DeploymentPhase::Ready);
    assert_eq!(observed.status.healthcheck_failures, 0);
    assert_eq!(
        prober.targets(),
        vec![
            HealthProbeTarget::Http {
                address: assignment.spec.workload_address,
                port: 8080,
                path: "/ready".to_owned(),
            },
            HealthProbeTarget::Http {
                address: assignment.spec.workload_address,
                port: 8080,
                path: "/ready".to_owned(),
            },
        ]
    );
    Ok(())
}

#[tokio::test]
async fn health_reconcile_marks_threshold_exhaustion_terminal()
-> Result<(), Box<dyn std::error::Error>> {
    let clock = Arc::new(TestClock::default());
    let store = Arc::new(InMemoryStore::new(clock.clone()));
    let prober = Arc::new(FakeHealthProber::new([
        Err(HealthProbeError::Unhealthy {
            message: "first failure".to_owned(),
        }),
        Err(HealthProbeError::Unhealthy {
            message: "second failure".to_owned(),
        }),
    ]));
    let (assignment, deployment, replica) = health_resources(2);
    seed(store.as_ref(), &assignment, &deployment, &replica).await?;
    let agent = agent(store.clone(), prober, clock.clone());

    assert_eq!(agent.reconcile_once().await?.unhealthy, 1);
    clock.advance(Duration::from_secs(5));
    assert_eq!(agent.reconcile_once().await?.terminal, 1);
    let observed = load_replica(store.as_ref()).await?;
    assert_eq!(observed.status.phase, DeploymentPhase::Crashed);
    assert_eq!(observed.status.healthcheck_failures, 2);
    assert_eq!(agent.reconcile_once().await?.probed, 0);
    Ok(())
}

#[tokio::test]
async fn health_reconcile_marks_unprobed_workloads_ready() -> Result<(), Box<dyn std::error::Error>>
{
    let clock = Arc::new(TestClock::default());
    let store = Arc::new(InMemoryStore::new(clock.clone()));
    let prober = Arc::new(FakeHealthProber::new([]));
    let mut resources = health_resources(3);
    resources.1.spec.service.health_check = None;
    seed(store.as_ref(), &resources.0, &resources.1, &resources.2).await?;
    let agent = agent(store.clone(), prober.clone(), clock);

    let report = agent.reconcile_once().await?;
    assert_eq!(report.ready, 1);
    assert_eq!(report.probed, 0);
    assert!(prober.targets().is_empty());
    assert_eq!(
        load_replica(store.as_ref()).await?.status.phase,
        DeploymentPhase::Ready
    );
    Ok(())
}

#[test]
fn healthy_probe_stagger_is_stable_and_bounded() {
    let interval = Duration::from_secs(60);
    let poll = Duration::from_secs(5);
    let first = healthy_stagger("assignment-1", interval, poll);
    assert_eq!(first, healthy_stagger("assignment-1", interval, poll));
    assert!(first >= poll);
    assert!(first <= interval);
}

fn health_resources(threshold: u32) -> (Assignment, Deployment, ReplicaState) {
    let mut assignment = assignment();
    assignment.status.phase = AssignmentPhase::Running;
    assignment.status.workload_id = Some(WorkloadId::new("assignment-1").unwrap());
    let mut deployment = deployment();
    deployment.spec.service.health_check = Some(HealthCheckSpec {
        probe: HealthProbe::Http {
            port: 8080,
            path: "/ready".to_owned(),
        },
        interval_secs: 60,
        unhealthy_threshold: threshold,
    });
    let replica = ReplicaState {
        meta: ObjectMeta {
            id: ReplicaStateId::new("replica-1").unwrap(),
            labels: Default::default(),
            annotations: Default::default(),
            revision: ResourceRevision::default(),
            generation: kernel_api::Generation(1),
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
            workload_id: assignment.status.workload_id.clone(),
            healthcheck_failures: 0,
            restart_attempts: 0,
            conditions: Vec::new(),
        },
    };
    (assignment, deployment, replica)
}

fn agent(
    store: Arc<InMemoryStore>,
    prober: Arc<FakeHealthProber>,
    clock: Arc<TestClock>,
) -> HealthAgent {
    HealthAgent::new(
        store,
        prober,
        HealthAgentSettings {
            cluster_id: cluster_id(),
            node_id: node_id("node-1"),
            poll_interval: Duration::from_secs(5),
        },
        clock,
        Arc::new(FixedStatusClock),
    )
    .unwrap()
}

async fn seed(
    store: &dyn Store,
    assignment: &Assignment,
    deployment: &Deployment,
    replica: &ReplicaState,
) -> Result<(), Box<dyn std::error::Error>> {
    for (kind, id, value) in [
        (
            "Assignment",
            assignment.meta.id.as_str(),
            serde_json::to_vec(assignment)?,
        ),
        (
            "Deployment",
            deployment.meta.id.as_str(),
            serde_json::to_vec(deployment)?,
        ),
        (
            "ReplicaState",
            replica.meta.id.as_str(),
            serde_json::to_vec(replica)?,
        ),
    ] {
        let key = Keyspace::new(&cluster_id())
            .resource(&ResourceKind::new(kind)?, &ResourceName::new(id)?);
        store
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

async fn load_replica(store: &dyn Store) -> Result<ReplicaState, Box<dyn std::error::Error>> {
    let key = Keyspace::new(&cluster_id()).resource(
        &ResourceKind::new("ReplicaState")?,
        &ResourceName::new("replica-1")?,
    );
    let stored = store.get(&key).await?.ok_or("replica missing")?;
    Ok(serde_json::from_slice(&stored.value)?)
}

struct FakeHealthProber {
    results: Mutex<VecDeque<Result<(), HealthProbeError>>>,
    targets: Mutex<Vec<HealthProbeTarget>>,
}

impl FakeHealthProber {
    fn new(results: impl IntoIterator<Item = Result<(), HealthProbeError>>) -> Self {
        Self {
            results: Mutex::new(results.into_iter().collect()),
            targets: Mutex::new(Vec::new()),
        }
    }

    fn targets(&self) -> Vec<HealthProbeTarget> {
        self.targets
            .lock()
            .map(|targets| targets.clone())
            .unwrap_or_default()
    }
}

#[async_trait]
impl HealthProber for FakeHealthProber {
    async fn probe(&self, target: &HealthProbeTarget) -> Result<(), HealthProbeError> {
        self.targets
            .lock()
            .map_err(|_| HealthProbeError::Unavailable {
                message: "fake target lock poisoned".to_owned(),
            })?
            .push(target.clone());
        self.results
            .lock()
            .map_err(|_| HealthProbeError::Unavailable {
                message: "fake result lock poisoned".to_owned(),
            })?
            .pop_front()
            .unwrap_or(Ok(()))
    }
}

#[derive(Default)]
struct TestClock {
    seconds: AtomicU64,
}

impl TestClock {
    fn advance(&self, duration: Duration) {
        self.seconds
            .fetch_add(duration.as_secs(), Ordering::Relaxed);
    }
}

#[async_trait]
impl Clock for TestClock {
    fn now(&self) -> MonotonicTime {
        MonotonicTime::from_duration(Duration::from_secs(self.seconds.load(Ordering::Relaxed)))
    }

    async fn sleep_until(&self, _deadline: MonotonicTime) {
        std::future::pending::<()>().await;
    }
}

struct FixedStatusClock;

impl StatusClock for FixedStatusClock {
    fn now(&self) -> Timestamp {
        Timestamp(1_750_000_000_000)
    }
}
