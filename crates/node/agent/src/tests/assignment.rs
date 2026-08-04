use std::collections::{BTreeMap, BTreeSet};
use std::net::{IpAddr, Ipv4Addr};
use std::os::unix::fs::MetadataExt;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use kernel_api::{
    ArtifactTemplate, Assignment, AssignmentId, AssignmentPhase, AssignmentSpec, AssignmentStatus,
    ClusterId, Deployment, DeploymentId, DeploymentPhase, DeploymentSpec, DeploymentStatus,
    ExecPolicy, Generation, NodeApiAccess, NodeId, ObjectMeta, PlacementConstraint, ReplicaState,
    ReplicaStateId, ReplicaStateSpec, ReplicaStateStatus, ResourceKind, ResourceName,
    ResourceRevision, SecretMountSpec, SecretValue, ServiceId, ServiceSpec, Timestamp,
    VolumeAccess, VolumeMountSpec, VolumeSource, WorkloadUserSpec,
};
use kernel_store::{
    Clock, DeleteRequest, ExpectedVersion, InMemoryStore, Keyspace, MonotonicTime, PutRequest,
    Store,
};
use runtime::{
    FakeNetworkProvider, FakeRuntime, FakeRuntimeOperation, NetworkAddressing, NetworkCidr,
    NetworkProvider, NetworkSpec, RuntimeError, ShutdownRequest, ValueSourceError,
    ValueSourceResolver, WorkloadRuntime, WorkloadSpec,
};
use tokio::sync::{Notify, watch};

use crate::{AssignmentAgent, AssignmentAgentSettings, NodeApiServices, StatusClock, WorkloadDns};

mod dns;
#[cfg(unix)]
mod node_api;

#[tokio::test]
async fn assignment_reconcile_runs_and_re_adopts_one_exactly_addressed_workload()
-> Result<(), Box<dyn std::error::Error>> {
    let world = World::new();
    world.seed(&deployment(), &assignment()).await?;

    let first = world.agent().reconcile_once().await?;
    assert_eq!(first.desired, 1);
    assert_eq!(first.running, 1);
    assert_eq!(world.network.lease_count(), 1);
    assert_running(&world).await?;

    let restarted = world.agent().reconcile_once().await?;
    assert_eq!(restarted.running, 1);
    assert_eq!(restarted.garbage_collected, 0);
    assert_eq!(
        world
            .runtime
            .list(&cluster_id(), &node_id("node-1"))
            .await?
            .len(),
        1
    );
    Ok(())
}

#[tokio::test]
async fn assignment_reconcile_publishes_a_runtime_delegated_address()
-> Result<(), Box<dyn std::error::Error>> {
    let world = World::new();
    let mut assignment = assignment();
    assignment.spec.workload_address = None;
    world.seed(&deployment(), &assignment).await?;

    let report = world.agent_delegated().reconcile_once().await?;
    assert_eq!(report.running, 1);
    let stored = world.load_assignment().await?;
    assert_eq!(stored.spec.workload_address, None);
    assert_eq!(
        stored.status.workload_address,
        Some(IpAddr::V4(Ipv4Addr::new(192, 0, 2, 2)))
    );
    Ok(())
}

#[tokio::test]
async fn assignment_reconcile_creates_and_reuses_missing_replica_state()
-> Result<(), Box<dyn std::error::Error>> {
    let world = World::new();
    let assignment = assignment();
    world
        .seed_without_replica(&deployment(), &assignment)
        .await?;

    let first = world.agent().reconcile_once().await?;
    assert_eq!(first.replica_states_created, 1);
    let key = Keyspace::new(&cluster_id()).resource(
        &ResourceKind::new("ReplicaState")?,
        &ResourceName::new(assignment.meta.id.as_str())?,
    );
    let stored = world.store.get(&key).await?.ok_or("replica missing")?;
    let replica: ReplicaState = serde_json::from_slice(&stored.value)?;
    assert_eq!(replica.spec.assignment_id, assignment.meta.id);
    assert_eq!(replica.status.phase, DeploymentPhase::PendingReady);
    assert_eq!(replica.status.node_id, Some(node_id("node-1")));

    let second = world.agent().reconcile_once().await?;
    assert_eq!(second.replica_states_created, 0);
    Ok(())
}

#[tokio::test]
async fn assignment_reconcile_retries_transient_runtime_failure_from_pending_status()
-> Result<(), Box<dyn std::error::Error>> {
    let world = World::new();
    world.seed(&deployment(), &assignment()).await?;
    world.runtime.fail_next(
        FakeRuntimeOperation::Start,
        RuntimeError::Unavailable {
            message: "injected outage".to_owned(),
        },
    )?;

    let first = world.agent().reconcile_once().await?;
    assert_eq!(first.unresolved, 1);
    let pending = world.load_assignment().await?;
    assert_eq!(pending.status.phase, AssignmentPhase::Pending);
    assert_eq!(
        pending
            .status
            .conditions
            .first()
            .map(|condition| condition.reason.0.as_str()),
        Some("RuntimeRetry")
    );

    let second = world.agent().reconcile_once().await?;
    assert_eq!(second.running, 1);
    assert_running(&world).await?;
    Ok(())
}

#[tokio::test]
async fn assignment_reconcile_reuses_one_durable_restart_reservation_after_failure()
-> Result<(), Box<dyn std::error::Error>> {
    let world = World::new();
    world.seed(&deployment(), &assignment()).await?;
    world.agent().reconcile_once().await?;
    let handle = world
        .runtime
        .list(&cluster_id(), &node_id("node-1"))
        .await?
        .into_iter()
        .next()
        .ok_or("workload missing")?
        .handle;
    world
        .runtime
        .stop(
            &handle,
            ShutdownRequest {
                timeout: Duration::from_secs(5),
            },
        )
        .await?;
    world.runtime.fail_next(
        FakeRuntimeOperation::Start,
        RuntimeError::Unavailable {
            message: "injected restart outage".to_owned(),
        },
    )?;

    let interrupted = world.agent().reconcile_once().await?;
    assert_eq!(interrupted.unresolved, 1);
    assert_eq!(interrupted.requeue_at, Some(Timestamp(1_750_000_005_000)));
    let reserved = world.load_replica().await?;
    assert_eq!(reserved.status.restart_attempts, 1);
    assert_eq!(reserved.status.restart_pending_attempt, Some(1));
    assert_eq!(
        reserved.status.restart_not_before,
        Some(Timestamp(1_750_000_005_000))
    );

    world.status_clock.advance(Duration::from_secs(5));
    let failed_start = world.agent().reconcile_once().await?;
    assert_eq!(failed_start.unresolved, 1);
    let still_reserved = world.load_replica().await?;
    assert_eq!(still_reserved.status.restart_attempts, 1);
    assert_eq!(still_reserved.status.restart_pending_attempt, Some(1));

    let recovered = world.agent().reconcile_once().await?;
    assert_eq!(recovered.running, 1);
    assert_eq!(recovered.restarted, 1);
    let finished = world.load_replica().await?;
    assert_eq!(finished.status.restart_attempts, 1);
    assert_eq!(finished.status.restart_pending_attempt, None);
    assert_eq!(finished.status.restart_not_before, None);
    assert_eq!(
        finished
            .status
            .conditions
            .iter()
            .find(|condition| condition.condition_type.0 == "RuntimeRestart")
            .map(|condition| condition.reason.0.as_str()),
        Some("RestartSucceeded")
    );
    Ok(())
}

#[tokio::test]
async fn assignment_reconcile_stops_after_restart_budget_is_exhausted()
-> Result<(), Box<dyn std::error::Error>> {
    let world = World::new();
    let mut deployment = deployment();
    deployment.spec.service.max_restarts = Some(1);
    world.seed(&deployment, &assignment()).await?;
    world.agent().reconcile_once().await?;
    let handle = world
        .runtime
        .list(&cluster_id(), &node_id("node-1"))
        .await?
        .into_iter()
        .next()
        .ok_or("workload missing")?
        .handle;
    let stop = ShutdownRequest {
        timeout: Duration::from_secs(5),
    };
    world.runtime.stop(&handle, stop).await?;
    let delayed = world.agent().reconcile_once().await?;
    assert_eq!(delayed.unresolved, 1);
    world.status_clock.advance(Duration::from_secs(5));
    assert_eq!(world.agent().reconcile_once().await?.restarted, 1);
    world.runtime.stop(&handle, stop).await?;

    let exhausted = world.agent().reconcile_once().await?;
    assert_eq!(exhausted.unresolved, 1);
    assert_eq!(
        world.load_assignment().await?.status.phase,
        AssignmentPhase::Failed
    );
    let replica = world.load_replica().await?;
    assert_eq!(replica.status.restart_attempts, 1);
    assert_eq!(replica.status.phase, DeploymentPhase::Crashed);
    assert_eq!(
        replica
            .status
            .conditions
            .iter()
            .find(|condition| condition.condition_type.0 == "RuntimeRestart")
            .map(|condition| condition.reason.0.as_str()),
        Some("RestartLimitReached")
    );
    Ok(())
}

#[tokio::test]
async fn assignment_run_restarts_an_exit_with_one_persistent_event_subscription()
-> Result<(), Box<dyn std::error::Error>> {
    let world = World::new();
    world.seed(&deployment(), &assignment()).await?;
    world.agent().reconcile_once().await?;
    let handle = world
        .runtime
        .list(&cluster_id(), &node_id("node-1"))
        .await?
        .into_iter()
        .next()
        .ok_or("workload missing")?
        .handle;
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let agent = world.agent();
    let task = tokio::spawn(async move { agent.run(shutdown_rx).await });
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let subscriptions = world
                .runtime
                .calls()
                .unwrap()
                .into_iter()
                .filter(|call| call.operation == FakeRuntimeOperation::Events)
                .count();
            if subscriptions == 1 {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await?;

    world
        .runtime
        .stop(
            &handle,
            ShutdownRequest {
                timeout: Duration::from_secs(5),
            },
        )
        .await?;
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let replica = world.load_replica().await.unwrap();
            if replica.status.restart_pending_attempt == Some(1) {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await?;
    world.status_clock.advance(Duration::from_secs(5));
    world.monotonic_clock.advance(Duration::from_secs(5));
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let replica = world.load_replica().await.unwrap();
            if replica.status.restart_attempts == 1
                && replica.status.restart_pending_attempt.is_none()
            {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await?;

    shutdown_tx.send(true)?;
    tokio::time::timeout(Duration::from_secs(1), task).await???;
    let starts = world
        .runtime
        .calls()?
        .into_iter()
        .filter(|call| call.operation == FakeRuntimeOperation::Start)
        .count();
    assert_eq!(starts, 2);
    let subscriptions = world
        .runtime
        .calls()?
        .into_iter()
        .filter(|call| call.operation == FakeRuntimeOperation::Events)
        .count();
    assert_eq!(subscriptions, 1);
    Ok(())
}

#[tokio::test]
async fn assignment_run_retries_a_transient_whole_snapshot_failure()
-> Result<(), Box<dyn std::error::Error>> {
    let world = World::new();
    world.seed(&deployment(), &assignment()).await?;
    world.runtime.fail_next(
        FakeRuntimeOperation::List,
        RuntimeError::Unavailable {
            message: "injected list outage".to_owned(),
        },
    )?;
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let agent = world.agent();
    let task = tokio::spawn(async move { agent.run(shutdown_rx).await });
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let lists = world
                .runtime
                .calls()
                .unwrap()
                .into_iter()
                .filter(|call| call.operation == FakeRuntimeOperation::List)
                .count();
            if lists >= 1 {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await?;

    world.monotonic_clock.advance(Duration::from_secs(1));
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let assignment = world.load_assignment().await.unwrap();
            if assignment.status.phase == AssignmentPhase::Running {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await?;
    let lists = world
        .runtime
        .calls()?
        .into_iter()
        .filter(|call| call.operation == FakeRuntimeOperation::List)
        .count();
    assert!(lists >= 2);
    assert_running(&world).await?;

    shutdown_tx.send(true)?;
    tokio::time::timeout(Duration::from_secs(1), task).await???;
    Ok(())
}

#[tokio::test]
async fn assignment_reconcile_mounts_and_cleans_private_secret_files()
-> Result<(), Box<dyn std::error::Error>> {
    let world = World::new();
    let mut deployment = deployment();
    deployment.spec.service.secrets = Some(SecretMountSpec::Dotenv {
        mount_path: "/run/secrets/maestro.env".to_owned(),
        source: None,
        items: BTreeMap::from([("TOKEN".to_owned(), SecretValue::new("sensitive"))]),
    });
    world.seed(&deployment, &assignment()).await?;
    world.agent().reconcile_once().await?;
    let secret_path = world.secrets.path().join("assignment-1/secrets.env");
    assert_eq!(
        std::fs::read_to_string(&secret_path)?,
        "TOKEN=\"sensitive\"\n"
    );

    let key = world.assignment_key();
    let stored = world.store.get(&key).await?.ok_or("assignment missing")?;
    world
        .store
        .delete_cas(DeleteRequest {
            key,
            expected: stored.version,
        })
        .await?;
    world.agent().reconcile_once().await?;
    assert!(!secret_path.exists());
    Ok(())
}

#[tokio::test]
async fn assignment_resolves_external_values_only_at_workload_creation()
-> Result<(), Box<dyn std::error::Error>> {
    let world = World::new();
    let mut deployment = deployment();
    deployment.spec.service.environment.clear();
    deployment.spec.service.environment_sources =
        vec!["aws-secret://runtime-environment".to_owned()];
    deployment.spec.service.secrets = Some(SecretMountSpec::Dotenv {
        mount_path: "/run/secrets/maestro.env".to_owned(),
        source: Some("aws-secret://runtime-secrets".to_owned()),
        items: BTreeMap::new(),
    });
    world.seed(&deployment, &assignment()).await?;

    let stored_before = world.load_deployment().await?;
    let encoded_before = serde_json::to_string(&stored_before)?;
    assert!(encoded_before.contains("aws-secret://runtime-environment"));
    assert!(encoded_before.contains("aws-secret://runtime-secrets"));
    assert!(!encoded_before.contains("resolved-mode"));
    assert!(!encoded_before.contains("resolved-token"));

    let resolver = Arc::new(FakeValueSources {
        values: BTreeMap::from([
            (
                "aws-secret://runtime-environment".to_owned(),
                BTreeMap::from([("MODE".to_owned(), SecretValue::new("resolved-mode"))]),
            ),
            (
                "aws-secret://runtime-secrets".to_owned(),
                BTreeMap::from([("TOKEN".to_owned(), SecretValue::new("resolved-token"))]),
            ),
        ]),
        calls: AtomicU64::new(0),
    });
    let agent = world.agent().with_value_source_resolver(resolver.clone());
    agent.reconcile_once().await?;
    agent.reconcile_once().await?;

    assert_eq!(resolver.calls.load(Ordering::SeqCst), 2);
    assert_eq!(
        std::fs::read_to_string(world.secrets.path().join("assignment-1/secrets.env"))?,
        "TOKEN=\"resolved-token\"\n"
    );
    let spec = world
        .runtime
        .workload_spec(&kernel_api::WorkloadId::new("assignment-1")?)?
        .ok_or("workload spec missing")?;
    let WorkloadSpec::Container(workload) = spec else {
        return Err("expected container workload".into());
    };
    assert_eq!(
        workload
            .configuration
            .environment
            .get("MODE")
            .map(SecretValue::expose),
        Some("resolved-mode")
    );

    let encoded_after = serde_json::to_string(&world.load_deployment().await?)?;
    assert!(encoded_after.contains("aws-secret://runtime-environment"));
    assert!(encoded_after.contains("aws-secret://runtime-secrets"));
    assert!(!encoded_after.contains("resolved-mode"));
    assert!(!encoded_after.contains("resolved-token"));
    Ok(())
}

struct FakeValueSources {
    values: BTreeMap<String, BTreeMap<String, SecretValue>>,
    calls: AtomicU64,
}

#[async_trait]
impl ValueSourceResolver for FakeValueSources {
    async fn resolve(
        &self,
        source: &str,
    ) -> Result<BTreeMap<String, SecretValue>, ValueSourceError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.values
            .get(source)
            .cloned()
            .ok_or_else(|| ValueSourceError::Rejected {
                message: format!("missing fake source `{source}`"),
            })
    }
}

pub(crate) fn cluster_id() -> ClusterId {
    ClusterId::new("cluster-1").unwrap()
}

pub(crate) fn node_id(value: &str) -> NodeId {
    NodeId::new(value).unwrap()
}

pub(crate) fn assignment() -> Assignment {
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
            node_id: node_id("node-1"),
            placement_epoch: 1,
            workload_address: Some(IpAddr::V4(Ipv4Addr::new(10, 42, 1, 8))),
            replaces_assignment_id: None,
        },
        status: AssignmentStatus {
            phase: AssignmentPhase::Pending,
            workload_id: None,
            workload_address: None,
            conditions: Vec::new(),
        },
    }
}

pub(crate) fn deployment() -> Deployment {
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
            bypass_rollout_freeze: false,
            goal: kernel_api::DeploymentGoal::Run,
            service: ServiceSpec {
                name: "API".to_owned(),
                version: "1.0.0".to_owned(),
                artifact: ArtifactTemplate::Image {
                    reference: "registry.test/api:latest".to_owned(),
                },
                preview: None,
                command: None,
                replicas: 1,
                exposed_ports: vec![8080],
                health_check: None,
                max_restarts: Some(3),
                environment: BTreeMap::from([("MODE".to_owned(), "production".to_owned())]),
                environment_sources: Vec::new(),
                user: None,
                node_api: kernel_api::NodeApiAccess::Disabled,
                secrets: None,
                volumes: vec![VolumeMountSpec {
                    source: VolumeSource::HostPath {
                        path: "/srv/api".to_owned(),
                        node_id: node_id("node-1"),
                    },
                    target: "/data".to_owned(),
                    access: VolumeAccess::ReadOnly,
                }],
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
            image_digest: Some(
                "registry.test/api@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                    .to_owned(),
            ),
            conditions: Vec::new(),
        },
    }
}

async fn assert_running(world: &World) -> Result<(), Box<dyn std::error::Error>> {
    let assignment = world.load_assignment().await?;
    assert_eq!(assignment.status.phase, AssignmentPhase::Running);
    assert_eq!(
        assignment.status.workload_id.as_ref().map(|id| id.as_str()),
        Some("assignment-1")
    );
    assert_eq!(
        assignment.status.workload_address,
        assignment.spec.workload_address
    );
    assert_eq!(
        assignment
            .status
            .conditions
            .first()
            .map(|condition| condition.reason.0.as_str()),
        Some("WorkloadRunning")
    );
    Ok(())
}

pub(super) struct World {
    pub(super) store: Arc<InMemoryStore>,
    pub(super) runtime: Arc<FakeRuntime>,
    pub(super) network: Arc<FakeNetworkProvider>,
    monotonic_clock: Arc<TestMonotonicClock>,
    status_clock: Arc<TestStatusClock>,
    secrets: tempfile::TempDir,
    node_api: tempfile::TempDir,
}

impl World {
    pub(super) fn new() -> Self {
        let monotonic_clock = Arc::new(TestMonotonicClock::default());
        Self {
            store: Arc::new(InMemoryStore::new(monotonic_clock.clone())),
            runtime: Arc::new(FakeRuntime::new()),
            network: Arc::new(FakeNetworkProvider::default()),
            monotonic_clock,
            status_clock: Arc::new(TestStatusClock::new(1_750_000_000_000)),
            secrets: tempfile::tempdir().unwrap(),
            node_api: tempfile::tempdir().unwrap(),
        }
    }

    pub(super) fn agent(&self) -> AssignmentAgent {
        self.agent_with_node_api(None)
    }

    fn agent_delegated(&self) -> AssignmentAgent {
        self.agent_with_network(
            NetworkSpec {
                name: "maestro-dev".to_owned(),
                addressing: NetworkAddressing::Delegated,
                mtu_bytes: 1_500,
            },
            None,
            None,
        )
    }

    fn agent_with_node_api(&self, services: Option<NodeApiServices>) -> AssignmentAgent {
        self.agent_with_network(
            NetworkSpec {
                name: "maestro-node-1".to_owned(),
                addressing: NetworkAddressing::Managed {
                    range: NetworkCidr::new(IpAddr::V4(Ipv4Addr::new(10, 42, 1, 0)), 24).unwrap(),
                    gateway: IpAddr::V4(Ipv4Addr::new(10, 42, 1, 1)),
                },
                mtu_bytes: 1_420,
            },
            Some(IpAddr::V4(Ipv4Addr::new(10, 42, 1, 1))),
            services,
        )
    }

    fn agent_with_network(
        &self,
        network_spec: NetworkSpec,
        dns_server: Option<IpAddr>,
        services: Option<NodeApiServices>,
    ) -> AssignmentAgent {
        let runtime: Arc<dyn WorkloadRuntime> = self.runtime.clone();
        let network: Arc<dyn NetworkProvider> = self.network.clone();
        AssignmentAgent::new(
            self.store.clone(),
            runtime,
            network,
            AssignmentAgentSettings {
                cluster_id: cluster_id(),
                node_id: node_id("node-1"),
                network: network_spec,
                dns: dns_server.map_or(WorkloadDns::Disabled, WorkloadDns::Static),
                system_host_ports: Default::default(),
                stop_timeout: Duration::from_secs(5),
                resync_interval: Duration::from_secs(30),
                restart_backoff_base: Duration::from_secs(5),
                restart_backoff_max: Duration::from_secs(60),
                reconcile_timeout: Duration::from_secs(10),
                secrets_root: self.secrets.path().to_path_buf(),
                node_api_root: self.node_api.path().join("mounts"),
            },
            services,
            self.monotonic_clock.clone(),
            self.status_clock.clone(),
        )
        .unwrap()
    }

    pub(super) async fn seed(
        &self,
        deployment: &Deployment,
        assignment: &Assignment,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.seed_without_replica(deployment, assignment).await?;
        put_resource(
            self.store.as_ref(),
            self.replica_key(),
            serde_json::to_vec(&replica(assignment))?,
        )
        .await
    }

    async fn seed_without_replica(
        &self,
        deployment: &Deployment,
        assignment: &Assignment,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let keyspace = Keyspace::new(&cluster_id());
        put_resource(
            self.store.as_ref(),
            keyspace.resource(
                &ResourceKind::new("Deployment")?,
                &ResourceName::new(deployment.meta.id.as_str())?,
            ),
            serde_json::to_vec(deployment)?,
        )
        .await?;
        put_resource(
            self.store.as_ref(),
            self.assignment_key(),
            serde_json::to_vec(assignment)?,
        )
        .await
    }

    async fn load_assignment(&self) -> Result<Assignment, Box<dyn std::error::Error>> {
        let stored = self
            .store
            .get(&self.assignment_key())
            .await?
            .ok_or("assignment missing")?;
        Ok(serde_json::from_slice(&stored.value)?)
    }

    async fn load_deployment(&self) -> Result<Deployment, Box<dyn std::error::Error>> {
        let key = Keyspace::new(&cluster_id()).resource(
            &ResourceKind::new("Deployment")?,
            &ResourceName::new("deployment-1")?,
        );
        let stored = self.store.get(&key).await?.ok_or("deployment missing")?;
        Ok(serde_json::from_slice(&stored.value)?)
    }

    async fn load_replica(&self) -> Result<ReplicaState, Box<dyn std::error::Error>> {
        let stored = self
            .store
            .get(&self.replica_key())
            .await?
            .ok_or("replica missing")?;
        Ok(serde_json::from_slice(&stored.value)?)
    }

    pub(super) fn assignment_key(&self) -> kernel_store::StoreKey {
        Keyspace::new(&cluster_id()).resource(
            &ResourceKind::new("Assignment").unwrap(),
            &ResourceName::new("assignment-1").unwrap(),
        )
    }

    fn replica_key(&self) -> kernel_store::StoreKey {
        Keyspace::new(&cluster_id()).resource(
            &ResourceKind::new("ReplicaState").unwrap(),
            &ResourceName::new("replica-1").unwrap(),
        )
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

pub(super) async fn put_resource(
    store: &dyn Store,
    key: kernel_store::StoreKey,
    value: Vec<u8>,
) -> Result<(), Box<dyn std::error::Error>> {
    store
        .put_cas(PutRequest {
            key,
            value,
            expected: ExpectedVersion::Missing,
            session: None,
        })
        .await?;
    Ok(())
}

#[derive(Default)]
struct TestMonotonicClock {
    milliseconds: AtomicU64,
    changed: Notify,
}

impl TestMonotonicClock {
    fn advance(&self, duration: Duration) {
        let milliseconds = u64::try_from(duration.as_millis()).unwrap();
        self.milliseconds.fetch_add(milliseconds, Ordering::SeqCst);
        self.changed.notify_waiters();
    }
}

#[async_trait]
impl Clock for TestMonotonicClock {
    fn now(&self) -> MonotonicTime {
        MonotonicTime::from_duration(Duration::from_millis(
            self.milliseconds.load(Ordering::SeqCst),
        ))
    }

    async fn sleep_until(&self, deadline: MonotonicTime) {
        loop {
            let changed = self.changed.notified();
            if self.now() >= deadline {
                return;
            }
            changed.await;
        }
    }
}

struct TestStatusClock(AtomicI64);

impl TestStatusClock {
    fn new(milliseconds: i64) -> Self {
        Self(AtomicI64::new(milliseconds))
    }

    fn advance(&self, duration: Duration) {
        let milliseconds = i64::try_from(duration.as_millis()).unwrap();
        self.0.fetch_add(milliseconds, Ordering::SeqCst);
    }
}

impl StatusClock for TestStatusClock {
    fn now(&self) -> Timestamp {
        Timestamp(self.0.load(Ordering::SeqCst))
    }
}
