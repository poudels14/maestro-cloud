use std::collections::{BTreeMap, BTreeSet};
use std::net::{IpAddr, Ipv4Addr};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use kernel_api::{
    AssignmentId, ClusterId, Generation, IngressBlocklist, IngressBlocklistId,
    IngressBlocklistSpec, IngressBlocklistStatus, IngressRoute, NodeId, NodeInstanceId, Object,
    ObjectMeta, ReplicaStateId, ResourceKind, ResourceName, ResourceRevision, Service, ServiceId,
    Timestamp, TrafficGeneration, TrafficGenerationPhase,
};
use kernel_controller::{
    Backoff, FencedStore, LeaderIdentity, LeadershipToken, RuntimeConfig, TimestampClock,
};
use kernel_store::{
    CasOutcome, Clock, ExpectedVersion, InMemoryStore, Keyspace, MonotonicTime, PutRequest,
    Session, SessionBinding, Store,
};

use super::plan::World as PlannedWorld;
use crate::snapshot::ResourceSnapshot;
use crate::{
    BackendChange, IngressBackend, IngressBackendError, IngressBlocklistChange,
    IngressBlocklistReconciler, IngressController, IngressReconciler, IngressSettings,
};

#[tokio::test]
async fn store_controller_stages_then_publishes_and_acknowledges_traffic()
-> Result<(), Box<dyn std::error::Error>> {
    let backend = Arc::new(RecordingBackend::default());
    let world = StoreWorld::new(backend.clone()).await?;

    let staged = world.reconcile(Timestamp(1_000)).await?;
    assert_eq!(staged.created_generations, 1);
    let generation = world.one::<TrafficGeneration>("TrafficGeneration").await?;
    assert_eq!(generation.status.phase, TrafficGenerationPhase::Staged);
    assert!(backend.changes()[0].active.is_none());

    let active = world.reconcile(Timestamp(2_000)).await?;
    assert_eq!((active.updated_generations, active.updated_routes), (1, 1));
    let generation = world.one::<TrafficGeneration>("TrafficGeneration").await?;
    let route = world.one::<IngressRoute>("IngressRoute").await?;
    assert_eq!(generation.status.phase, TrafficGenerationPhase::Active);
    assert_eq!(generation.status.activated_at, Some(Timestamp(2_000)));
    assert_eq!(route.status.applied_generation, route.meta.generation);
    assert_eq!(
        backend.changes()[1]
            .active
            .as_ref()
            .expect("published traffic")
            .generation_id,
        generation.meta.id
    );
    Ok(())
}

#[tokio::test]
async fn backend_failure_does_not_acknowledge_a_staged_generation()
-> Result<(), Box<dyn std::error::Error>> {
    let backend = Arc::new(RecordingBackend::default());
    let world = StoreWorld::new(backend.clone()).await?;
    world.reconcile(Timestamp(1_000)).await?;
    backend.fail.store(true, Ordering::SeqCst);

    assert!(matches!(
        world.reconcile(Timestamp(2_000)).await,
        Err(crate::IngressError::Backend(_))
    ));
    let generation = world.one::<TrafficGeneration>("TrafficGeneration").await?;
    let route = world.one::<IngressRoute>("IngressRoute").await?;
    assert_eq!(generation.status.phase, TrafficGenerationPhase::Staged);
    assert_ne!(route.status.applied_generation, route.meta.generation);
    Ok(())
}

#[tokio::test]
async fn singleton_runtime_publishes_and_acknowledges_without_any_services()
-> Result<(), Box<dyn std::error::Error>> {
    let backend = Arc::new(RecordingBackend::default());
    let world = StoreWorld::empty(backend.clone()).await?;
    let blocklist = Object {
        meta: ObjectMeta {
            id: IngressBlocklistId::new("global")?,
            labels: BTreeMap::new(),
            annotations: BTreeMap::new(),
            revision: ResourceRevision::default(),
            generation: Generation(4),
            owner_refs: Vec::new(),
            finalizers: BTreeSet::new(),
            deletion_timestamp: None,
        },
        spec: IngressBlocklistSpec {
            addresses: vec![IpAddr::V4(Ipv4Addr::new(203, 0, 113, 44))],
        },
        status: IngressBlocklistStatus {
            applied_generation: Generation::default(),
            configuration_digest: None,
            conditions: Vec::new(),
        },
    };
    world
        .put("IngressBlocklist", &blocklist.meta.id, &blocklist)
        .await?;
    let reconciler = Arc::new(IngressBlocklistReconciler::new(
        world.cluster_id.clone(),
        settings(),
        backend.clone(),
        Arc::new(ManualTimestampClock::new(1_000)),
    )?);
    let runtime = reconciler.runtime(
        Arc::new(world.fenced.clone()),
        Arc::new(NoopClock),
        RuntimeConfig::new(
            Duration::from_secs(60),
            Backoff::new(Duration::from_millis(10), Duration::from_secs(1))?,
        )?,
    );

    assert_eq!(runtime.reconcile_snapshot().await?, 0);
    assert_eq!(runtime.reconcile_snapshot().await?, 1);
    let published = backend.blocklists();
    assert_eq!(published.len(), 1);
    assert_eq!(published[0].generation, Generation(4));
    assert_eq!(published[0].addresses, blocklist.spec.addresses);
    let stored = world.one::<IngressBlocklist>("IngressBlocklist").await?;
    assert_eq!(stored.status.applied_generation, Generation(4));
    assert_eq!(
        stored.status.configuration_digest,
        Some(published[0].configuration_digest.clone())
    );
    Ok(())
}

#[tokio::test]
async fn backend_ahead_of_a_store_conflict_is_replayed_and_acknowledged()
-> Result<(), Box<dyn std::error::Error>> {
    let backend = Arc::new(RacingBackend::default());
    let world = StoreWorld::new(backend.clone()).await?;
    world.reconcile(Timestamp(1_000)).await?;
    let route_key = world.key("IngressRoute", "route-api")?;
    backend.arm(world.store.clone(), route_key);

    let conflicted = world.reconcile(Timestamp(2_000)).await?;
    assert!(conflicted.conflict);
    assert_eq!(
        world
            .one::<TrafficGeneration>("TrafficGeneration")
            .await?
            .status
            .phase,
        TrafficGenerationPhase::Staged
    );

    let replayed = world.reconcile(Timestamp(3_000)).await?;
    assert!(!replayed.conflict);
    assert_eq!(backend.active_applies.load(Ordering::SeqCst), 2);
    assert_eq!(
        world
            .one::<TrafficGeneration>("TrafficGeneration")
            .await?
            .status
            .phase,
        TrafficGenerationPhase::Active
    );
    Ok(())
}

#[tokio::test]
async fn service_snapshot_excludes_unrelated_replica_cardinality()
-> Result<(), Box<dyn std::error::Error>> {
    let backend = Arc::new(RecordingBackend::default());
    let world = StoreWorld::new(backend).await?;
    let planned = PlannedWorld::ready();
    let mut unrelated = planned.replicas[0].clone();
    unrelated.spec.service_id = ServiceId::new("unrelated")?;
    for index in 0..130 {
        unrelated.meta.id = ReplicaStateId::new(format!("unrelated-{index}"))?;
        unrelated.spec.assignment_id = AssignmentId::new(format!("unrelated-{index}"))?;
        world
            .put("ReplicaState", &unrelated.meta.id, &unrelated)
            .await?;
    }

    let service_id = ServiceId::new("api")?;
    let snapshot = ResourceSnapshot::load_service(&world.fenced, &world.keys, &service_id).await?;

    assert_eq!(snapshot.services.len(), 1);
    assert_eq!(snapshot.replicas.len(), planned.replicas.len());
    assert_eq!(snapshot.primary_compares().len(), 1);
    assert_eq!(
        world.reconcile(Timestamp(1_000)).await?.created_generations,
        1
    );
    Ok(())
}

#[tokio::test]
async fn runtime_holds_service_finalizer_until_retirement_deadline()
-> Result<(), Box<dyn std::error::Error>> {
    let backend = Arc::new(RecordingBackend::default());
    let world = StoreWorld::new(backend.clone()).await?;
    let wall = Arc::new(ManualTimestampClock::new(5_000));
    let reconciler = Arc::new(IngressReconciler::new(
        world.cluster_id.clone(),
        settings(),
        backend,
        wall.clone(),
    )?);
    let runtime = reconciler.runtime(
        Arc::new(world.fenced.clone()),
        Arc::new(NoopClock),
        RuntimeConfig::new(
            Duration::from_secs(60),
            Backoff::new(Duration::from_millis(10), Duration::from_secs(1))?,
        )?,
    );
    assert_eq!(runtime.reconcile_snapshot().await?, 0);
    assert_eq!(runtime.reconcile_snapshot().await?, 1);
    assert_eq!(runtime.reconcile_snapshot().await?, 1);
    world
        .update::<Service>("Service", "api", |service| {
            service.meta.deletion_timestamp = Some(Timestamp(5_000));
        })
        .await?;

    assert_eq!(runtime.reconcile_snapshot().await?, 1);
    assert_eq!(world.list::<Service>("Service").await?.len(), 1);
    assert_eq!(
        world
            .one::<TrafficGeneration>("TrafficGeneration")
            .await?
            .status
            .phase,
        TrafficGenerationPhase::Retired
    );

    wall.set(35_000);
    assert_eq!(runtime.reconcile_snapshot().await?, 1);
    assert!(world.list::<Service>("Service").await?.is_empty());
    assert!(
        world
            .list::<TrafficGeneration>("TrafficGeneration")
            .await?
            .is_empty()
    );
    Ok(())
}

#[derive(Default)]
struct RecordingBackend {
    changes: Mutex<Vec<BackendChange>>,
    blocklists: Mutex<Vec<IngressBlocklistChange>>,
    fail: AtomicBool,
}

impl RecordingBackend {
    fn changes(&self) -> Vec<BackendChange> {
        self.changes.lock().expect("backend changes").clone()
    }

    fn blocklists(&self) -> Vec<IngressBlocklistChange> {
        self.blocklists.lock().expect("backend blocklists").clone()
    }
}

#[async_trait]
impl IngressBackend for RecordingBackend {
    async fn apply(&self, change: &BackendChange) -> Result<(), IngressBackendError> {
        if self.fail.load(Ordering::SeqCst) {
            return Err(IngressBackendError::new("injected publication failure"));
        }
        self.changes
            .lock()
            .expect("backend changes")
            .push(change.clone());
        Ok(())
    }

    async fn apply_blocklist(
        &self,
        change: &IngressBlocklistChange,
    ) -> Result<(), IngressBackendError> {
        if self.fail.load(Ordering::SeqCst) {
            Err(IngressBackendError::new("injected publication failure"))
        } else {
            self.blocklists
                .lock()
                .expect("backend blocklists")
                .push(change.clone());
            Ok(())
        }
    }
}

#[derive(Default)]
struct RacingBackend {
    race: Mutex<Option<(Arc<InMemoryStore>, kernel_store::StoreKey)>>,
    active_applies: std::sync::atomic::AtomicUsize,
}

impl RacingBackend {
    fn arm(&self, store: Arc<InMemoryStore>, key: kernel_store::StoreKey) {
        *self.race.lock().expect("race state") = Some((store, key));
    }
}

#[async_trait]
impl IngressBackend for RacingBackend {
    async fn apply(&self, change: &BackendChange) -> Result<(), IngressBackendError> {
        if change.active.is_none() {
            return Ok(());
        }
        self.active_applies.fetch_add(1, Ordering::SeqCst);
        let race = self.race.lock().expect("race state").take();
        if let Some((store, key)) = race {
            let stored = store
                .get(&key)
                .await
                .map_err(|error| IngressBackendError::new(error.to_string()))?
                .ok_or_else(|| IngressBackendError::new("route disappeared"))?;
            let route: IngressRoute = serde_json::from_slice(&stored.value)
                .map_err(|error| IngressBackendError::new(error.to_string()))?;
            let outcome = store
                .put_cas(PutRequest {
                    key,
                    value: serde_json::to_vec(&route)
                        .map_err(|error| IngressBackendError::new(error.to_string()))?,
                    expected: ExpectedVersion::Exact(stored.version),
                    session: None,
                })
                .await
                .map_err(|error| IngressBackendError::new(error.to_string()))?;
            if !matches!(outcome, CasOutcome::Applied(_)) {
                return Err(IngressBackendError::new("injected route update conflicted"));
            }
        }
        Ok(())
    }

    async fn apply_blocklist(
        &self,
        _change: &IngressBlocklistChange,
    ) -> Result<(), IngressBackendError> {
        Ok(())
    }
}

struct StoreWorld {
    cluster_id: ClusterId,
    keys: Keyspace,
    store: Arc<InMemoryStore>,
    fenced: FencedStore,
    controller: IngressController,
    _session: Box<dyn Session>,
}

impl StoreWorld {
    async fn new(backend: Arc<dyn IngressBackend>) -> Result<Self, Box<dyn std::error::Error>> {
        Self::build(backend, true).await
    }

    async fn empty(backend: Arc<dyn IngressBackend>) -> Result<Self, Box<dyn std::error::Error>> {
        Self::build(backend, false).await
    }

    async fn build(
        backend: Arc<dyn IngressBackend>,
        seed: bool,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let cluster_id = ClusterId::new("cluster-1")?;
        let keys = Keyspace::new(&cluster_id);
        let store = Arc::new(InMemoryStore::new(Arc::new(NoopClock)));
        let session = store.session(Duration::from_secs(30)).await?;
        let leader = store
            .put_cas(PutRequest {
                key: keys.leader(),
                value: b"ingress-controller".to_vec(),
                expected: ExpectedVersion::Missing,
                session: Some(SessionBinding {
                    session_id: session.id(),
                }),
            })
            .await?;
        let CasOutcome::Applied(leader) = leader else {
            return Err("leader campaign conflicted".into());
        };
        let fenced = FencedStore::new(
            store.clone(),
            keys.leader(),
            LeadershipToken::from_campaign(
                LeaderIdentity {
                    node_id: NodeId::new("node-1")?,
                    instance_id: NodeInstanceId::new("ingress-controller")?,
                },
                session.id(),
                leader.version,
            ),
        );
        let controller = IngressController::new(cluster_id.clone(), settings(), backend)?;
        let world = Self {
            cluster_id,
            keys,
            store,
            fenced,
            controller,
            _session: session,
        };
        if seed {
            world.seed().await?;
        }
        Ok(world)
    }

    async fn seed(&self) -> Result<(), Box<dyn std::error::Error>> {
        let planned = PlannedWorld::ready();
        self.put("Service", &planned.service.meta.id, &planned.service)
            .await?;
        self.put(
            "Deployment",
            &planned.deployment.meta.id,
            &planned.deployment,
        )
        .await?;
        for route in &planned.routes {
            self.put("IngressRoute", &route.meta.id, route).await?;
        }
        for assignment in &planned.assignments {
            self.put("Assignment", &assignment.meta.id, assignment)
                .await?;
        }
        for replica in &planned.replicas {
            self.put("ReplicaState", &replica.meta.id, replica).await?;
        }
        Ok(())
    }

    async fn reconcile(&self, now: Timestamp) -> Result<crate::IngressReport, crate::IngressError> {
        self.controller
            .reconcile_service(
                &self.fenced,
                &kernel_api::ServiceId::new("api").expect("fixture service id"),
                now,
            )
            .await
    }

    fn key(
        &self,
        kind: &str,
        id: &str,
    ) -> Result<kernel_store::StoreKey, kernel_api::InvalidIdentifier> {
        Ok(self.keys.resource(
            &ResourceKind::new(kind)?,
            &ResourceName::new(id.to_string())?,
        ))
    }

    async fn put<Id: Clone + Into<ResourceName>>(
        &self,
        kind: &str,
        id: &Id,
        resource: &impl serde::Serialize,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let outcome = self
            .store
            .put_cas(PutRequest {
                key: self
                    .keys
                    .resource(&ResourceKind::new(kind)?, &id.clone().into()),
                value: serde_json::to_vec(resource)?,
                expected: ExpectedVersion::Missing,
                session: None,
            })
            .await?;
        if matches!(outcome, CasOutcome::Applied(_)) {
            Ok(())
        } else {
            Err(format!("{kind} create conflicted").into())
        }
    }

    async fn update<Resource: serde::de::DeserializeOwned + serde::Serialize>(
        &self,
        kind: &str,
        id: &str,
        change: impl FnOnce(&mut Resource),
    ) -> Result<(), Box<dyn std::error::Error>> {
        let key = self.key(kind, id)?;
        let stored = self.store.get(&key).await?.ok_or("resource missing")?;
        let mut resource = serde_json::from_slice::<Resource>(&stored.value)?;
        change(&mut resource);
        let outcome = self
            .store
            .put_cas(PutRequest {
                key,
                value: serde_json::to_vec(&resource)?,
                expected: ExpectedVersion::Exact(stored.version),
                session: None,
            })
            .await?;
        if matches!(outcome, CasOutcome::Applied(_)) {
            Ok(())
        } else {
            Err(format!("{kind} update conflicted").into())
        }
    }

    async fn list<Resource: serde::de::DeserializeOwned>(
        &self,
        kind: &str,
    ) -> Result<Vec<Resource>, Box<dyn std::error::Error>> {
        self.store
            .list(&self.keys.resource_kind(&ResourceKind::new(kind)?))
            .await?
            .values
            .into_iter()
            .map(|stored| serde_json::from_slice(&stored.value).map_err(Into::into))
            .collect()
    }

    async fn one<Resource: serde::de::DeserializeOwned>(
        &self,
        kind: &str,
    ) -> Result<Resource, Box<dyn std::error::Error>> {
        let mut resources = self.list(kind).await?;
        if resources.len() != 1 {
            return Err(format!("expected one {kind}, found {}", resources.len()).into());
        }
        Ok(resources.remove(0))
    }
}

fn settings() -> IngressSettings {
    IngressSettings {
        retirement_grace: Duration::from_secs(30),
    }
}

struct ManualTimestampClock(AtomicI64);

impl ManualTimestampClock {
    fn new(millis: i64) -> Self {
        Self(AtomicI64::new(millis))
    }

    fn set(&self, millis: i64) {
        self.0.store(millis, Ordering::SeqCst);
    }
}

impl TimestampClock for ManualTimestampClock {
    fn now(&self) -> Timestamp {
        Timestamp(self.0.load(Ordering::SeqCst))
    }
}

struct NoopClock;

#[async_trait]
impl Clock for NoopClock {
    fn now(&self) -> MonotonicTime {
        MonotonicTime::default()
    }

    async fn sleep_until(&self, _deadline: MonotonicTime) {
        std::future::pending::<()>().await;
    }
}
