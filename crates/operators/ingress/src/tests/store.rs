use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use kernel_api::{
    ClusterId, IngressRoute, NodeId, NodeInstanceId, ResourceKind, ResourceName, Service,
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
use crate::{
    BackendChange, IngressBackend, IngressBackendError, IngressController, IngressReconciler,
    IngressSettings,
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
    fail: AtomicBool,
}

impl RecordingBackend {
    fn changes(&self) -> Vec<BackendChange> {
        self.changes.lock().expect("backend changes").clone()
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
        world.seed().await?;
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
        self.controller.reconcile_once(&self.fenced, now).await
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
