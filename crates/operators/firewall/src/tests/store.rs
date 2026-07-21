use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use kernel_api::{FirewallPolicy, NodeId, NodeInstanceId, ResourceKind, ResourceName, Timestamp};
use kernel_controller::{Backoff, FencedStore, LeaderIdentity, LeadershipToken, RuntimeConfig};
use kernel_store::{
    CasOutcome, Clock, ExpectedVersion, InMemoryStore, Keyspace, MonotonicTime, PutRequest,
    Session, SessionBinding, Store,
};

use super::plan::World as PlannedWorld;
use crate::{
    FirewallBackend, FirewallBackendError, FirewallBaselineReconciler, FirewallBundle,
    FirewallController, FirewallError, FirewallInput, FirewallPolicyReconciler,
};

#[tokio::test]
async fn controller_applies_exact_bundle_before_acknowledging_policies()
-> Result<(), Box<dyn std::error::Error>> {
    let backend = Arc::new(RecordingBackend::default());
    let world = StoreWorld::new(PlannedWorld::standard().input(), backend.clone()).await?;

    let report = world.controller.reconcile_once(&world.fenced).await?;

    assert_eq!((report.applied_rulesets, report.updated_policies), (2, 3));
    assert_eq!(backend.bundles().len(), 1);
    assert_eq!(backend.bundles()[0].digest, report.bundle_digest);
    for policy in world.list::<FirewallPolicy>("FirewallPolicy").await? {
        assert_eq!(policy.status.applied_generation, policy.meta.generation);
        assert_eq!(
            policy.status.ruleset_digest,
            Some(report.bundle_digest.clone())
        );
    }
    Ok(())
}

#[tokio::test]
async fn backend_failure_does_not_acknowledge_policy_status()
-> Result<(), Box<dyn std::error::Error>> {
    let backend = Arc::new(RecordingBackend::default());
    backend.fail.store(true, Ordering::SeqCst);
    let world = StoreWorld::new(PlannedWorld::standard().input(), backend).await?;

    assert!(matches!(
        world.controller.reconcile_once(&world.fenced).await,
        Err(FirewallError::Backend(_))
    ));
    for policy in world.list::<FirewallPolicy>("FirewallPolicy").await? {
        assert_ne!(policy.status.applied_generation, policy.meta.generation);
        assert!(policy.status.ruleset_digest.is_none());
    }
    Ok(())
}

#[tokio::test]
async fn backend_ahead_of_store_conflict_is_replayed_before_acknowledgement()
-> Result<(), Box<dyn std::error::Error>> {
    let backend = Arc::new(RacingBackend::default());
    let world = StoreWorld::new(PlannedWorld::standard().input(), backend.clone()).await?;
    backend.arm(
        world.store.clone(),
        world.key("FirewallPolicy", "api-egress")?,
    );

    let conflicted = world.controller.reconcile_once(&world.fenced).await?;
    assert!(conflicted.conflict);
    assert!(
        world
            .policy("api-egress")
            .await?
            .status
            .ruleset_digest
            .is_none()
    );

    let replayed = world.controller.reconcile_once(&world.fenced).await?;
    assert!(!replayed.conflict);
    assert_eq!(backend.applies.load(Ordering::SeqCst), 2);
    assert_eq!(
        world.policy("api-egress").await?.status.ruleset_digest,
        Some(replayed.bundle_digest)
    );
    Ok(())
}

#[tokio::test]
async fn policy_finalizer_applies_bundle_without_deleting_policy()
-> Result<(), Box<dyn std::error::Error>> {
    let backend = Arc::new(RecordingBackend::default());
    let mut input = PlannedWorld::standard().input();
    input
        .policies
        .retain(|policy| policy.meta.id.as_str() == "api-egress");
    let world = StoreWorld::new(input, backend.clone()).await?;
    let reconciler = Arc::new(FirewallPolicyReconciler::new(world.controller.clone())?);
    let runtime = reconciler.runtime(
        Arc::new(world.fenced.clone()),
        Arc::new(NoopClock),
        runtime_config()?,
    );
    assert_eq!(runtime.reconcile_snapshot().await?, 0);
    assert_eq!(runtime.reconcile_snapshot().await?, 1);
    world
        .update::<FirewallPolicy>("FirewallPolicy", "api-egress", |policy| {
            policy.meta.deletion_timestamp = Some(Timestamp(1_000));
        })
        .await?;

    assert_eq!(runtime.reconcile_snapshot().await?, 1);
    assert!(
        world
            .list::<FirewallPolicy>("FirewallPolicy")
            .await?
            .is_empty()
    );
    let last = backend.bundles().pop().ok_or("backend was not invoked")?;
    assert!(
        last.rulesets
            .iter()
            .all(|ruleset| !ruleset.script.contains("192.0.2.0/24"))
    );
    Ok(())
}

#[tokio::test]
async fn baseline_reconciler_installs_guards_without_user_policies()
-> Result<(), Box<dyn std::error::Error>> {
    let backend = Arc::new(RecordingBackend::default());
    let mut input = PlannedWorld::standard().input();
    input.policies.clear();
    let world = StoreWorld::new(input, backend.clone()).await?;
    let reconciler = Arc::new(FirewallBaselineReconciler::new(world.controller.clone())?);
    let runtime = reconciler.runtime(
        Arc::new(world.fenced.clone()),
        Arc::new(NoopClock),
        runtime_config()?,
    );

    assert_eq!(runtime.reconcile_snapshot().await?, 2);
    let bundles = backend.bundles();
    assert_eq!(bundles.len(), 2);
    assert_eq!(bundles[0].rulesets.len(), 2);
    for ruleset in &bundles[0].rulesets {
        assert!(ruleset.script.contains("tcp dport 53 accept"));
        assert!(ruleset.script.contains("tcp dport { 3000, 3001 } reject"));
        assert!(ruleset.script.contains("ip saddr @all_workloads_v4 reject"));
    }
    Ok(())
}

#[derive(Default)]
struct RecordingBackend {
    bundles: Mutex<Vec<FirewallBundle>>,
    fail: AtomicBool,
}

impl RecordingBackend {
    fn bundles(&self) -> Vec<FirewallBundle> {
        self.bundles.lock().expect("backend bundles").clone()
    }
}

#[async_trait]
impl FirewallBackend for RecordingBackend {
    async fn apply(&self, bundle: &FirewallBundle) -> Result<(), FirewallBackendError> {
        if self.fail.load(Ordering::SeqCst) {
            return Err(FirewallBackendError::new("injected application failure"));
        }
        self.bundles
            .lock()
            .expect("backend bundles")
            .push(bundle.clone());
        Ok(())
    }
}

#[derive(Default)]
struct RacingBackend {
    race: Mutex<Option<(Arc<InMemoryStore>, kernel_store::StoreKey)>>,
    applies: AtomicUsize,
}

impl RacingBackend {
    fn arm(&self, store: Arc<InMemoryStore>, key: kernel_store::StoreKey) {
        *self.race.lock().expect("race state") = Some((store, key));
    }
}

#[async_trait]
impl FirewallBackend for RacingBackend {
    async fn apply(&self, _bundle: &FirewallBundle) -> Result<(), FirewallBackendError> {
        self.applies.fetch_add(1, Ordering::SeqCst);
        let race = self.race.lock().expect("race state").take();
        if let Some((store, key)) = race {
            let stored = store
                .get(&key)
                .await
                .map_err(|error| FirewallBackendError::new(error.to_string()))?
                .ok_or_else(|| FirewallBackendError::new("policy disappeared"))?;
            let outcome = store
                .put_cas(PutRequest {
                    key,
                    value: stored.value,
                    expected: ExpectedVersion::Exact(stored.version),
                    session: None,
                })
                .await
                .map_err(|error| FirewallBackendError::new(error.to_string()))?;
            if !matches!(outcome, CasOutcome::Applied(_)) {
                return Err(FirewallBackendError::new("injected update conflicted"));
            }
        }
        Ok(())
    }
}

struct StoreWorld {
    keys: Keyspace,
    store: Arc<InMemoryStore>,
    fenced: FencedStore,
    controller: Arc<FirewallController>,
    _session: Box<dyn Session>,
}

impl StoreWorld {
    async fn new(
        input: FirewallInput,
        backend: Arc<dyn FirewallBackend>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let cluster_id = kernel_api::ClusterId::new("cluster-1")?;
        let keys = Keyspace::new(&cluster_id);
        let store = Arc::new(InMemoryStore::new(Arc::new(NoopClock)));
        let session = store.session(Duration::from_secs(30)).await?;
        let leader = store
            .put_cas(PutRequest {
                key: keys.leader(),
                value: b"firewall-controller".to_vec(),
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
                    instance_id: NodeInstanceId::new("firewall-controller")?,
                },
                session.id(),
                leader.version,
            ),
        );
        let controller = Arc::new(FirewallController::new(
            cluster_id,
            input.settings.clone(),
            backend,
        ));
        let world = Self {
            keys,
            store,
            fenced,
            controller,
            _session: session,
        };
        world.seed(input).await?;
        Ok(world)
    }

    async fn seed(&self, input: FirewallInput) -> Result<(), Box<dyn std::error::Error>> {
        for policy in &input.policies {
            self.put("FirewallPolicy", &policy.meta.id, policy).await?;
        }
        for service in &input.services {
            self.put("Service", &service.meta.id, service).await?;
        }
        for assignment in &input.assignments {
            self.put("Assignment", &assignment.meta.id, assignment)
                .await?;
        }
        for network in &input.node_networks {
            self.put("NodeNetwork", &network.meta.id, network).await?;
        }
        Ok(())
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

    async fn policy(&self, id: &str) -> Result<FirewallPolicy, Box<dyn std::error::Error>> {
        let stored = self.store.get(&self.key("FirewallPolicy", id)?).await?;
        Ok(serde_json::from_slice(
            &stored.ok_or("policy missing")?.value,
        )?)
    }
}

fn runtime_config() -> Result<RuntimeConfig, Box<dyn std::error::Error>> {
    Ok(RuntimeConfig::new(
        Duration::from_secs(60),
        Backoff::new(Duration::from_millis(10), Duration::from_secs(1))?,
    )?)
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
