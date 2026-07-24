use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::net::{IpAddr, Ipv4Addr};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use async_trait::async_trait;
use kernel_api::{
    ArtifactTemplate, ClusterId, Deployment, DeploymentGoal, DeploymentId, DeploymentPhase,
    DeploymentSpec, DeploymentStatus, ExecPolicy, Generation, Node, NodeApiAccess, NodeId,
    NodeInstanceId, NodeRole, NodeSpec, NodeStatus, Object, ObjectMeta, PlacementConstraint,
    ResourceKind, ResourceName, ResourceRevision, SecretValue, ServiceId, ServiceSpec, Timestamp,
    Webhook, WebhookCategory, WebhookEvent, WebhookFormat, WebhookId, WebhookNodeAvailability,
    WebhookObservedState, WebhookSpec, WebhookStatus,
};
use kernel_controller::{
    Backoff, FencedStore, LeaderIdentity, LeadershipToken, RuntimeConfig, TimestampClock,
};
use kernel_store::{
    CasOutcome, Clock, ExpectedVersion, InMemoryStore, Keyspace, MonotonicTime, PutRequest,
    SessionBinding, Store,
};

use crate::{
    WebhookDelivery, WebhookDeliveryBackend, WebhookDeliveryError, WebhookReconciler,
    WebhookSettings,
};

#[tokio::test]
async fn transitions_baseline_retry_and_acknowledge_without_anonymous_duplicates()
-> Result<(), Box<dyn std::error::Error>> {
    let cluster_id = ClusterId::new("webhook-test")?;
    let keys = Keyspace::new(&cluster_id);
    let monotonic: Arc<dyn Clock> = Arc::new(NoopClock);
    let store = Arc::new(InMemoryStore::new(monotonic.clone()));
    let session = store.session(Duration::from_secs(30)).await?;
    let leader = store
        .put_cas(PutRequest {
            key: keys.leader(),
            value: b"webhook-test".to_vec(),
            expected: ExpectedVersion::Missing,
            session: Some(SessionBinding {
                session_id: session.id(),
            }),
        })
        .await?;
    let CasOutcome::Applied(leader) = leader else {
        return Err("leader campaign conflicted".into());
    };
    let fenced = Arc::new(FencedStore::new(
        store.clone(),
        keys.leader(),
        LeadershipToken::from_campaign(
            LeaderIdentity {
                node_id: kernel_api::NodeId::new("node-1")?,
                instance_id: kernel_api::NodeInstanceId::new("webhook-test")?,
            },
            session.id(),
            leader.version,
        ),
    ));
    let deployment = deployment(DeploymentPhase::Queued)?;
    put(&store, &keys, "Deployment", &deployment).await?;
    put(&store, &keys, "Node", &node()?).await?;
    put(&store, &keys, "Webhook", &webhook()?).await?;
    let timestamp = Arc::new(TestTimestamp::new(10_000));
    let backend = Arc::new(RecordingBackend::with_outcomes([
        Err(WebhookDeliveryError::Unavailable {
            message: "receiver is offline".to_string(),
        }),
        Ok(()),
    ]));
    let runtime = Arc::new(WebhookReconciler::new(
        cluster_id,
        backend.clone(),
        timestamp.clone(),
        WebhookSettings {
            retry_delay: Duration::from_secs(30),
        },
    )?)
    .runtime(
        fenced,
        monotonic,
        RuntimeConfig::new(
            Duration::from_secs(60),
            Backoff::new(Duration::from_millis(10), Duration::from_secs(1))?,
        )?,
    );

    assert_eq!(runtime.reconcile_snapshot().await?, 1);
    assert!(backend.deliveries().is_empty());
    let baseline: Webhook = get(&store, &keys, "Webhook", "deployments").await?;
    assert_eq!(baseline.status.observed_generation, Some(Generation(1)));
    assert_eq!(baseline.status.observations.len(), 2);

    replace_deployment(&store, &keys, DeploymentPhase::Ready).await?;
    assert_eq!(runtime.reconcile_snapshot().await?, 1);
    let failed: Webhook = get(&store, &keys, "Webhook", "deployments").await?;
    assert_eq!(failed.status.consecutive_failures, 1);
    assert_eq!(failed.status.retry_at, Some(Timestamp(40_000)));
    assert_eq!(backend.deliveries().len(), 1);

    assert_eq!(runtime.reconcile_snapshot().await?, 1);
    assert_eq!(backend.deliveries().len(), 1);
    timestamp.set(40_000);
    assert_eq!(runtime.reconcile_snapshot().await?, 1);
    let delivered = backend.deliveries();
    assert_eq!(delivered.len(), 2);
    let first = delivered.first().ok_or("first delivery is missing")?;
    let retried = delivered.get(1).ok_or("retried delivery is missing")?;
    assert_eq!(first.delivery_id, retried.delivery_id);
    assert_eq!(
        retried.previous,
        Some(kernel_api::WebhookObservedState::DeploymentTransition(
            DeploymentPhase::Queued
        ))
    );
    assert_eq!(
        retried.current,
        kernel_api::WebhookObservedState::DeploymentTransition(DeploymentPhase::Ready)
    );
    let succeeded: Webhook = get(&store, &keys, "Webhook", "deployments").await?;
    assert_eq!(succeeded.status.consecutive_failures, 0);
    assert_eq!(succeeded.status.retry_at, None);
    assert_eq!(succeeded.status.last_success_at, Some(Timestamp(40_000)));

    assert_eq!(runtime.reconcile_snapshot().await?, 1);
    assert_eq!(backend.deliveries().len(), 2);
    let liveness = store
        .put_cas(PutRequest {
            key: keys.node_liveness(&NodeId::new("node-1")?),
            value: b"instance-1".to_vec(),
            expected: ExpectedVersion::Missing,
            session: None,
        })
        .await?;
    assert!(matches!(liveness, CasOutcome::Applied(_)));
    assert_eq!(runtime.reconcile_snapshot().await?, 1);
    let delivered = backend.deliveries();
    let availability = delivered.get(2).ok_or("availability delivery is missing")?;
    assert_eq!(availability.event, WebhookEvent::NodeAvailability);
    assert_eq!(
        availability.previous,
        Some(WebhookObservedState::NodeAvailability(
            WebhookNodeAvailability::Unavailable
        ))
    );
    assert_eq!(
        availability.current,
        WebhookObservedState::NodeAvailability(WebhookNodeAvailability::Available)
    );

    replace_webhook_categories(&store, &keys, vec![WebhookCategory::Error]).await?;
    assert_eq!(runtime.reconcile_snapshot().await?, 1);
    replace_deployment(&store, &keys, DeploymentPhase::Draining).await?;
    assert_eq!(runtime.reconcile_snapshot().await?, 1);
    assert_eq!(backend.deliveries().len(), 3);
    let filtered: Webhook = get(&store, &keys, "Webhook", "deployments").await?;
    assert!(
        filtered
            .status
            .conditions
            .iter()
            .any(|condition| condition.reason.0 == "DeliveryFiltered")
    );

    replace_deployment(&store, &keys, DeploymentPhase::Crashed).await?;
    assert_eq!(runtime.reconcile_snapshot().await?, 1);
    let deliveries = backend.deliveries();
    let crashed = deliveries.get(3).ok_or("crash delivery is missing")?;
    assert_eq!(crashed.category(), WebhookCategory::Error);
    Ok(())
}

#[derive(Default)]
struct RecordingBackend {
    deliveries: Mutex<Vec<WebhookDelivery>>,
    outcomes: Mutex<VecDeque<Result<(), WebhookDeliveryError>>>,
}

impl RecordingBackend {
    fn with_outcomes(outcomes: impl IntoIterator<Item = Result<(), WebhookDeliveryError>>) -> Self {
        Self {
            deliveries: Mutex::new(Vec::new()),
            outcomes: Mutex::new(outcomes.into_iter().collect()),
        }
    }

    fn deliveries(&self) -> Vec<WebhookDelivery> {
        lock(&self.deliveries).clone()
    }
}

#[async_trait]
impl WebhookDeliveryBackend for RecordingBackend {
    async fn deliver(
        &self,
        _endpoint: &str,
        _format: WebhookFormat,
        _signing_secret: Option<&SecretValue>,
        delivery: &WebhookDelivery,
    ) -> Result<(), WebhookDeliveryError> {
        lock(&self.deliveries).push(delivery.clone());
        lock(&self.outcomes).pop_front().unwrap_or(Ok(()))
    }
}

fn webhook() -> Result<Webhook, kernel_api::InvalidIdentifier> {
    Ok(Object {
        meta: metadata(WebhookId::new("deployments")?),
        spec: WebhookSpec {
            name: "Deployments".to_string(),
            endpoint: SecretValue::new("https://hooks.example.test/events"),
            events: vec![
                WebhookEvent::DeploymentTransition,
                WebhookEvent::NodeAvailability,
            ],
            categories: vec![WebhookCategory::Info, WebhookCategory::Error],
            enabled: true,
            format: WebhookFormat::Maestro,
            signing_secret: Some(SecretValue::new("0123456789abcdef0123456789abcdef")),
        },
        status: WebhookStatus {
            last_success_at: None,
            consecutive_failures: 0,
            retry_at: None,
            observed_generation: None,
            observations: Vec::new(),
            conditions: Vec::new(),
        },
    })
}

fn node() -> Result<Node, kernel_api::InvalidIdentifier> {
    Ok(Object {
        meta: metadata(NodeId::new("node-1")?),
        spec: NodeSpec {
            hostname: "node-1.internal".to_string(),
            host_address: IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)),
            role: NodeRole::Master,
            workload_network_mode: kernel_api::WorkloadNetworkMode::ClusterRouted,
            scheduling_labels: BTreeMap::new(),
        },
        status: NodeStatus {
            instance_id: NodeInstanceId::new("instance-1")?,
            version: "1.0.0".to_string(),
            last_seen: Timestamp(10_000),
            conditions: Vec::new(),
        },
    })
}

fn deployment(phase: DeploymentPhase) -> Result<Deployment, kernel_api::InvalidIdentifier> {
    Ok(Object {
        meta: metadata(DeploymentId::new("deployment-1")?),
        spec: DeploymentSpec {
            service_id: ServiceId::new("api")?,
            service_generation: Generation(1),
            restart_generation: Generation(1),
            bypass_rollout_freeze: false,
            service: ServiceSpec {
                name: "API".to_string(),
                version: "1.0.0".to_string(),
                artifact: ArtifactTemplate::Image {
                    reference: "registry.test/api:latest".to_string(),
                },
                preview: None,
                command: None,
                replicas: 1,
                exposed_ports: vec![8080],
                health_check: None,
                max_restarts: Some(3),
                environment: BTreeMap::new(),
                user: None,
                node_api: NodeApiAccess::Disabled,
                secrets: None,
                volumes: Vec::new(),
                placement: PlacementConstraint::default(),
                exec: ExecPolicy::Allowed,
            },
            goal: DeploymentGoal::Run,
            build_id: None,
        },
        status: DeploymentStatus {
            phase,
            created_at: Timestamp(1_000),
            ready_at: None,
            draining_at: None,
            image_digest: None,
            conditions: Vec::new(),
        },
    })
}

fn metadata<Id>(id: Id) -> ObjectMeta<Id> {
    ObjectMeta {
        id,
        labels: BTreeMap::new(),
        annotations: BTreeMap::new(),
        revision: ResourceRevision::default(),
        generation: Generation(1),
        owner_refs: Vec::new(),
        finalizers: BTreeSet::new(),
        deletion_timestamp: None,
    }
}

async fn put<Resource, Id>(
    store: &InMemoryStore,
    keys: &Keyspace,
    kind: &str,
    resource: &Object<Id, Resource, impl serde::Serialize>,
) -> Result<(), Box<dyn std::error::Error>>
where
    Id: Clone + Into<ResourceName> + serde::Serialize,
    Resource: serde::Serialize,
{
    let outcome = store
        .put_cas(PutRequest {
            key: keys.resource(&ResourceKind::new(kind)?, &resource.meta.id.clone().into()),
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

async fn replace_deployment(
    store: &InMemoryStore,
    keys: &Keyspace,
    phase: DeploymentPhase,
) -> Result<(), Box<dyn std::error::Error>> {
    let key = keys.resource(
        &ResourceKind::new("Deployment")?,
        &ResourceName::new("deployment-1")?,
    );
    let stored = store.get(&key).await?.ok_or("Deployment is missing")?;
    let mut deployment: Deployment = serde_json::from_slice(&stored.value)?;
    deployment.status.phase = phase;
    let outcome = store
        .put_cas(PutRequest {
            key,
            value: serde_json::to_vec(&deployment)?,
            expected: ExpectedVersion::Exact(stored.version),
            session: None,
        })
        .await?;
    if matches!(outcome, CasOutcome::Applied(_)) {
        Ok(())
    } else {
        Err("Deployment update conflicted".into())
    }
}

async fn replace_webhook_categories(
    store: &InMemoryStore,
    keys: &Keyspace,
    categories: Vec<WebhookCategory>,
) -> Result<(), Box<dyn std::error::Error>> {
    let key = keys.resource(
        &ResourceKind::new("Webhook")?,
        &ResourceName::new("deployments")?,
    );
    let stored = store.get(&key).await?.ok_or("Webhook is missing")?;
    let mut webhook: Webhook = serde_json::from_slice(&stored.value)?;
    webhook.meta.generation = Generation(webhook.meta.generation.0.saturating_add(1));
    webhook.spec.categories = categories;
    let outcome = store
        .put_cas(PutRequest {
            key,
            value: serde_json::to_vec(&webhook)?,
            expected: ExpectedVersion::Exact(stored.version),
            session: None,
        })
        .await?;
    if matches!(outcome, CasOutcome::Applied(_)) {
        Ok(())
    } else {
        Err("Webhook update conflicted".into())
    }
}

async fn get<Resource: serde::de::DeserializeOwned>(
    store: &InMemoryStore,
    keys: &Keyspace,
    kind: &str,
    id: &str,
) -> Result<Resource, Box<dyn std::error::Error>> {
    let stored = store
        .get(&keys.resource(&ResourceKind::new(kind)?, &ResourceName::new(id)?))
        .await?
        .ok_or_else(|| format!("{kind} is missing"))?;
    Ok(serde_json::from_slice(&stored.value)?)
}

struct TestTimestamp(AtomicI64);

impl TestTimestamp {
    fn new(now: i64) -> Self {
        Self(AtomicI64::new(now))
    }

    fn set(&self, now: i64) {
        self.0.store(now, Ordering::SeqCst);
    }
}

impl TimestampClock for TestTimestamp {
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

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}
