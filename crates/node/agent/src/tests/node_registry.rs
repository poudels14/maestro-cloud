use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use kernel_api::{
    ClusterId, Condition, ConditionReason, ConditionState, ConditionType, Generation, Node, NodeId,
    NodeInstanceId, NodeRole, NodeSpec, ResourceKind, ResourceName, Timestamp,
};
use kernel_store::{
    CasOutcome, Clock, ExpectedVersion, InMemoryStore, Keyspace, MonotonicTime, PutRequest, Store,
};
use semver::Version;

use crate::{
    NodeRegistryAction, NodeRegistryAgent, NodeRegistryError, NodeRegistrySettings, StatusClock,
};

#[tokio::test]
async fn registry_atomically_publishes_durable_status_and_session_liveness()
-> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(InMemoryStore::new(Arc::new(FixedMonotonicClock)));
    let clock = Arc::new(MutableStatusClock::new(1_000));
    let agent = agent(store.clone(), "instance-1", clock)?;

    let registration = agent.register().await?;
    let session_id = registration.session_id();
    let node = stored_node(&store).await?;
    assert_eq!(node.status.instance_id.as_str(), "instance-1");
    assert_eq!(node.status.version, "1.2.3");
    assert_eq!(node.status.last_seen, Timestamp(1_000));
    assert_eq!(
        stored_liveness(&store).await?.as_deref(),
        Some("instance-1")
    );
    assert_eq!(registration.session_id(), session_id);

    registration.close().await?;
    assert!(stored_liveness(&store).await?.is_none());
    assert_eq!(stored_node(&store).await?.meta.id.as_str(), "node-1");
    Ok(())
}

#[tokio::test]
async fn registry_rejects_duplicate_live_instances_then_preserves_operator_state_on_takeover()
-> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(InMemoryStore::new(Arc::new(FixedMonotonicClock)));
    let clock = Arc::new(MutableStatusClock::new(1_000));
    let first = agent(store.clone(), "instance-1", clock.clone())?;
    let first_session = store.session(Duration::from_secs(30)).await?;
    assert_eq!(
        first.reconcile_once(first_session.as_ref()).await?,
        NodeRegistryAction::Registered
    );
    clock.set(1_500);
    first_session.keep_alive().await?;
    assert_eq!(
        first.reconcile_once(first_session.as_ref()).await?,
        NodeRegistryAction::Renewed
    );
    add_operator_state(&store).await?;

    let second = agent(store.clone(), "instance-2", clock.clone())?;
    let second_session = store.session(Duration::from_secs(30)).await?;
    assert!(matches!(
        second.reconcile_once(second_session.as_ref()).await,
        Err(NodeRegistryError::DuplicateLiveInstance { instance_id })
            if instance_id.as_str() == "instance-1"
    ));

    first_session.close().await?;
    clock.set(2_000);
    assert_eq!(
        second.reconcile_once(second_session.as_ref()).await?,
        NodeRegistryAction::Registered
    );
    let node = stored_node(&store).await?;
    assert_eq!(
        node.spec.scheduling_labels.get("zone"),
        Some(&"west".to_string())
    );
    assert_eq!(node.status.conditions.len(), 1);
    assert_eq!(node.status.instance_id.as_str(), "instance-2");
    assert_eq!(node.status.last_seen, Timestamp(2_000));
    Ok(())
}

#[tokio::test]
async fn registry_rejects_topology_drift_without_replacing_the_node()
-> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(InMemoryStore::new(Arc::new(FixedMonotonicClock)));
    let clock = Arc::new(MutableStatusClock::new(1_000));
    let first = agent(store.clone(), "instance-1", clock.clone())?;
    let first_session = store.session(Duration::from_secs(30)).await?;
    first.reconcile_once(first_session.as_ref()).await?;
    first_session.close().await?;

    let key = node_key()?;
    let stored = store.get(&key).await?.ok_or("Node missing")?;
    let mut node: Node = serde_json::from_slice(&stored.value)?;
    node.spec.hostname = "different.internal".to_string();
    store
        .put_cas(PutRequest {
            key,
            value: serde_json::to_vec(&node)?,
            expected: ExpectedVersion::Exact(stored.version),
            session: None,
        })
        .await?;

    let second = agent(store.clone(), "instance-2", clock)?;
    let second_session = store.session(Duration::from_secs(30)).await?;
    assert!(matches!(
        second.reconcile_once(second_session.as_ref()).await,
        Err(NodeRegistryError::DefinitionConflict)
    ));
    assert_eq!(
        stored_node(&store).await?.spec.hostname,
        "different.internal"
    );
    assert!(stored_liveness(&store).await?.is_none());
    Ok(())
}

#[tokio::test]
async fn removed_identity_cannot_recreate_node_or_liveness_state()
-> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(InMemoryStore::new(Arc::new(FixedMonotonicClock)));
    let cluster_id = ClusterId::new("registry-test")?;
    let node_id = NodeId::new("node-1")?;
    store
        .put_cas(PutRequest {
            key: Keyspace::new(&cluster_id).node_tombstone(&node_id),
            value: b"removed".to_vec(),
            expected: ExpectedVersion::Missing,
            session: None,
        })
        .await?;
    let agent = agent(
        store.clone(),
        "instance-1",
        Arc::new(MutableStatusClock::new(1_000)),
    )?;

    assert!(matches!(
        agent.register().await,
        Err(NodeRegistryError::NodeRemoved)
    ));
    assert!(store.get(&node_key()?).await?.is_none());
    assert!(stored_liveness(&store).await?.is_none());
    Ok(())
}

#[test]
fn registry_settings_require_renewal_before_expiry() -> Result<(), Box<dyn std::error::Error>> {
    let mut zero_interval = settings("instance-1")?;
    zero_interval.keepalive_interval = Duration::ZERO;
    assert!(zero_interval.validate().is_err());
    let mut expired_interval = settings("instance-1")?;
    expired_interval.keepalive_interval = expired_interval.session_ttl;
    assert!(expired_interval.validate().is_err());
    Ok(())
}

fn agent(
    store: Arc<InMemoryStore>,
    instance_id: &str,
    clock: Arc<MutableStatusClock>,
) -> Result<NodeRegistryAgent, Box<dyn std::error::Error>> {
    Ok(NodeRegistryAgent::new(
        store,
        settings(instance_id)?,
        Arc::new(FixedMonotonicClock),
        clock,
    )?)
}

fn settings(instance_id: &str) -> Result<NodeRegistrySettings, Box<dyn std::error::Error>> {
    Ok(NodeRegistrySettings {
        cluster_id: ClusterId::new("registry-test")?,
        node_id: NodeId::new("node-1")?,
        node_spec: NodeSpec {
            hostname: "node-1.internal".to_string(),
            host_address: IpAddr::V4(Ipv4Addr::new(10, 20, 0, 11)),
            role: NodeRole::Worker,
            scheduling_labels: BTreeMap::new(),
        },
        instance_id: NodeInstanceId::new(instance_id)?,
        running_version: Version::new(1, 2, 3),
        session_ttl: Duration::from_secs(30),
        keepalive_interval: Duration::from_secs(10),
    })
}

async fn stored_node(store: &Arc<InMemoryStore>) -> Result<Node, Box<dyn std::error::Error>> {
    let stored = store.get(&node_key()?).await?.ok_or("Node missing")?;
    Ok(serde_json::from_slice(&stored.value)?)
}

async fn stored_liveness(
    store: &Arc<InMemoryStore>,
) -> Result<Option<String>, Box<dyn std::error::Error>> {
    store
        .get(
            &Keyspace::new(&ClusterId::new("registry-test")?)
                .node_liveness(&NodeId::new("node-1")?),
        )
        .await?
        .map(|stored| String::from_utf8(stored.value).map_err(Into::into))
        .transpose()
}

fn node_key() -> Result<kernel_store::StoreKey, Box<dyn std::error::Error>> {
    Ok(Keyspace::new(&ClusterId::new("registry-test")?)
        .resource(&ResourceKind::new("Node")?, &ResourceName::new("node-1")?))
}

async fn add_operator_state(store: &Arc<InMemoryStore>) -> Result<(), Box<dyn std::error::Error>> {
    let key = node_key()?;
    let stored = store.get(&key).await?.ok_or("Node missing")?;
    let mut node: Node = serde_json::from_slice(&stored.value)?;
    node.spec
        .scheduling_labels
        .insert("zone".to_string(), "west".to_string());
    node.status.conditions.push(Condition {
        condition_type: ConditionType("Maintenance".to_string()),
        state: ConditionState::True,
        reason: ConditionReason("Operator".to_string()),
        message: "maintenance requested".to_string(),
        observed_generation: Generation(1),
        last_transition_time: Timestamp(1_000),
    });
    assert!(matches!(
        store
            .put_cas(PutRequest {
                key,
                value: serde_json::to_vec(&node)?,
                expected: ExpectedVersion::Exact(stored.version),
                session: None,
            })
            .await?,
        CasOutcome::Applied(_)
    ));
    Ok(())
}

struct FixedMonotonicClock;

#[async_trait::async_trait]
impl Clock for FixedMonotonicClock {
    fn now(&self) -> MonotonicTime {
        MonotonicTime::from_duration(Duration::ZERO)
    }

    async fn sleep_until(&self, _deadline: MonotonicTime) {
        std::future::pending::<()>().await;
    }
}

struct MutableStatusClock(AtomicI64);

impl MutableStatusClock {
    fn new(now: i64) -> Self {
        Self(AtomicI64::new(now))
    }

    fn set(&self, now: i64) {
        self.0.store(now, Ordering::Relaxed);
    }
}

impl StatusClock for MutableStatusClock {
    fn now(&self) -> Timestamp {
        Timestamp(self.0.load(Ordering::Relaxed))
    }
}
