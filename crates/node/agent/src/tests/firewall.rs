use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use kernel_api::{
    ClusterId, Generation, NodeFirewall, NodeFirewallId, NodeFirewallSpec, NodeFirewallStatus,
    NodeId, Object, ObjectMeta, ResourceKind, ResourceName, ResourceRevision, Timestamp,
};
use kernel_store::{
    CasOutcome, Clock, ExpectedVersion, InMemoryStore, Keyspace, MonotonicTime, PutRequest, Store,
};
use sha2::{Digest, Sha256};

use crate::{
    FirewallAgentError, FirewallBackend, FirewallBackendError, NodeFirewallAgent, StatusClock,
};

#[tokio::test]
async fn local_agent_reapplies_exact_state_and_acknowledges_only_its_node()
-> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(InMemoryStore::new(Arc::new(TestClock)));
    let cluster_id = ClusterId::new("firewall-agent")?;
    put(
        store.as_ref(),
        &cluster_id,
        resource("node-1", 1, "allow-v1")?,
    )
    .await?;
    put(
        store.as_ref(),
        &cluster_id,
        resource("node-2", 1, "allow-other")?,
    )
    .await?;
    let backend = RecordingBackend::default();
    let agent = agent(store.clone(), &cluster_id, "node-1", backend)?;

    let first = agent.reconcile_once().await?;
    assert!(first.resource_present && first.applied && first.acknowledgement_updated);
    let current = get(store.as_ref(), &cluster_id, "node-1").await?;
    assert_eq!(current.status.applied_generation, Generation(1));
    assert_eq!(
        current.status.applied_digest.as_deref(),
        Some(current.spec.digest.as_str())
    );
    assert_eq!(
        agent.backend().applied()?.as_slice(),
        std::slice::from_ref(&current.spec)
    );

    let repaired = agent.reconcile_once().await?;
    assert!(repaired.applied && !repaired.acknowledgement_updated);
    assert_eq!(agent.backend().applied()?.len(), 2);
    let untouched = get(store.as_ref(), &cluster_id, "node-2").await?;
    assert_eq!(untouched.status.applied_generation, Generation::default());
    Ok(())
}

#[tokio::test]
async fn digest_or_backend_failure_never_advances_the_applied_generation()
-> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(InMemoryStore::new(Arc::new(TestClock)));
    let cluster_id = ClusterId::new("firewall-failure")?;
    let mut invalid = resource("node-1", 2, "deny-v2")?;
    invalid.spec.digest = "not-the-script-digest".to_string();
    put(store.as_ref(), &cluster_id, invalid).await?;
    let agent = agent(
        store.clone(),
        &cluster_id,
        "node-1",
        RecordingBackend::default(),
    )?;
    assert!(matches!(
        agent.reconcile_once().await,
        Err(FirewallAgentError::DigestMismatch { .. })
    ));
    assert!(agent.backend().applied()?.is_empty());
    let rejected = get(store.as_ref(), &cluster_id, "node-1").await?;
    assert_eq!(rejected.status.applied_generation, Generation::default());
    assert!(rejected.status.conditions.first().is_some_and(|condition| {
        condition.condition_type.0 == "FirewallReady"
            && condition.state == kernel_api::ConditionState::False
            && condition.message.contains("digest mismatch")
    }));

    update(store.as_ref(), &cluster_id, "node-1", |resource| {
        resource.spec.digest = digest(&resource.spec.script);
    })
    .await?;
    agent.backend().set_failure(true)?;
    assert!(matches!(
        agent.reconcile_once().await,
        Err(FirewallAgentError::Backend(_))
    ));
    let failed = get(store.as_ref(), &cluster_id, "node-1").await?;
    assert_eq!(failed.status.applied_generation, Generation::default());
    assert!(failed.status.applied_digest.is_none());
    assert!(failed.status.conditions.first().is_some_and(|condition| {
        condition.condition_type.0 == "FirewallReady"
            && condition.state == kernel_api::ConditionState::False
    }));
    Ok(())
}

#[derive(Default)]
struct RecordingBackend {
    state: Mutex<RecordingState>,
}

#[derive(Default)]
struct RecordingState {
    applied: Vec<NodeFirewallSpec>,
    fail: bool,
}

impl RecordingBackend {
    fn applied(&self) -> Result<Vec<NodeFirewallSpec>, FirewallBackendError> {
        self.state
            .lock()
            .map(|state| state.applied.clone())
            .map_err(|_| FirewallBackendError::new("recording backend lock poisoned"))
    }

    fn set_failure(&self, fail: bool) -> Result<(), FirewallBackendError> {
        self.state
            .lock()
            .map_err(|_| FirewallBackendError::new("recording backend lock poisoned"))?
            .fail = fail;
        Ok(())
    }
}

#[async_trait]
impl FirewallBackend for RecordingBackend {
    async fn apply(&self, desired: &NodeFirewallSpec) -> Result<(), FirewallBackendError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| FirewallBackendError::new("recording backend lock poisoned"))?;
        if state.fail {
            Err(FirewallBackendError::new("injected nft failure"))
        } else {
            state.applied.push(desired.clone());
            Ok(())
        }
    }
}

fn agent(
    store: Arc<InMemoryStore>,
    cluster_id: &ClusterId,
    node_id: &str,
    backend: RecordingBackend,
) -> Result<NodeFirewallAgent<RecordingBackend>, FirewallAgentError> {
    let store: Arc<dyn Store> = store;
    NodeFirewallAgent::new(
        store,
        cluster_id,
        NodeId::new(node_id)?,
        backend,
        Arc::new(TestClock),
        Arc::new(TestStatusClock),
        Duration::from_secs(30),
    )
}

fn resource(
    node_id: &str,
    generation: u64,
    script: &str,
) -> Result<NodeFirewall, kernel_api::InvalidIdentifier> {
    let script = format!("destroy table inet maestro_firewall\n# {script}\n");
    Ok(Object {
        meta: ObjectMeta {
            id: NodeFirewallId::new(node_id)?,
            labels: BTreeMap::new(),
            annotations: BTreeMap::new(),
            revision: ResourceRevision::default(),
            generation: Generation(generation),
            owner_refs: Vec::new(),
            finalizers: BTreeSet::new(),
            deletion_timestamp: None,
        },
        spec: NodeFirewallSpec {
            node_id: NodeId::new(node_id)?,
            table_name: "maestro_firewall".to_string(),
            digest: digest(&script),
            script,
        },
        status: NodeFirewallStatus {
            applied_generation: Generation::default(),
            applied_digest: None,
            conditions: Vec::new(),
        },
    })
}

fn digest(script: &str) -> String {
    Sha256::digest(script.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

async fn put(
    store: &InMemoryStore,
    cluster_id: &ClusterId,
    resource: NodeFirewall,
) -> Result<(), Box<dyn std::error::Error>> {
    let outcome = store
        .put_cas(PutRequest {
            key: key(cluster_id, resource.meta.id.as_str())?,
            value: serde_json::to_vec(&resource)?,
            expected: ExpectedVersion::Missing,
            session: None,
        })
        .await?;
    assert!(matches!(outcome, CasOutcome::Applied(_)));
    Ok(())
}

async fn update(
    store: &InMemoryStore,
    cluster_id: &ClusterId,
    node_id: &str,
    change: impl FnOnce(&mut NodeFirewall),
) -> Result<(), Box<dyn std::error::Error>> {
    let key = key(cluster_id, node_id)?;
    let stored = store.get(&key).await?.ok_or("NodeFirewall missing")?;
    let mut resource: NodeFirewall = serde_json::from_slice(&stored.value)?;
    change(&mut resource);
    let outcome = store
        .put_cas(PutRequest {
            key,
            value: serde_json::to_vec(&resource)?,
            expected: ExpectedVersion::Exact(stored.version),
            session: None,
        })
        .await?;
    assert!(matches!(outcome, CasOutcome::Applied(_)));
    Ok(())
}

async fn get(
    store: &InMemoryStore,
    cluster_id: &ClusterId,
    node_id: &str,
) -> Result<NodeFirewall, Box<dyn std::error::Error>> {
    let stored = store
        .get(&key(cluster_id, node_id)?)
        .await?
        .ok_or("NodeFirewall missing")?;
    Ok(serde_json::from_slice(&stored.value)?)
}

fn key(
    cluster_id: &ClusterId,
    node_id: &str,
) -> Result<kernel_store::StoreKey, kernel_api::InvalidIdentifier> {
    Ok(Keyspace::new(cluster_id).resource(
        &ResourceKind::new("NodeFirewall")?,
        &ResourceName::new(node_id)?,
    ))
}

struct TestClock;

#[async_trait]
impl Clock for TestClock {
    fn now(&self) -> MonotonicTime {
        MonotonicTime::default()
    }

    async fn sleep_until(&self, _deadline: MonotonicTime) {
        std::future::pending::<()>().await;
    }
}

struct TestStatusClock;

impl StatusClock for TestStatusClock {
    fn now(&self) -> Timestamp {
        Timestamp(1_750_000_000_000)
    }
}
