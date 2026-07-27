use std::collections::{BTreeMap, BTreeSet};
use std::net::Ipv4Addr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use kernel_api::{
    ClusterId, DnsRecord, DnsRecordId, DnsRecordSpec, DnsRecordStatus, DnsRecordValue, Generation,
    NodeId, ObjectMeta, ResourceKind, ResourceName, ResourceRevision, Timestamp,
};
use kernel_store::{
    CasOutcome, Clock, ExpectedVersion, InMemoryStore, Keyspace, MonotonicTime, PutRequest, Store,
};
use tokio::sync::{Notify, watch};

use super::store_fault::FailFirstListStore;
use crate::{
    AuthoritativeDnsResolver, DnsQueryType, DnsResourceAgent, DnsResourceError, DnsResponseCode,
};

#[tokio::test]
async fn dns_resource_agents_publish_snapshots_and_acknowledge_each_node()
-> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(InMemoryStore::new(Arc::new(TestClock)));
    let cluster_id = cluster_id();
    put_new(
        store.as_ref(),
        &cluster_id,
        record("api", "api.maestro.internal.", Ipv4Addr::new(10, 42, 1, 11)),
    )
    .await?;
    let resolver_1 = AuthoritativeDnsResolver::new()?;
    let resolver_2 = AuthoritativeDnsResolver::new()?;
    let node_1 = agent(store.clone(), &cluster_id, "node-1", resolver_1.clone())?;
    let node_2 = agent(store.clone(), &cluster_id, "node-2", resolver_2.clone())?;

    let first = node_1.reconcile_once().await?;
    assert_eq!(first.observed_resources, 1);
    assert_eq!(first.zone.record_sets, 1);
    assert_eq!(first.acknowledgements, 1);
    assert_eq!(first.stale_acknowledgements, 0);
    assert_eq!(
        resolver_1
            .lookup("api.maestro.internal.", DnsQueryType::A)
            .await?
            .answers
            .len(),
        1
    );

    assert_eq!(node_2.reconcile_once().await?.acknowledgements, 1);
    assert_eq!(node_2.reconcile_once().await?.acknowledgements, 0);
    let published = get_record(store.as_ref(), &cluster_id, "api").await?;
    assert_eq!(published.status.applied_generation, Generation(1));
    assert_eq!(
        published.status.published_nodes,
        vec![NodeId::new("node-1")?, NodeId::new("node-2")?]
    );
    Ok(())
}

#[tokio::test]
async fn dns_resource_agent_replaces_changed_and_deleting_record_snapshots()
-> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(InMemoryStore::new(Arc::new(TestClock)));
    let cluster_id = cluster_id();
    put_new(
        store.as_ref(),
        &cluster_id,
        record("api", "api.maestro.internal.", Ipv4Addr::new(10, 42, 1, 11)),
    )
    .await?;
    let resolver = AuthoritativeDnsResolver::new()?;
    let agent = agent(store.clone(), &cluster_id, "node-1", resolver.clone())?;
    agent.reconcile_once().await?;

    update_record(store.as_ref(), &cluster_id, "api", |resource| {
        resource.meta.generation = Generation(2);
        resource.spec.values = vec![DnsRecordValue::A(Ipv4Addr::new(10, 42, 1, 22))];
    })
    .await?;
    let changed = agent.reconcile_once().await?;
    assert_eq!(changed.acknowledgements, 1);
    let answer = resolver
        .lookup("api.maestro.internal.", DnsQueryType::A)
        .await?;
    assert_eq!(
        answer.answers.first().expect("changed answer").value,
        DnsRecordValue::A(Ipv4Addr::new(10, 42, 1, 22))
    );
    let acknowledged = get_record(store.as_ref(), &cluster_id, "api").await?;
    assert_eq!(acknowledged.status.applied_generation, Generation(2));
    assert_eq!(
        acknowledged.status.published_nodes,
        vec![NodeId::new("node-1")?]
    );

    update_record(store.as_ref(), &cluster_id, "api", |resource| {
        resource.meta.deletion_timestamp = Some(Timestamp(1_750_000_000_000));
    })
    .await?;
    let deleting = agent.reconcile_once().await?;
    assert_eq!(deleting.observed_resources, 0);
    assert_eq!(deleting.zone.record_sets, 0);
    let missing = resolver
        .lookup("api.maestro.internal.", DnsQueryType::A)
        .await?;
    assert_eq!(missing.response_code, DnsResponseCode::NameError);
    Ok(())
}

#[tokio::test]
async fn invalid_store_snapshots_preserve_the_last_published_zone()
-> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(InMemoryStore::new(Arc::new(TestClock)));
    let cluster_id = cluster_id();
    put_new(
        store.as_ref(),
        &cluster_id,
        record("api", "api.maestro.internal.", Ipv4Addr::new(10, 42, 1, 11)),
    )
    .await?;
    let resolver = AuthoritativeDnsResolver::new()?;
    let agent = agent(store.clone(), &cluster_id, "node-1", resolver.clone())?;
    agent.reconcile_once().await?;

    let malformed_key = dns_key(&cluster_id, "malformed")?;
    store
        .put_cas(PutRequest {
            key: malformed_key.clone(),
            value: b"not-json".to_vec(),
            expected: ExpectedVersion::Missing,
            session: None,
        })
        .await?;
    let degraded = agent.reconcile_once().await?;
    assert_eq!(degraded.observed_resources, 1);
    assert_eq!(degraded.malformed_resources, 1);
    assert_eq!(degraded.snapshot_rejections, 1);
    assert_eq!(degraded.acknowledgements, 0);
    assert_eq!(degraded.zone.record_sets, 1);
    assert_eq!(
        resolver
            .lookup("api.maestro.internal.", DnsQueryType::A)
            .await?
            .answers
            .len(),
        1
    );

    let malformed = store
        .get(&malformed_key)
        .await?
        .ok_or("malformed missing")?;
    let repaired = record(
        "malformed",
        "worker.maestro.internal.",
        Ipv4Addr::new(10, 42, 1, 12),
    );
    assert!(matches!(
        store
            .put_cas(PutRequest {
                key: malformed_key,
                value: serde_json::to_vec(&repaired)?,
                expected: ExpectedVersion::Exact(malformed.version),
                session: None,
            })
            .await?,
        CasOutcome::Applied(_)
    ));
    let recovered = agent.reconcile_once().await?;
    assert_eq!(recovered.observed_resources, 2);
    assert_eq!(recovered.malformed_resources, 0);
    assert_eq!(recovered.snapshot_rejections, 0);
    assert_eq!(recovered.zone.record_sets, 2);

    update_record(store.as_ref(), &cluster_id, "malformed", |resource| {
        resource.meta.generation = Generation(2);
        resource.spec.name = "outside.example.".to_owned();
    })
    .await?;
    let invalid_zone = agent.reconcile_once().await?;
    assert_eq!(invalid_zone.observed_resources, 2);
    assert_eq!(invalid_zone.malformed_resources, 0);
    assert_eq!(invalid_zone.snapshot_rejections, 1);
    assert_eq!(invalid_zone.acknowledgements, 0);
    assert_eq!(invalid_zone.zone.record_sets, 2);
    assert_eq!(
        resolver
            .lookup("worker.maestro.internal.", DnsQueryType::A)
            .await?
            .answers
            .len(),
        1
    );
    Ok(())
}

#[test]
fn dns_resource_agent_rejects_zero_resync_interval() -> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(InMemoryStore::new(Arc::new(TestClock)));
    let store: Arc<dyn Store> = store;
    let result = DnsResourceAgent::new(
        store,
        &cluster_id(),
        NodeId::new("node-1")?,
        AuthoritativeDnsResolver::new()?,
        Arc::new(TestClock),
        Duration::ZERO,
    );
    assert!(matches!(result, Err(DnsResourceError::ZeroResyncInterval)));
    Ok(())
}

#[tokio::test]
async fn dns_resource_run_recovers_after_store_unavailability()
-> Result<(), Box<dyn std::error::Error>> {
    let clock = Arc::new(ManualClock::default());
    let store = Arc::new(InMemoryStore::new(clock.clone()));
    let cluster_id = cluster_id();
    put_new(
        store.as_ref(),
        &cluster_id,
        record("api", "api.maestro.internal.", Ipv4Addr::new(10, 42, 1, 11)),
    )
    .await?;
    let unavailable = Arc::new(FailFirstListStore::new(store));
    let resolver = AuthoritativeDnsResolver::new()?;
    let store: Arc<dyn Store> = unavailable.clone();
    let agent = Arc::new(DnsResourceAgent::new(
        store,
        &cluster_id,
        NodeId::new("node-1")?,
        resolver.clone(),
        clock.clone(),
        Duration::from_secs(30),
    )?);
    let (shutdown, shutdown_rx) = watch::channel(false);
    let running_agent = agent.clone();
    let task = tokio::spawn(async move { running_agent.run(shutdown_rx).await });

    wait_for_sleeps(clock.as_ref(), 1).await?;
    assert!(!task.is_finished());
    assert_eq!(unavailable.list_calls(), 1);
    assert_eq!(
        resolver
            .lookup("api.maestro.internal.", DnsQueryType::A)
            .await?
            .response_code,
        DnsResponseCode::NameError
    );

    clock.advance(Duration::from_secs(1));
    wait_for_dns_answer(&resolver, "api.maestro.internal.").await?;
    assert!(unavailable.list_calls() >= 2);

    shutdown.send(true)?;
    task.await??;
    Ok(())
}

fn agent(
    store: Arc<InMemoryStore>,
    cluster_id: &ClusterId,
    node_id: &str,
    resolver: AuthoritativeDnsResolver,
) -> Result<DnsResourceAgent, DnsResourceError> {
    let store: Arc<dyn Store> = store;
    DnsResourceAgent::new(
        store,
        cluster_id,
        NodeId::new(node_id)?,
        resolver,
        Arc::new(TestClock),
        Duration::from_secs(30),
    )
}

async fn put_new(
    store: &InMemoryStore,
    cluster_id: &ClusterId,
    resource: DnsRecord,
) -> Result<(), Box<dyn std::error::Error>> {
    let key = dns_key(cluster_id, resource.meta.id.as_str())?;
    let outcome = store
        .put_cas(PutRequest {
            key,
            value: serde_json::to_vec(&resource)?,
            expected: ExpectedVersion::Missing,
            session: None,
        })
        .await?;
    assert!(matches!(outcome, CasOutcome::Applied(_)));
    Ok(())
}

async fn update_record(
    store: &InMemoryStore,
    cluster_id: &ClusterId,
    id: &str,
    update: impl FnOnce(&mut DnsRecord),
) -> Result<(), Box<dyn std::error::Error>> {
    let key = dns_key(cluster_id, id)?;
    let stored = store.get(&key).await?.ok_or("DnsRecord missing")?;
    let mut resource: DnsRecord = serde_json::from_slice(&stored.value)?;
    update(&mut resource);
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

async fn get_record(
    store: &InMemoryStore,
    cluster_id: &ClusterId,
    id: &str,
) -> Result<DnsRecord, Box<dyn std::error::Error>> {
    let stored = store
        .get(&dns_key(cluster_id, id)?)
        .await?
        .ok_or("DnsRecord missing")?;
    Ok(serde_json::from_slice(&stored.value)?)
}

fn dns_key(
    cluster_id: &ClusterId,
    id: &str,
) -> Result<kernel_store::StoreKey, kernel_api::InvalidIdentifier> {
    Ok(Keyspace::new(cluster_id)
        .resource(&ResourceKind::new("DnsRecord")?, &ResourceName::new(id)?))
}

fn record(id: &str, name: &str, address: Ipv4Addr) -> DnsRecord {
    DnsRecord {
        meta: ObjectMeta {
            id: DnsRecordId::new(id).expect("dns record id"),
            labels: BTreeMap::new(),
            annotations: BTreeMap::new(),
            revision: ResourceRevision::default(),
            generation: Generation(1),
            owner_refs: Vec::new(),
            finalizers: BTreeSet::new(),
            deletion_timestamp: None,
        },
        spec: DnsRecordSpec {
            name: name.to_owned(),
            values: vec![DnsRecordValue::A(address)],
            ttl_secs: 30,
        },
        status: DnsRecordStatus {
            applied_generation: Generation::default(),
            published_nodes: Vec::new(),
            conditions: Vec::new(),
        },
    }
}

fn cluster_id() -> ClusterId {
    ClusterId::new("dns-resource-test").expect("cluster id")
}

struct TestClock;

#[async_trait]
impl Clock for TestClock {
    fn now(&self) -> MonotonicTime {
        MonotonicTime::from_duration(Duration::ZERO)
    }

    async fn sleep_until(&self, _deadline: MonotonicTime) {
        std::future::pending::<()>().await;
    }
}

#[derive(Default)]
struct ManualClock {
    milliseconds: AtomicU64,
    sleeps: AtomicU64,
    advanced: Notify,
}

impl ManualClock {
    fn advance(&self, duration: Duration) {
        let milliseconds = u64::try_from(duration.as_millis()).unwrap_or(u64::MAX);
        self.milliseconds.fetch_add(milliseconds, Ordering::SeqCst);
        self.advanced.notify_waiters();
    }
}

#[async_trait]
impl Clock for ManualClock {
    fn now(&self) -> MonotonicTime {
        MonotonicTime::from_duration(Duration::from_millis(
            self.milliseconds.load(Ordering::SeqCst),
        ))
    }

    async fn sleep_until(&self, deadline: MonotonicTime) {
        self.sleeps.fetch_add(1, Ordering::SeqCst);
        loop {
            let advanced = self.advanced.notified();
            if self.now() >= deadline {
                return;
            }
            advanced.await;
        }
    }
}

async fn wait_for_sleeps(
    clock: &ManualClock,
    expected: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    for _attempt in 0..128 {
        if clock.sleeps.load(Ordering::SeqCst) >= expected {
            return Ok(());
        }
        tokio::task::yield_now().await;
    }
    Err(format!("DNS resource agent did not begin sleep {expected}").into())
}

async fn wait_for_dns_answer(
    resolver: &AuthoritativeDnsResolver,
    name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    for _attempt in 0..128 {
        if !resolver
            .lookup(name, DnsQueryType::A)
            .await?
            .answers
            .is_empty()
        {
            return Ok(());
        }
        tokio::task::yield_now().await;
    }
    Err(format!("DNS resource agent did not publish `{name}` after recovery").into())
}
