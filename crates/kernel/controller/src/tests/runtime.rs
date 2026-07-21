use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use kernel_api::{
    ClusterId, Generation, Object, ObjectMeta, ResourceKind, ResourceName, ResourceRevision,
    Timestamp,
};
use kernel_store::{
    CasOutcome, ExpectedVersion, InMemoryStore, Keyspace, PutRequest, SessionBinding, Store,
};
use serde::{Deserialize, Serialize};

use super::clock::{ManualClock, NoopClock};
use crate::{
    Action, Backoff, ControllerRuntime, FencedStore, LeaderIdentity, LeadershipToken,
    ReconcileContext, ReconcileError, Reconciler, RuntimeConfig,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ToySpec {
    desired: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ToyStatus;

struct ToyReconciler {
    reconciles: AtomicUsize,
    finalizes: AtomicUsize,
}

#[async_trait]
impl Reconciler for ToyReconciler {
    type Id = ResourceName;
    type Spec = ToySpec;
    type Status = ToyStatus;

    const KIND: &'static str = "Toy";
    const FINALIZER: Option<&'static str> = Some("toy.maestro.dev/cleanup");

    async fn reconcile(
        &self,
        _resource: Object<Self::Id, Self::Spec, Self::Status>,
        _context: ReconcileContext,
    ) -> Result<Action, ReconcileError> {
        self.reconciles.fetch_add(1, Ordering::SeqCst);
        Ok(Action::Done)
    }

    async fn finalize(
        &self,
        _resource: Object<Self::Id, Self::Spec, Self::Status>,
        _context: ReconcileContext,
    ) -> Result<Action, ReconcileError> {
        self.finalizes.fetch_add(1, Ordering::SeqCst);
        Ok(Action::Done)
    }
}

#[tokio::test]
async fn runtime_installs_and_executes_finalizers_before_physical_deletion()
-> Result<(), Box<dyn std::error::Error>> {
    let clock = Arc::new(NoopClock);
    let store = Arc::new(InMemoryStore::new(clock.clone()));
    let keys = Keyspace::new(&ClusterId::new("runtime")?);
    let session = store.session(Duration::from_secs(30)).await?;
    let leader = store
        .put_cas(PutRequest {
            key: keys.leader(),
            value: b"leader".to_vec(),
            expected: ExpectedVersion::Missing,
            session: Some(SessionBinding {
                session_id: session.id(),
            }),
        })
        .await?;
    let CasOutcome::Applied(leader) = leader else {
        return Err("leader should be created".into());
    };
    let fenced = Arc::new(FencedStore::new(
        store.clone(),
        keys.leader(),
        LeadershipToken::from_campaign(
            LeaderIdentity {
                node_id: kernel_api::NodeId::new("node-1")?,
                instance_id: kernel_api::NodeInstanceId::new("instance-1")?,
            },
            session.id(),
            leader.version,
        ),
    ));
    let kind = ResourceKind::new("Toy")?;
    let key = keys.resource(&kind, &ResourceName::new("sample")?);
    let created = store
        .put_cas(PutRequest {
            key: key.clone(),
            value: serde_json::to_vec(&toy_resource_named("sample", None)?)?,
            expected: ExpectedVersion::Missing,
            session: None,
        })
        .await?;
    assert!(matches!(created, CasOutcome::Applied(_)));

    let reconciler = Arc::new(ToyReconciler {
        reconciles: AtomicUsize::new(0),
        finalizes: AtomicUsize::new(0),
    });
    let runtime = ControllerRuntime::new(
        reconciler.clone(),
        keys.resource_kind(&kind),
        fenced,
        clock,
        RuntimeConfig::new(
            Duration::from_secs(30),
            Backoff::new(Duration::from_millis(10), Duration::from_secs(1))?,
        )?,
    );

    assert_eq!(runtime.reconcile_snapshot().await?, 0);
    assert_eq!(runtime.reconcile_snapshot().await?, 1);
    assert_eq!(reconciler.reconciles.load(Ordering::SeqCst), 1);

    let stored = store.get(&key).await?.ok_or("resource should exist")?;
    let mut deleting: Object<ResourceName, ToySpec, ToyStatus> =
        serde_json::from_slice(&stored.value)?;
    deleting.meta.deletion_timestamp = Some(Timestamp(1));
    let marked = store
        .put_cas(PutRequest {
            key: key.clone(),
            value: serde_json::to_vec(&deleting)?,
            expected: ExpectedVersion::Exact(stored.version),
            session: None,
        })
        .await?;
    assert!(matches!(marked, CasOutcome::Applied(_)));

    assert_eq!(runtime.reconcile_snapshot().await?, 1);
    assert_eq!(reconciler.finalizes.load(Ordering::SeqCst), 1);
    assert!(store.get(&key).await?.is_none());
    #[cfg(feature = "test-util")]
    {
        let entries = runtime.journal().entries();
        assert_eq!(entries.len(), 2);
        let first = entries.first().ok_or("first journal entry should exist")?;
        let second = entries.get(1).ok_or("second journal entry should exist")?;
        assert_eq!(first.sequence, 1);
        assert!(!first.deleting);
        assert_eq!(second.sequence, 2);
        assert!(second.deleting);
        assert!(entries.iter().all(|entry| entry.observed_revision.0 > 0));
    }
    Ok(())
}

#[tokio::test]
async fn runtime_processes_primary_dependency_and_level_triggered_resyncs()
-> Result<(), Box<dyn std::error::Error>> {
    let clock = Arc::new(ManualClock::new());
    let store = Arc::new(InMemoryStore::new(clock.clone()));
    let keys = Keyspace::new(&ClusterId::new("watch-runtime")?);
    let session = store.session(Duration::from_secs(300)).await?;
    let leader = store
        .put_cas(PutRequest {
            key: keys.leader(),
            value: b"leader".to_vec(),
            expected: ExpectedVersion::Missing,
            session: Some(SessionBinding {
                session_id: session.id(),
            }),
        })
        .await?;
    let CasOutcome::Applied(leader) = leader else {
        return Err("leader should be created".into());
    };
    let fenced = Arc::new(FencedStore::new(
        store.clone(),
        keys.leader(),
        LeadershipToken::from_campaign(
            LeaderIdentity {
                node_id: kernel_api::NodeId::new("node-1")?,
                instance_id: kernel_api::NodeInstanceId::new("instance-1")?,
            },
            session.id(),
            leader.version,
        ),
    ));
    let kind = ResourceKind::new("Toy")?;
    let first_key = keys.resource(&kind, &ResourceName::new("first")?);
    let first = store
        .put_cas(PutRequest {
            key: first_key,
            value: serde_json::to_vec(&toy_resource_named("first", None)?)?,
            expected: ExpectedVersion::Missing,
            session: None,
        })
        .await?;
    assert!(matches!(first, CasOutcome::Applied(_)));

    let reconciler = Arc::new(ToyReconciler {
        reconciles: AtomicUsize::new(0),
        finalizes: AtomicUsize::new(0),
    });
    let runtime = Arc::new(ControllerRuntime::new_with_trigger_prefix(
        reconciler.clone(),
        keys.resource_kind(&kind),
        keys.resources(),
        fenced,
        clock.clone(),
        RuntimeConfig::new(
            Duration::from_secs(30),
            Backoff::new(Duration::from_secs(1), Duration::from_secs(8))?,
        )?,
    ));
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let runtime_task = {
        let runtime = runtime.clone();
        tokio::spawn(async move { runtime.run(shutdown_rx).await })
    };

    wait_for_count(&reconciler.reconciles, 1).await?;
    let second_key = keys.resource(&kind, &ResourceName::new("second")?);
    let second = store
        .put_cas(PutRequest {
            key: second_key,
            value: serde_json::to_vec(&toy_resource_named("second", None)?)?,
            expected: ExpectedVersion::Missing,
            session: None,
        })
        .await?;
    assert!(matches!(second, CasOutcome::Applied(_)));
    wait_for_count(&reconciler.reconciles, 2).await?;

    let dependency_kind = ResourceKind::new("Dependency")?;
    let dependency_key = keys.resource(&dependency_kind, &ResourceName::new("shared")?);
    let dependency = store
        .put_cas(PutRequest {
            key: dependency_key,
            value: b"dependency events are never decoded as Toy resources".to_vec(),
            expected: ExpectedVersion::Missing,
            session: None,
        })
        .await?;
    assert!(matches!(dependency, CasOutcome::Applied(_)));
    wait_for_count(&reconciler.reconciles, 4).await?;

    clock.advance(Duration::from_secs(30));
    wait_for_count(&reconciler.reconciles, 6).await?;
    shutdown_tx.send_replace(true);
    runtime_task.await??;
    Ok(())
}

async fn wait_for_count(
    counter: &AtomicUsize,
    minimum: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    for _ in 0..100 {
        if counter.load(Ordering::SeqCst) >= minimum {
            return Ok(());
        }
        tokio::task::yield_now().await;
    }
    Err(format!(
        "counter did not reach {minimum}; observed {}",
        counter.load(Ordering::SeqCst)
    )
    .into())
}

fn toy_resource_named(
    name: &str,
    deletion_timestamp: Option<Timestamp>,
) -> Result<Object<ResourceName, ToySpec, ToyStatus>, kernel_api::InvalidIdentifier> {
    Ok(Object {
        meta: ObjectMeta {
            id: ResourceName::new(name)?,
            labels: BTreeMap::new(),
            annotations: BTreeMap::new(),
            revision: ResourceRevision::default(),
            generation: Generation::default(),
            owner_refs: Vec::new(),
            finalizers: BTreeSet::new(),
            deletion_timestamp,
        },
        spec: ToySpec {
            desired: "ready".to_string(),
        },
        status: ToyStatus,
    })
}
