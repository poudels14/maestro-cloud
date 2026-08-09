use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use kernel_api::{
    BuildSource, DeploymentId, IngressRoute, NodeId, NodeInstanceId, Preview, PreviewPhase,
    ResourceKind, ResourceName, RolloutState, Service, Timestamp,
};
use kernel_controller::{
    Backoff, FencedStore, LeaderIdentity, LeadershipToken, RuntimeConfig, TimestampClock,
};
use kernel_store::{
    CasOutcome, Clock, ExpectedVersion, InMemoryStore, Keyspace, MonotonicTime, Mutation,
    PutRequest, Session, SessionBinding, Store, Transaction, TransactionOutcome,
};

use crate::{PreviewReconciler, PreviewSettings};

use super::support::{base_route, base_service, preview};

#[tokio::test]
async fn failed_preview_recovers_directly_to_its_active_child()
-> Result<(), Box<dyn std::error::Error>> {
    let world = World::new().await?;
    world.put("Service", &base_service()).await?;
    world.put("IngressRoute", &base_route()).await?;
    world.put("Preview", &preview()).await?;

    world.runtime.reconcile_snapshot().await?;
    world.runtime.reconcile_snapshot().await?;
    world
        .update::<Service>("Service", "api-pr-42", |service| {
            service.status.active_deployment_id = Some(DeploymentId::new("ready-preview").unwrap());
        })
        .await?;
    world.runtime.reconcile_snapshot().await?;
    assert_eq!(world.preview().await?.status.phase, PreviewPhase::Active);

    world.delete("IngressRoute", "api-route").await?;
    world.runtime.reconcile_snapshot().await?;
    assert_eq!(world.preview().await?.status.phase, PreviewPhase::Failed);

    world.put("IngressRoute", &base_route()).await?;
    world.runtime.reconcile_snapshot().await?;
    assert_eq!(world.preview().await?.status.phase, PreviewPhase::Active);
    Ok(())
}

#[tokio::test]
async fn preview_derives_updates_reopens_and_expires_owned_resources()
-> Result<(), Box<dyn std::error::Error>> {
    let world = World::new().await?;
    world.put("Service", &base_service()).await?;
    world.put("IngressRoute", &base_route()).await?;
    world.put("Preview", &preview()).await?;

    assert_eq!(world.runtime.reconcile_snapshot().await?, 0);
    assert_eq!(world.runtime.reconcile_snapshot().await?, 1);
    let child = world.child_service().await?;
    let route = world.child_route().await?;
    assert_eq!(child.meta.id.as_str(), "api-pr-42");
    assert_eq!(route.spec.hosts, ["api-pr-42.preview.example.test"]);
    assert_eq!(world.preview().await?.status.phase, PreviewPhase::Pending);

    world
        .update::<Service>("Service", "api-pr-42", |service| {
            service.status.active_deployment_id = Some(DeploymentId::new("ready-preview").unwrap());
        })
        .await?;
    world.runtime.reconcile_snapshot().await?;
    assert_eq!(world.preview().await?.status.phase, PreviewPhase::Active);

    world
        .update::<Service>("Service", "api-pr-42", |service| {
            service.status.active_deployment_id = None;
        })
        .await?;
    world.runtime.reconcile_snapshot().await?;
    assert_eq!(world.preview().await?.status.phase, PreviewPhase::Pending);

    world
        .update::<Service>("Service", "api-pr-42", |service| {
            service.status.active_deployment_id = Some(DeploymentId::new("ready-preview").unwrap());
        })
        .await?;
    world.runtime.reconcile_snapshot().await?;
    assert_eq!(world.preview().await?.status.phase, PreviewPhase::Active);

    world
        .update::<Service>("Service", "api", |service| {
            service.spec.version = "base-v2".to_string();
            service.status.rollout = RolloutState::Active;
        })
        .await?;
    world
        .update::<Preview>("Preview", "api-pr-42", |preview| {
            preview.spec.head_revision = "89abcdef0123456789abcdef0123456789abcdef".to_string();
        })
        .await?;
    world.runtime.reconcile_snapshot().await?;
    let pushed = world.child_service().await?;
    assert_eq!(pushed.meta.id.as_str(), "api-pr-42");
    assert_eq!(pushed.status.rollout, RolloutState::Active);
    assert_eq!(pushed.status.active_deployment_id, None);
    assert_eq!(world.preview().await?.status.phase, PreviewPhase::Pending);
    let kernel_api::ArtifactTemplate::Build { template } = pushed.spec.artifact else {
        return Err("preview artifact was not a build".into());
    };
    let BuildSource::Git { revision, .. } = template.source else {
        return Err("preview source was not Git".into());
    };
    assert_eq!(revision, "89abcdef0123456789abcdef0123456789abcdef");

    world.clock.set(20_000);
    world.close_preview(Timestamp(20_000)).await?;
    world.runtime.reconcile_snapshot().await?;
    let closing = world.preview().await?;
    assert_eq!(closing.status.phase, PreviewPhase::Closing);
    assert_eq!(closing.status.teardown_at, Some(Timestamp(30_000)));

    world.reopen_preview().await?;
    world.runtime.reconcile_snapshot().await?;
    assert_eq!(world.preview().await?.status.phase, PreviewPhase::Pending);

    world.close_preview(Timestamp(20_000)).await?;
    world.clock.set(30_000);
    world.runtime.reconcile_snapshot().await?;
    assert_eq!(
        world.child_service().await?.meta.deletion_timestamp,
        Some(Timestamp(30_000))
    );
    world.delete("Service", "api-pr-42").await?;
    world.runtime.reconcile_snapshot().await?;
    assert_eq!(world.preview().await?.status.phase, PreviewPhase::Expired);
    assert_eq!(world.list::<IngressRoute>("IngressRoute").await?.len(), 1);
    world.runtime.reconcile_snapshot().await?;
    assert!(world.list::<Preview>("Preview").await?.is_empty());
    Ok(())
}

struct World {
    keys: Keyspace,
    store: Arc<InMemoryStore>,
    runtime: kernel_controller::ControllerRuntime<PreviewReconciler>,
    clock: Arc<ManualClock>,
    _session: Box<dyn Session>,
}

impl World {
    async fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let cluster_id = kernel_api::ClusterId::new("preview-test")?;
        let keys = Keyspace::new(&cluster_id);
        let clock = Arc::new(ManualClock::new(10_000));
        let store = Arc::new(InMemoryStore::new(clock.clone()));
        let session = store.session(Duration::from_secs(30)).await?;
        let leader = store
            .put_cas(PutRequest {
                key: keys.leader(),
                value: b"preview-test".to_vec(),
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
                    node_id: NodeId::new("node-1")?,
                    instance_id: NodeInstanceId::new("preview-test")?,
                },
                session.id(),
                leader.version,
            ),
        ));
        let reconciler = Arc::new(PreviewReconciler::new(
            cluster_id,
            clock.clone(),
            clock.clone(),
            PreviewSettings::new("preview.example.test")?,
        )?);
        let runtime = reconciler.runtime(
            fenced,
            RuntimeConfig::new(
                Duration::from_secs(30),
                Backoff::new(Duration::from_millis(10), Duration::from_secs(1))?,
            )?,
        );
        Ok(Self {
            keys,
            store,
            runtime,
            clock,
            _session: session,
        })
    }

    async fn child_service(&self) -> Result<Service, Box<dyn std::error::Error>> {
        self.one("Service", "api-pr-42").await
    }

    async fn child_route(&self) -> Result<IngressRoute, Box<dyn std::error::Error>> {
        self.list::<IngressRoute>("IngressRoute")
            .await?
            .into_iter()
            .find(|route| route.spec.service_id.as_str() == "api-pr-42")
            .ok_or_else(|| "derived route missing".into())
    }

    async fn preview(&self) -> Result<Preview, Box<dyn std::error::Error>> {
        self.one("Preview", "api-pr-42").await
    }

    async fn close_preview(&self, at: Timestamp) -> Result<(), Box<dyn std::error::Error>> {
        self.update::<Preview>("Preview", "api-pr-42", |preview| {
            preview.meta.deletion_timestamp = Some(at);
        })
        .await
    }

    async fn reopen_preview(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.update::<Preview>("Preview", "api-pr-42", |preview| {
            preview.meta.deletion_timestamp = None;
        })
        .await
    }

    async fn put<Id, Spec, Status>(
        &self,
        kind: &str,
        resource: &kernel_api::Object<Id, Spec, Status>,
    ) -> Result<(), Box<dyn std::error::Error>>
    where
        Id: Clone + Into<ResourceName> + serde::Serialize,
        Spec: serde::Serialize,
        Status: serde::Serialize,
    {
        let resource_name = resource.meta.id.clone().into();
        let outcome = self
            .store
            .put_cas(PutRequest {
                key: self
                    .keys
                    .resource(&ResourceKind::new(kind)?, &resource_name),
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
        let key = self.keys.resource(
            &ResourceKind::new(kind)?,
            &ResourceName::new(id.to_string())?,
        );
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

    async fn delete(&self, kind: &str, id: &str) -> Result<(), Box<dyn std::error::Error>> {
        let key = self.keys.resource(
            &ResourceKind::new(kind)?,
            &ResourceName::new(id.to_string())?,
        );
        let stored = self.store.get(&key).await?.ok_or("resource missing")?;
        let outcome = self
            .store
            .txn(Transaction {
                compares: vec![kernel_store::Compare {
                    key: key.clone(),
                    expected: ExpectedVersion::Exact(stored.version),
                }],
                mutations: vec![Mutation::Delete { key }],
            })
            .await?;
        if matches!(outcome, TransactionOutcome::Applied { .. }) {
            Ok(())
        } else {
            Err(format!("{kind} delete conflicted").into())
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
        id: &str,
    ) -> Result<Resource, Box<dyn std::error::Error>> {
        self.store
            .get(&self.keys.resource(
                &ResourceKind::new(kind)?,
                &ResourceName::new(id.to_string())?,
            ))
            .await?
            .ok_or_else(|| format!("{kind} `{id}` missing").into())
            .and_then(|stored| serde_json::from_slice(&stored.value).map_err(Into::into))
    }
}

struct ManualClock(AtomicI64);

impl ManualClock {
    fn new(millis: i64) -> Self {
        Self(AtomicI64::new(millis))
    }

    fn set(&self, millis: i64) {
        self.0.store(millis, Ordering::SeqCst);
    }
}

impl TimestampClock for ManualClock {
    fn now(&self) -> Timestamp {
        Timestamp(self.0.load(Ordering::SeqCst))
    }
}

#[async_trait]
impl Clock for ManualClock {
    fn now(&self) -> MonotonicTime {
        let millis = u64::try_from(self.0.load(Ordering::SeqCst)).unwrap_or_default();
        MonotonicTime::from_duration(Duration::from_millis(millis))
    }

    async fn sleep_until(&self, _deadline: MonotonicTime) {
        std::future::pending::<()>().await;
    }
}
