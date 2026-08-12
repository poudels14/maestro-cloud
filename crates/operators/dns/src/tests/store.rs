use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use kernel_api::{
    ClusterId, DnsRecord, NodeId, NodeInstanceId, ResourceKind, ResourceName, Service, Timestamp,
};
use kernel_controller::{Backoff, FencedStore, LeaderIdentity, LeadershipToken, RuntimeConfig};
use kernel_store::{
    CasOutcome, Clock, ExpectedVersion, InMemoryStore, Keyspace, MonotonicTime, PutRequest,
    Session, SessionBinding, Store,
};

use super::plan::World as PlannedWorld;
use crate::snapshot::ResourceSnapshot;
use crate::writer::DnsWriter;
use crate::{DnsController, DnsReconciler, DnsSettings};

#[tokio::test]
async fn store_controller_atomically_publishes_and_replaces_service_records()
-> Result<(), Box<dyn std::error::Error>> {
    let world = StoreWorld::new().await?;
    let created = world.controller.reconcile_once(&world.fenced).await?;
    assert_eq!(created.created_records, 4);
    assert_eq!(world.list::<DnsRecord>("DnsRecord").await?.len(), 4);

    world
        .update::<DnsRecord>("DnsRecord", None, |record| {
            record.status.applied_generation = record.meta.generation;
            record.status.published_nodes = vec![NodeId::new("node-1").unwrap()];
        })
        .await?;
    let current = world.controller.reconcile_once(&world.fenced).await?;
    assert_eq!(
        (
            current.created_records,
            current.replaced_records,
            current.deleted_records,
            current.conflict,
        ),
        (0, 0, 0, false)
    );
    assert!(
        world
            .list::<DnsRecord>("DnsRecord")
            .await?
            .iter()
            .any(|record| !record.status.published_nodes.is_empty())
    );

    world
        .update::<kernel_api::Assignment>("Assignment", Some("assignment-0"), |assignment| {
            assignment.spec.workload_address = Some("10.42.1.99".parse().unwrap());
            assignment.status.workload_address = assignment.spec.workload_address;
        })
        .await?;
    let replaced = world.controller.reconcile_once(&world.fenced).await?;
    assert_eq!(replaced.replaced_records, 2);
    assert_eq!(world.list::<DnsRecord>("DnsRecord").await?.len(), 4);
    Ok(())
}

#[tokio::test]
async fn dependency_conflict_commits_no_partial_record_generation()
-> Result<(), Box<dyn std::error::Error>> {
    let world = StoreWorld::new().await?;
    let service_id = kernel_api::ServiceId::new("api")?;
    let snapshot = ResourceSnapshot::load_service(&world.fenced, &world.keys, &service_id).await?;
    let plan = crate::plan(snapshot.input(world.cluster_id.clone(), settings()))?;
    world
        .update::<Service>("Service", Some("api"), |_| {})
        .await?;

    let report = DnsWriter::new(&world.cluster_id)?
        .apply(&world.fenced, &snapshot, &plan)
        .await?;
    assert!(report.conflict);
    assert!(world.list::<DnsRecord>("DnsRecord").await?.is_empty());
    Ok(())
}

#[tokio::test]
async fn service_reconciliation_ignores_unrelated_cluster_cardinality()
-> Result<(), Box<dyn std::error::Error>> {
    let world = StoreWorld::new().await?;
    let template = PlannedWorld::ready().service;
    for index in 0..130 {
        let mut service = template.clone();
        service.meta.id = kernel_api::ServiceId::new(format!("unrelated-{index}"))?;
        world.put("Service", &service.meta.id, &service).await?;
    }

    let report = world
        .controller
        .reconcile_service(&world.fenced, &kernel_api::ServiceId::new("api")?)
        .await?;

    assert_eq!(report.created_records, 4);
    assert_eq!(world.list::<DnsRecord>("DnsRecord").await?.len(), 4);
    Ok(())
}

#[tokio::test]
async fn runtime_finalizes_service_only_after_owned_records_are_deleted()
-> Result<(), Box<dyn std::error::Error>> {
    let world = StoreWorld::new().await?;
    let reconciler = Arc::new(DnsReconciler::new(world.cluster_id.clone(), settings())?);
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
    assert_eq!(world.list::<DnsRecord>("DnsRecord").await?.len(), 4);
    world
        .update::<Service>("Service", Some("api"), |service| {
            service.meta.deletion_timestamp = Some(Timestamp(5_000));
        })
        .await?;

    assert_eq!(runtime.reconcile_snapshot().await?, 1);
    assert!(world.list::<Service>("Service").await?.is_empty());
    assert!(world.list::<DnsRecord>("DnsRecord").await?.is_empty());
    Ok(())
}

struct StoreWorld {
    cluster_id: ClusterId,
    keys: Keyspace,
    store: Arc<InMemoryStore>,
    fenced: FencedStore,
    controller: DnsController,
    _session: Box<dyn Session>,
}

impl StoreWorld {
    async fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let cluster_id = ClusterId::new("cluster-1")?;
        let keys = Keyspace::new(&cluster_id);
        let store = Arc::new(InMemoryStore::new(Arc::new(NoopClock)));
        let session = store.session(Duration::from_secs(30)).await?;
        let leader = store
            .put_cas(PutRequest {
                key: keys.leader(),
                value: b"dns-controller".to_vec(),
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
                    instance_id: NodeInstanceId::new("dns-controller")?,
                },
                session.id(),
                leader.version,
            ),
        );
        let controller = DnsController::new(cluster_id.clone(), settings())?;
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
        for assignment in &planned.assignments {
            self.put("Assignment", &assignment.meta.id, assignment)
                .await?;
        }
        for replica in &planned.replicas {
            self.put("ReplicaState", &replica.meta.id, replica).await?;
        }
        Ok(())
    }

    async fn put<Id: Clone + Into<ResourceName>>(
        &self,
        kind: &str,
        id: &Id,
        resource: &impl serde::Serialize,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let value = serde_json::to_vec(resource)?;
        if kind == "Assignment" {
            let assignment: kernel_api::Assignment = serde_json::from_slice(&value)?;
            let _ = self
                .store
                .put_cas(PutRequest {
                    key: self.keys.node_liveness(&assignment.spec.node_id),
                    value: b"live".to_vec(),
                    expected: ExpectedVersion::Missing,
                    session: Some(SessionBinding {
                        session_id: self._session.id(),
                    }),
                })
                .await?;
        }
        let outcome = self
            .store
            .put_cas(PutRequest {
                key: self
                    .keys
                    .resource(&ResourceKind::new(kind)?, &id.clone().into()),
                value,
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
        id: Option<&str>,
        change: impl FnOnce(&mut Resource),
    ) -> Result<(), Box<dyn std::error::Error>> {
        let stored = if let Some(id) = id {
            self.store
                .get(&self.key(kind, id)?)
                .await?
                .ok_or("resource missing")?
        } else {
            self.store
                .list(&self.keys.resource_kind(&ResourceKind::new(kind)?))
                .await?
                .values
                .into_iter()
                .next()
                .ok_or("resource missing")?
        };
        let mut resource = serde_json::from_slice::<Resource>(&stored.value)?;
        change(&mut resource);
        let outcome = self
            .store
            .put_cas(PutRequest {
                key: stored.key,
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
}

fn settings() -> DnsSettings {
    DnsSettings { ttl_secs: 5 }
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
