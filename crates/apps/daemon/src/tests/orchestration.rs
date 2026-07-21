use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use firewall::{FirewallBackend, FirewallBackendError, FirewallBundle};
use ingress::{BackendChange, IngressBackend, IngressBackendError};
use kernel_api::{
    Assignment, AssignmentId, AssignmentPhase, ClusterId, Deployment, DeploymentPhase, DnsRecord,
    NodeId, NodeInstanceId, Object, ResourceKind, ResourceName, Service, TrafficGeneration,
    TrafficGenerationPhase,
};
use kernel_controller::{FencedStore, LeaderIdentity, LeadershipToken};
use kernel_store::{
    CasOutcome, Clock, ExpectedVersion, InMemoryStore, Keyspace, PutRequest, Session,
    SessionBinding, Store,
};

use super::orchestration_fixture::{
    FixedTimestampClock, NoopClock, network, node, ready_replica, route, service, settings,
};
use crate::{OperatorBackends, OperatorSuite};

#[tokio::test]
async fn rollout_converges_across_one_and_three_node_topologies()
-> Result<(), Box<dyn std::error::Error>> {
    for node_count in [1_u8, 3_u8] {
        let world = RolloutWorld::new(node_count).await?;
        world.converge().await?;
        world.assert_ready(node_count).await?;
    }
    Ok(())
}

struct RolloutWorld {
    keys: Keyspace,
    store: Arc<InMemoryStore>,
    suite: OperatorSuite,
    ingress: Arc<RecordingIngress>,
    firewall: Arc<RecordingFirewall>,
    _session: Box<dyn Session>,
}

impl RolloutWorld {
    async fn new(node_count: u8) -> Result<Self, Box<dyn std::error::Error>> {
        let cluster_id = ClusterId::new(format!("rollout-{node_count}"))?;
        let keys = Keyspace::new(&cluster_id);
        let monotonic: Arc<dyn Clock> = Arc::new(NoopClock);
        let store = Arc::new(InMemoryStore::new(monotonic.clone()));
        let session = store.session(Duration::from_secs(30)).await?;
        let leader = store
            .put_cas(PutRequest {
                key: keys.leader(),
                value: b"rollout-suite".to_vec(),
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
                    instance_id: NodeInstanceId::new("rollout-suite")?,
                },
                session.id(),
                leader.version,
            ),
        ));
        for index in 1..=node_count {
            let node_id = NodeId::new(format!("node-{index}"))?;
            put(&store, &keys, "Node", &node(&node_id, index)?).await?;
            put(&store, &keys, "NodeNetwork", &network(&node_id, index)?).await?;
            put_key(&store, keys.node_liveness(&node_id), b"live".to_vec()).await?;
        }
        put(&store, &keys, "Service", &service(u32::from(node_count))?).await?;
        put(&store, &keys, "IngressRoute", &route()?).await?;

        let ingress = Arc::new(RecordingIngress::default());
        let firewall = Arc::new(RecordingFirewall::default());
        let suite = OperatorSuite::new(
            cluster_id,
            fenced,
            monotonic,
            Arc::new(FixedTimestampClock),
            settings()?,
            OperatorBackends {
                ingress: ingress.clone(),
                firewall: firewall.clone(),
            },
        )?;
        Ok(Self {
            keys,
            store,
            suite,
            ingress,
            firewall,
            _session: session,
        })
    }

    async fn converge(&self) -> Result<(), Box<dyn std::error::Error>> {
        let mut quiet_passes = 0_u8;
        for _pass in 0..32 {
            let before = self.store.list(&self.keys.cluster()).await?.cursor;
            self.suite.reconcile_snapshot().await?;
            self.publish_ready_replicas().await?;
            let after = self.store.list(&self.keys.cluster()).await?.cursor;
            quiet_passes = if before == after {
                quiet_passes.saturating_add(1)
            } else {
                0
            };
            if quiet_passes == 2 {
                return Ok(());
            }
        }
        Err("operator suite did not reach two quiet passes".into())
    }

    async fn publish_ready_replicas(&self) -> Result<(), Box<dyn std::error::Error>> {
        for assignment in self.list::<Assignment>("Assignment").await? {
            if assignment.status.phase != AssignmentPhase::Running {
                self.update::<Assignment>("Assignment", &assignment.meta.id, |current| {
                    current.status.phase = AssignmentPhase::Running;
                })
                .await?;
            }
            let replica = ready_replica(&assignment)?;
            let key = self.keys.resource(
                &ResourceKind::new("ReplicaState")?,
                &ResourceName::from(replica.meta.id.clone()),
            );
            if self.store.get(&key).await?.is_none() {
                put(&self.store, &self.keys, "ReplicaState", &replica).await?;
            }
        }
        Ok(())
    }

    async fn assert_ready(&self, node_count: u8) -> Result<(), Box<dyn std::error::Error>> {
        let service = self.one::<Service>("Service").await?;
        assert!(service.status.active_deployment_id.is_some());
        let deployment = self.one::<Deployment>("Deployment").await?;
        assert_eq!(deployment.status.phase, DeploymentPhase::Ready);

        let assignments = self.list::<Assignment>("Assignment").await?;
        assert_eq!(assignments.len(), usize::from(node_count));
        assert_eq!(
            assignments
                .iter()
                .map(|assignment| &assignment.spec.node_id)
                .collect::<BTreeSet<_>>()
                .len(),
            usize::from(node_count)
        );

        let traffic = self.one::<TrafficGeneration>("TrafficGeneration").await?;
        assert_eq!(traffic.status.phase, TrafficGenerationPhase::Active);
        assert_eq!(traffic.spec.targets.len(), usize::from(node_count));
        assert!(!self.list::<DnsRecord>("DnsRecord").await?.is_empty());

        let ingress = self
            .ingress
            .changes
            .lock()
            .map_err(|_| "ingress change lock poisoned")?
            .last()
            .cloned()
            .ok_or("ingress never published")?;
        assert_eq!(
            ingress
                .active
                .ok_or("active ingress missing")?
                .spec
                .targets
                .len(),
            usize::from(node_count)
        );
        let firewall = self
            .firewall
            .bundles
            .lock()
            .map_err(|_| "firewall bundle lock poisoned")?
            .last()
            .cloned()
            .ok_or("firewall never applied")?;
        assert_eq!(firewall.rulesets.len(), usize::from(node_count));
        Ok(())
    }

    async fn update<Resource: serde::de::DeserializeOwned + serde::Serialize>(
        &self,
        kind: &str,
        id: &AssignmentId,
        change: impl FnOnce(&mut Resource),
    ) -> Result<(), Box<dyn std::error::Error>> {
        let key = self
            .keys
            .resource(&ResourceKind::new(kind)?, &ResourceName::from(id.clone()));
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
        list(&self.store, &self.keys, kind).await
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

#[derive(Default)]
struct RecordingIngress {
    changes: Mutex<Vec<BackendChange>>,
}

#[async_trait]
impl IngressBackend for RecordingIngress {
    async fn apply(&self, change: &BackendChange) -> Result<(), IngressBackendError> {
        self.changes
            .lock()
            .map_err(|_| IngressBackendError::new("ingress change lock poisoned"))?
            .push(change.clone());
        Ok(())
    }
}

#[derive(Default)]
struct RecordingFirewall {
    bundles: Mutex<Vec<FirewallBundle>>,
}

#[async_trait]
impl FirewallBackend for RecordingFirewall {
    async fn apply(&self, bundle: &FirewallBundle) -> Result<(), FirewallBackendError> {
        self.bundles
            .lock()
            .map_err(|_| FirewallBackendError::new("firewall bundle lock poisoned"))?
            .push(bundle.clone());
        Ok(())
    }
}

async fn put<Id, Spec, Status>(
    store: &InMemoryStore,
    keys: &Keyspace,
    kind: &str,
    resource: &Object<Id, Spec, Status>,
) -> Result<(), Box<dyn std::error::Error>>
where
    Id: Clone + Into<ResourceName> + serde::Serialize,
    Spec: serde::Serialize,
    Status: serde::Serialize,
{
    put_key(
        store,
        keys.resource(&ResourceKind::new(kind)?, &resource.meta.id.clone().into()),
        serde_json::to_vec(resource)?,
    )
    .await
}

async fn put_key(
    store: &InMemoryStore,
    key: kernel_store::StoreKey,
    value: Vec<u8>,
) -> Result<(), Box<dyn std::error::Error>> {
    let outcome = store
        .put_cas(PutRequest {
            key,
            value,
            expected: ExpectedVersion::Missing,
            session: None,
        })
        .await?;
    if matches!(outcome, CasOutcome::Applied(_)) {
        Ok(())
    } else {
        Err("resource create conflicted".into())
    }
}

async fn list<Resource: serde::de::DeserializeOwned>(
    store: &InMemoryStore,
    keys: &Keyspace,
    kind: &str,
) -> Result<Vec<Resource>, Box<dyn std::error::Error>> {
    store
        .list(&keys.resource_kind(&ResourceKind::new(kind)?))
        .await?
        .values
        .into_iter()
        .map(|stored| serde_json::from_slice(&stored.value).map_err(Into::into))
        .collect()
}
