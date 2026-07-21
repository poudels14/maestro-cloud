use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use firewall::{FirewallBackend, FirewallBackendError, FirewallBundle};
use ingress::{BackendChange, IngressBackend, IngressBackendError};
use kernel_api::{
    Assignment, AssignmentId, AssignmentPhase, ClusterId, Deployment, DeploymentId,
    DeploymentPhase, DnsRecord, Generation, NodeId, NodeInstanceId, Object, ResourceKind,
    ResourceName, Service, TrafficGeneration, TrafficGenerationPhase,
};
use kernel_controller::{FencedStore, LeaderIdentity, LeadershipToken};
use kernel_store::{
    CasOutcome, Clock, ExpectedVersion, InMemoryStore, Keyspace, PutRequest, Session,
    SessionBinding, Store,
};

use super::orchestration_fixture::{
    ManualTimestampClock, NoopClock, network, node, ready_replica, route, service, settings,
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

#[tokio::test]
async fn redeploy_cuts_over_before_collecting_drained_generation()
-> Result<(), Box<dyn std::error::Error>> {
    for node_count in [1_u8, 3_u8] {
        let world = RolloutWorld::new(node_count).await?;
        world.converge().await?;
        let old_deployment = world.begin_redeploy().await?;
        let ingress_start = world.ingress_len()?;

        world.converge().await?;

        let deployments = world.list::<Deployment>("Deployment").await?;
        assert_eq!(deployments.len(), 2);
        assert!(
            deployments.iter().any(|deployment| {
                deployment.meta.id == old_deployment
                    && deployment.status.phase == DeploymentPhase::Draining
            }),
            "deployments after cutover: {deployments:#?}"
        );
        assert!(deployments.iter().any(|deployment| {
            deployment.meta.id != old_deployment
                && deployment.status.phase == DeploymentPhase::Ready
        }));
        assert_eq!(
            world.list::<Assignment>("Assignment").await?.len(),
            usize::from(node_count) * 2
        );
        assert!(
            world
                .ingress_since(ingress_start)?
                .iter()
                .all(|change| change.active.is_some())
        );

        world.timestamp.set(41_000);
        world.converge().await?;

        let deployments = world.list::<Deployment>("Deployment").await?;
        assert!(deployments.iter().any(|deployment| {
            deployment.meta.id == old_deployment
                && deployment.status.phase == DeploymentPhase::Removed
        }));
        assert_eq!(
            world.list::<Assignment>("Assignment").await?.len(),
            usize::from(node_count)
        );
        let traffic = world.list::<TrafficGeneration>("TrafficGeneration").await?;
        assert_eq!(traffic.len(), 1);
        assert_eq!(
            traffic
                .first()
                .ok_or("active traffic missing")?
                .status
                .phase,
            TrafficGenerationPhase::Active
        );
    }
    Ok(())
}

pub(super) struct RolloutWorld {
    pub(super) keys: Keyspace,
    pub(super) store: Arc<InMemoryStore>,
    suite: OperatorSuite,
    ingress: Arc<RecordingIngress>,
    firewall: Arc<RecordingFirewall>,
    timestamp: Arc<ManualTimestampClock>,
    _session: Box<dyn Session>,
}

impl RolloutWorld {
    pub(super) async fn new(node_count: u8) -> Result<Self, Box<dyn std::error::Error>> {
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
        let timestamp = Arc::new(ManualTimestampClock::new(10_000));
        let suite = OperatorSuite::new(
            cluster_id.clone(),
            fenced.clone(),
            monotonic,
            timestamp.clone(),
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
            timestamp,
            _session: session,
        })
    }

    pub(super) async fn converge(&self) -> Result<(), Box<dyn std::error::Error>> {
        let mut quiet_passes = 0_u8;
        for _pass in 0..32 {
            let before = self.store.list(&self.keys.cluster()).await?.cursor;
            self.reconcile_pass().await?;
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

    pub(super) async fn reconcile_pass(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.suite.reconcile_snapshot().await?;
        self.publish_ready_replicas().await
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

    async fn begin_redeploy(&self) -> Result<DeploymentId, Box<dyn std::error::Error>> {
        let service = self
            .update_service(|service| {
                service.meta.generation = Generation(service.meta.generation.0.saturating_add(1));
                service.spec.version = "2.0.0".to_string();
            })
            .await?;
        let old_deployment = service
            .status
            .active_deployment_id
            .clone()
            .ok_or("service has no active deployment")?;
        Ok(old_deployment)
    }

    pub(super) fn set_time(&self, millis: i64) {
        self.timestamp.set(millis);
    }

    fn ingress_len(&self) -> Result<usize, Box<dyn std::error::Error>> {
        Ok(self
            .ingress
            .changes
            .lock()
            .map_err(|_| "ingress change lock poisoned")?
            .len())
    }

    fn ingress_since(
        &self,
        start: usize,
    ) -> Result<Vec<BackendChange>, Box<dyn std::error::Error>> {
        Ok(self
            .ingress
            .changes
            .lock()
            .map_err(|_| "ingress change lock poisoned")?
            .get(start..)
            .ok_or("invalid ingress history cursor")?
            .to_vec())
    }

    pub(super) fn latest_firewall_bundle(
        &self,
    ) -> Result<FirewallBundle, Box<dyn std::error::Error>> {
        let bundles = self
            .firewall
            .bundles
            .lock()
            .map_err(|_| "firewall bundle lock poisoned")?;
        bundles
            .last()
            .cloned()
            .ok_or_else(|| "firewall never applied".into())
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

    pub(super) async fn list<Resource: serde::de::DeserializeOwned>(
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

pub(super) async fn put<Id, Spec, Status>(
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
