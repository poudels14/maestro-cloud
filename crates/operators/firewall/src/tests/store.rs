use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use kernel_api::{
    FirewallPolicy, NodeFirewall, NodeId, NodeInstanceId, ResourceKind, ResourceName, Timestamp,
};
use kernel_controller::{Backoff, FencedStore, LeaderIdentity, LeadershipToken, RuntimeConfig};
use kernel_store::{
    CasOutcome, Clock, ExpectedVersion, InMemoryStore, Keyspace, MonotonicTime, PutRequest,
    Session, SessionBinding, Store,
};

use super::plan::World as PlannedWorld;
use crate::{
    FirewallBaselineReconciler, FirewallController, FirewallInput, FirewallPolicyReconciler,
};

#[tokio::test]
async fn controller_publishes_rulesets_then_waits_for_every_node_acknowledgement()
-> Result<(), Box<dyn std::error::Error>> {
    let world = StoreWorld::new(PlannedWorld::standard().input()).await?;

    let published = world.controller.reconcile_once(&world.fenced).await?;
    assert_eq!(
        (
            published.published_rulesets,
            published.pending_rulesets,
            published.updated_policies,
        ),
        (2, 2, 0)
    );
    let desired = world.list::<NodeFirewall>("NodeFirewall").await?;
    assert_eq!(desired.len(), 2);
    assert!(desired.iter().all(|resource| {
        resource.status.applied_digest.is_none()
            && resource.spec.script.contains("table inet maestro_firewall")
    }));
    for policy in world.list::<FirewallPolicy>("FirewallPolicy").await? {
        assert!(policy.status.ruleset_digest.is_none());
    }

    world.ack_firewalls().await?;
    let acknowledged = world.controller.reconcile_once(&world.fenced).await?;
    assert_eq!(acknowledged.pending_rulesets, 0);
    assert_eq!(acknowledged.updated_policies, 3);
    for policy in world.list::<FirewallPolicy>("FirewallPolicy").await? {
        assert_eq!(policy.status.applied_generation, policy.meta.generation);
        assert_eq!(
            policy.status.ruleset_digest,
            Some(acknowledged.bundle_digest.clone())
        );
    }
    Ok(())
}

#[tokio::test]
async fn policy_finalizer_waits_for_replacement_ruleset_acknowledgement()
-> Result<(), Box<dyn std::error::Error>> {
    let mut input = PlannedWorld::standard().input();
    input
        .policies
        .retain(|policy| policy.meta.id.as_str() == "api-egress");
    let world = StoreWorld::new(input).await?;
    let reconciler = Arc::new(FirewallPolicyReconciler::new(world.controller.clone())?);
    let runtime = reconciler.runtime(
        Arc::new(world.fenced.clone()),
        Arc::new(NoopClock),
        runtime_config()?,
    );
    assert_eq!(runtime.reconcile_snapshot().await?, 0);
    assert_eq!(runtime.reconcile_snapshot().await?, 1);
    world.ack_firewalls().await?;
    assert_eq!(runtime.reconcile_snapshot().await?, 1);
    world
        .update::<FirewallPolicy>("FirewallPolicy", "api-egress", |policy| {
            policy.meta.deletion_timestamp = Some(Timestamp(1_000));
        })
        .await?;

    assert_eq!(runtime.reconcile_snapshot().await?, 1);
    assert_eq!(
        world.list::<FirewallPolicy>("FirewallPolicy").await?.len(),
        1
    );
    world.ack_firewalls().await?;
    for _pass in 0..3 {
        runtime.reconcile_snapshot().await?;
        if world
            .list::<FirewallPolicy>("FirewallPolicy")
            .await?
            .is_empty()
        {
            break;
        }
    }
    assert!(
        world
            .list::<FirewallPolicy>("FirewallPolicy")
            .await?
            .is_empty()
    );
    let desired = world.list::<NodeFirewall>("NodeFirewall").await?;
    assert!(
        desired
            .iter()
            .all(|resource| !resource.spec.script.contains("192.0.2.0/24"))
    );
    Ok(())
}

#[tokio::test]
async fn baseline_reconciler_publishes_host_guards_without_user_policies()
-> Result<(), Box<dyn std::error::Error>> {
    let mut input = PlannedWorld::standard().input();
    input.policies.clear();
    let world = StoreWorld::new(input).await?;
    let reconciler = Arc::new(FirewallBaselineReconciler::new(world.controller.clone())?);
    let runtime = reconciler.runtime(
        Arc::new(world.fenced.clone()),
        Arc::new(NoopClock),
        runtime_config()?,
    );

    assert_eq!(runtime.reconcile_snapshot().await?, 2);
    let desired = world.list::<NodeFirewall>("NodeFirewall").await?;
    assert_eq!(desired.len(), 2);
    for resource in desired {
        assert!(resource.spec.script.contains("tcp dport 53 accept"));
        assert!(
            resource
                .spec
                .script
                .contains("tcp dport { 3000, 3001 } reject")
        );
        assert!(
            resource
                .spec
                .script
                .contains("ip saddr @all_workloads_v4 ct direction original reject")
        );
    }
    Ok(())
}

struct StoreWorld {
    keys: Keyspace,
    store: Arc<InMemoryStore>,
    fenced: FencedStore,
    controller: Arc<FirewallController>,
    _session: Box<dyn Session>,
}

impl StoreWorld {
    async fn new(input: FirewallInput) -> Result<Self, Box<dyn std::error::Error>> {
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
        let controller = Arc::new(FirewallController::new(cluster_id, input.settings.clone()));
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

    async fn ack_firewalls(&self) -> Result<(), Box<dyn std::error::Error>> {
        let kind = ResourceKind::new("NodeFirewall")?;
        for stored in self
            .store
            .list(&self.keys.resource_kind(&kind))
            .await?
            .values
        {
            let mut resource: NodeFirewall = serde_json::from_slice(&stored.value)?;
            resource.meta.revision = stored.version.resource_revision();
            resource.status.applied_generation = resource.meta.generation;
            resource.status.applied_digest = Some(resource.spec.digest.clone());
            let outcome = self
                .store
                .put_cas(PutRequest {
                    key: stored.key,
                    value: serde_json::to_vec(&resource)?,
                    expected: ExpectedVersion::Exact(stored.version),
                    session: None,
                })
                .await?;
            if !matches!(outcome, CasOutcome::Applied(_)) {
                return Err("NodeFirewall acknowledgement conflicted".into());
            }
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
