use std::collections::{BTreeMap, BTreeSet};
use std::future::pending;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use kernel_api::{
    ArtifactTemplate, Assignment, ClusterId, Condition, ConditionReason, ConditionState,
    ConditionType, Deployment, DeploymentId, DeploymentPhase, DeploymentSpec, DeploymentStatus,
    ExecPolicy, Generation, Node, NodeApiAccess, NodeId, NodeInstanceId, NodeNetwork,
    NodeNetworkId, NodeNetworkSpec, NodeNetworkStatus, NodeRole, NodeSpec, NodeStatus, ObjectMeta,
    PlacementConstraint, ResourceKind, ResourceName, ResourceRevision, RolloutState, Service,
    ServiceId, ServiceSpec, ServiceStatus, Timestamp, UnschedulableReplica, VolumeAccess,
    VolumeMountSpec, VolumeSource,
};
use kernel_controller::{FencedStore, LeaderIdentity, LeadershipToken};
use kernel_store::{
    CasOutcome, Clock, DeleteRequest, ExpectedVersion, InMemoryStore, Keyspace, MonotonicTime,
    PutRequest, Session, SessionBinding, Store,
};

use crate::{Scheduler, SchedulerError, SchedulerSettings};

#[tokio::test]
async fn scheduler_scales_one_service_across_three_nodes_atomically()
-> Result<(), Box<dyn std::error::Error>> {
    let world = World::new(5).await?;
    let first = world.reconcile(Timestamp(1_000)).await?;
    assert_eq!((first.desired, first.created, first.deleted), (5, 5, 0));
    assert!(first.unschedulable.is_empty());
    assert_eq!(
        placement_counts(&world.assignments().await?),
        BTreeMap::from([
            (node_id("node-1"), 2),
            (node_id("node-2"), 2),
            (node_id("node-3"), 1),
        ])
    );
    let first_observation = world.scheduler_observation().await?;
    assert_eq!(
        serde_json::from_slice::<Vec<UnschedulableReplica>>(&first_observation.value)?,
        Vec::new()
    );
    let original_zero = assignment_for_slot(&world.assignments().await?, 0)?
        .meta
        .id
        .clone();

    let unchanged = world.reconcile(Timestamp(2_000)).await?;
    assert_eq!((unchanged.created, unchanged.deleted), (0, 0));
    assert_eq!(
        world.scheduler_observation().await?.version,
        first_observation.version
    );

    world.set_replica_override(2).await?;
    let reduced = world.reconcile(Timestamp(3_000)).await?;
    assert_eq!(
        (reduced.desired, reduced.created, reduced.deleted),
        (2, 0, 3)
    );
    let assignments = world.assignments().await?;
    assert_eq!(assignments.len(), 2);
    assert_eq!(assignment_for_slot(&assignments, 0)?.meta.id, original_zero);
    Ok(())
}

#[tokio::test]
async fn scheduler_holds_transient_node_loss_then_replaces_after_grace()
-> Result<(), Box<dyn std::error::Error>> {
    let world = World::new(1).await?;
    world.reconcile(Timestamp(1_000)).await?;
    let original = world.assignments().await?.remove(0);
    assert_eq!(original.spec.node_id, node_id("node-1"));
    world.remove_liveness(&original.spec.node_id).await?;

    let held = world.reconcile(Timestamp(10_000)).await?;
    assert_eq!((held.created, held.deleted), (0, 0));
    assert_eq!(world.assignments().await?, vec![original.clone()]);

    let replaced = world.reconcile(Timestamp(40_000)).await?;
    assert_eq!((replaced.created, replaced.deleted), (1, 1));
    let successor = world.assignments().await?.remove(0);
    assert_ne!(successor.meta.id, original.meta.id);
    assert_ne!(successor.spec.node_id, original.spec.node_id);
    assert_eq!(successor.spec.placement_epoch, 2);
    assert_eq!(
        successor.spec.replaces_assignment_id,
        Some(original.meta.id)
    );
    Ok(())
}

#[tokio::test]
async fn malformed_resource_preserves_the_last_committed_assignment_generation()
-> Result<(), Box<dyn std::error::Error>> {
    let world = World::new(1).await?;
    world.reconcile(Timestamp(1_000)).await?;
    let committed = world.assignments().await?;
    world.corrupt_first_network().await?;

    assert!(matches!(
        world.reconcile(Timestamp(2_000)).await,
        Err(SchedulerError::MalformedResource {
            kind: "NodeNetwork",
            ..
        })
    ));
    assert_eq!(world.assignments().await?, committed);
    Ok(())
}

#[tokio::test]
async fn host_volumes_pin_placement_and_invalid_changes_preserve_running_work()
-> Result<(), Box<dyn std::error::Error>> {
    let world = World::new(1).await?;
    world.set_host_volume_nodes(&["node-3"]).await?;
    world.reconcile(Timestamp(1_000)).await?;
    let pinned = world.assignments().await?.remove(0);
    assert_eq!(pinned.spec.node_id, node_id("node-3"));

    world.set_host_volume_nodes(&["node-1", "node-2"]).await?;
    let invalid = world.reconcile(Timestamp(2_000)).await?;
    assert_eq!((invalid.created, invalid.deleted), (0, 0));
    assert_eq!(invalid.unschedulable.len(), 1);
    assert_eq!(world.assignments().await?, vec![pinned]);
    assert_eq!(
        serde_json::from_slice::<Vec<UnschedulableReplica>>(
            &world.scheduler_observation().await?.value
        )?,
        vec![UnschedulableReplica {
            service_id: service_id(),
            deployment_id: deployment_id(),
            replica_index: 0,
            reason: "host-backed volumes require different nodes".to_string(),
        }]
    );
    Ok(())
}

pub(super) struct World {
    pub(super) store: Arc<InMemoryStore>,
    pub(super) keys: Keyspace,
    scheduler: Scheduler,
    pub(super) fenced: FencedStore,
    _leader_session: Box<dyn Session>,
}

impl World {
    pub(super) async fn new(replicas: u32) -> Result<Self, Box<dyn std::error::Error>> {
        let cluster_id = ClusterId::new("cluster-1")?;
        let keys = Keyspace::new(&cluster_id);
        let store = Arc::new(InMemoryStore::new(Arc::new(NoopClock)));
        let leader_session = store.session(Duration::from_secs(30)).await?;
        let leader = store
            .put_cas(PutRequest {
                key: keys.leader(),
                value: b"scheduler".to_vec(),
                expected: ExpectedVersion::Missing,
                session: Some(SessionBinding {
                    session_id: leader_session.id(),
                }),
            })
            .await?;
        let CasOutcome::Applied(leader) = leader else {
            return Err("leader campaign conflicted".into());
        };
        let token = LeadershipToken::from_campaign(
            LeaderIdentity {
                node_id: node_id("node-1"),
                instance_id: NodeInstanceId::new("instance-controller")?,
            },
            leader_session.id(),
            leader.version,
        );
        let fenced = FencedStore::new(store.clone(), keys.leader(), token);
        let scheduler = Scheduler::new(
            cluster_id,
            SchedulerSettings {
                replacement_grace: Duration::from_secs(30),
                deployment_drain_grace: Duration::from_secs(30),
            },
        )?;
        let world = Self {
            store,
            keys,
            scheduler,
            fenced,
            _leader_session: leader_session,
        };
        world.seed(replicas).await?;
        Ok(world)
    }

    async fn seed(&self, replicas: u32) -> Result<(), Box<dyn std::error::Error>> {
        for index in 1..=3 {
            let id = node_id(&format!("node-{index}"));
            self.put("Node", id.as_str(), &node(id.clone())).await?;
            self.put(
                "NodeNetwork",
                &format!("network-node-{index}"),
                &network(id.clone(), index),
            )
            .await?;
            self.store
                .put_cas(PutRequest {
                    key: self.keys.node_liveness(&id),
                    value: b"live".to_vec(),
                    expected: ExpectedVersion::Missing,
                    session: None,
                })
                .await?;
        }
        self.put("Service", "api", &service(replicas)).await?;
        self.put("Deployment", "deployment-1", &deployment())
            .await?;
        Ok(())
    }

    pub(super) async fn reconcile(
        &self,
        now: Timestamp,
    ) -> Result<crate::SchedulerReport, SchedulerError> {
        self.scheduler.reconcile_once(&self.fenced, now).await
    }

    async fn put(
        &self,
        kind: &str,
        id: &str,
        resource: &impl serde::Serialize,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let key = self
            .keys
            .resource(&ResourceKind::new(kind)?, &ResourceName::new(id)?);
        self.store
            .put_cas(PutRequest {
                key,
                value: serde_json::to_vec(resource)?,
                expected: ExpectedVersion::Missing,
                session: None,
            })
            .await?;
        Ok(())
    }

    pub(super) async fn assignments(&self) -> Result<Vec<Assignment>, Box<dyn std::error::Error>> {
        self.store
            .list(&self.keys.resource_kind(&ResourceKind::new("Assignment")?))
            .await?
            .values
            .into_iter()
            .map(|stored| serde_json::from_slice(&stored.value).map_err(Into::into))
            .collect()
    }

    async fn scheduler_observation(
        &self,
    ) -> Result<kernel_store::StoredValue, Box<dyn std::error::Error>> {
        self.store
            .get(&self.keys.scheduler_observation())
            .await?
            .ok_or_else(|| "scheduler observation missing".into())
    }

    pub(super) async fn set_replica_override(
        &self,
        replicas: u32,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let key = self
            .keys
            .resource(&ResourceKind::new("Service")?, &ResourceName::new("api")?);
        let stored = self.store.get(&key).await?.ok_or("service missing")?;
        let mut service: Service = serde_json::from_slice(&stored.value)?;
        service.status.replica_override = Some(replicas);
        self.store
            .put_cas(PutRequest {
                key,
                value: serde_json::to_vec(&service)?,
                expected: ExpectedVersion::Exact(stored.version),
                session: None,
            })
            .await?;
        Ok(())
    }

    pub(super) async fn set_deployment_draining(
        &self,
        draining_at: Timestamp,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let key = self.keys.resource(
            &ResourceKind::new("Deployment")?,
            &ResourceName::new("deployment-1")?,
        );
        let stored = self.store.get(&key).await?.ok_or("deployment missing")?;
        let mut deployment: Deployment = serde_json::from_slice(&stored.value)?;
        deployment.status.phase = DeploymentPhase::Draining;
        deployment.status.draining_at = Some(draining_at);
        self.store
            .put_cas(PutRequest {
                key,
                value: serde_json::to_vec(&deployment)?,
                expected: ExpectedVersion::Exact(stored.version),
                session: None,
            })
            .await?;
        Ok(())
    }

    async fn set_host_volume_nodes(
        &self,
        node_ids: &[&str],
    ) -> Result<(), Box<dyn std::error::Error>> {
        let key = self
            .keys
            .resource(&ResourceKind::new("Service")?, &ResourceName::new("api")?);
        let stored = self.store.get(&key).await?.ok_or("service missing")?;
        let mut service: Service = serde_json::from_slice(&stored.value)?;
        service.meta.generation = Generation(service.meta.generation.0.saturating_add(1));
        service.spec.volumes = node_ids
            .iter()
            .map(|node| VolumeMountSpec {
                source: VolumeSource::HostPath {
                    path: format!("/srv/{node}"),
                    node_id: node_id(node),
                },
                target: format!("/data/{node}"),
                access: VolumeAccess::ReadWrite,
            })
            .collect();
        self.store
            .put_cas(PutRequest {
                key,
                value: serde_json::to_vec(&service)?,
                expected: ExpectedVersion::Exact(stored.version),
                session: None,
            })
            .await?;
        Ok(())
    }

    pub(super) async fn remove_liveness(
        &self,
        node_id: &NodeId,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let key = self.keys.node_liveness(node_id);
        let stored = self.store.get(&key).await?.ok_or("liveness missing")?;
        self.store
            .delete_cas(DeleteRequest {
                key,
                expected: stored.version,
            })
            .await?;
        Ok(())
    }

    async fn corrupt_first_network(&self) -> Result<(), Box<dyn std::error::Error>> {
        let key = self.keys.resource(
            &ResourceKind::new("NodeNetwork")?,
            &ResourceName::new("network-node-1")?,
        );
        let stored = self.store.get(&key).await?.ok_or("network missing")?;
        self.store
            .put_cas(PutRequest {
                key,
                value: b"not-json".to_vec(),
                expected: ExpectedVersion::Exact(stored.version),
                session: None,
            })
            .await?;
        Ok(())
    }
}

fn node(id: NodeId) -> Node {
    kernel_api::Object {
        meta: metadata(id.clone()),
        spec: NodeSpec {
            hostname: id.to_string(),
            host_address: IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)),
            role: NodeRole::Hybrid,
            scheduling_labels: BTreeMap::new(),
        },
        status: NodeStatus {
            instance_id: NodeInstanceId::new(format!("instance-{id}")).unwrap(),
            version: "1.0.0".to_string(),
            last_seen: Timestamp(0),
            conditions: Vec::new(),
        },
    }
}

fn network(node_id: NodeId, index: u8) -> NodeNetwork {
    kernel_api::Object {
        meta: metadata(NodeNetworkId::new(format!("network-{node_id}")).unwrap()),
        spec: NodeNetworkSpec {
            node_id,
            public_key: format!("public-key-{index}"),
            endpoint: SocketAddr::from(([10, 0, 0, index], 51_820)),
            workload_subnet: format!("10.42.{index}.0/24"),
            mtu_bytes: 1420,
        },
        status: NodeNetworkStatus {
            applied_generation: Generation(1),
            conditions: vec![condition("MeshReady", ConditionState::True)],
        },
    }
}

fn service(replicas: u32) -> Service {
    kernel_api::Object {
        meta: metadata(service_id()),
        spec: ServiceSpec {
            name: "API".to_owned(),
            version: "1.0.0".to_owned(),
            artifact: ArtifactTemplate::Image {
                reference: "registry.test/api:latest".to_owned(),
            },
            preview: None,
            command: None,
            replicas,
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
        status: ServiceStatus {
            active_deployment_id: Some(deployment_id()),
            replica_override: None,
            rollout: RolloutState::Active,
            rollout_bypass_generation: None,
            conditions: Vec::new(),
        },
    }
}

fn deployment() -> Deployment {
    kernel_api::Object {
        meta: metadata(deployment_id()),
        spec: DeploymentSpec {
            service_id: service_id(),
            service_generation: Generation(1),
            restart_generation: Generation(1),
            bypass_rollout_freeze: false,
            service: service(1).spec,
            goal: kernel_api::DeploymentGoal::Run,
            build_id: None,
        },
        status: DeploymentStatus {
            phase: DeploymentPhase::PendingReady,
            created_at: Timestamp(0),
            ready_at: None,
            draining_at: None,
            image_digest: Some("registry.test/api@sha256:abc".to_owned()),
            conditions: Vec::new(),
        },
    }
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

fn condition(kind: &str, state: ConditionState) -> Condition {
    Condition {
        condition_type: ConditionType(kind.to_owned()),
        state,
        reason: ConditionReason("TestFixture".to_owned()),
        message: String::new(),
        observed_generation: Generation(1),
        last_transition_time: Timestamp(0),
    }
}

fn placement_counts(assignments: &[Assignment]) -> BTreeMap<NodeId, usize> {
    assignments
        .iter()
        .fold(BTreeMap::new(), |mut counts, assignment| {
            *counts.entry(assignment.spec.node_id.clone()).or_default() += 1;
            counts
        })
}

fn assignment_for_slot(
    assignments: &[Assignment],
    replica_index: u32,
) -> Result<&Assignment, Box<dyn std::error::Error>> {
    assignments
        .iter()
        .find(|assignment| assignment.spec.replica_index == replica_index)
        .ok_or_else(|| "assignment slot missing".into())
}

fn service_id() -> ServiceId {
    ServiceId::new("api").unwrap()
}

fn deployment_id() -> DeploymentId {
    DeploymentId::new("deployment-1").unwrap()
}

fn node_id(value: &str) -> NodeId {
    NodeId::new(value).unwrap()
}

struct NoopClock;

#[async_trait]
impl Clock for NoopClock {
    fn now(&self) -> MonotonicTime {
        MonotonicTime::default()
    }

    async fn sleep_until(&self, _deadline: MonotonicTime) {
        pending().await
    }
}
