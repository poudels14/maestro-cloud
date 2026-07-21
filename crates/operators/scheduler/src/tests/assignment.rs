use std::collections::{BTreeMap, BTreeSet};
use std::future::pending;
use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use kernel_api::{
    Assignment, AssignmentId, AssignmentPhase, AssignmentSpec, AssignmentStatus, ClusterId,
    Generation, NodeId, NodeInstanceId, ObjectMeta, ResourceKind, ResourceName, ResourceRevision,
    ServiceId,
};
use kernel_controller::{FencedStore, LeaderIdentity, LeadershipToken};
use kernel_store::{
    CasOutcome, Clock, ExpectedVersion, InMemoryStore, Keyspace, MonotonicTime, PutRequest,
    SessionBinding, Store,
};

use crate::assignment::{AssignmentWriteError, AssignmentWriter};

#[tokio::test]
async fn assignment_writer_applies_create_and_delete_as_one_generation()
-> Result<(), Box<dyn std::error::Error>> {
    let world = World::new().await?;
    let old = assignment("old", "node-a", [10, 42, 1, 2]);
    world.put_assignment(&old).await?;
    let current = world.assignment_snapshot().await?;
    let generation = world.scheduler_generation().await?;
    let replacement = assignment("replacement", "node-b", [10, 42, 2, 2]);

    let report = world
        .writer
        .apply(
            &world.fenced,
            &current,
            std::slice::from_ref(&replacement),
            generation,
            Vec::new(),
        )
        .await?;
    assert_eq!(
        (report.created, report.deleted, report.conflict),
        (1, 1, false)
    );
    assert_eq!(world.load_assignments().await?, vec![replacement]);
    Ok(())
}

#[tokio::test]
async fn assignment_writer_preserves_agent_status_for_retained_specs()
-> Result<(), Box<dyn std::error::Error>> {
    let world = World::new().await?;
    let mut running = assignment("assignment-1", "node-a", [10, 42, 1, 2]);
    running.status.phase = AssignmentPhase::Running;
    running.status.workload_id = Some(kernel_api::WorkloadId::new("workload-1")?);
    world.put_assignment(&running).await?;
    let current = world.assignment_snapshot().await?;
    let generation = world.scheduler_generation().await?;
    let mut stale_planner_copy = running.clone();
    stale_planner_copy.status.phase = AssignmentPhase::Pending;
    stale_planner_copy.status.workload_id = None;

    let report = world
        .writer
        .apply(
            &world.fenced,
            &current,
            &[stale_planner_copy],
            generation,
            Vec::new(),
        )
        .await?;
    assert_eq!(report, Default::default());
    assert_eq!(world.load_assignments().await?, vec![running]);
    Ok(())
}

#[tokio::test]
async fn assignment_writer_conflict_commits_no_partial_generation()
-> Result<(), Box<dyn std::error::Error>> {
    let world = World::new().await?;
    let old = assignment("old", "node-a", [10, 42, 1, 2]);
    world.put_assignment(&old).await?;
    let current = world.assignment_snapshot().await?;
    let generation = world.scheduler_generation().await?;
    let concurrent = assignment("concurrent", "node-a", [10, 42, 1, 3]);
    world.put_assignment(&concurrent).await?;
    world.advance_scheduler_generation(generation).await?;
    let replacement = assignment("replacement", "node-b", [10, 42, 2, 2]);

    let report = world
        .writer
        .apply(
            &world.fenced,
            &current,
            &[replacement],
            generation,
            Vec::new(),
        )
        .await?;
    assert!(report.conflict);
    assert_eq!(world.load_assignments().await?, vec![concurrent, old]);
    Ok(())
}

#[tokio::test]
async fn assignment_writer_rejects_identity_collision_and_malformed_ownership()
-> Result<(), Box<dyn std::error::Error>> {
    let world = World::new().await?;
    let existing = assignment("stable", "node-a", [10, 42, 1, 2]);
    world.put_assignment(&existing).await?;
    let current = world.assignment_snapshot().await?;
    let generation = world.scheduler_generation().await?;
    let conflicting = assignment("stable", "node-b", [10, 42, 2, 2]);
    assert!(matches!(
        world
            .writer
            .apply(
                &world.fenced,
                &current,
                &[conflicting],
                generation,
                Vec::new(),
            )
            .await,
        Err(AssignmentWriteError::IdentityCollision { .. })
    ));

    let wrong_key = world.keys.resource(
        &ResourceKind::new("Assignment")?,
        &ResourceName::new("wrong-key")?,
    );
    let malformed = kernel_store::StoredValue {
        key: wrong_key,
        value: serde_json::to_vec(&existing)?,
        version: current[0].version,
    };
    assert!(matches!(
        world
            .writer
            .apply(&world.fenced, &[malformed], &[], generation, Vec::new(),)
            .await,
        Err(AssignmentWriteError::ResourceIdentityMismatch { .. })
    ));
    Ok(())
}

struct World {
    store: Arc<InMemoryStore>,
    keys: Keyspace,
    writer: AssignmentWriter,
    fenced: FencedStore,
}

impl World {
    async fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let cluster_id = ClusterId::new("cluster-1")?;
        let keys = Keyspace::new(&cluster_id);
        let store = Arc::new(InMemoryStore::new(Arc::new(NoopClock)));
        let session = store.session(Duration::from_secs(30)).await?;
        let leader = store
            .put_cas(PutRequest {
                key: keys.leader(),
                value: b"scheduler".to_vec(),
                expected: ExpectedVersion::Missing,
                session: Some(SessionBinding {
                    session_id: session.id(),
                }),
            })
            .await?;
        let CasOutcome::Applied(leader) = leader else {
            return Err("leader campaign conflicted".into());
        };
        let token = LeadershipToken::from_campaign(
            LeaderIdentity {
                node_id: NodeId::new("node-1")?,
                instance_id: NodeInstanceId::new("instance-1")?,
            },
            session.id(),
            leader.version,
        );
        let fenced = FencedStore::new(store.clone(), keys.leader(), token);
        Ok(Self {
            store,
            keys,
            writer: AssignmentWriter::new(&cluster_id)?,
            fenced,
        })
    }

    async fn put_assignment(
        &self,
        assignment: &Assignment,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let key = self.keys.resource(
            &ResourceKind::new("Assignment")?,
            &ResourceName::from(assignment.meta.id.clone()),
        );
        self.store
            .put_cas(PutRequest {
                key,
                value: serde_json::to_vec(assignment)?,
                expected: ExpectedVersion::Missing,
                session: None,
            })
            .await?;
        Ok(())
    }

    async fn assignment_snapshot(
        &self,
    ) -> Result<Vec<kernel_store::StoredValue>, Box<dyn std::error::Error>> {
        Ok(self
            .store
            .list(&self.keys.resource_kind(&ResourceKind::new("Assignment")?))
            .await?
            .values)
    }

    async fn load_assignments(&self) -> Result<Vec<Assignment>, Box<dyn std::error::Error>> {
        self.assignment_snapshot()
            .await?
            .into_iter()
            .map(|stored| serde_json::from_slice(&stored.value).map_err(Into::into))
            .collect()
    }

    async fn scheduler_generation(&self) -> Result<ExpectedVersion, Box<dyn std::error::Error>> {
        Ok(
            match self.store.get(&self.keys.scheduler_generation()).await? {
                Some(stored) => ExpectedVersion::Exact(stored.version),
                None => ExpectedVersion::Missing,
            },
        )
    }

    async fn advance_scheduler_generation(
        &self,
        expected: ExpectedVersion,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.store
            .put_cas(PutRequest {
                key: self.keys.scheduler_generation(),
                value: b"concurrent scheduler".to_vec(),
                expected,
                session: None,
            })
            .await?;
        Ok(())
    }
}

fn assignment(id: &str, node: &str, address: [u8; 4]) -> Assignment {
    Assignment {
        meta: ObjectMeta {
            id: AssignmentId::new(id).unwrap(),
            labels: BTreeMap::new(),
            annotations: BTreeMap::new(),
            revision: ResourceRevision::default(),
            generation: Generation(1),
            owner_refs: Vec::new(),
            finalizers: BTreeSet::new(),
            deletion_timestamp: None,
        },
        spec: AssignmentSpec {
            service_id: ServiceId::new("api").unwrap(),
            deployment_id: kernel_api::DeploymentId::new("deployment-1").unwrap(),
            replica_index: 0,
            node_id: NodeId::new(node).unwrap(),
            placement_epoch: 1,
            workload_address: IpAddr::V4(Ipv4Addr::from(address)),
            replaces_assignment_id: None,
        },
        status: AssignmentStatus {
            phase: AssignmentPhase::Pending,
            workload_id: None,
            conditions: Vec::new(),
        },
    }
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
