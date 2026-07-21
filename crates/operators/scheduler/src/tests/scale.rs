use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;

use kernel_api::{
    Assignment, DeploymentId, Generation, ObjectMeta, ResourceKind, ResourceName, ResourceRevision,
    ServiceId, Timestamp, TrafficGeneration, TrafficGenerationId, TrafficGenerationPhase,
    TrafficGenerationSpec, TrafficGenerationStatus, TrafficTarget,
};
use kernel_store::{CasOutcome, ExpectedVersion, PutRequest, Store};

use super::scheduler::World;

#[tokio::test]
async fn scheduler_holds_scaled_down_targets_until_traffic_cutover()
-> Result<(), Box<dyn std::error::Error>> {
    let world = World::new(3).await?;
    world.reconcile(Timestamp(1_000)).await?;
    let original = world.assignments().await?;
    let original_ids = assignment_ids(&original);
    activate_traffic(&world, 1, &original).await?;

    world.set_replica_override(1).await?;
    let held = world.reconcile(Timestamp(2_000)).await?;
    assert_eq!((held.desired, held.created, held.deleted), (3, 0, 0));
    assert_eq!(assignment_ids(&world.assignments().await?), original_ids);

    let survivor = original
        .iter()
        .find(|assignment| assignment.spec.replica_index == 0)
        .ok_or("replica zero assignment missing")?;
    activate_traffic(&world, 2, std::slice::from_ref(survivor)).await?;
    let released = world.reconcile(Timestamp(3_000)).await?;
    assert_eq!(
        (released.desired, released.created, released.deleted),
        (1, 0, 2)
    );
    assert_eq!(world.assignments().await?, vec![survivor.clone()]);
    Ok(())
}

fn assignment_ids(assignments: &[Assignment]) -> BTreeSet<kernel_api::AssignmentId> {
    assignments
        .iter()
        .map(|assignment| assignment.meta.id.clone())
        .collect()
}

async fn activate_traffic(
    world: &World,
    epoch: u64,
    assignments: &[Assignment],
) -> Result<(), Box<dyn std::error::Error>> {
    let traffic_kind = ResourceKind::new("TrafficGeneration")?;
    for stored in world
        .store
        .list(&world.keys.resource_kind(&traffic_kind))
        .await?
        .values
    {
        let mut generation: TrafficGeneration = serde_json::from_slice(&stored.value)?;
        if generation.status.phase == TrafficGenerationPhase::Active {
            generation.status.phase = TrafficGenerationPhase::Retired;
            generation.status.retired_at = Some(Timestamp(2_000));
            world
                .store
                .put_cas(PutRequest {
                    key: stored.key,
                    value: serde_json::to_vec(&generation)?,
                    expected: ExpectedVersion::Exact(stored.version),
                    session: None,
                })
                .await?;
        }
    }

    let id = TrafficGenerationId::new(format!("traffic-{epoch}"))?;
    let generation = TrafficGeneration {
        meta: ObjectMeta {
            id: id.clone(),
            labels: BTreeMap::new(),
            annotations: BTreeMap::new(),
            revision: ResourceRevision::default(),
            generation: Generation(1),
            owner_refs: Vec::new(),
            finalizers: BTreeSet::new(),
            deletion_timestamp: None,
        },
        spec: TrafficGenerationSpec {
            service_id: ServiceId::new("api")?,
            deployment_id: DeploymentId::new("deployment-1")?,
            epoch,
            routes: Vec::new(),
            targets: assignments
                .iter()
                .map(|assignment| TrafficTarget {
                    assignment_id: assignment.meta.id.clone(),
                    node_id: assignment.spec.node_id.clone(),
                    endpoint: SocketAddr::new(assignment.spec.workload_address, 8080),
                })
                .collect(),
        },
        status: TrafficGenerationStatus {
            phase: TrafficGenerationPhase::Active,
            staged_at: Timestamp(1_000),
            activated_at: Some(Timestamp(1_000)),
            retired_at: None,
            conditions: Vec::new(),
        },
    };
    let outcome = world
        .store
        .put_cas(PutRequest {
            key: world.keys.resource(
                &traffic_kind,
                &ResourceName::from(generation.meta.id.clone()),
            ),
            value: serde_json::to_vec(&generation)?,
            expected: ExpectedVersion::Missing,
            session: None,
        })
        .await?;
    if matches!(outcome, CasOutcome::Applied(_)) {
        Ok(())
    } else {
        Err("traffic generation create conflicted".into())
    }
}
