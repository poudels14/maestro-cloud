use kernel_api::{
    Condition, ConditionReason, ConditionState, ConditionType, Node, NodeId, ResourceKind,
    ResourceName, Service, Timestamp,
};
use kernel_store::{CasOutcome, ExpectedVersion, PutRequest, Store};

use super::scheduler::World;

#[tokio::test]
async fn scheduler_releases_draining_assignments_only_after_traffic_grace()
-> Result<(), Box<dyn std::error::Error>> {
    let world = World::new(1).await?;
    world.reconcile(Timestamp(1_000)).await?;
    let original = world.assignments().await?;
    world.set_deployment_draining(Timestamp(10_000)).await?;

    let held = world.reconcile(Timestamp(39_999)).await?;
    assert_eq!((held.created, held.deleted), (0, 0));
    assert_eq!(world.assignments().await?, original);

    let released = world.reconcile(Timestamp(40_000)).await?;
    assert_eq!((released.created, released.deleted), (0, 1));
    assert!(world.assignments().await?.is_empty());
    Ok(())
}

#[tokio::test]
async fn deleting_service_holds_existing_assignments_until_drain_deadline()
-> Result<(), Box<dyn std::error::Error>> {
    let world = World::new(1).await?;
    world.reconcile(Timestamp(1_000)).await?;
    let original = world.assignments().await?;
    world.set_deployment_draining(Timestamp(10_000)).await?;
    mark_service_deleting(&world, Timestamp(10_000)).await?;

    let held = world.reconcile(Timestamp(39_999)).await?;
    assert_eq!((held.created, held.deleted), (0, 0));
    assert_eq!(world.assignments().await?, original);

    let released = world.reconcile(Timestamp(40_000)).await?;
    assert_eq!((released.created, released.deleted), (0, 1));
    assert!(world.assignments().await?.is_empty());
    Ok(())
}

#[tokio::test]
async fn explicit_node_drain_replaces_assignments_without_liveness_grace()
-> Result<(), Box<dyn std::error::Error>> {
    let world = World::new(3).await?;
    world.reconcile(Timestamp(1_000)).await?;
    let drained_node = NodeId::new("node-1")?;
    set_node_draining(&world, &drained_node).await?;

    let report = world.reconcile(Timestamp(2_000)).await?;
    assert_eq!((report.created, report.deleted), (1, 1));
    assert!(report.unschedulable.is_empty());
    assert!(
        world
            .assignments()
            .await?
            .iter()
            .all(|assignment| assignment.spec.node_id != drained_node)
    );
    Ok(())
}

async fn mark_service_deleting(
    world: &World,
    at: Timestamp,
) -> Result<(), Box<dyn std::error::Error>> {
    let key = world
        .keys
        .resource(&ResourceKind::new("Service")?, &ResourceName::new("api")?);
    let stored = world.store.get(&key).await?.ok_or("service missing")?;
    let mut service: Service = serde_json::from_slice(&stored.value)?;
    service.meta.deletion_timestamp = Some(at);
    let outcome = world
        .store
        .put_cas(PutRequest {
            key,
            value: serde_json::to_vec(&service)?,
            expected: ExpectedVersion::Exact(stored.version),
            session: None,
        })
        .await?;
    if matches!(outcome, CasOutcome::Applied(_)) {
        Ok(())
    } else {
        Err("service deletion marker conflicted".into())
    }
}

async fn set_node_draining(
    world: &World,
    node_id: &NodeId,
) -> Result<(), Box<dyn std::error::Error>> {
    let key = world.keys.resource(
        &ResourceKind::new("Node")?,
        &ResourceName::from(node_id.clone()),
    );
    let stored = world.store.get(&key).await?.ok_or("node missing")?;
    let mut node: Node = serde_json::from_slice(&stored.value)?;
    node.status.conditions.push(Condition {
        condition_type: ConditionType("Draining".to_string()),
        state: ConditionState::True,
        reason: ConditionReason("Requested".to_string()),
        message: "node drain requested".to_string(),
        observed_generation: node.meta.generation,
        last_transition_time: Timestamp(2_000),
    });
    let outcome = world
        .store
        .put_cas(PutRequest {
            key,
            value: serde_json::to_vec(&node)?,
            expected: ExpectedVersion::Exact(stored.version),
            session: None,
        })
        .await?;
    if matches!(outcome, CasOutcome::Applied(_)) {
        Ok(())
    } else {
        Err("node drain update conflicted".into())
    }
}
