use kernel_api::{AssignmentId, ResourceKind, ResourceName};
use kernel_store::{CasOutcome, DeleteRequest, ExpectedVersion, Keyspace, PutRequest, Store};
use runtime::WorkloadRuntime;

use super::assignment::{World, assignment, cluster_id, deployment, node_id, put_resource};

#[tokio::test]
async fn assignment_reconcile_garbage_collects_workloads_after_assignment_loss()
-> Result<(), Box<dyn std::error::Error>> {
    let world = World::new();
    world.seed(&deployment(), &assignment()).await?;
    world.agent().reconcile_once().await?;
    let key = world.assignment_key();
    let stored = world.store.get(&key).await?.ok_or("assignment missing")?;
    world
        .store
        .delete_cas(DeleteRequest {
            key,
            expected: stored.version,
        })
        .await?;

    let report = world.agent().reconcile_once().await?;
    assert_eq!(report.garbage_collected, 1);
    assert!(
        world
            .runtime
            .list(&cluster_id(), &node_id("node-1"))
            .await?
            .is_empty()
    );
    assert_eq!(world.network.lease_count(), 0);
    Ok(())
}

#[tokio::test]
async fn assignment_reconcile_skips_gc_when_assignment_ownership_is_malformed()
-> Result<(), Box<dyn std::error::Error>> {
    let world = World::new();
    world.seed(&deployment(), &assignment()).await?;
    world.agent().reconcile_once().await?;
    let key = world.assignment_key();
    let stored = world.store.get(&key).await?.ok_or("assignment missing")?;
    world
        .store
        .delete_cas(DeleteRequest {
            key,
            expected: stored.version,
        })
        .await?;
    let malformed_key = Keyspace::new(&cluster_id()).resource(
        &ResourceKind::new("Assignment")?,
        &ResourceName::new("malformed-assignment")?,
    );
    put_resource(world.store.as_ref(), malformed_key, b"not-json".to_vec()).await?;

    let report = world.agent().reconcile_once().await?;
    assert_eq!(report.malformed_resources, 1);
    assert_eq!(report.garbage_collected, 0);
    assert_eq!(
        world
            .runtime
            .list(&cluster_id(), &node_id("node-1"))
            .await?
            .len(),
        1
    );
    Ok(())
}

#[tokio::test]
async fn assignment_reconcile_skips_gc_when_the_stored_identity_mismatches_its_key()
-> Result<(), Box<dyn std::error::Error>> {
    let world = World::new();
    world.seed(&deployment(), &assignment()).await?;
    world.agent().reconcile_once().await?;
    let key = world.assignment_key();
    let stored = world.store.get(&key).await?.ok_or("assignment missing")?;
    let mut mismatched = assignment();
    mismatched.meta.id = AssignmentId::new("mismatched-assignment")?;
    assert!(matches!(
        world
            .store
            .put_cas(PutRequest {
                key,
                value: serde_json::to_vec(&mismatched)?,
                expected: ExpectedVersion::Exact(stored.version),
                session: None,
            })
            .await?,
        CasOutcome::Applied(_)
    ));

    let report = world.agent().reconcile_once().await?;
    assert_eq!(report.malformed_resources, 1);
    assert_eq!(report.garbage_collected, 0);
    assert_eq!(
        world
            .runtime
            .list(&cluster_id(), &node_id("node-1"))
            .await?
            .len(),
        1
    );
    Ok(())
}
