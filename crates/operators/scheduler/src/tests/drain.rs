use kernel_api::Timestamp;

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
