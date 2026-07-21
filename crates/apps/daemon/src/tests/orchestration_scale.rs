use kernel_api::{Assignment, TrafficGeneration, TrafficGenerationPhase};

use super::orchestration::RolloutWorld;

#[tokio::test]
async fn scale_down_keeps_old_targets_until_ingress_cutover()
-> Result<(), Box<dyn std::error::Error>> {
    let world = RolloutWorld::new(3).await?;
    world.converge().await?;
    world.set_replica_override(Some(1)).await?;

    let mut cut_over = false;
    for _pass in 0..8 {
        world.reconcile_pass().await?;
        let active_targets = active_target_count(&world).await?;
        let assignment_count = world.list::<Assignment>("Assignment").await?.len();
        if active_targets == 1 {
            assert_eq!(assignment_count, 3);
            cut_over = true;
            break;
        }
        assert_eq!(active_targets, 3);
        assert_eq!(assignment_count, 3);
    }
    assert!(
        cut_over,
        "ingress did not activate the scaled-down generation"
    );

    world.reconcile_pass().await?;
    assert_eq!(world.list::<Assignment>("Assignment").await?.len(), 1);
    assert_eq!(active_target_count(&world).await?, 1);
    Ok(())
}

async fn active_target_count(world: &RolloutWorld) -> Result<usize, Box<dyn std::error::Error>> {
    world
        .list::<TrafficGeneration>("TrafficGeneration")
        .await?
        .into_iter()
        .find(|generation| generation.status.phase == TrafficGenerationPhase::Active)
        .map(|generation| generation.spec.targets.len())
        .ok_or_else(|| "active traffic generation missing".into())
}
