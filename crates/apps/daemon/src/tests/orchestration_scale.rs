use kernel_api::{Assignment, Service, TrafficGeneration, TrafficGenerationPhase};

use super::orchestration::RolloutWorld;

#[tokio::test]
async fn replica_override_set_and_clear_preserve_ingress_cutover()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    for node_count in [1_u8, 3_u8] {
        let configured = usize::from(node_count);
        let overridden = if node_count == 1 { 3 } else { 1 };
        let world = RolloutWorld::new(node_count).await?;
        world.converge().await?;

        world
            .set_replica_override(Some(u32::try_from(overridden)?))
            .await?;
        converge_replica_change(&world, configured, overridden).await?;
        assert_eq!(
            world
                .list::<Service>("Service")
                .await?
                .first()
                .ok_or("service missing")?
                .status
                .replica_override,
            Some(u32::try_from(overridden)?)
        );

        world.set_replica_override(None).await?;
        converge_replica_change(&world, overridden, configured).await?;
        assert_eq!(
            world
                .list::<Service>("Service")
                .await?
                .first()
                .ok_or("service missing")?
                .status
                .replica_override,
            None
        );
    }
    Ok(())
}

async fn converge_replica_change(
    world: &RolloutWorld,
    current: usize,
    desired: usize,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if desired < current {
        converge_scale_down(world, current, desired).await
    } else {
        world.converge().await?;
        assert_eq!(world.list::<Assignment>("Assignment").await?.len(), desired);
        assert_eq!(active_target_count(world).await?, desired);
        Ok(())
    }
}

async fn converge_scale_down(
    world: &RolloutWorld,
    current: usize,
    desired: usize,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut cut_over = false;
    for _pass in 0..8 {
        world.reconcile_pass().await?;
        let active_targets = active_target_count(world).await?;
        let assignment_count = world.list::<Assignment>("Assignment").await?.len();
        if active_targets == desired {
            assert_eq!(assignment_count, current);
            cut_over = true;
            break;
        }
        assert_eq!(active_targets, current);
        assert_eq!(assignment_count, current);
    }
    assert!(
        cut_over,
        "ingress did not activate the scaled-down generation"
    );

    world.reconcile_pass().await?;
    assert_eq!(world.list::<Assignment>("Assignment").await?.len(), desired);
    assert_eq!(active_target_count(world).await?, desired);
    Ok(())
}

async fn active_target_count(
    world: &RolloutWorld,
) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
    world
        .list::<TrafficGeneration>("TrafficGeneration")
        .await?
        .into_iter()
        .find(|generation| generation.status.phase == TrafficGenerationPhase::Active)
        .map(|generation| generation.spec.targets.len())
        .ok_or_else(|| "active traffic generation missing".into())
}
