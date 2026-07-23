use clustertest::NodeDrainState;
use kernel_api::{Assignment, NodeId, TrafficGeneration, TrafficGenerationPhase};

use super::orchestration::RolloutWorld;

#[tokio::test]
async fn node_drain_replaces_when_possible_and_restore_accepts_new_work()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    for node_count in [1_u8, 3_u8] {
        let world = RolloutWorld::new(node_count).await?;
        world.converge().await?;
        let selected = NodeId::new(format!("node-{node_count}"))?;

        world
            .set_node_draining(&selected, NodeDrainState::Draining)
            .await?;
        world.converge().await?;

        let drained = world.list::<Assignment>("Assignment").await?;
        assert_eq!(drained.len(), usize::from(node_count));
        if node_count == 1 {
            assert!(
                drained
                    .iter()
                    .all(|assignment| assignment.spec.node_id == selected)
            );
        } else {
            assert!(
                drained
                    .iter()
                    .all(|assignment| assignment.spec.node_id != selected)
            );
        }
        assert_eq!(active_target_count(&world).await?, usize::from(node_count));

        world
            .set_node_draining(&selected, NodeDrainState::Available)
            .await?;
        world
            .set_replica_override(Some(u32::from(node_count).saturating_add(1)))
            .await?;
        world.converge().await?;

        let restored = world.list::<Assignment>("Assignment").await?;
        assert_eq!(restored.len(), usize::from(node_count) + 1);
        assert!(
            restored
                .iter()
                .any(|assignment| assignment.spec.node_id == selected)
        );
        assert_eq!(
            active_target_count(&world).await?,
            usize::from(node_count) + 1
        );
    }
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
