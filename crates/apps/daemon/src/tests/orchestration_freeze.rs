use kernel_api::{Assignment, Deployment, DeploymentPhase, RolloutState, TrafficGeneration};

use super::orchestration::RolloutWorld;

#[tokio::test]
async fn frozen_rollout_stays_queued_until_explicit_unfreeze()
-> Result<(), Box<dyn std::error::Error>> {
    for node_count in [1_u8, 3_u8] {
        let world = RolloutWorld::new(node_count).await?;
        world.set_rollout_state(RolloutState::Frozen).await?;
        world.converge().await?;

        let deployments = world.list::<Deployment>("Deployment").await?;
        assert_eq!(deployments.len(), 1);
        assert_eq!(
            deployments
                .first()
                .ok_or("queued deployment missing")?
                .status
                .phase,
            DeploymentPhase::Queued
        );
        assert!(world.list::<Assignment>("Assignment").await?.is_empty());
        assert!(
            world
                .list::<TrafficGeneration>("TrafficGeneration")
                .await?
                .is_empty()
        );

        world.set_rollout_state(RolloutState::Active).await?;
        world.converge().await?;

        assert_eq!(
            world
                .list::<Deployment>("Deployment")
                .await?
                .first()
                .ok_or("ready deployment missing")?
                .status
                .phase,
            DeploymentPhase::Ready
        );
        assert_eq!(
            world.list::<Assignment>("Assignment").await?.len(),
            usize::from(node_count)
        );
        assert_eq!(
            world
                .list::<TrafficGeneration>("TrafficGeneration")
                .await?
                .first()
                .ok_or("active traffic missing")?
                .spec
                .targets
                .len(),
            usize::from(node_count)
        );
    }
    Ok(())
}
