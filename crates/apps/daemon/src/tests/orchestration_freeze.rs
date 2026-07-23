use kernel_api::{
    Assignment, Deployment, DeploymentPhase, Generation, RolloutState, Service, TrafficGeneration,
};

use super::orchestration::RolloutWorld;

#[tokio::test]
async fn frozen_rollout_stays_queued_until_explicit_unfreeze()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
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

#[tokio::test]
async fn forced_frozen_rollout_is_one_shot_across_supported_topologies()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    for node_count in [1_u8, 3_u8] {
        let world = RolloutWorld::new(node_count).await?;
        world.converge().await?;
        let forced_generation = world
            .update_service(|service| {
                service.status.rollout = RolloutState::Frozen;
                service.meta.generation = Generation(service.meta.generation.0.saturating_add(1));
                service.spec.version = "2.0.0".to_string();
                service.status.rollout_bypass_generation = Some(service.meta.generation);
            })
            .await?
            .meta
            .generation;
        world.converge().await?;

        let service = world
            .list::<Service>("Service")
            .await?
            .into_iter()
            .next()
            .ok_or("service missing after forced rollout")?;
        let forced = world
            .list::<Deployment>("Deployment")
            .await?
            .into_iter()
            .find(|deployment| deployment.spec.service_generation == forced_generation)
            .ok_or("forced deployment missing")?;
        assert_eq!(service.status.rollout, RolloutState::Frozen);
        assert_eq!(service.status.rollout_bypass_generation, None);
        assert_eq!(
            service.status.active_deployment_id,
            Some(forced.meta.id.clone())
        );
        assert_eq!(forced.status.phase, DeploymentPhase::Ready);
        assert!(forced.spec.bypass_rollout_freeze);

        let held_generation = world
            .update_service(|service| {
                service.meta.generation = Generation(service.meta.generation.0.saturating_add(1));
                service.spec.version = "3.0.0".to_string();
            })
            .await?
            .meta
            .generation;
        world.converge().await?;

        let service = world
            .list::<Service>("Service")
            .await?
            .into_iter()
            .next()
            .ok_or("service missing after later rollout")?;
        let held = world
            .list::<Deployment>("Deployment")
            .await?
            .into_iter()
            .find(|deployment| deployment.spec.service_generation == held_generation)
            .ok_or("later deployment missing")?;
        assert_eq!(service.status.rollout, RolloutState::Frozen);
        assert_eq!(service.status.active_deployment_id, Some(forced.meta.id));
        assert_eq!(held.status.phase, DeploymentPhase::Queued);
        assert!(!held.spec.bypass_rollout_freeze);
    }
    Ok(())
}
