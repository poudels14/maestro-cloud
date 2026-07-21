use kernel_api::{
    Assignment, Deployment, DeploymentGoal, DeploymentPhase, RolloutState, Service,
    TrafficGeneration, TrafficGenerationPhase,
};

use super::orchestration::RolloutWorld;

#[tokio::test]
async fn cancel_queued_same_spec_redeploy_preserves_the_serving_deployment()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    for node_count in [1_u8, 3_u8] {
        let world = RolloutWorld::new(node_count).await?;
        world.converge().await?;
        let active_id = active_service_deployment(&world).await?;

        world.set_rollout_state(RolloutState::Frozen).await?;
        world.redeploy_service().await?;
        world.converge().await?;
        let queued = world
            .list::<Deployment>("Deployment")
            .await?
            .into_iter()
            .find(|deployment| deployment.status.phase == DeploymentPhase::Queued)
            .ok_or("queued same-spec redeploy missing")?;

        world
            .request_deployment_goal(&queued.meta.id, DeploymentGoal::Cancel)
            .await?;
        world.converge().await?;

        let deployments = world.list::<Deployment>("Deployment").await?;
        assert!(deployments.iter().any(|deployment| {
            deployment.meta.id == queued.meta.id
                && deployment.status.phase == DeploymentPhase::Canceled
        }));
        assert_eq!(active_service_deployment(&world).await?, active_id);
        assert_eq!(
            world.list::<Assignment>("Assignment").await?.len(),
            usize::from(node_count)
        );
        assert_eq!(active_traffic(&world).await?.spec.deployment_id, active_id);
    }
    Ok(())
}

#[tokio::test]
async fn remove_active_deployment_drains_then_same_spec_redeploy_recovers()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    for node_count in [1_u8, 3_u8] {
        let world = RolloutWorld::new(node_count).await?;
        world.converge().await?;
        let removed_id = active_service_deployment(&world).await?;

        world
            .request_deployment_goal(&removed_id, DeploymentGoal::Remove)
            .await?;
        world.converge().await?;
        assert_eq!(active_service_deployment_optional(&world).await?, None);
        assert_eq!(
            world.list::<Assignment>("Assignment").await?.len(),
            usize::from(node_count)
        );
        assert!(
            world
                .list::<TrafficGeneration>("TrafficGeneration")
                .await?
                .iter()
                .all(|generation| generation.status.phase != TrafficGenerationPhase::Active)
        );

        world.set_time(41_000);
        world.converge().await?;
        assert!(world.list::<Assignment>("Assignment").await?.is_empty());
        assert!(
            world
                .list::<Deployment>("Deployment")
                .await?
                .iter()
                .any(|deployment| {
                    deployment.meta.id == removed_id
                        && deployment.status.phase == DeploymentPhase::Removed
                })
        );

        world.redeploy_service().await?;
        world.converge().await?;
        let restarted_id = active_service_deployment(&world).await?;
        assert_ne!(restarted_id, removed_id);
        assert_eq!(
            world.list::<Assignment>("Assignment").await?.len(),
            usize::from(node_count)
        );
        assert_eq!(
            active_traffic(&world).await?.spec.deployment_id,
            restarted_id
        );
    }
    Ok(())
}

#[tokio::test]
async fn restart_replaces_workloads_under_the_same_deployment()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    for node_count in [1_u8, 3_u8] {
        let world = RolloutWorld::new(node_count).await?;
        world.converge().await?;
        let deployment_id = active_service_deployment(&world).await?;
        let previous = world
            .list::<Assignment>("Assignment")
            .await?
            .into_iter()
            .map(|assignment| assignment.meta.id)
            .collect::<std::collections::BTreeSet<_>>();

        world.restart_deployment(&deployment_id).await?;
        world.converge().await?;

        let deployments = world.list::<Deployment>("Deployment").await?;
        assert_eq!(deployments.len(), 1);
        assert_eq!(
            deployments.first().ok_or("deployment missing")?.meta.id,
            deployment_id
        );
        assert_eq!(active_service_deployment(&world).await?, deployment_id);
        let current = world.list::<Assignment>("Assignment").await?;
        assert_eq!(current.len(), usize::from(node_count));
        assert!(current.iter().all(|assignment| {
            assignment.spec.deployment_id == deployment_id
                && assignment.spec.restart_generation == kernel_api::Generation(2)
                && !previous.contains(&assignment.meta.id)
        }));
        let traffic = active_traffic(&world).await?;
        assert_eq!(traffic.spec.deployment_id, deployment_id);
        assert!(traffic.spec.targets.iter().all(|target| {
            current
                .iter()
                .any(|assignment| assignment.meta.id == target.assignment_id)
        }));
    }
    Ok(())
}

async fn active_service_deployment(
    world: &RolloutWorld,
) -> Result<kernel_api::DeploymentId, Box<dyn std::error::Error + Send + Sync>> {
    active_service_deployment_optional(world)
        .await?
        .ok_or_else(|| "active service deployment missing".into())
}

async fn active_service_deployment_optional(
    world: &RolloutWorld,
) -> Result<Option<kernel_api::DeploymentId>, Box<dyn std::error::Error + Send + Sync>> {
    Ok(world
        .list::<Service>("Service")
        .await?
        .into_iter()
        .next()
        .ok_or("service missing")?
        .status
        .active_deployment_id)
}

async fn active_traffic(
    world: &RolloutWorld,
) -> Result<TrafficGeneration, Box<dyn std::error::Error + Send + Sync>> {
    world
        .list::<TrafficGeneration>("TrafficGeneration")
        .await?
        .into_iter()
        .find(|generation| generation.status.phase == TrafficGenerationPhase::Active)
        .ok_or_else(|| "active traffic generation missing".into())
}
