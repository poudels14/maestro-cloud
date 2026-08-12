use std::collections::BTreeSet;

use kernel_api::{NodeId, UpgradeMode, UpgradePhase};
use kernel_controller::ControllerError;

use super::reconciler_world::World;

#[tokio::test]
async fn runtime_replays_an_accepted_batch_then_completes_from_node_observations()
-> Result<(), Box<dyn std::error::Error>> {
    let world = World::new(true).await?;
    world.pass().await?;
    world.pass().await?;
    assert_eq!(world.run().await?.status.phase, UpgradePhase::Draining);
    assert_eq!(world.maintained_nodes().await?.len(), 3);

    world.pass().await?;
    assert_eq!(world.run().await?.status.phase, UpgradePhase::Applying);
    world.pass().await?;
    assert_eq!(world.run().await?.status.phase, UpgradePhase::Applying);
    world.pass().await?;
    assert_eq!(world.run().await?.status.phase, UpgradePhase::Restarting);
    assert_eq!(world.requests().len(), 2);
    assert_eq!(world.requests().first(), world.requests().get(1));

    world.pass().await?;
    assert_eq!(world.run().await?.status.phase, UpgradePhase::Verifying);
    world.pass().await?;
    let completed = world.run().await?;
    assert_eq!(completed.status.phase, UpgradePhase::Completed);
    assert!(
        completed
            .status
            .nodes
            .iter()
            .all(|status| status.phase == UpgradePhase::Completed && status.attempts == 1)
    );
    assert!(world.maintained_nodes().await?.is_empty());
    Ok(())
}

#[tokio::test]
async fn all_node_upgrade_ignores_irrelevant_assignment_cardinality()
-> Result<(), Box<dyn std::error::Error>> {
    let world = World::new(false).await?;
    world.add_assignments(130).await?;

    world.pass().await?;
    world.pass().await?;

    assert_eq!(world.run().await?.status.phase, UpgradePhase::Draining);
    Ok(())
}

#[tokio::test]
async fn deletion_finalizer_restores_scheduling_before_collecting_the_run()
-> Result<(), Box<dyn std::error::Error>> {
    let world = World::new(false).await?;
    world.pass().await?;
    world.pass().await?;
    assert_eq!(world.maintained_nodes().await?.len(), 3);

    world.mark_run_deleting().await?;
    world.pass().await?;
    assert!(world.run_optional().await?.is_some());
    assert!(world.maintained_nodes().await?.is_empty());
    world.pass().await?;
    assert!(world.run_optional().await?.is_none());
    assert!(world.requests().is_empty());
    Ok(())
}

#[tokio::test]
async fn rolling_leader_batch_resumes_after_fenced_leadership_handoff()
-> Result<(), Box<dyn std::error::Error>> {
    let world = World::new_with_mode(true, UpgradeMode::Rolling).await?;
    for _pass in 0..32 {
        world.pass().await?;
        let run = world.run().await?;
        let leader_is_draining =
            run.status.nodes.iter().any(|node| {
                node.node_id.as_str() == "node-1" && node.phase == UpgradePhase::Draining
            });
        let completed = run
            .status
            .nodes
            .iter()
            .filter(|node| node.phase == UpgradePhase::Completed)
            .count();
        if leader_is_draining && completed == 2 {
            break;
        }
    }
    assert_eq!(
        world.maintained_nodes().await?,
        BTreeSet::from([NodeId::new("node-1")?])
    );

    let (successor, _successor_session) = world.handoff("node-2").await?;
    let stale_error = world
        .stale_pass()
        .await
        .expect_err("stale leader must be fenced");
    assert!(matches!(stale_error, ControllerError::LeadershipLost));

    for _pass in 0..16 {
        successor.reconcile_snapshot().await?;
        if world.run().await?.status.phase == UpgradePhase::Completed {
            break;
        }
    }
    let completed = world.run().await?;
    assert_eq!(completed.status.phase, UpgradePhase::Completed);
    assert!(
        completed
            .status
            .nodes
            .iter()
            .all(|node| node.phase == UpgradePhase::Completed && node.attempts == 1)
    );
    assert!(world.maintained_nodes().await?.is_empty());
    Ok(())
}
