use super::lifecycle::LifecycleWorld;
use crate::{ScenarioError, scenarios};

#[tokio::test]
async fn basic_lifecycle_scenarios_pass_fast_fake() -> Result<(), ScenarioError> {
    scenarios::rollout_reaches_ready(&mut LifecycleWorld::new()).await?;
    scenarios::redeploy_drains_previous(&mut LifecycleWorld::new()).await?;
    scenarios::queued_deployment_can_be_canceled(&mut LifecycleWorld::new()).await?;
    scenarios::replica_override_round_trips(&mut LifecycleWorld::new()).await?;
    scenarios::drained_deployment_finalizes(&mut LifecycleWorld::new()).await?;
    scenarios::build_failure_marks_deployment_crashed(&mut LifecycleWorld::new()).await?;
    scenarios::prepare_failure_marks_deployment_crashed(&mut LifecycleWorld::new()).await?;
    scenarios::crashed_replica_restarts_in_place(&mut LifecycleWorld::new()).await?;
    Ok(())
}

#[tokio::test]
async fn lifecycle_fault_scenarios_pass_fast_fake() -> Result<(), ScenarioError> {
    scenarios::exhausted_replica_crashes_deployment_while_peers_stay_running(
        &mut LifecycleWorld::new(),
    )
    .await?;
    scenarios::all_exhausted_replicas_crash_deployment(&mut LifecycleWorld::new()).await?;
    scenarios::initial_replica_crash_preserves_pending_peers(&mut LifecycleWorld::new()).await?;
    scenarios::missing_workload_record_is_recovered(&mut LifecycleWorld::new()).await?;
    scenarios::rollout_failure_is_isolated_between_services(&mut LifecycleWorld::new()).await?;
    scenarios::old_workload_crash_does_not_break_redeployment(&mut LifecycleWorld::new()).await?;
    Ok(())
}
