use super::lifecycle::LifecycleWorld;
use crate::scenarios;

#[tokio::test]
async fn lifecycle_stress_scenarios_pass_fast_fake() {
    scenarios::multi_replica_rollout_starts_all_replicas(&mut LifecycleWorld::new())
        .await
        .expect("multi-replica scenario");
    scenarios::sequential_redeploys_supersede_history(&mut LifecycleWorld::new())
        .await
        .expect("sequential redeploy scenario");
    scenarios::many_services_roll_out_independently(&mut LifecycleWorld::new())
        .await
        .expect("many services scenario");
    scenarios::back_to_back_redeploys_keep_only_latest_ready(&mut LifecycleWorld::new())
        .await
        .expect("back-to-back redeploy scenario");
    scenarios::replica_override_scales_up(&mut LifecycleWorld::new())
        .await
        .expect("scale-up scenario");
    scenarios::replica_override_respects_configured_floor(&mut LifecycleWorld::new())
        .await
        .expect("replica-floor scenario");
    scenarios::many_rapid_redeploys_settle_to_one_ready(&mut LifecycleWorld::new())
        .await
        .expect("rapid redeploy scenario");
}
