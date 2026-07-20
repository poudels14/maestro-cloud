use async_trait::async_trait;

use super::lifecycle::{LifecycleWorld, LifecycleWorldError};
use crate::{
    ArtifactBehavior, ArtifactObservation, ArtifactStageState, DeploymentPhase, FixtureName,
    HealthObservation, LifecycleControlCluster, ReplicaHealth, ReplicaIndex, ResourceAvailability,
    scenarios,
};

#[async_trait]
impl LifecycleControlCluster for LifecycleWorld {
    async fn freeze_service(&mut self, _service: &FixtureName) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn configure_artifact(
        &mut self,
        deployment_id: &Self::DeploymentId,
        behavior: ArtifactBehavior,
    ) -> Result<(), Self::Error> {
        self.artifact_behaviors.insert(*deployment_id, behavior);
        Ok(())
    }

    async fn artifact_observation(
        &mut self,
        deployment_id: &Self::DeploymentId,
    ) -> Result<ArtifactObservation, Self::Error> {
        let persisted_artifact = self.deployment_mut(*deployment_id)?.artifact.clone();
        Ok(ArtifactObservation {
            preparation: if self.prepared.contains(deployment_id) {
                ArtifactStageState::Complete
            } else {
                ArtifactStageState::Pending
            },
            build: if self.built.contains(deployment_id) {
                ArtifactStageState::Complete
            } else {
                ArtifactStageState::Pending
            },
            persisted_artifact,
        })
    }

    fn health_failure_threshold(&self) -> u32 {
        10
    }

    async fn seed_replica_health(
        &mut self,
        deployment_id: &Self::DeploymentId,
        replica_index: ReplicaIndex,
        phase: DeploymentPhase,
        failures: u32,
    ) -> Result<(), Self::Error> {
        let replica = self
            .deployment_mut(*deployment_id)?
            .replicas
            .iter_mut()
            .find(|replica| replica.index == replica_index.get())
            .ok_or(LifecycleWorldError("unknown replica"))?;
        replica.phase = phase;
        replica.healthcheck_failures = failures;
        Ok(())
    }

    async fn health_observation(
        &mut self,
        deployment_id: &Self::DeploymentId,
        replica_index: ReplicaIndex,
    ) -> Result<HealthObservation, Self::Error> {
        let store_writes = self.health_writes;
        let replica = self
            .deployment_mut(*deployment_id)?
            .replicas
            .iter()
            .find(|replica| replica.index == replica_index.get())
            .ok_or(LifecycleWorldError("unknown replica"))?;
        Ok(HealthObservation {
            phase: replica.phase,
            failures: replica.healthcheck_failures,
            workload: replica.workload,
            store_writes,
        })
    }

    async fn report_health(
        &mut self,
        deployment_id: &Self::DeploymentId,
        replica_index: ReplicaIndex,
        health: ReplicaHealth,
    ) -> Result<HealthObservation, Self::Error> {
        let threshold = self.health_failure_threshold();
        let mut wrote = false;
        {
            let replica = self
                .deployment_mut(*deployment_id)?
                .replicas
                .iter_mut()
                .find(|replica| replica.index == replica_index.get())
                .ok_or(LifecycleWorldError("unknown replica"))?;
            match health {
                ReplicaHealth::Healthy => {
                    if replica.phase != DeploymentPhase::Ready || replica.healthcheck_failures != 0
                    {
                        replica.phase = DeploymentPhase::Ready;
                        replica.healthcheck_failures = 0;
                        wrote = true;
                    }
                }
                ReplicaHealth::Unhealthy => {
                    replica.healthcheck_failures = replica.healthcheck_failures.saturating_add(1);
                    if replica.healthcheck_failures >= threshold {
                        replica.phase = DeploymentPhase::Crashed;
                        replica.workload = ResourceAvailability::Unavailable;
                    }
                    wrote = true;
                }
            }
        }
        if wrote {
            self.health_writes = self.health_writes.saturating_add(1);
        }
        self.health_observation(deployment_id, replica_index).await
    }
}

#[tokio::test]
async fn lifecycle_control_scenarios_pass_fast_fake() {
    scenarios::queued_rollout_ignores_later_freeze(&mut LifecycleWorld::new())
        .await
        .expect("queued rollout freeze scenario");
    scenarios::artifact_preparation_precedes_build(&mut LifecycleWorld::new())
        .await
        .expect("artifact ordering scenario");
    scenarios::built_artifact_is_persisted(&mut LifecycleWorld::new())
        .await
        .expect("artifact persistence scenario");
    scenarios::unhealthy_threshold_restarts_replica(&mut LifecycleWorld::new())
        .await
        .expect("health threshold scenario");
    scenarios::repeated_healthy_reports_are_write_free(&mut LifecycleWorld::new())
        .await
        .expect("healthy no-op scenario");
    scenarios::healthy_report_updates_pending_replica(&mut LifecycleWorld::new())
        .await
        .expect("pending health scenario");
    scenarios::unhealthy_report_increments_and_persists(&mut LifecycleWorld::new())
        .await
        .expect("unhealthy write scenario");
    scenarios::healthy_report_resets_failure_count(&mut LifecycleWorld::new())
        .await
        .expect("health reset scenario");
    scenarios::health_monitor_readies_deployment(&mut LifecycleWorld::new())
        .await
        .expect("health monitor rollout scenario");
    scenarios::in_progress_build_can_be_canceled(&mut LifecycleWorld::new())
        .await
        .expect("build cancellation scenario");
    scenarios::hanging_build_crashes_after_timeout(&mut LifecycleWorld::new())
        .await
        .expect("build timeout scenario");
}
