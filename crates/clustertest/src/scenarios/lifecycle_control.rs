use std::time::Duration;

use crate::{
    ArtifactBehavior, ArtifactStageState, ClusterSnapshot, DeploymentPhase, FixtureArtifact,
    FixtureName, FixtureVersion, LifecycleControlCluster, ReplicaCount, ReplicaHealth,
    ReplicaIndex, ResourceAvailability, ScenarioError, ServiceFixture,
};

/// Proves a queued deployment continues after later deploy admission freezes.
pub async fn queued_rollout_ignores_later_freeze<Cluster>(
    cluster: &mut Cluster,
) -> Result<(), ScenarioError>
where
    Cluster: LifecycleControlCluster,
{
    let name = FixtureName::new("acceptance-frozen-queue");
    let deployment = rollout(cluster, name.clone()).await?;
    cluster
        .freeze_service(&name)
        .await
        .map_err(|error| driver_error("freeze service after queue", error))?;
    let snapshot = converge(cluster, "await frozen queued rollout").await?;
    assert_phase(&snapshot, &name, &deployment, DeploymentPhase::Ready)
}

/// Proves artifact preparation completes before a build can complete.
pub async fn artifact_preparation_precedes_build<Cluster>(
    cluster: &mut Cluster,
) -> Result<(), ScenarioError>
where
    Cluster: LifecycleControlCluster,
{
    let deployment = rollout(cluster, FixtureName::new("acceptance-artifact-order")).await?;
    cluster
        .await_converged()
        .await
        .map_err(|error| driver_error("await artifact pipeline", error))?;
    let observation = cluster
        .artifact_observation(&deployment)
        .await
        .map_err(|error| driver_error("observe artifact pipeline", error))?;
    if observation.preparation == ArtifactStageState::Complete
        && observation.build == ArtifactStageState::Complete
    {
        Ok(())
    } else {
        Err(ScenarioError::Assertion(format!(
            "artifact build completed without preparation evidence: {observation:?}"
        )))
    }
}

/// Proves the exact built artifact is persisted into deployment history.
pub async fn built_artifact_is_persisted<Cluster>(
    cluster: &mut Cluster,
) -> Result<(), ScenarioError>
where
    Cluster: LifecycleControlCluster,
{
    let name = FixtureName::new("acceptance-artifact-persist");
    let deployment = rollout(cluster, name.clone()).await?;
    let artifact = FixtureArtifact::new("registry.test/service:acceptance");
    cluster
        .configure_artifact(
            &deployment,
            ArtifactBehavior::CompleteWith(artifact.clone()),
        )
        .await
        .map_err(|error| driver_error("configure artifact result", error))?;
    let snapshot = converge(cluster, "await persisted artifact").await?;
    let observed = require_deployment(&snapshot, &name, &deployment)?;
    let pipeline = cluster
        .artifact_observation(&deployment)
        .await
        .map_err(|error| driver_error("observe persisted artifact", error))?;
    if observed.artifact.as_ref() == Some(&artifact)
        && pipeline.persisted_artifact.as_ref() == Some(&artifact)
    {
        Ok(())
    } else {
        Err(ScenarioError::Assertion(format!(
            "built artifact was not persisted: deployment={:?}, pipeline={pipeline:?}",
            observed.artifact
        )))
    }
}

/// Proves the unhealthy threshold restarts one replica without failing its deployment.
pub async fn unhealthy_threshold_restarts_replica<Cluster>(
    cluster: &mut Cluster,
) -> Result<(), ScenarioError>
where
    Cluster: LifecycleControlCluster,
{
    let name = FixtureName::new("acceptance-health-threshold");
    let deployment = rollout_started(cluster, name.clone()).await?;
    cluster
        .seed_replica_health(&deployment, ReplicaIndex::new(0), DeploymentPhase::Ready, 0)
        .await
        .map_err(|error| driver_error("seed healthy replica", error))?;
    for _ in 0..cluster.health_failure_threshold() {
        cluster
            .report_health(&deployment, ReplicaIndex::new(0), ReplicaHealth::Unhealthy)
            .await
            .map_err(|error| driver_error("report unhealthy replica", error))?;
    }
    let snapshot = cluster
        .await_stable_without_readiness()
        .await
        .map_err(|error| driver_error("await health-triggered restart", error))?;
    let observed = require_deployment(&snapshot, &name, &deployment)?;
    let replica = require_replica(observed, 0)?;
    if observed.phase != DeploymentPhase::Crashed
        && replica.phase != DeploymentPhase::Crashed
        && replica.restart_attempts > 0
        && replica.workload == ResourceAvailability::Available
    {
        Ok(())
    } else {
        Err(ScenarioError::Assertion(format!(
            "unhealthy threshold did not recover replica: {observed:?}"
        )))
    }
}

/// Proves repeated healthy verdicts do not rewrite an unchanged state.
pub async fn repeated_healthy_reports_are_write_free<Cluster>(
    cluster: &mut Cluster,
) -> Result<(), ScenarioError>
where
    Cluster: LifecycleControlCluster,
{
    let deployment = rollout_started(cluster, FixtureName::new("acceptance-health-noop")).await?;
    cluster
        .seed_replica_health(&deployment, ReplicaIndex::new(0), DeploymentPhase::Ready, 0)
        .await
        .map_err(|error| driver_error("seed unchanged healthy state", error))?;
    let baseline = observe_health(cluster, &deployment).await?;
    let mut after = baseline.clone();
    for _ in 0..5 {
        after = cluster
            .report_health(&deployment, ReplicaIndex::new(0), ReplicaHealth::Healthy)
            .await
            .map_err(|error| driver_error("repeat healthy report", error))?;
    }
    if after.phase == DeploymentPhase::Ready
        && after.failures == 0
        && after.store_writes == baseline.store_writes
    {
        Ok(())
    } else {
        Err(ScenarioError::Assertion(format!(
            "unchanged healthy reports wrote state: before={baseline:?}, after={after:?}"
        )))
    }
}

/// Proves a healthy verdict writes when the phase differs despite zero failures.
pub async fn healthy_report_updates_pending_replica<Cluster>(
    cluster: &mut Cluster,
) -> Result<(), ScenarioError>
where
    Cluster: LifecycleControlCluster,
{
    let deployment =
        rollout_started(cluster, FixtureName::new("acceptance-health-pending")).await?;
    cluster
        .seed_replica_health(
            &deployment,
            ReplicaIndex::new(0),
            DeploymentPhase::PendingReady,
            0,
        )
        .await
        .map_err(|error| driver_error("seed pending health state", error))?;
    let baseline = observe_health(cluster, &deployment).await?;
    let after = cluster
        .report_health(&deployment, ReplicaIndex::new(0), ReplicaHealth::Healthy)
        .await
        .map_err(|error| driver_error("report pending replica healthy", error))?;
    if after.phase == DeploymentPhase::Ready && after.store_writes > baseline.store_writes {
        Ok(())
    } else {
        Err(ScenarioError::Assertion(format!(
            "healthy verdict did not update pending state: before={baseline:?}, after={after:?}"
        )))
    }
}

/// Proves an unhealthy verdict increments failures and persists the change.
pub async fn unhealthy_report_increments_and_persists<Cluster>(
    cluster: &mut Cluster,
) -> Result<(), ScenarioError>
where
    Cluster: LifecycleControlCluster,
{
    let deployment = rollout_started(cluster, FixtureName::new("acceptance-health-write")).await?;
    cluster
        .seed_replica_health(&deployment, ReplicaIndex::new(0), DeploymentPhase::Ready, 0)
        .await
        .map_err(|error| driver_error("seed health write state", error))?;
    let baseline = observe_health(cluster, &deployment).await?;
    let after = cluster
        .report_health(&deployment, ReplicaIndex::new(0), ReplicaHealth::Unhealthy)
        .await
        .map_err(|error| driver_error("report first unhealthy verdict", error))?;
    if after.failures == 1 && after.store_writes > baseline.store_writes {
        Ok(())
    } else {
        Err(ScenarioError::Assertion(format!(
            "unhealthy verdict was not persisted: before={baseline:?}, after={after:?}"
        )))
    }
}

/// Proves a healthy verdict resets a partial failure streak.
pub async fn healthy_report_resets_failure_count<Cluster>(
    cluster: &mut Cluster,
) -> Result<(), ScenarioError>
where
    Cluster: LifecycleControlCluster,
{
    let deployment = rollout_started(cluster, FixtureName::new("acceptance-health-reset")).await?;
    cluster
        .seed_replica_health(&deployment, ReplicaIndex::new(0), DeploymentPhase::Ready, 0)
        .await
        .map_err(|error| driver_error("seed health reset state", error))?;
    for _ in 1..cluster.health_failure_threshold() {
        cluster
            .report_health(&deployment, ReplicaIndex::new(0), ReplicaHealth::Unhealthy)
            .await
            .map_err(|error| driver_error("accumulate health failures", error))?;
    }
    let after = cluster
        .report_health(&deployment, ReplicaIndex::new(0), ReplicaHealth::Healthy)
        .await
        .map_err(|error| driver_error("report recovered health", error))?;
    if after.phase == DeploymentPhase::Ready
        && after.failures == 0
        && after.workload == ResourceAvailability::Available
    {
        Ok(())
    } else {
        Err(ScenarioError::Assertion(format!(
            "healthy verdict did not reset failures: {after:?}"
        )))
    }
}

/// Proves monitor-reported readiness completes an initial rollout.
pub async fn health_monitor_readies_deployment<Cluster>(
    cluster: &mut Cluster,
) -> Result<(), ScenarioError>
where
    Cluster: LifecycleControlCluster,
{
    let name = FixtureName::new("acceptance-health-rollout");
    let deployment = rollout_started(cluster, name.clone()).await?;
    cluster
        .seed_replica_health(
            &deployment,
            ReplicaIndex::new(0),
            DeploymentPhase::PendingReady,
            0,
        )
        .await
        .map_err(|error| driver_error("seed monitor rollout state", error))?;
    cluster
        .report_health(&deployment, ReplicaIndex::new(0), ReplicaHealth::Healthy)
        .await
        .map_err(|error| driver_error("report rollout healthy", error))?;
    let snapshot = cluster
        .await_stable_without_readiness()
        .await
        .map_err(|error| driver_error("await monitor rollout", error))?;
    assert_phase(&snapshot, &name, &deployment, DeploymentPhase::Ready)
}

/// Proves a hanging in-progress build can be canceled deterministically.
pub async fn in_progress_build_can_be_canceled<Cluster>(
    cluster: &mut Cluster,
) -> Result<(), ScenarioError>
where
    Cluster: LifecycleControlCluster,
{
    let name = FixtureName::new("acceptance-cancel-build");
    let deployment = rollout(cluster, name.clone()).await?;
    cluster
        .configure_artifact(&deployment, ArtifactBehavior::NeverCompletes)
        .await
        .map_err(|error| driver_error("hang build before cancellation", error))?;
    let building = cluster
        .await_stable_without_readiness()
        .await
        .map_err(|error| driver_error("await in-progress build", error))?;
    assert_phase(&building, &name, &deployment, DeploymentPhase::Building)?;
    cluster
        .cancel(&deployment)
        .await
        .map_err(|error| driver_error("cancel in-progress build", error))?;
    let canceled = cluster
        .await_stable_without_readiness()
        .await
        .map_err(|error| driver_error("await build cancellation", error))?;
    assert_phase(&canceled, &name, &deployment, DeploymentPhase::Canceled)
}

/// Proves injected logical time terminates a build that never completes.
pub async fn hanging_build_crashes_after_timeout<Cluster>(
    cluster: &mut Cluster,
) -> Result<(), ScenarioError>
where
    Cluster: LifecycleControlCluster,
{
    let name = FixtureName::new("acceptance-build-timeout");
    let deployment = rollout(cluster, name.clone()).await?;
    cluster
        .configure_artifact(&deployment, ArtifactBehavior::NeverCompletes)
        .await
        .map_err(|error| driver_error("hang build before timeout", error))?;
    let building = cluster
        .await_stable_without_readiness()
        .await
        .map_err(|error| driver_error("await hanging build", error))?;
    assert_phase(&building, &name, &deployment, DeploymentPhase::Building)?;
    cluster
        .advance(Duration::from_secs(31 * 60))
        .await
        .map_err(|error| driver_error("advance build timeout", error))?;
    let timed_out = cluster
        .await_stable_without_readiness()
        .await
        .map_err(|error| driver_error("await build timeout", error))?;
    assert_phase(&timed_out, &name, &deployment, DeploymentPhase::Crashed)
}

async fn rollout<Cluster>(
    cluster: &mut Cluster,
    name: FixtureName,
) -> Result<Cluster::DeploymentId, ScenarioError>
where
    Cluster: LifecycleControlCluster,
{
    cluster
        .rollout(ServiceFixture::new(
            name,
            FixtureVersion::new("v1"),
            ReplicaCount::new(1),
        ))
        .await
        .map_err(|error| driver_error("queue lifecycle control rollout", error))
}

async fn rollout_started<Cluster>(
    cluster: &mut Cluster,
    name: FixtureName,
) -> Result<Cluster::DeploymentId, ScenarioError>
where
    Cluster: LifecycleControlCluster,
{
    let deployment = rollout(cluster, name).await?;
    cluster
        .await_started(&deployment, ReplicaCount::new(1))
        .await
        .map_err(|error| driver_error("await lifecycle control workload", error))?;
    Ok(deployment)
}

async fn converge<Cluster>(
    cluster: &mut Cluster,
    operation: &'static str,
) -> Result<ClusterSnapshot<Cluster::DeploymentId>, ScenarioError>
where
    Cluster: LifecycleControlCluster,
{
    cluster
        .await_converged()
        .await
        .map_err(|error| driver_error(operation, error))
}

async fn observe_health<Cluster>(
    cluster: &mut Cluster,
    deployment: &Cluster::DeploymentId,
) -> Result<crate::HealthObservation, ScenarioError>
where
    Cluster: LifecycleControlCluster,
{
    cluster
        .health_observation(deployment, ReplicaIndex::new(0))
        .await
        .map_err(|error| driver_error("observe replica health", error))
}

fn assert_phase<DeploymentId: std::fmt::Debug + Eq>(
    snapshot: &ClusterSnapshot<DeploymentId>,
    name: &FixtureName,
    deployment: &DeploymentId,
    expected: DeploymentPhase,
) -> Result<(), ScenarioError> {
    let observed = require_deployment(snapshot, name, deployment)?;
    if observed.phase == expected {
        Ok(())
    } else {
        Err(ScenarioError::Assertion(format!(
            "deployment {deployment:?} was {:?}, expected {expected:?}",
            observed.phase
        )))
    }
}

fn require_deployment<'snapshot, DeploymentId: std::fmt::Debug + Eq>(
    snapshot: &'snapshot ClusterSnapshot<DeploymentId>,
    name: &FixtureName,
    deployment: &DeploymentId,
) -> Result<&'snapshot crate::DeploymentSnapshot<DeploymentId>, ScenarioError> {
    snapshot
        .service(name)
        .and_then(|service| service.deployment(deployment))
        .ok_or_else(|| ScenarioError::Assertion(format!("deployment {deployment:?} is absent")))
}

fn require_replica<DeploymentId>(
    deployment: &crate::DeploymentSnapshot<DeploymentId>,
    index: u32,
) -> Result<&crate::ReplicaSnapshot, ScenarioError> {
    deployment
        .replicas
        .iter()
        .find(|replica| replica.index == index)
        .ok_or_else(|| ScenarioError::Assertion(format!("replica {index} is absent")))
}

fn driver_error(error_operation: &'static str, error: impl std::fmt::Display) -> ScenarioError {
    ScenarioError::Driver {
        operation: error_operation,
        message: error.to_string(),
    }
}
