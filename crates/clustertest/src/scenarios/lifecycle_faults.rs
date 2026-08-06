use crate::{
    ClusterSnapshot, DeploymentPhase, FaultInjectableCluster, FixtureName, FixtureVersion,
    LifecycleFaultCluster, ReplicaCount, ReplicaIndex, ReplicaRecordDisposition,
    ResourceAvailability, RolloutFailure, ScenarioError, ServiceFixture,
};

/// Proves one exhausted replica fails its deployment without stopping healthy peers.
pub async fn exhausted_replica_crashes_deployment_while_peers_stay_running<Cluster>(
    cluster: &mut Cluster,
) -> Result<(), ScenarioError>
where
    Cluster: LifecycleFaultCluster,
{
    let name = FixtureName::new("acceptance-exhausted-replica");
    let deployment = rollout_ready(cluster, name.clone(), 3).await?;
    cluster
        .inject_exhausted_replica(&deployment, ReplicaIndex::new(0))
        .await
        .map_err(|error| driver_error("exhaust replica restart budget", error))?;
    let snapshot = cluster
        .await_stable_without_readiness()
        .await
        .map_err(|error| driver_error("settle exhausted replica", error))?;
    let observed = require_deployment(&snapshot, &name, &deployment)?;
    if observed.phase != DeploymentPhase::Crashed {
        return Err(ScenarioError::Assertion(format!(
            "one exhausted replica left deployment in {:?}, expected Crashed",
            observed.phase
        )));
    }
    assert_workload(observed, 0, ResourceAvailability::Unavailable)?;
    for index in 1..3 {
        assert_workload(observed, index, ResourceAvailability::Available)?;
    }
    Ok(())
}

/// Proves a deployment crashes only after every replica exhausts its budget.
pub async fn all_exhausted_replicas_crash_deployment<Cluster>(
    cluster: &mut Cluster,
) -> Result<(), ScenarioError>
where
    Cluster: LifecycleFaultCluster,
{
    let name = FixtureName::new("acceptance-all-exhausted");
    let deployment = rollout_ready(cluster, name.clone(), 3).await?;
    for index in 0..3 {
        cluster
            .inject_exhausted_replica(&deployment, ReplicaIndex::new(index))
            .await
            .map_err(|error| driver_error("exhaust all replica restart budgets", error))?;
    }
    let snapshot = cluster
        .await_stable_without_readiness()
        .await
        .map_err(|error| driver_error("settle all exhausted replicas", error))?;
    let observed = require_deployment(&snapshot, &name, &deployment)?;
    if observed.phase != DeploymentPhase::Crashed {
        return Err(ScenarioError::Assertion(format!(
            "all exhausted replicas left deployment in {:?}",
            observed.phase
        )));
    }
    for index in 0..3 {
        assert_workload(observed, index, ResourceAvailability::Unavailable)?;
    }
    Ok(())
}

/// Proves one early crash does not kill peers still awaiting readiness.
pub async fn initial_replica_crash_preserves_pending_peers<Cluster>(
    cluster: &mut Cluster,
) -> Result<(), ScenarioError>
where
    Cluster: LifecycleFaultCluster,
{
    let name = FixtureName::new("acceptance-initial-crash");
    let deployment = cluster
        .rollout(ServiceFixture::new(
            name.clone(),
            FixtureVersion::new("v1"),
            ReplicaCount::new(3),
        ))
        .await
        .map_err(|error| driver_error("rollout before initial crash", error))?;
    cluster
        .await_started(&deployment, ReplicaCount::new(3))
        .await
        .map_err(|error| driver_error("await pending replica workloads", error))?;
    cluster
        .inject_replica_crash(&deployment, ReplicaIndex::new(0))
        .await
        .map_err(|error| driver_error("crash first pending replica", error))?;
    let snapshot = cluster
        .await_stable_without_readiness()
        .await
        .map_err(|error| driver_error("settle initial replica crash", error))?;
    let observed = require_deployment(&snapshot, &name, &deployment)?;
    if observed.phase == DeploymentPhase::Crashed {
        return Err(ScenarioError::Assertion(
            "one initial replica crash failed the deployment".to_string(),
        ));
    }
    for index in 1..3 {
        assert_workload(observed, index, ResourceAvailability::Available)?;
    }
    Ok(())
}

/// Proves a workload lost without a probe record reaches a terminal phase.
pub async fn missing_workload_record_is_recovered<Cluster>(
    cluster: &mut Cluster,
) -> Result<(), ScenarioError>
where
    Cluster: LifecycleFaultCluster,
{
    let name = FixtureName::new("acceptance-missing-workload");
    let deployment = cluster
        .rollout(ServiceFixture::new(
            name.clone(),
            FixtureVersion::new("v1"),
            ReplicaCount::new(1),
        ))
        .await
        .map_err(|error| driver_error("rollout before workload loss", error))?;
    cluster
        .await_started(&deployment, ReplicaCount::new(1))
        .await
        .map_err(|error| driver_error("await workload before loss", error))?;
    cluster
        .inject_workload_termination(
            &deployment,
            ReplicaIndex::new(0),
            ReplicaRecordDisposition::Missing,
        )
        .await
        .map_err(|error| driver_error("terminate workload without record", error))?;
    let snapshot = cluster
        .await_stable_without_readiness()
        .await
        .map_err(|error| driver_error("settle missing workload recovery", error))?;
    let observed = require_deployment(&snapshot, &name, &deployment)?;
    if matches!(
        observed.phase,
        DeploymentPhase::Terminated | DeploymentPhase::Crashed
    ) {
        Ok(())
    } else {
        Err(ScenarioError::Assertion(format!(
            "missing workload record converged to {:?}",
            observed.phase
        )))
    }
}

/// Proves one service's build failure does not block an independent rollout.
pub async fn rollout_failure_is_isolated_between_services<Cluster>(
    cluster: &mut Cluster,
) -> Result<(), ScenarioError>
where
    Cluster: FaultInjectableCluster,
{
    let good_name = FixtureName::new("acceptance-isolated-good");
    let bad_name = FixtureName::new("acceptance-isolated-bad");
    let good = cluster
        .rollout(ServiceFixture::new(
            good_name.clone(),
            FixtureVersion::new("v1"),
            ReplicaCount::new(1),
        ))
        .await
        .map_err(|error| driver_error("queue healthy service", error))?;
    let bad = cluster
        .rollout(ServiceFixture::new(
            bad_name.clone(),
            FixtureVersion::new("v1"),
            ReplicaCount::new(1),
        ))
        .await
        .map_err(|error| driver_error("queue failing service", error))?;
    cluster
        .inject_rollout_failure(&bad, RolloutFailure::Build("isolated failure".to_string()))
        .await
        .map_err(|error| driver_error("inject isolated build failure", error))?;
    let snapshot = cluster
        .await_converged()
        .await
        .map_err(|error| driver_error("await isolated rollouts", error))?;
    let good = require_deployment(&snapshot, &good_name, &good)?;
    let bad = require_deployment(&snapshot, &bad_name, &bad)?;
    if good.phase == DeploymentPhase::Ready && bad.phase == DeploymentPhase::Crashed {
        Ok(())
    } else {
        Err(ScenarioError::Assertion(format!(
            "failure crossed service boundary: good={:?}, bad={:?}",
            good.phase, bad.phase
        )))
    }
}

/// Proves loss of the old workload cannot block a replacement deployment.
pub async fn old_workload_crash_does_not_break_redeployment<Cluster>(
    cluster: &mut Cluster,
) -> Result<(), ScenarioError>
where
    Cluster: LifecycleFaultCluster,
{
    let name = FixtureName::new("acceptance-rollover-crash");
    let previous = rollout_ready(cluster, name.clone(), 1).await?;
    let candidate = cluster
        .rollout(ServiceFixture::new(
            name.clone(),
            FixtureVersion::new("v2"),
            ReplicaCount::new(1),
        ))
        .await
        .map_err(|error| driver_error("queue replacement before crash", error))?;
    cluster
        .inject_workload_termination(
            &previous,
            ReplicaIndex::new(0),
            ReplicaRecordDisposition::Retained,
        )
        .await
        .map_err(|error| driver_error("terminate previous workload", error))?;
    let snapshot = cluster
        .await_converged()
        .await
        .map_err(|error| driver_error("await replacement after crash", error))?;
    let candidate = require_deployment(&snapshot, &name, &candidate)?;
    if candidate.phase == DeploymentPhase::Ready {
        Ok(())
    } else {
        Err(ScenarioError::Assertion(format!(
            "replacement converged to {:?} after old workload loss",
            candidate.phase
        )))
    }
}

async fn rollout_ready<Cluster>(
    cluster: &mut Cluster,
    name: FixtureName,
    replicas: u32,
) -> Result<Cluster::DeploymentId, ScenarioError>
where
    Cluster: LifecycleFaultCluster,
{
    let deployment = cluster
        .rollout(ServiceFixture::new(
            name,
            FixtureVersion::new("v1"),
            ReplicaCount::new(replicas),
        ))
        .await
        .map_err(|error| driver_error("rollout before fault", error))?;
    cluster
        .await_converged()
        .await
        .map_err(|error| driver_error("await rollout before fault", error))?;
    Ok(deployment)
}

fn require_deployment<'snapshot, DeploymentId: std::fmt::Debug + Eq>(
    snapshot: &'snapshot ClusterSnapshot<DeploymentId>,
    name: &FixtureName,
    deployment: &DeploymentId,
) -> Result<&'snapshot crate::DeploymentSnapshot<DeploymentId>, ScenarioError> {
    snapshot
        .service(name)
        .and_then(|service| service.deployment(deployment))
        .ok_or_else(|| {
            ScenarioError::Assertion(format!("deployment {deployment:?} for {name:?} is absent"))
        })
}

fn assert_workload<DeploymentId>(
    deployment: &crate::DeploymentSnapshot<DeploymentId>,
    index: u32,
    expected: ResourceAvailability,
) -> Result<(), ScenarioError> {
    let replica = deployment
        .replicas
        .iter()
        .find(|replica| replica.index == index)
        .ok_or_else(|| ScenarioError::Assertion(format!("replica {index} is absent")))?;
    if replica.workload == expected {
        Ok(())
    } else {
        Err(ScenarioError::Assertion(format!(
            "replica {index} workload was {:?}, expected {expected:?}",
            replica.workload
        )))
    }
}

fn driver_error(error_operation: &'static str, error: impl std::fmt::Display) -> ScenarioError {
    ScenarioError::Driver {
        operation: error_operation,
        message: error.to_string(),
    }
}
