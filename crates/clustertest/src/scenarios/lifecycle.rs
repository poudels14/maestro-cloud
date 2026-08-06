//! Shared lifecycle scenarios that every Maestro implementation must pass.

use std::fmt::Debug;
use std::time::Duration;

use crate::{
    AcceptanceCluster, ClusterSnapshot, DeploymentPhase, FaultInjectableCluster, FixtureName,
    FixtureVersion, ReplicaCount, ReplicaIndex, ReplicaOverride, RolloutFailure, ScenarioError,
    ServiceFixture, ServiceSnapshot,
};

/// Proves that a declarative rollout converges to ready replicas.
pub async fn rollout_reaches_ready<Cluster>(cluster: &mut Cluster) -> Result<(), ScenarioError>
where
    Cluster: AcceptanceCluster,
{
    let service_name = FixtureName::new("acceptance-rollout");
    let service = ServiceFixture::new(
        service_name.clone(),
        FixtureVersion::new("v1"),
        ReplicaCount::new(2),
    );
    let deployment_id = cluster
        .rollout(service)
        .await
        .map_err(|error| driver_error("rollout", error))?;
    let snapshot = cluster
        .await_converged()
        .await
        .map_err(|error| driver_error("await rollout convergence", error))?;
    let service = require_service(&snapshot, &service_name)?;
    let deployment = service.deployment(&deployment_id).ok_or_else(|| {
        ScenarioError::Assertion(format!(
            "rollout deployment {deployment_id:?} is absent from the converged snapshot"
        ))
    })?;

    if deployment.phase != DeploymentPhase::Ready {
        Err(ScenarioError::Assertion(format!(
            "rollout deployment {deployment_id:?} converged to {:?}, expected Ready",
            deployment.phase
        )))
    } else if ready_replica_count(deployment) != 2 {
        Err(ScenarioError::Assertion(format!(
            "rollout deployment {deployment_id:?} has {} ready replicas, expected 2",
            ready_replica_count(deployment)
        )))
    } else {
        Ok(())
    }
}

/// Proves that a redeploy readies the new version and drains the old one.
pub async fn redeploy_drains_previous<Cluster>(cluster: &mut Cluster) -> Result<(), ScenarioError>
where
    Cluster: AcceptanceCluster,
{
    let service_name = FixtureName::new("acceptance-redeploy");
    let first_deployment = cluster
        .rollout(
            ServiceFixture::new(
                service_name.clone(),
                FixtureVersion::new("v1"),
                ReplicaCount::new(1),
            )
            .with_ingress("acceptance-redeploy.test"),
        )
        .await
        .map_err(|error| driver_error("initial rollout", error))?;
    cluster
        .await_converged()
        .await
        .map_err(|error| driver_error("await initial rollout convergence", error))?;

    let second_deployment = cluster
        .rollout(
            ServiceFixture::new(
                service_name.clone(),
                FixtureVersion::new("v2"),
                ReplicaCount::new(1),
            )
            .with_ingress("acceptance-redeploy.test"),
        )
        .await
        .map_err(|error| driver_error("redeploy", error))?;
    let snapshot = cluster
        .await_converged()
        .await
        .map_err(|error| driver_error("await redeploy convergence", error))?;
    let service = require_service(&snapshot, &service_name)?;
    let previous = service.deployment(&first_deployment).ok_or_else(|| {
        ScenarioError::Assertion(format!(
            "previous deployment {first_deployment:?} is absent after redeploy"
        ))
    })?;
    let current = service.deployment(&second_deployment).ok_or_else(|| {
        ScenarioError::Assertion(format!(
            "new deployment {second_deployment:?} is absent after redeploy"
        ))
    })?;

    if current.phase != DeploymentPhase::Ready {
        Err(ScenarioError::Assertion(format!(
            "new deployment {second_deployment:?} converged to {:?}, expected Ready",
            current.phase
        )))
    } else if !matches!(
        previous.phase,
        DeploymentPhase::Draining | DeploymentPhase::Removed
    ) {
        Err(ScenarioError::Assertion(format!(
            "previous deployment {first_deployment:?} converged to {:?}, expected Draining or Removed",
            previous.phase
        )))
    } else {
        Ok(())
    }
}

/// Proves that canceling a queued deployment prevents it from becoming ready.
pub async fn queued_deployment_can_be_canceled<Cluster>(
    cluster: &mut Cluster,
) -> Result<(), ScenarioError>
where
    Cluster: AcceptanceCluster,
{
    let service_name = FixtureName::new("acceptance-cancel");
    let deployment_id = cluster
        .rollout(ServiceFixture::new(
            service_name.clone(),
            FixtureVersion::new("v1"),
            ReplicaCount::new(1),
        ))
        .await
        .map_err(|error| driver_error("queue rollout", error))?;
    cluster
        .cancel(&deployment_id)
        .await
        .map_err(|error| driver_error("cancel queued deployment", error))?;
    let snapshot = cluster
        .await_converged()
        .await
        .map_err(|error| driver_error("await cancellation convergence", error))?;
    let service = require_service(&snapshot, &service_name)?;
    let deployment = service.deployment(&deployment_id).ok_or_else(|| {
        ScenarioError::Assertion(format!(
            "canceled deployment {deployment_id:?} is absent from history"
        ))
    })?;

    if deployment.phase == DeploymentPhase::Canceled {
        Ok(())
    } else {
        Err(ScenarioError::Assertion(format!(
            "canceled deployment {deployment_id:?} converged to {:?}, expected Canceled",
            deployment.phase
        )))
    }
}

/// Proves that setting and clearing replica overrides converges both ways.
pub async fn replica_override_round_trips<Cluster>(
    cluster: &mut Cluster,
) -> Result<(), ScenarioError>
where
    Cluster: AcceptanceCluster,
{
    let service_name = FixtureName::new("acceptance-replicas");
    let deployment_id = cluster
        .rollout(ServiceFixture::new(
            service_name.clone(),
            FixtureVersion::new("v1"),
            ReplicaCount::new(1),
        ))
        .await
        .map_err(|error| driver_error("rollout before replica override", error))?;
    cluster
        .await_converged()
        .await
        .map_err(|error| driver_error("await initial replica convergence", error))?;

    cluster
        .set_replicas(&service_name, ReplicaOverride::Set(ReplicaCount::new(3)))
        .await
        .map_err(|error| driver_error("set replica override", error))?;
    let scaled_snapshot = cluster
        .await_converged()
        .await
        .map_err(|error| driver_error("await scale-up convergence", error))?;
    assert_ready_replicas(&scaled_snapshot, &service_name, &deployment_id, 3)?;

    cluster
        .set_replicas(&service_name, ReplicaOverride::Clear)
        .await
        .map_err(|error| driver_error("clear replica override", error))?;
    let restored_snapshot = cluster
        .await_converged()
        .await
        .map_err(|error| driver_error("await override-clear convergence", error))?;
    assert_ready_replicas(&restored_snapshot, &service_name, &deployment_id, 1)
}

/// Proves that a drained deployment finalizes after its grace period.
pub async fn drained_deployment_finalizes<Cluster>(
    cluster: &mut Cluster,
) -> Result<(), ScenarioError>
where
    Cluster: AcceptanceCluster,
{
    let service_name = FixtureName::new("acceptance-drain-finalize");
    let first_deployment = cluster
        .rollout(ServiceFixture::new(
            service_name.clone(),
            FixtureVersion::new("v1"),
            ReplicaCount::new(1),
        ))
        .await
        .map_err(|error| driver_error("initial rollout before drain", error))?;
    cluster
        .await_converged()
        .await
        .map_err(|error| driver_error("await initial drain rollout", error))?;
    cluster
        .rollout(ServiceFixture::new(
            service_name.clone(),
            FixtureVersion::new("v2"),
            ReplicaCount::new(1),
        ))
        .await
        .map_err(|error| driver_error("rollout replacement before drain", error))?;
    cluster
        .await_converged()
        .await
        .map_err(|error| driver_error("await draining state", error))?;

    cluster
        .advance(Duration::from_secs(1))
        .await
        .map_err(|error| driver_error("advance drain grace period", error))?;
    let snapshot = cluster
        .await_converged()
        .await
        .map_err(|error| driver_error("await drain finalization", error))?;
    let service = require_service(&snapshot, &service_name)?;
    let deployment = service.deployment(&first_deployment).ok_or_else(|| {
        ScenarioError::Assertion(format!(
            "drained deployment {first_deployment:?} is absent from history"
        ))
    })?;

    if deployment.phase == DeploymentPhase::Removed {
        Ok(())
    } else {
        Err(ScenarioError::Assertion(format!(
            "drained deployment {first_deployment:?} converged to {:?}, expected Removed",
            deployment.phase
        )))
    }
}

/// Proves that an artifact build failure terminates only its deployment.
pub async fn build_failure_marks_deployment_crashed<Cluster>(
    cluster: &mut Cluster,
) -> Result<(), ScenarioError>
where
    Cluster: FaultInjectableCluster,
{
    let service_name = FixtureName::new("acceptance-build-failure");
    let deployment_id = cluster
        .rollout(ServiceFixture::new(
            service_name.clone(),
            FixtureVersion::new("v1"),
            ReplicaCount::new(1),
        ))
        .await
        .map_err(|error| driver_error("queue build-failure rollout", error))?;
    cluster
        .inject_rollout_failure(
            &deployment_id,
            RolloutFailure::Build("injected build failure".to_string()),
        )
        .await
        .map_err(|error| driver_error("inject build failure", error))?;
    let snapshot = cluster
        .await_converged()
        .await
        .map_err(|error| driver_error("await build failure convergence", error))?;
    assert_deployment_phase(
        &snapshot,
        &service_name,
        &deployment_id,
        DeploymentPhase::Crashed,
    )
}

/// Proves that an artifact preparation failure terminates its deployment.
pub async fn prepare_failure_marks_deployment_crashed<Cluster>(
    cluster: &mut Cluster,
) -> Result<(), ScenarioError>
where
    Cluster: FaultInjectableCluster,
{
    let service_name = FixtureName::new("acceptance-prepare-failure");
    let deployment_id = cluster
        .rollout(ServiceFixture::new(
            service_name.clone(),
            FixtureVersion::new("v1"),
            ReplicaCount::new(1),
        ))
        .await
        .map_err(|error| driver_error("queue prepare-failure rollout", error))?;
    cluster
        .inject_rollout_failure(
            &deployment_id,
            RolloutFailure::Prepare("injected preparation failure".to_string()),
        )
        .await
        .map_err(|error| driver_error("inject preparation failure", error))?;
    let snapshot = cluster
        .await_converged()
        .await
        .map_err(|error| driver_error("await preparation failure convergence", error))?;
    assert_deployment_phase(
        &snapshot,
        &service_name,
        &deployment_id,
        DeploymentPhase::Crashed,
    )
}

/// Proves that one crashed replica restarts without failing healthy peers.
pub async fn crashed_replica_restarts_in_place<Cluster>(
    cluster: &mut Cluster,
) -> Result<(), ScenarioError>
where
    Cluster: FaultInjectableCluster,
{
    let service_name = FixtureName::new("acceptance-replica-restart");
    let deployment_id = cluster
        .rollout(ServiceFixture::new(
            service_name.clone(),
            FixtureVersion::new("v1"),
            ReplicaCount::new(3),
        ))
        .await
        .map_err(|error| driver_error("rollout before replica crash", error))?;
    cluster
        .await_converged()
        .await
        .map_err(|error| driver_error("await pre-crash convergence", error))?;
    cluster
        .inject_replica_crash(&deployment_id, ReplicaIndex::new(0))
        .await
        .map_err(|error| driver_error("inject replica crash", error))?;
    let snapshot = cluster
        .await_converged()
        .await
        .map_err(|error| driver_error("await replica recovery", error))?;
    let service = require_service(&snapshot, &service_name)?;
    let deployment = service.deployment(&deployment_id).ok_or_else(|| {
        ScenarioError::Assertion(format!(
            "deployment {deployment_id:?} is absent after replica recovery"
        ))
    })?;
    if deployment.phase != DeploymentPhase::Ready {
        Err(ScenarioError::Assertion(format!(
            "deployment {deployment_id:?} converged to {:?} after one crash, expected Ready",
            deployment.phase
        )))
    } else if ready_replica_count(deployment) != 3 {
        Err(ScenarioError::Assertion(format!(
            "deployment {deployment_id:?} recovered with {} ready replicas, expected 3",
            ready_replica_count(deployment)
        )))
    } else {
        Ok(())
    }
}

fn require_service<'snapshot, DeploymentId>(
    snapshot: &'snapshot ClusterSnapshot<DeploymentId>,
    service_name: &FixtureName,
) -> Result<&'snapshot ServiceSnapshot<DeploymentId>, ScenarioError> {
    snapshot.service(service_name).ok_or_else(|| {
        ScenarioError::Assertion(format!(
            "service fixture {:?} is absent from the converged snapshot",
            service_name
        ))
    })
}

fn assert_ready_replicas<DeploymentId: Debug + Eq>(
    snapshot: &ClusterSnapshot<DeploymentId>,
    service_name: &FixtureName,
    deployment_id: &DeploymentId,
    expected: usize,
) -> Result<(), ScenarioError> {
    let service = require_service(snapshot, service_name)?;
    let deployment = service.deployment(deployment_id).ok_or_else(|| {
        ScenarioError::Assertion(format!(
            "deployment {deployment_id:?} is absent while checking replicas"
        ))
    })?;
    let actual = ready_replica_count(deployment);

    if deployment.phase != DeploymentPhase::Ready {
        Err(ScenarioError::Assertion(format!(
            "deployment {deployment_id:?} converged to {:?}, expected Ready",
            deployment.phase
        )))
    } else if actual != expected {
        Err(ScenarioError::Assertion(format!(
            "deployment {deployment_id:?} has {actual} ready replicas, expected {expected}"
        )))
    } else {
        Ok(())
    }
}

fn assert_deployment_phase<DeploymentId: Debug + Eq>(
    snapshot: &ClusterSnapshot<DeploymentId>,
    service_name: &FixtureName,
    deployment_id: &DeploymentId,
    expected: DeploymentPhase,
) -> Result<(), ScenarioError> {
    let service = require_service(snapshot, service_name)?;
    let deployment = service.deployment(deployment_id).ok_or_else(|| {
        ScenarioError::Assertion(format!(
            "deployment {deployment_id:?} is absent while checking its phase"
        ))
    })?;

    if deployment.phase == expected {
        Ok(())
    } else {
        Err(ScenarioError::Assertion(format!(
            "deployment {deployment_id:?} converged to {:?}, expected {expected:?}",
            deployment.phase
        )))
    }
}

fn ready_replica_count<DeploymentId>(
    deployment: &crate::DeploymentSnapshot<DeploymentId>,
) -> usize {
    deployment
        .replicas
        .iter()
        .filter(|replica| replica.phase == DeploymentPhase::Ready)
        .count()
}

fn driver_error(error_operation: &'static str, error: impl std::fmt::Display) -> ScenarioError {
    ScenarioError::Driver {
        operation: error_operation,
        message: error.to_string(),
    }
}
