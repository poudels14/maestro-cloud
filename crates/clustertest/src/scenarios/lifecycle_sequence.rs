use std::collections::BTreeMap;
use std::time::Duration;

use crate::{
    ClusterSnapshot, DeploymentPhase, FixtureName, LifecycleFaultCluster, LifecycleOperation,
    ReplicaIndex, ReplicaRecordDisposition, ResourceAvailability, ScenarioError, ServiceFixture,
};

/// Runs generated lifecycle operations and checks global state-machine invariants.
pub async fn lifecycle_operation_sequence_preserves_invariants<Cluster>(
    cluster: &mut Cluster,
    operations: &[LifecycleOperation],
) -> Result<(), ScenarioError>
where
    Cluster: LifecycleFaultCluster,
{
    let mut deployments = BTreeMap::<FixtureName, Vec<Cluster::DeploymentId>>::new();
    for operation in operations {
        match operation {
            LifecycleOperation::Rollout {
                service,
                version,
                replicas,
            } => {
                let deployment = cluster
                    .rollout(ServiceFixture::new(
                        service.clone(),
                        version.clone(),
                        *replicas,
                    ))
                    .await
                    .map_err(|error| driver_error("apply generated rollout", error))?;
                deployments
                    .entry(service.clone())
                    .or_default()
                    .push(deployment);
                cluster
                    .reconcile_once()
                    .await
                    .map_err(|error| driver_error("reconcile generated rollout", error))?;
            }
            LifecycleOperation::CancelLatest { service } => {
                if let Some(deployment) =
                    deployments.get(service).and_then(|history| history.last())
                {
                    cluster
                        .cancel(deployment)
                        .await
                        .map_err(|error| driver_error("cancel generated deployment", error))?;
                }
            }
            LifecycleOperation::CrashLatest { service, replica } => {
                crash_latest_workload(cluster, service, *replica).await?;
            }
            LifecycleOperation::Advance { millis } => {
                cluster
                    .advance(Duration::from_millis(u64::from(*millis)))
                    .await
                    .map_err(|error| driver_error("advance generated logical time", error))?;
            }
            LifecycleOperation::Reconcile => {
                cluster
                    .reconcile_once()
                    .await
                    .map_err(|error| driver_error("apply generated reconcile", error))?;
            }
        }
    }
    let snapshot = cluster
        .settle_operation_sequence()
        .await
        .map_err(|error| driver_error("settle generated lifecycle sequence", error))?;
    assert_lifecycle_invariants(&snapshot)
}

/// Checks uniqueness, supersession, and terminal-phase monotonicity.
pub fn assert_lifecycle_invariants<DeploymentId: std::fmt::Debug + Eq>(
    snapshot: &ClusterSnapshot<DeploymentId>,
) -> Result<(), ScenarioError> {
    for service in &snapshot.services {
        let ready = service
            .deployments
            .iter()
            .filter(|deployment| deployment.phase == DeploymentPhase::Ready)
            .collect::<Vec<_>>();
        if ready.len() > 1 {
            return Err(ScenarioError::Assertion(format!(
                "service {:?} has multiple ready deployments: {ready:?}",
                service.name
            )));
        }
        if let Some(latest_ready) = ready.first()
            && service.deployments.iter().any(|deployment| {
                deployment.id != latest_ready.id
                    && matches!(
                        deployment.phase,
                        DeploymentPhase::Queued | DeploymentPhase::Building
                    )
            })
        {
            return Err(ScenarioError::Assertion(format!(
                "service {:?} has queued or building history beside ready deployment {:?}",
                service.name, latest_ready.id
            )));
        }
        for deployment in &service.deployments {
            for phases in deployment.phase_history.windows(2) {
                let Some(previous) = phases.first() else {
                    continue;
                };
                let Some(next) = phases.get(1) else {
                    continue;
                };
                if matches!(
                    previous,
                    DeploymentPhase::Terminated
                        | DeploymentPhase::Removed
                        | DeploymentPhase::Canceled
                ) && previous != next
                {
                    return Err(ScenarioError::Assertion(format!(
                        "deployment {:?} regressed from terminal phase {previous:?} to {next:?}",
                        deployment.id
                    )));
                }
            }
        }
    }
    Ok(())
}

async fn crash_latest_workload<Cluster>(
    cluster: &mut Cluster,
    service_name: &FixtureName,
    requested_replica: ReplicaIndex,
) -> Result<(), ScenarioError>
where
    Cluster: LifecycleFaultCluster,
{
    let snapshot = cluster
        .snapshot_now()
        .await
        .map_err(|error| driver_error("observe generated crash target", error))?;
    let Some(service) = snapshot.service(service_name) else {
        return Ok(());
    };
    let Some(deployment) = service.deployments.iter().rev().find(|deployment| {
        matches!(
            deployment.phase,
            DeploymentPhase::Ready
                | DeploymentPhase::PendingReady
                | DeploymentPhase::Publishing
                | DeploymentPhase::Building
        )
    }) else {
        return Ok(());
    };
    let Some(maximum_index) = deployment
        .replicas
        .iter()
        .map(|replica| replica.index)
        .max()
    else {
        return Ok(());
    };
    let replica_index = ReplicaIndex::new(requested_replica.get().min(maximum_index));
    if deployment.replicas.iter().any(|replica| {
        replica.index == replica_index.get() && replica.workload == ResourceAvailability::Available
    }) {
        cluster
            .inject_workload_termination(
                &deployment.id,
                replica_index,
                ReplicaRecordDisposition::Retained,
            )
            .await
            .map_err(|error| driver_error("apply generated workload crash", error))?;
    }
    Ok(())
}

fn driver_error(error_operation: &'static str, error: impl std::fmt::Display) -> ScenarioError {
    ScenarioError::Driver {
        operation: error_operation,
        message: error.to_string(),
    }
}
