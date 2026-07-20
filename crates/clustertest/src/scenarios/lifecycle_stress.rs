use crate::{
    AcceptanceCluster, ClusterSnapshot, DeploymentPhase, FixtureName, FixtureVersion, ReplicaCount,
    ReplicaOverride, ScenarioError, ServiceFixture,
};

/// Proves an exact three-replica rollout starts and readies every slot.
pub async fn multi_replica_rollout_starts_all_replicas<Cluster>(
    cluster: &mut Cluster,
) -> Result<(), ScenarioError>
where
    Cluster: AcceptanceCluster,
{
    let name = FixtureName::new("acceptance-multi-replica");
    let deployment = rollout(cluster, name.clone(), "v1", 3).await?;
    let snapshot = converge(cluster, "await multi-replica rollout").await?;
    assert_ready_count(&snapshot, &name, &deployment, 3)
}

/// Proves sequential redeployments supersede every older generation.
pub async fn sequential_redeploys_supersede_history<Cluster>(
    cluster: &mut Cluster,
) -> Result<(), ScenarioError>
where
    Cluster: AcceptanceCluster,
{
    let name = FixtureName::new("acceptance-sequential-redeploy");
    let first = rollout(cluster, name.clone(), "v1", 1).await?;
    converge(cluster, "await first sequential rollout").await?;
    let second = rollout(cluster, name.clone(), "v2", 1).await?;
    converge(cluster, "await second sequential rollout").await?;
    let third = rollout(cluster, name.clone(), "v3", 1).await?;
    let snapshot = converge(cluster, "await third sequential rollout").await?;
    let service = require_service(&snapshot, &name)?;
    if service.deployments.len() != 3 {
        return Err(ScenarioError::Assertion(format!(
            "sequential history has {} deployments, expected 3",
            service.deployments.len()
        )));
    }
    assert_superseded(service.deployment(&first), "first")?;
    assert_superseded(service.deployment(&second), "second")?;
    assert_ready_count(&snapshot, &name, &third, 1)
}

/// Proves independent services all progress under one controller.
pub async fn many_services_roll_out_independently<Cluster>(
    cluster: &mut Cluster,
) -> Result<(), ScenarioError>
where
    Cluster: AcceptanceCluster,
{
    let mut deployments = Vec::new();
    for index in 0..8 {
        let name = FixtureName::new(format!("acceptance-service-{index}"));
        let deployment = rollout(cluster, name.clone(), "v1", 1).await?;
        deployments.push((name, deployment));
    }
    let snapshot = converge(cluster, "await independent service rollouts").await?;
    for (name, deployment) in deployments {
        assert_ready_count(&snapshot, &name, &deployment, 1)?;
    }
    Ok(())
}

/// Proves three queued generations settle with only the latest active.
pub async fn back_to_back_redeploys_keep_only_latest_ready<Cluster>(
    cluster: &mut Cluster,
) -> Result<(), ScenarioError>
where
    Cluster: AcceptanceCluster,
{
    assert_rapid_redeploy_settles(cluster, "acceptance-back-to-back", 3).await
}

/// Proves an override above the configured floor starts additional replicas.
pub async fn replica_override_scales_up<Cluster>(cluster: &mut Cluster) -> Result<(), ScenarioError>
where
    Cluster: AcceptanceCluster,
{
    let name = FixtureName::new("acceptance-scale-up");
    let deployment = rollout(cluster, name.clone(), "v1", 2).await?;
    converge(cluster, "await scale-up baseline").await?;
    cluster
        .set_replicas(&name, ReplicaOverride::Set(ReplicaCount::new(4)))
        .await
        .map_err(|error| driver_error("set scale-up override", error))?;
    let snapshot = converge(cluster, "await scale-up override").await?;
    assert_ready_count(&snapshot, &name, &deployment, 4)
}

/// Proves an override below configuration cannot reduce the replica floor.
pub async fn replica_override_respects_configured_floor<Cluster>(
    cluster: &mut Cluster,
) -> Result<(), ScenarioError>
where
    Cluster: AcceptanceCluster,
{
    let name = FixtureName::new("acceptance-replica-floor");
    let deployment = rollout(cluster, name.clone(), "v1", 4).await?;
    converge(cluster, "await replica-floor baseline").await?;
    cluster
        .set_replicas(&name, ReplicaOverride::Set(ReplicaCount::new(1)))
        .await
        .map_err(|error| driver_error("set below-floor override", error))?;
    let snapshot = converge(cluster, "await below-floor override").await?;
    assert_ready_count(&snapshot, &name, &deployment, 4)
}

/// Proves eight rapid generations eventually leave exactly one ready deployment.
pub async fn many_rapid_redeploys_settle_to_one_ready<Cluster>(
    cluster: &mut Cluster,
) -> Result<(), ScenarioError>
where
    Cluster: AcceptanceCluster,
{
    assert_rapid_redeploy_settles(cluster, "acceptance-rapid-redeploy", 8).await
}

async fn assert_rapid_redeploy_settles<Cluster>(
    cluster: &mut Cluster,
    service_name: &'static str,
    generations: u32,
) -> Result<(), ScenarioError>
where
    Cluster: AcceptanceCluster,
{
    let name = FixtureName::new(service_name);
    let mut deployment_ids = Vec::new();
    for generation in 1..=generations {
        deployment_ids.push(rollout(cluster, name.clone(), &format!("v{generation}"), 1).await?);
    }
    let snapshot = converge(cluster, "await rapid redeploy convergence").await?;
    let service = require_service(&snapshot, &name)?;
    let ready = service
        .deployments
        .iter()
        .filter(|deployment| deployment.phase == DeploymentPhase::Ready)
        .collect::<Vec<_>>();
    let latest = deployment_ids.last().ok_or_else(|| {
        ScenarioError::Assertion("rapid redeploy generated no deployments".to_string())
    })?;
    let older_active = service.deployments.iter().any(|deployment| {
        deployment.id != *latest
            && matches!(
                deployment.phase,
                DeploymentPhase::Queued
                    | DeploymentPhase::Building
                    | DeploymentPhase::PendingReady
                    | DeploymentPhase::Ready
            )
    });
    if ready.len() == 1
        && ready
            .first()
            .is_some_and(|deployment| deployment.id == *latest)
        && !older_active
    {
        Ok(())
    } else {
        Err(ScenarioError::Assertion(format!(
            "rapid redeploy did not settle on latest generation: {:?}",
            service.deployments
        )))
    }
}

async fn rollout<Cluster>(
    cluster: &mut Cluster,
    name: FixtureName,
    version: &str,
    replicas: u32,
) -> Result<Cluster::DeploymentId, ScenarioError>
where
    Cluster: AcceptanceCluster,
{
    cluster
        .rollout(ServiceFixture::new(
            name,
            FixtureVersion::new(version),
            ReplicaCount::new(replicas),
        ))
        .await
        .map_err(|error| driver_error("queue stress rollout", error))
}

async fn converge<Cluster>(
    cluster: &mut Cluster,
    operation: &'static str,
) -> Result<ClusterSnapshot<Cluster::DeploymentId>, ScenarioError>
where
    Cluster: AcceptanceCluster,
{
    cluster
        .await_converged()
        .await
        .map_err(|error| driver_error(operation, error))
}

fn require_service<'snapshot, DeploymentId>(
    snapshot: &'snapshot ClusterSnapshot<DeploymentId>,
    name: &FixtureName,
) -> Result<&'snapshot crate::ServiceSnapshot<DeploymentId>, ScenarioError> {
    snapshot
        .service(name)
        .ok_or_else(|| ScenarioError::Assertion(format!("service {name:?} is absent")))
}

fn assert_ready_count<DeploymentId: std::fmt::Debug + Eq>(
    snapshot: &ClusterSnapshot<DeploymentId>,
    name: &FixtureName,
    deployment: &DeploymentId,
    expected: usize,
) -> Result<(), ScenarioError> {
    let observed = require_service(snapshot, name)?
        .deployment(deployment)
        .ok_or_else(|| ScenarioError::Assertion(format!("deployment {deployment:?} is absent")))?;
    let ready = observed
        .replicas
        .iter()
        .filter(|replica| replica.phase == DeploymentPhase::Ready)
        .count();
    if observed.phase == DeploymentPhase::Ready && ready == expected {
        Ok(())
    } else {
        Err(ScenarioError::Assertion(format!(
            "deployment {deployment:?} was {:?} with {ready} ready replicas, expected {expected}",
            observed.phase
        )))
    }
}

fn assert_superseded<DeploymentId: std::fmt::Debug>(
    deployment: Option<&crate::DeploymentSnapshot<DeploymentId>>,
    label: &'static str,
) -> Result<(), ScenarioError> {
    match deployment {
        Some(deployment)
            if matches!(
                deployment.phase,
                DeploymentPhase::Draining | DeploymentPhase::Removed
            ) =>
        {
            Ok(())
        }
        observed => Err(ScenarioError::Assertion(format!(
            "{label} deployment was not superseded: {observed:?}"
        ))),
    }
}

fn driver_error(error_operation: &'static str, error: impl std::fmt::Display) -> ScenarioError {
    ScenarioError::Driver {
        operation: error_operation,
        message: error.to_string(),
    }
}
