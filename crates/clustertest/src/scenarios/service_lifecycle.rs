use std::collections::BTreeSet;
use std::time::Duration;

use crate::{
    ClusterSnapshot, DeploymentPhase, FixtureName, FixtureVersion, ReplicaCount, ReplicaOverride,
    ScenarioError, ServiceFixture, ServiceLifecycleCluster,
};

/// Proves restart replaces workloads without creating a new deployment.
pub async fn restart_recycles_workloads_in_place<Cluster>(
    cluster: &mut Cluster,
) -> Result<(), ScenarioError>
where
    Cluster: ServiceLifecycleCluster,
{
    let name = FixtureName::new("acceptance-restart");
    let deployment_id = rollout(cluster, name.clone(), 1).await?;
    let before = converge(cluster, "await restart baseline").await?;
    let previous_instances = workload_instances(&before, &name, &deployment_id)?;

    cluster
        .restart_deployment(&deployment_id)
        .await
        .map_err(|error| driver_error("restart deployment", error))?;
    let after = converge(cluster, "await restart convergence").await?;
    let service = require_service(&after, &name)?;
    let deployment = service.deployment(&deployment_id).ok_or_else(|| {
        ScenarioError::Assertion(format!("restarted deployment {deployment_id:?} is absent"))
    })?;
    let current_instances = workload_instances(&after, &name, &deployment_id)?;

    if service.active_deployment_id.as_ref() != Some(&deployment_id) {
        Err(ScenarioError::Assertion(format!(
            "restart changed the active deployment from {deployment_id:?} to {:?}",
            service.active_deployment_id
        )))
    } else if deployment.phase != DeploymentPhase::Ready {
        Err(ScenarioError::Assertion(format!(
            "restarted deployment {deployment_id:?} converged to {:?}",
            deployment.phase
        )))
    } else if previous_instances.is_empty()
        || current_instances.len() != previous_instances.len()
        || !previous_instances.is_disjoint(&current_instances)
    {
        Err(ScenarioError::Assertion(format!(
            "restart did not replace each workload instance: before={previous_instances:?}, after={current_instances:?}"
        )))
    } else {
        Ok(())
    }
}

/// Proves remove drains workloads and retains a terminal deployment history record.
pub async fn remove_deployment_retains_history<Cluster>(
    cluster: &mut Cluster,
) -> Result<(), ScenarioError>
where
    Cluster: ServiceLifecycleCluster,
{
    let name = FixtureName::new("acceptance-remove");
    let deployment_id = rollout(cluster, name.clone(), 1).await?;
    converge(cluster, "await remove baseline").await?;
    cluster
        .remove_deployment(&deployment_id)
        .await
        .map_err(|error| driver_error("remove deployment", error))?;
    converge(cluster, "await remove drain").await?;
    cluster
        .advance(Duration::from_secs(1))
        .await
        .map_err(|error| driver_error("advance remove grace", error))?;
    let snapshot = converge(cluster, "await remove finalization").await?;
    let service = require_service(&snapshot, &name)?;
    let deployment = service.deployment(&deployment_id).ok_or_else(|| {
        ScenarioError::Assertion(format!("removed deployment {deployment_id:?} is absent"))
    })?;
    if service.active_deployment_id.is_none()
        && deployment.phase == DeploymentPhase::Removed
        && deployment.replicas.is_empty()
    {
        Ok(())
    } else {
        Err(ScenarioError::Assertion(format!(
            "deployment removal did not settle: service={service:?}"
        )))
    }
}

/// Proves service deletion cascades only after traffic and workload drain grace.
pub async fn delete_service_collects_owned_state<Cluster>(
    cluster: &mut Cluster,
) -> Result<(), ScenarioError>
where
    Cluster: ServiceLifecycleCluster,
{
    let name = FixtureName::new("acceptance-delete");
    rollout(cluster, name.clone(), 1).await?;
    converge(cluster, "await delete baseline").await?;
    cluster
        .delete_service(&name)
        .await
        .map_err(|error| driver_error("delete service", error))?;
    converge(cluster, "await delete drain").await?;
    cluster
        .advance(Duration::from_secs(1))
        .await
        .map_err(|error| driver_error("advance delete grace", error))?;
    let snapshot = converge(cluster, "await service collection").await?;
    if snapshot.service(&name).is_none() {
        Ok(())
    } else {
        Err(ScenarioError::Assertion(format!(
            "deleted service {name:?} remains in snapshot"
        )))
    }
}

/// Proves freeze holds a new generation queued until explicit unfreeze.
pub async fn freeze_and_unfreeze_gate_rollout<Cluster>(
    cluster: &mut Cluster,
) -> Result<(), ScenarioError>
where
    Cluster: ServiceLifecycleCluster,
{
    let name = FixtureName::new("acceptance-freeze");
    let active_id = rollout(cluster, name.clone(), 1).await?;
    converge(cluster, "await freeze baseline").await?;
    cluster
        .set_service_frozen(&name, crate::ServiceFreezeState::Frozen)
        .await
        .map_err(|error| driver_error("freeze service", error))?;
    let queued_id = cluster
        .rollout(ServiceFixture::new(
            name.clone(),
            FixtureVersion::new("v2"),
            ReplicaCount::new(1),
        ))
        .await
        .map_err(|error| driver_error("rollout while frozen", error))?;
    let frozen = converge(cluster, "await frozen rollout").await?;
    let service = require_service(&frozen, &name)?;
    let queued = service.deployment(&queued_id).ok_or_else(|| {
        ScenarioError::Assertion(format!("frozen deployment {queued_id:?} is absent"))
    })?;
    if queued.phase != DeploymentPhase::Queued
        || service.active_deployment_id.as_ref() != Some(&active_id)
    {
        return Err(ScenarioError::Assertion(format!(
            "freeze did not hold the new deployment: service={service:?}"
        )));
    }

    cluster
        .set_service_frozen(&name, crate::ServiceFreezeState::Active)
        .await
        .map_err(|error| driver_error("unfreeze service", error))?;
    let unfrozen = converge(cluster, "await unfrozen rollout").await?;
    let service = require_service(&unfrozen, &name)?;
    let deployment = service.deployment(&queued_id).ok_or_else(|| {
        ScenarioError::Assertion(format!("unfrozen deployment {queued_id:?} is absent"))
    })?;
    if deployment.phase == DeploymentPhase::Ready
        && service.active_deployment_id.as_ref() == Some(&queued_id)
    {
        Ok(())
    } else {
        Err(ScenarioError::Assertion(format!(
            "unfreeze did not release the queued deployment: service={service:?}"
        )))
    }
}

/// Proves drain relocates when possible and restore admits new placement.
pub async fn drain_and_restore_move_placement<Cluster>(
    cluster: &mut Cluster,
) -> Result<(), ScenarioError>
where
    Cluster: ServiceLifecycleCluster,
{
    let nodes = cluster.topology_nodes();
    let target = nodes
        .last()
        .cloned()
        .ok_or_else(|| ScenarioError::Assertion("topology has no nodes".to_string()))?;
    let replica_count = u32::try_from(nodes.len())
        .map_err(|_error| ScenarioError::Assertion("topology is too large".to_string()))?;
    let name = FixtureName::new("acceptance-node-drain");
    let deployment_id = rollout(cluster, name.clone(), replica_count).await?;
    converge(cluster, "await drain baseline").await?;
    cluster
        .set_node_draining(&target, crate::NodeDrainState::Draining)
        .await
        .map_err(|error| driver_error("drain node", error))?;
    let drained = converge(cluster, "await node drain").await?;
    let drained_nodes = replica_nodes(&drained, &name, &deployment_id)?;
    let valid_drain = if nodes.len() == 1 {
        drained_nodes.iter().all(|node| node == &target)
    } else {
        drained_nodes.len() == nodes.len() && drained_nodes.iter().all(|node| node != &target)
    };
    if !valid_drain {
        return Err(ScenarioError::Assertion(format!(
            "node drain produced invalid placement: target={target:?}, replicas={drained_nodes:?}"
        )));
    }

    cluster
        .set_node_draining(&target, crate::NodeDrainState::Available)
        .await
        .map_err(|error| driver_error("restore node", error))?;
    cluster
        .set_replicas(
            &name,
            ReplicaOverride::Set(ReplicaCount::new(replica_count.saturating_add(1))),
        )
        .await
        .map_err(|error| driver_error("scale after node restore", error))?;
    let restored = converge(cluster, "await restored placement").await?;
    let restored_nodes = replica_nodes(&restored, &name, &deployment_id)?;
    if restored_nodes.iter().any(|node| node == &target) {
        Ok(())
    } else {
        Err(ScenarioError::Assertion(format!(
            "restored node {target:?} did not accept new work: {restored_nodes:?}"
        )))
    }
}

/// Proves a hard node affinity constraint pins every replica in the new generation.
pub async fn hard_node_affinity_pins_placement<Cluster>(
    cluster: &mut Cluster,
) -> Result<(), ScenarioError>
where
    Cluster: ServiceLifecycleCluster,
{
    let nodes = cluster.topology_nodes();
    let target = nodes
        .last()
        .cloned()
        .ok_or_else(|| ScenarioError::Assertion("topology has no nodes".to_string()))?;
    let replicas = u32::try_from(nodes.len())
        .map_err(|_error| ScenarioError::Assertion("topology is too large".to_string()))?;
    let name = FixtureName::new("acceptance-hard-affinity");
    rollout(cluster, name.clone(), replicas).await?;
    converge(cluster, "await affinity baseline").await?;
    cluster
        .set_service_node_affinity(&name, &target)
        .await
        .map_err(|error| driver_error("set hard node affinity", error))?;
    let snapshot = converge(cluster, "await affinity deployment").await?;
    let service = require_service(&snapshot, &name)?;
    let active_id = service.active_deployment_id.as_ref().ok_or_else(|| {
        ScenarioError::Assertion("affinity service has no active deployment".to_string())
    })?;
    let active = service.deployment(active_id).ok_or_else(|| {
        ScenarioError::Assertion(format!(
            "active affinity deployment {active_id:?} is absent"
        ))
    })?;
    let placements = active
        .replicas
        .iter()
        .map(|replica| replica.node.as_ref())
        .collect::<Vec<_>>();
    if active.replicas.len() == nodes.len() && placements.iter().all(|node| *node == Some(&target))
    {
        Ok(())
    } else {
        Err(ScenarioError::Assertion(format!(
            "hard affinity did not pin every replica to {target:?}: {placements:?}"
        )))
    }
}

async fn rollout<Cluster>(
    cluster: &mut Cluster,
    name: FixtureName,
    replicas: u32,
) -> Result<Cluster::DeploymentId, ScenarioError>
where
    Cluster: ServiceLifecycleCluster,
{
    cluster
        .rollout(ServiceFixture::new(
            name,
            FixtureVersion::new("v1"),
            ReplicaCount::new(replicas),
        ))
        .await
        .map_err(|error| driver_error("rollout lifecycle fixture", error))
}

async fn converge<Cluster>(
    cluster: &mut Cluster,
    operation: &'static str,
) -> Result<ClusterSnapshot<Cluster::DeploymentId>, ScenarioError>
where
    Cluster: ServiceLifecycleCluster,
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

fn workload_instances<DeploymentId: std::fmt::Debug + Eq>(
    snapshot: &ClusterSnapshot<DeploymentId>,
    name: &FixtureName,
    deployment_id: &DeploymentId,
) -> Result<BTreeSet<String>, ScenarioError> {
    let deployment = require_service(snapshot, name)?
        .deployment(deployment_id)
        .ok_or_else(|| {
            ScenarioError::Assertion(format!("deployment {deployment_id:?} is absent"))
        })?;
    deployment
        .replicas
        .iter()
        .map(|replica| {
            replica.workload_instance.clone().ok_or_else(|| {
                ScenarioError::Assertion(format!(
                    "deployment {deployment_id:?} replica {} has no workload instance",
                    replica.index
                ))
            })
        })
        .collect()
}

fn replica_nodes<DeploymentId: std::fmt::Debug + Eq>(
    snapshot: &ClusterSnapshot<DeploymentId>,
    name: &FixtureName,
    deployment_id: &DeploymentId,
) -> Result<Vec<crate::FixtureNodeName>, ScenarioError> {
    let deployment = require_service(snapshot, name)?
        .deployment(deployment_id)
        .ok_or_else(|| {
            ScenarioError::Assertion(format!("deployment {deployment_id:?} is absent"))
        })?;
    deployment
        .replicas
        .iter()
        .map(|replica| {
            replica.node.clone().ok_or_else(|| {
                ScenarioError::Assertion(format!(
                    "deployment {deployment_id:?} replica {} has no node",
                    replica.index
                ))
            })
        })
        .collect()
}

fn driver_error(operation: &'static str, error: impl std::fmt::Display) -> ScenarioError {
    ScenarioError::Driver {
        operation,
        message: error.to_string(),
    }
}
