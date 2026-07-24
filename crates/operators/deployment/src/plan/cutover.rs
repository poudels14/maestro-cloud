use std::collections::BTreeMap;

use kernel_api::{
    Deployment, DeploymentId, DeploymentPhase, DeploymentStatus, Service, ServiceStatus, Timestamp,
    TrafficGeneration, TrafficGenerationPhase,
};

pub(super) fn coordinate_active_deployment(
    service: &Service,
    deployments: &[&Deployment],
    traffic: &[TrafficGeneration],
    now: Timestamp,
    desired_deployment_id: Option<&DeploymentId>,
    desired_statuses: &mut BTreeMap<DeploymentId, DeploymentStatus>,
    desired_service: &mut ServiceStatus,
) {
    let desired_candidate = desired_deployment_id.and_then(|desired_id| {
        deployments.iter().find(|deployment| {
            deployment.meta.id == *desired_id
                && desired_statuses
                    .get(&deployment.meta.id)
                    .is_some_and(|status| status.phase == DeploymentPhase::Ready)
        })
    });
    let candidate = desired_candidate.copied().or_else(|| {
        deployments
            .iter()
            .filter(|deployment| {
                desired_statuses
                    .get(&deployment.meta.id)
                    .is_some_and(|status| status.phase == DeploymentPhase::Ready)
            })
            .max_by(|left, right| rollout_order(left, right))
            .copied()
    });
    let active_deployment = desired_service
        .active_deployment_id
        .as_ref()
        .and_then(|id| {
            deployments
                .iter()
                .find(|deployment| &deployment.meta.id == id)
        })
        .copied();
    let should_activate = candidate.is_some_and(|candidate| {
        (desired_deployment_id == Some(&candidate.meta.id)
            && active_deployment.is_none_or(|active| active.meta.id != candidate.meta.id))
            || active_deployment.is_none_or(|active| rollout_order(candidate, active).is_gt())
    });
    if should_activate {
        desired_service.active_deployment_id =
            candidate.map(|deployment| deployment.meta.id.clone());
    }
    if desired_service
        .active_deployment_id
        .as_ref()
        .is_some_and(|deployment_id| {
            desired_statuses
                .get(deployment_id)
                .is_none_or(|status| status.phase != DeploymentPhase::Ready)
        })
    {
        desired_service.active_deployment_id = None;
    }
    let Some(active_id) = desired_service.active_deployment_id.clone() else {
        return;
    };
    let Some(active_deployment) = deployments
        .iter()
        .find(|deployment| deployment.meta.id == active_id)
    else {
        return;
    };
    let cutover_acknowledged = traffic.iter().any(|generation| {
        generation.meta.deletion_timestamp.is_none()
            && generation.spec.service_id == service.meta.id
            && generation.spec.deployment_id == active_id
            && generation.status.phase == TrafficGenerationPhase::Active
    });
    if !cutover_acknowledged {
        return;
    }
    let active_is_desired = desired_deployment_id == Some(&active_id);
    for deployment in deployments {
        if deployment.meta.id == active_id
            || desired_deployment_id == Some(&deployment.meta.id)
            || (!active_is_desired && !rollout_order(deployment, active_deployment).is_lt())
        {
            continue;
        }
        let Some(status) = desired_statuses.get_mut(&deployment.meta.id) else {
            continue;
        };
        if matches!(
            status.phase,
            DeploymentPhase::Building | DeploymentPhase::PendingReady | DeploymentPhase::Ready
        ) {
            status.phase = DeploymentPhase::Draining;
            status.draining_at.get_or_insert(now);
        }
    }
}

fn rollout_order(left: &Deployment, right: &Deployment) -> std::cmp::Ordering {
    left.spec
        .service_generation
        .cmp(&right.spec.service_generation)
        .then_with(|| left.status.created_at.cmp(&right.status.created_at))
        .then_with(|| left.meta.id.cmp(&right.meta.id))
}
