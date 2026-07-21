use std::collections::BTreeMap;

use kernel_api::{
    ArtifactTemplate, Assignment, Build, BuildId, BuildPhase, Deployment, DeploymentId,
    DeploymentPhase, DeploymentStatus, InvalidIdentifier, ReplicaState, Service, ServiceId,
    Timestamp, TrafficGenerationPhase,
};

use crate::readiness::{all_exhausted, all_ready, current_slots, drain_elapsed, has_assignments};
use crate::resource::{index, new_build, new_deployment, validate_ownership};
use crate::{DeploymentInput, DeploymentPlan, ResourceStatusUpdate};

const SERVICE_KIND: &str = "Service";
const DEPLOYMENT_KIND: &str = "Deployment";

/// Computes one deterministic deployment lifecycle generation.
pub fn plan(input: DeploymentInput) -> Result<DeploymentPlan, DeploymentPlanError> {
    if input.settings.drain_grace.is_zero() {
        return Err(DeploymentPlanError::ZeroDrainGrace);
    }
    let services = index(
        input.services,
        |service| service.meta.id.clone(),
        SERVICE_KIND,
    )?;
    let deployments = index(
        input.deployments,
        |deployment| deployment.meta.id.clone(),
        DEPLOYMENT_KIND,
    )?;
    let builds = index(input.builds, |build| build.meta.id.clone(), "Build")?;
    validate_ownership(&services, &deployments)?;

    let mut output = DeploymentPlan::default();
    let mut desired_statuses = deployments
        .iter()
        .map(|(id, deployment)| (id.clone(), deployment.status.clone()))
        .collect::<BTreeMap<_, _>>();

    for service in services.values() {
        let related = deployments
            .values()
            .filter(|deployment| deployment.spec.service_id == service.meta.id)
            .collect::<Vec<_>>();
        let desired_deployment_id = if service.meta.deletion_timestamp.is_none() {
            let desired = new_deployment(&input.cluster_id, service, input.now)?;
            let desired_id = desired.meta.id.clone();
            let watched_revision = desired.spec.service != service.spec;
            let exists = if watched_revision {
                deployments.contains_key(&desired.meta.id)
            } else {
                related
                    .iter()
                    .any(|deployment| deployment.spec.service_generation == service.meta.generation)
            };
            if !exists {
                output.create_deployments.push(desired);
            }
            Some(desired_id)
        } else {
            None
        };

        for deployment in &related {
            if service.meta.deletion_timestamp.is_some()
                && deployment.status.phase == DeploymentPhase::Removed
            {
                output.delete_deployments.push(deployment.meta.id.clone());
                desired_statuses.remove(&deployment.meta.id);
                continue;
            }
            let desired = desired_deployment_status(
                service,
                deployment,
                &builds,
                &input.assignments,
                &input.replicas,
                input.now,
                input.settings.drain_grace,
                &mut output.create_builds,
            )?;
            desired_statuses.insert(deployment.meta.id.clone(), desired);
        }

        if service.meta.deletion_timestamp.is_none() {
            coordinate_active_deployment(
                service,
                &related,
                &input.traffic_generations,
                input.now,
                desired_deployment_id.as_ref(),
                &mut desired_statuses,
                &mut output.service_updates,
            );
        } else {
            collect_finalized_children(
                service,
                &deployments,
                &builds,
                &input.replicas,
                &output.delete_deployments,
                &mut output.delete_builds,
                &mut output.delete_replicas,
            );
        }
    }

    for (deployment_id, desired) in desired_statuses {
        let deployment = deployments.get(&deployment_id).ok_or_else(|| {
            DeploymentPlanError::MissingIndexedDeployment {
                deployment_id: deployment_id.clone(),
            }
        })?;
        if desired != deployment.status {
            if !deployment.status.phase.can_transition_to(desired.phase) {
                return Err(DeploymentPlanError::InvalidTransition {
                    deployment_id,
                    from: deployment.status.phase,
                    to: desired.phase,
                });
            }
            output.deployment_updates.push(ResourceStatusUpdate {
                id: deployment.meta.id.clone(),
                observed_revision: deployment.meta.revision,
                status: desired,
            });
        }
    }
    output
        .create_deployments
        .sort_by(|left, right| left.meta.id.cmp(&right.meta.id));
    output
        .create_builds
        .sort_by(|left, right| left.meta.id.cmp(&right.meta.id));
    output.delete_deployments.sort();
    output.delete_builds.sort();
    output.delete_builds.dedup();
    output.delete_replicas.sort();
    output.delete_replicas.dedup();
    output
        .deployment_updates
        .sort_by(|left, right| left.id.cmp(&right.id));
    output
        .service_updates
        .sort_by(|left, right| left.id.cmp(&right.id));
    Ok(output)
}

fn collect_finalized_children(
    service: &Service,
    deployments: &BTreeMap<DeploymentId, Deployment>,
    builds: &BTreeMap<BuildId, Build>,
    replicas: &[ReplicaState],
    removed: &[DeploymentId],
    delete_builds: &mut Vec<BuildId>,
    delete_replicas: &mut Vec<kernel_api::ReplicaStateId>,
) {
    let removed = removed.iter().collect::<std::collections::BTreeSet<_>>();
    delete_builds.extend(
        builds
            .values()
            .filter(|build| {
                removed.contains(&build.spec.deployment_id)
                    || (build.spec.service_id == service.meta.id
                        && !deployments.contains_key(&build.spec.deployment_id))
            })
            .map(|build| build.meta.id.clone()),
    );
    delete_replicas.extend(
        replicas
            .iter()
            .filter(|replica| {
                removed.contains(&replica.spec.deployment_id)
                    || (replica.spec.service_id == service.meta.id
                        && !deployments.contains_key(&replica.spec.deployment_id))
            })
            .map(|replica| replica.meta.id.clone()),
    );
}

fn desired_deployment_status(
    service: &Service,
    deployment: &Deployment,
    builds: &BTreeMap<BuildId, Build>,
    assignments: &[Assignment],
    replicas: &[ReplicaState],
    now: Timestamp,
    drain_grace: std::time::Duration,
    create_builds: &mut Vec<Build>,
) -> Result<DeploymentStatus, DeploymentPlanError> {
    if let Some(desired) =
        crate::goal::requested_status(service, deployment, assignments, now, drain_grace)
    {
        return Ok(desired);
    }
    let mut desired = deployment.status.clone();
    match desired.phase {
        DeploymentPhase::Queued if service.status.rollout == kernel_api::RolloutState::Active => {
            if matches!(
                deployment.spec.service.artifact,
                ArtifactTemplate::Build { .. }
            ) {
                ensure_build(deployment, builds, create_builds)?;
            }
            desired.phase = DeploymentPhase::Building;
        }
        DeploymentPhase::Building => {
            let artifact_ready = match &deployment.spec.service.artifact {
                ArtifactTemplate::Image { .. } => true,
                ArtifactTemplate::Build { .. } => {
                    let build = ensure_build(deployment, builds, create_builds)?;
                    match build {
                        Some(build) if build.status.phase == BuildPhase::Succeeded => {
                            desired.image_digest =
                                Some(build.status.image_digest.clone().ok_or_else(|| {
                                    DeploymentPlanError::SucceededBuildMissingImage {
                                        build_id: build.meta.id.clone(),
                                    }
                                })?);
                            true
                        }
                        Some(build) if build.status.phase == BuildPhase::Failed => {
                            desired.phase = DeploymentPhase::Crashed;
                            false
                        }
                        Some(build) if build.status.phase == BuildPhase::Canceled => {
                            desired.phase = DeploymentPhase::Canceled;
                            false
                        }
                        Some(_) | None => false,
                    }
                }
            };
            if artifact_ready {
                advance_readiness(
                    service,
                    deployment,
                    assignments,
                    replicas,
                    now,
                    &mut desired,
                );
            }
        }
        DeploymentPhase::PendingReady | DeploymentPhase::Ready => {
            advance_readiness(
                service,
                deployment,
                assignments,
                replicas,
                now,
                &mut desired,
            );
        }
        DeploymentPhase::Draining => {
            if desired.draining_at.is_none() {
                desired.draining_at = Some(now);
            } else if drain_elapsed(desired.draining_at, now, drain_grace)
                && !has_assignments(&deployment.meta.id, assignments)
            {
                desired.phase = DeploymentPhase::Removed;
            }
        }
        DeploymentPhase::Queued
        | DeploymentPhase::Crashed
        | DeploymentPhase::Terminated
        | DeploymentPhase::Removed
        | DeploymentPhase::Canceled => {}
    }
    Ok(desired)
}

fn advance_readiness(
    service: &Service,
    deployment: &Deployment,
    assignments: &[Assignment],
    replicas: &[ReplicaState],
    now: Timestamp,
    desired: &mut DeploymentStatus,
) {
    let count = service
        .status
        .replica_override
        .unwrap_or(service.spec.replicas);
    let slots = current_slots(&deployment.meta.id, assignments, count);
    if all_exhausted(deployment, &slots, replicas, count) {
        desired.phase = DeploymentPhase::Crashed;
    } else if all_ready(deployment, &slots, replicas, count) {
        desired.phase = DeploymentPhase::Ready;
        desired.ready_at.get_or_insert(now);
    } else if slots.len() == usize::try_from(count).unwrap_or(usize::MAX) {
        desired.phase = DeploymentPhase::PendingReady;
    }
}

fn coordinate_active_deployment(
    service: &Service,
    deployments: &[&Deployment],
    traffic: &[kernel_api::TrafficGeneration],
    now: Timestamp,
    desired_deployment_id: Option<&DeploymentId>,
    desired_statuses: &mut BTreeMap<DeploymentId, DeploymentStatus>,
    service_updates: &mut Vec<ResourceStatusUpdate<ServiceId, kernel_api::ServiceStatus>>,
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
            .max_by(|left, right| {
                left.spec
                    .service_generation
                    .cmp(&right.spec.service_generation)
                    .then_with(|| left.status.created_at.cmp(&right.status.created_at))
                    .then_with(|| left.meta.id.cmp(&right.meta.id))
            })
            .copied()
    });
    let active_deployment = service
        .status
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
    let mut desired_service = service.status.clone();
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
    if desired_service != service.status {
        service_updates.push(ResourceStatusUpdate {
            id: service.meta.id.clone(),
            observed_revision: service.meta.revision,
            status: desired_service.clone(),
        });
    }

    let Some(active_id) = desired_service.active_deployment_id else {
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

fn ensure_build<'a>(
    deployment: &Deployment,
    builds: &'a BTreeMap<BuildId, Build>,
    create_builds: &mut Vec<Build>,
) -> Result<Option<&'a Build>, DeploymentPlanError> {
    let build_id = deployment.spec.build_id.as_ref().ok_or_else(|| {
        DeploymentPlanError::BuildDeploymentMissingId {
            deployment_id: deployment.meta.id.clone(),
        }
    })?;
    if let Some(build) = builds.get(build_id) {
        validate_build(deployment, build)?;
        return Ok(Some(build));
    }
    if !create_builds.iter().any(|build| &build.meta.id == build_id) {
        create_builds.push(new_build(deployment, build_id.clone())?);
    }
    Ok(None)
}

fn validate_build(deployment: &Deployment, build: &Build) -> Result<(), DeploymentPlanError> {
    let ArtifactTemplate::Build { template } = &deployment.spec.service.artifact else {
        return Err(DeploymentPlanError::UnexpectedBuild {
            deployment_id: deployment.meta.id.clone(),
            build_id: build.meta.id.clone(),
        });
    };
    if build.spec.service_id != deployment.spec.service_id
        || build.spec.deployment_id != deployment.meta.id
        || &build.spec.template != template
    {
        Err(DeploymentPlanError::BuildIdentityMismatch {
            deployment_id: deployment.meta.id.clone(),
            build_id: build.meta.id.clone(),
        })
    } else {
        Ok(())
    }
}

/// Invalid lifecycle input or impossible persisted transition.
#[derive(Debug, thiserror::Error)]
pub enum DeploymentPlanError {
    /// A zero grace period could remove serving workloads before cutover settles.
    #[error("deployment drain grace must be greater than zero")]
    ZeroDrainGrace,
    /// A generated built-in identity was invalid.
    #[error(transparent)]
    InvalidIdentifier(#[from] InvalidIdentifier),
    /// One identity appeared more than once in a typed input collection.
    #[error("{kind} `{resource_id}` appears more than once")]
    DuplicateResource {
        kind: &'static str,
        resource_id: String,
    },
    /// A Deployment referenced a Service absent from the complete input snapshot.
    #[error("Deployment `{deployment_id}` references missing Service `{service_id}`")]
    MissingService {
        deployment_id: DeploymentId,
        service_id: ServiceId,
    },
    /// Internal indexed state disappeared while producing updates.
    #[error("indexed Deployment `{deployment_id}` disappeared during planning")]
    MissingIndexedDeployment { deployment_id: DeploymentId },
    /// A build-backed Deployment omitted its immutable Build identity.
    #[error("build-backed Deployment `{deployment_id}` has no build id")]
    BuildDeploymentMissingId { deployment_id: DeploymentId },
    /// A Build was associated with an image-backed Deployment.
    #[error("Deployment `{deployment_id}` unexpectedly references Build `{build_id}`")]
    UnexpectedBuild {
        deployment_id: DeploymentId,
        build_id: BuildId,
    },
    /// Build spec identity or immutable template did not match its Deployment.
    #[error("Build `{build_id}` does not match Deployment `{deployment_id}`")]
    BuildIdentityMismatch {
        deployment_id: DeploymentId,
        build_id: BuildId,
    },
    /// A successful Build did not publish its immutable image digest.
    #[error("successful Build `{build_id}` has no image digest")]
    SucceededBuildMissingImage { build_id: BuildId },
    /// Persisted state requested a transition outside the reviewed nine-state matrix.
    #[error("Deployment `{deployment_id}` cannot transition from {from:?} to {to:?}")]
    InvalidTransition {
        deployment_id: DeploymentId,
        from: DeploymentPhase,
        to: DeploymentPhase,
    },
}
