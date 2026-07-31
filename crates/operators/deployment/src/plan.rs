mod cutover;

use std::collections::BTreeMap;

use kernel_api::{
    ArtifactTemplate, Assignment, Build, BuildId, BuildPhase, Deployment, DeploymentId,
    DeploymentPhase, DeploymentStatus, InvalidIdentifier, ReplicaState, Service, ServiceId,
    Timestamp,
};

use crate::readiness::{all_exhausted, all_ready, current_slots, drain_elapsed, has_assignments};
use crate::resource::{index, new_build, new_deployment, validate_ownership};
use crate::{DeploymentInput, DeploymentPlan, ResourceStatusUpdate};

use self::cutover::coordinate_active_deployment;

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
        let service_routes = input
            .ingress_routes
            .iter()
            .filter(|route| route.spec.service_id == service.meta.id)
            .cloned()
            .collect::<Vec<_>>();
        let related = deployments
            .values()
            .filter(|deployment| deployment.spec.service_id == service.meta.id)
            .collect::<Vec<_>>();
        let mut desired_service_status = service.status.clone();
        let desired_deployment_id = if service.meta.deletion_timestamp.is_none() {
            let desired = new_deployment(&input.cluster_id, service, &service_routes, input.now)?;
            let captured_service_changed = desired.spec.service != service.spec;
            let existing = if captured_service_changed {
                deployments.get(&desired.meta.id).or_else(|| {
                    related.iter().copied().find(|deployment| {
                        deployment.spec.service_generation == service.meta.generation
                            && deployment.spec.service == desired.spec.service
                    })
                })
            } else {
                related.iter().copied().find(|deployment| {
                    deployment.spec.service_generation == service.meta.generation
                })
            };
            let desired_id = existing.map_or_else(
                || {
                    let desired_id = desired.meta.id.clone();
                    output.create_deployments.push(desired);
                    desired_id
                },
                |deployment| deployment.meta.id.clone(),
            );
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
            if deployment.status.phase == DeploymentPhase::Queued
                && desired.phase != DeploymentPhase::Queued
                && deployment.spec.bypass_rollout_freeze
                && desired_service_status.rollout_bypass_generation
                    == Some(deployment.spec.service_generation)
            {
                desired_service_status.rollout_bypass_generation = None;
            }
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
                &mut desired_service_status,
            );
            if desired_service_status != service.status {
                output.service_updates.push(ResourceStatusUpdate {
                    id: service.meta.id.clone(),
                    observed_revision: service.meta.revision,
                    status: desired_service_status,
                });
            }
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

    collect_orphan_replicas(
        &input.assignments,
        &input.replicas,
        &mut output.delete_replicas,
    );

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

fn collect_orphan_replicas(
    assignments: &[Assignment],
    replicas: &[ReplicaState],
    delete_replicas: &mut Vec<kernel_api::ReplicaStateId>,
) {
    let current_assignments = assignments
        .iter()
        .map(|assignment| &assignment.meta.id)
        .collect::<std::collections::BTreeSet<_>>();
    delete_replicas.extend(
        replicas
            .iter()
            .filter(|replica| !current_assignments.contains(&replica.spec.assignment_id))
            .map(|replica| replica.meta.id.clone()),
    );
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

#[allow(clippy::too_many_arguments)]
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
        DeploymentPhase::Queued
            if service.status.rollout == kernel_api::RolloutState::Active
                || deployment.spec.bypass_rollout_freeze =>
        {
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
    } else if desired.phase != DeploymentPhase::Ready
        && slots.len() == usize::try_from(count).unwrap_or(usize::MAX)
    {
        desired.phase = DeploymentPhase::PendingReady;
    }
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
    /// A runtime environment value contained malformed template syntax.
    #[error("Service `{service_id}` environment `{key}` has an invalid template: {message}")]
    InvalidEnvironmentTemplate {
        service_id: ServiceId,
        key: String,
        message: String,
    },
    /// A supported runtime variable had no unambiguous value for this service.
    #[error("Service `{service_id}` environment `{key}` cannot resolve `{variable}`: {message}")]
    UnavailableEnvironmentVariable {
        service_id: ServiceId,
        key: String,
        variable: String,
        message: String,
    },
    /// Persisted state requested a transition outside the reviewed nine-state matrix.
    #[error("Deployment `{deployment_id}` cannot transition from {from:?} to {to:?}")]
    InvalidTransition {
        deployment_id: DeploymentId,
        from: DeploymentPhase,
        to: DeploymentPhase,
    },
}
