use std::collections::BTreeMap;

use kernel_api::{
    Deployment, DeploymentId, IngressRoute, IngressRouteId, IngressRouteStatus, Service, ServiceId,
    TrafficGeneration, TrafficGenerationId, TrafficGenerationPhase, TrafficGenerationStatus,
};

use crate::lifecycle::{collect_expired, converge_desired, published, retire_all, select_active};
use crate::target::desired_spec;
use crate::validation::{capture_routes, validate_route_ownership};
use crate::{BackendChange, IngressInput, IngressPlan, PublishedTraffic, ResourceStatusUpdate};

/// Computes one deterministic ingress traffic generation.
pub fn plan(input: IngressInput) -> Result<IngressPlan, IngressPlanError> {
    if input.settings.retirement_grace.is_zero() {
        return Err(IngressPlanError::ZeroRetirementGrace);
    }
    let cluster_id = input.cluster_id.clone();
    let now = input.now;
    let services = index(
        input.services,
        |resource| resource.meta.id.clone(),
        "Service",
    )?;
    let deployments = index(
        input.deployments,
        |resource| resource.meta.id.clone(),
        "Deployment",
    )?;
    let generations = index(
        input.traffic_generations,
        |resource| resource.meta.id.clone(),
        "TrafficGeneration",
    )?;
    validate_route_ownership(&services, &input.routes)?;
    validate_generation_ownership(&services, &deployments, &generations)?;

    let mut output = IngressPlan::default();
    let mut desired_statuses = generations
        .iter()
        .map(|(id, generation)| (id.clone(), generation.status.clone()))
        .collect::<BTreeMap<_, _>>();

    for service in services.values() {
        let related = generations
            .values()
            .filter(|generation| generation.spec.service_id == service.meta.id)
            .collect::<Vec<_>>();
        let routes = input.routes.iter().filter(|route| {
            route.meta.deletion_timestamp.is_none() && route.spec.service_id == service.meta.id
        });
        let captured_routes = capture_routes(service, routes.cloned())?;
        let mut remove = collect_expired(
            &related,
            &mut desired_statuses,
            &mut output.delete_generations,
            now,
            input.settings.retirement_grace,
        );

        let active = if service.meta.deletion_timestamp.is_some() {
            retire_all(
                &related,
                &mut desired_statuses,
                &mut output.delete_generations,
                &mut remove,
                now,
            );
            None
        } else if let Some(spec) = desired_spec(
            service,
            &deployments,
            &captured_routes,
            &input.assignments,
            &input.replicas,
        )? {
            converge_desired(
                &cluster_id,
                now,
                service,
                spec,
                &related,
                &mut desired_statuses,
                &mut output,
                &mut remove,
            )?
        } else {
            select_active(&related, &desired_statuses).map(published)
        };

        if let Some(active) = active.as_ref() {
            acknowledge_routes(&input.routes, active, &mut output.route_updates);
        }
        remove.sort();
        remove.dedup();
        output.backend_changes.push(BackendChange {
            service_id: service.meta.id.clone(),
            active,
            remove,
        });
    }

    append_status_updates(&generations, desired_statuses, &mut output)?;
    sort_plan(&mut output);
    Ok(output)
}

fn append_status_updates(
    generations: &BTreeMap<TrafficGenerationId, TrafficGeneration>,
    desired_statuses: BTreeMap<TrafficGenerationId, TrafficGenerationStatus>,
    output: &mut IngressPlan,
) -> Result<(), IngressPlanError> {
    for (generation_id, status) in desired_statuses {
        if output.delete_generations.contains(&generation_id) {
            continue;
        }
        let generation = generations.get(&generation_id).ok_or_else(|| {
            IngressPlanError::MissingIndexedGeneration {
                generation_id: generation_id.clone(),
            }
        })?;
        if status != generation.status {
            if !generation.status.phase.can_transition_to(status.phase) {
                return Err(IngressPlanError::InvalidTransition {
                    generation_id,
                    from: generation.status.phase,
                    to: status.phase,
                });
            }
            output.generation_updates.push(ResourceStatusUpdate {
                id: generation.meta.id.clone(),
                observed_revision: generation.meta.revision,
                status,
            });
        }
    }
    Ok(())
}

fn acknowledge_routes(
    routes: &[IngressRoute],
    active: &PublishedTraffic,
    updates: &mut Vec<ResourceStatusUpdate<IngressRouteId, IngressRouteStatus>>,
) {
    for captured in &active.spec.routes {
        let Some(route) = routes.iter().find(|route| {
            route.meta.id == captured.route_id
                && route.meta.generation == captured.route_generation
                && route.meta.deletion_timestamp.is_none()
        }) else {
            continue;
        };
        if route.status.applied_generation != route.meta.generation {
            let mut status = route.status.clone();
            status.applied_generation = route.meta.generation;
            updates.push(ResourceStatusUpdate {
                id: route.meta.id.clone(),
                observed_revision: route.meta.revision,
                status,
            });
        }
    }
}

fn validate_generation_ownership(
    services: &BTreeMap<ServiceId, Service>,
    deployments: &BTreeMap<DeploymentId, Deployment>,
    generations: &BTreeMap<TrafficGenerationId, TrafficGeneration>,
) -> Result<(), IngressPlanError> {
    for generation in generations.values() {
        if !services.contains_key(&generation.spec.service_id) {
            return Err(IngressPlanError::MissingGenerationService {
                generation_id: generation.meta.id.clone(),
                service_id: generation.spec.service_id.clone(),
            });
        }
        if !deployments.contains_key(&generation.spec.deployment_id) {
            return Err(IngressPlanError::MissingGenerationDeployment {
                generation_id: generation.meta.id.clone(),
                deployment_id: generation.spec.deployment_id.clone(),
            });
        }
    }
    Ok(())
}

fn index<Id, Resource>(
    resources: Vec<Resource>,
    id: impl Fn(&Resource) -> Id,
    kind: &'static str,
) -> Result<BTreeMap<Id, Resource>, IngressPlanError>
where
    Id: Clone + Ord + std::fmt::Display,
{
    let mut indexed = BTreeMap::new();
    for resource in resources {
        let resource_id = id(&resource);
        if indexed.insert(resource_id.clone(), resource).is_some() {
            return Err(IngressPlanError::DuplicateResource {
                kind,
                resource_id: resource_id.to_string(),
            });
        }
    }
    Ok(indexed)
}

fn sort_plan(output: &mut IngressPlan) {
    output
        .create_generations
        .sort_by(|left, right| left.meta.id.cmp(&right.meta.id));
    output.delete_generations.sort();
    output.delete_generations.dedup();
    output
        .generation_updates
        .sort_by(|left, right| left.id.cmp(&right.id));
    output
        .route_updates
        .sort_by(|left, right| left.id.cmp(&right.id));
    output
        .backend_changes
        .sort_by(|left, right| left.service_id.cmp(&right.service_id));
}

/// Invalid ingress input or impossible persisted transition.
#[derive(Debug, thiserror::Error)]
pub enum IngressPlanError {
    /// A zero grace period could remove in-flight generation configuration.
    #[error("ingress retirement grace must be greater than zero")]
    ZeroRetirementGrace,
    /// A generated built-in identity was invalid.
    #[error(transparent)]
    InvalidIdentifier(#[from] kernel_api::InvalidIdentifier),
    /// One identity appeared more than once in a typed input collection.
    #[error("{kind} `{resource_id}` appears more than once")]
    DuplicateResource {
        kind: &'static str,
        resource_id: String,
    },
    /// A route referenced a Service absent from the complete snapshot.
    #[error("IngressRoute `{route_id}` references missing Service `{service_id}`")]
    MissingRouteService {
        route_id: IngressRouteId,
        service_id: ServiceId,
    },
    /// A route did not select any hostname.
    #[error("IngressRoute `{route_id}` must contain at least one host")]
    RouteHasNoHosts { route_id: IngressRouteId },
    /// A route hostname was not canonical or syntactically safe.
    #[error("IngressRoute `{route_id}` has invalid canonical host `{host}`")]
    InvalidRouteHost {
        route_id: IngressRouteId,
        host: String,
    },
    /// A route repeated one hostname.
    #[error("IngressRoute `{route_id}` contains a duplicate host")]
    DuplicateRouteHost { route_id: IngressRouteId },
    /// Two routes claimed the same host and path match.
    #[error("IngressRoutes `{first_route_id}` and `{second_route_id}` both claim host `{host}`")]
    ConflictingRoute {
        first_route_id: IngressRouteId,
        second_route_id: IngressRouteId,
        host: String,
    },
    /// A path prefix could not be represented safely in backend routing rules.
    #[error("IngressRoute `{route_id}` has invalid path prefix `{path}`")]
    InvalidPathPrefix {
        route_id: IngressRouteId,
        path: String,
    },
    /// A session-affinity header was not a valid HTTP field name.
    #[error("IngressRoute `{route_id}` has invalid affinity header `{header}`")]
    InvalidAffinityHeader {
        route_id: IngressRouteId,
        header: String,
    },
    /// A route selected a port not exposed by its Service.
    #[error(
        "IngressRoute `{route_id}` selects unexposed port {target_port} on Service `{service_id}`"
    )]
    UnexposedTargetPort {
        route_id: IngressRouteId,
        service_id: ServiceId,
        target_port: u16,
    },
    /// Service status referenced a Deployment absent from the complete snapshot.
    #[error("Service `{service_id}` references missing active Deployment `{deployment_id}`")]
    MissingActiveDeployment {
        service_id: ServiceId,
        deployment_id: DeploymentId,
    },
    /// Service status selected a Deployment owned by another Service.
    #[error("Service `{service_id}` selected foreign Deployment `{deployment_id}`")]
    ActiveDeploymentOwnershipMismatch {
        service_id: ServiceId,
        deployment_id: DeploymentId,
    },
    /// A generation referenced a Service absent from the complete snapshot.
    #[error("TrafficGeneration `{generation_id}` references missing Service `{service_id}`")]
    MissingGenerationService {
        generation_id: TrafficGenerationId,
        service_id: ServiceId,
    },
    /// A generation referenced a Deployment absent from the complete snapshot.
    #[error("TrafficGeneration `{generation_id}` references missing Deployment `{deployment_id}`")]
    MissingGenerationDeployment {
        generation_id: TrafficGenerationId,
        deployment_id: DeploymentId,
    },
    /// Internal indexed state disappeared while producing updates.
    #[error("indexed TrafficGeneration `{generation_id}` disappeared during planning")]
    MissingIndexedGeneration { generation_id: TrafficGenerationId },
    /// Persisted state requested an unsupported traffic transition.
    #[error("TrafficGeneration `{generation_id}` cannot transition from {from:?} to {to:?}")]
    InvalidTransition {
        generation_id: TrafficGenerationId,
        from: TrafficGenerationPhase,
        to: TrafficGenerationPhase,
    },
}
