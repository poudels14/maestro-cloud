use std::collections::{BTreeMap, BTreeSet};

use kernel_api::{
    Deployment, DeploymentId, Generation, IngressBlocklist, IngressRoute, IngressRouteId,
    IngressRouteStatus, Service, ServiceId, TrafficGeneration, TrafficGenerationId,
    TrafficGenerationPhase, TrafficGenerationStatus,
};
use sha2::{Digest, Sha256};

use crate::lifecycle::{collect_expired, converge_desired, published, retire_all, select_active};
use crate::target::desired_spec;
use crate::validation::{capture_routes, validate_route_ownership};
use crate::{
    BackendChange, IngressBlocklistChange, IngressInput, IngressPlan, PublishedTraffic,
    ResourceStatusUpdate,
};

const BLOCKLIST_ID: &str = "global";
const BLOCKLIST_DIGEST_DOMAIN: &[u8] = b"maestro-ingress-blocklist-v1\0";

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
    let blocklists = index(
        input.blocklists,
        |resource| resource.meta.id.clone(),
        "IngressBlocklist",
    )?;
    validate_blocklists(&blocklists)?;
    validate_route_ownership(&services, &input.routes)?;
    validate_generation_ownership(&services, &deployments, &generations)?;

    let mut output = IngressPlan::default();
    if let Some(blocklist) = blocklists.values().next() {
        plan_blocklist(blocklist, &mut output);
    }
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

        let active_deployment_missing = service
            .status
            .active_deployment_id
            .as_ref()
            .is_some_and(|deployment_id| !deployments.contains_key(deployment_id));
        let active = if service.meta.deletion_timestamp.is_some()
            || service.status.active_deployment_id.is_none()
            || active_deployment_missing
        {
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

    output.requeue_at =
        next_retirement_deadline(&desired_statuses, now, input.settings.retirement_grace);
    append_status_updates(&generations, desired_statuses, &mut output)?;
    sort_plan(&mut output);
    Ok(output)
}

fn next_retirement_deadline(
    statuses: &BTreeMap<TrafficGenerationId, TrafficGenerationStatus>,
    now: kernel_api::Timestamp,
    grace: std::time::Duration,
) -> Option<kernel_api::Timestamp> {
    let grace = i64::try_from(grace.as_millis()).unwrap_or(i64::MAX);
    statuses
        .values()
        .filter(|status| status.phase == TrafficGenerationPhase::Retired)
        .filter_map(|status| status.retired_at)
        .map(|retired| kernel_api::Timestamp(retired.0.saturating_add(grace)))
        .filter(|deadline| *deadline > now)
        .min()
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
        if let Some(deployment) = deployments.get(&generation.spec.deployment_id)
            && deployment.spec.service_id != generation.spec.service_id
        {
            return Err(IngressPlanError::GenerationDeploymentOwnershipMismatch {
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
        .blocklist_updates
        .sort_by(|left, right| left.id.cmp(&right.id));
    output
        .backend_changes
        .sort_by(|left, right| left.service_id.cmp(&right.service_id));
}

fn validate_blocklists(
    blocklists: &BTreeMap<kernel_api::IngressBlocklistId, IngressBlocklist>,
) -> Result<(), IngressPlanError> {
    if blocklists.len() > 1 {
        return Err(IngressPlanError::MultipleIngressBlocklists);
    }
    let Some(blocklist) = blocklists.values().next() else {
        return Ok(());
    };
    if blocklist.meta.id.as_str() != BLOCKLIST_ID {
        return Err(IngressPlanError::UnexpectedBlocklistId {
            blocklist_id: blocklist.meta.id.clone(),
        });
    }
    let normalized = blocklist
        .spec
        .addresses
        .iter()
        .copied()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    if normalized != blocklist.spec.addresses {
        return Err(IngressPlanError::NonCanonicalBlocklist);
    }
    Ok(())
}

fn plan_blocklist(blocklist: &IngressBlocklist, output: &mut IngressPlan) {
    let addresses = if blocklist.meta.deletion_timestamp.is_some() {
        Vec::new()
    } else {
        blocklist.spec.addresses.clone()
    };
    let change = blocklist_change(blocklist.meta.generation, addresses);
    if blocklist.status.applied_generation != blocklist.meta.generation
        || blocklist.status.configuration_digest.as_deref()
            != Some(change.configuration_digest.as_str())
    {
        let mut status = blocklist.status.clone();
        status.applied_generation = blocklist.meta.generation;
        status.configuration_digest = Some(change.configuration_digest.clone());
        output.blocklist_change = Some(change);
        output.blocklist_updates.push(ResourceStatusUpdate {
            id: blocklist.meta.id.clone(),
            observed_revision: blocklist.meta.revision,
            status,
        });
    }
}

pub(crate) fn blocklist_change(
    generation: Generation,
    addresses: Vec<std::net::IpAddr>,
) -> IngressBlocklistChange {
    let mut digest = Sha256::new();
    digest.update(BLOCKLIST_DIGEST_DOMAIN);
    for address in &addresses {
        digest.update(address.to_string().as_bytes());
        digest.update([0]);
    }
    IngressBlocklistChange {
        generation,
        addresses,
        configuration_digest: format!("{:x}", digest.finalize()),
    }
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
    /// More than one singleton ingress blocklist was stored.
    #[error("more than one IngressBlocklist resource exists")]
    MultipleIngressBlocklists,
    /// The singleton blocklist used an identity other than `global`.
    #[error("IngressBlocklist `{blocklist_id}` must use the singleton identity `global`")]
    UnexpectedBlocklistId {
        /// Unexpected stored identity.
        blocklist_id: kernel_api::IngressBlocklistId,
    },
    /// Stored blocklist addresses were duplicated or not deterministically ordered.
    #[error("IngressBlocklist addresses must be unique and in canonical network order")]
    NonCanonicalBlocklist,
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
    /// A generation referenced a Deployment owned by another Service.
    #[error("TrafficGeneration `{generation_id}` references foreign Deployment `{deployment_id}`")]
    GenerationDeploymentOwnershipMismatch {
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
