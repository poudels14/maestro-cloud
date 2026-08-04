use std::collections::{BTreeMap, BTreeSet};

use http::HeaderName;
use kernel_api::{
    IngressRoute, IngressRouteId, IngressRouteSpec, Service, ServiceSpec, TrafficRoute,
    WildcardDnsName,
};

use crate::IngressPlanError;

pub(crate) fn capture_routes(
    service: &Service,
    routes: impl Iterator<Item = IngressRoute>,
) -> Result<Vec<TrafficRoute>, IngressPlanError> {
    let mut captured = Vec::new();
    let mut claims = BTreeMap::<(String, Option<String>), kernel_api::IngressRouteId>::new();
    for route in routes {
        validate_route_spec(&route.meta.id, &service.meta.id, &service.spec, &route.spec)?;
        let mut hosts = route.spec.hosts.clone();
        hosts.sort();
        for host in &hosts {
            let claim = (host.clone(), route.spec.path_prefix.clone());
            if let Some(existing) = claims.insert(claim, route.meta.id.clone()) {
                return Err(IngressPlanError::ConflictingRoute {
                    first_route_id: existing,
                    second_route_id: route.meta.id,
                    host: host.clone(),
                });
            }
        }
        captured.push(TrafficRoute {
            route_id: route.meta.id,
            route_generation: route.meta.generation,
            hosts,
            path_prefix: route.spec.path_prefix,
            target_port: route.spec.target_port,
            session_affinity: route.spec.session_affinity,
        });
    }
    captured.sort_by(|left, right| left.route_id.cmp(&right.route_id));
    Ok(captured)
}

/// Validates one desired route against its owning service before persistence.
pub fn validate_route_spec(
    route_id: &IngressRouteId,
    service_id: &kernel_api::ServiceId,
    service: &ServiceSpec,
    route: &IngressRouteSpec,
) -> Result<(), IngressPlanError> {
    if &route.service_id != service_id {
        return Err(IngressPlanError::MissingRouteService {
            route_id: route_id.clone(),
            service_id: route.service_id.clone(),
        });
    }
    if route.hosts.is_empty() {
        return Err(IngressPlanError::RouteHasNoHosts {
            route_id: route_id.clone(),
        });
    }
    if !service.exposed_ports.contains(&route.target_port) {
        return Err(IngressPlanError::UnexposedTargetPort {
            route_id: route_id.clone(),
            service_id: service_id.clone(),
            target_port: route.target_port,
        });
    }
    if let Some(path) = route.path_prefix.as_deref()
        && (!path.starts_with('/')
            || path.chars().any(|character| {
                matches!(character, '?' | '#' | '`' | '\\') || character.is_control()
            }))
    {
        return Err(IngressPlanError::InvalidPathPrefix {
            route_id: route_id.clone(),
            path: path.to_string(),
        });
    }
    if let Some(affinity) = route.session_affinity.as_ref()
        && !valid_header_name(&affinity.header)
    {
        return Err(IngressPlanError::InvalidAffinityHeader {
            route_id: route_id.clone(),
            header: affinity.header.clone(),
        });
    }
    let mut hosts = route.hosts.clone();
    hosts.sort();
    let original_count = hosts.len();
    hosts.dedup();
    if hosts.len() != original_count {
        return Err(IngressPlanError::DuplicateRouteHost {
            route_id: route_id.clone(),
        });
    }
    for host in hosts {
        if !valid_host(&host) {
            return Err(IngressPlanError::InvalidRouteHost {
                route_id: route_id.clone(),
                host,
            });
        }
    }
    Ok(())
}

fn valid_host(host: &str) -> bool {
    WildcardDnsName::parse(host).is_ok()
}

fn valid_header_name(header: &str) -> bool {
    HeaderName::from_bytes(header.as_bytes()).is_ok()
}

pub(crate) fn validate_route_ownership(
    services: &BTreeMap<kernel_api::ServiceId, Service>,
    routes: &[IngressRoute],
) -> Result<(), IngressPlanError> {
    let mut ids = BTreeSet::new();
    for route in routes {
        if !ids.insert(route.meta.id.clone()) {
            return Err(IngressPlanError::DuplicateResource {
                kind: "IngressRoute",
                resource_id: route.meta.id.to_string(),
            });
        }
        if !services.contains_key(&route.spec.service_id) {
            return Err(IngressPlanError::MissingRouteService {
                route_id: route.meta.id.clone(),
                service_id: route.spec.service_id.clone(),
            });
        }
    }
    Ok(())
}
