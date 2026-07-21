use std::collections::{BTreeMap, BTreeSet};

use kernel_api::{IngressRoute, Service, TrafficRoute};

use crate::IngressPlanError;

pub(crate) fn capture_routes(
    service: &Service,
    routes: impl Iterator<Item = IngressRoute>,
) -> Result<Vec<TrafficRoute>, IngressPlanError> {
    let mut captured = Vec::new();
    let mut claims = BTreeMap::<(String, Option<String>), kernel_api::IngressRouteId>::new();
    for route in routes {
        if route.spec.hosts.is_empty() {
            return Err(IngressPlanError::RouteHasNoHosts {
                route_id: route.meta.id,
            });
        }
        if !service.spec.exposed_ports.contains(&route.spec.target_port) {
            return Err(IngressPlanError::UnexposedTargetPort {
                route_id: route.meta.id,
                service_id: service.meta.id.clone(),
                target_port: route.spec.target_port,
            });
        }
        if let Some(path) = route.spec.path_prefix.as_deref()
            && (!path.starts_with('/')
                || path.chars().any(|character| {
                    matches!(character, '?' | '#' | '`' | '\\') || character.is_control()
                }))
        {
            return Err(IngressPlanError::InvalidPathPrefix {
                route_id: route.meta.id,
                path: path.to_string(),
            });
        }
        if let Some(affinity) = route.spec.session_affinity.as_ref()
            && !valid_header_name(&affinity.header)
        {
            return Err(IngressPlanError::InvalidAffinityHeader {
                route_id: route.meta.id,
                header: affinity.header.clone(),
            });
        }
        let mut hosts = route.spec.hosts.clone();
        hosts.sort();
        let original_count = hosts.len();
        hosts.dedup();
        if hosts.len() != original_count {
            return Err(IngressPlanError::DuplicateRouteHost {
                route_id: route.meta.id,
            });
        }
        for host in &hosts {
            if !valid_host(host) {
                return Err(IngressPlanError::InvalidRouteHost {
                    route_id: route.meta.id.clone(),
                    host: host.clone(),
                });
            }
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

fn valid_host(host: &str) -> bool {
    if host.is_empty()
        || host.len() > 253
        || host.ends_with('.')
        || host.bytes().any(|byte| byte.is_ascii_uppercase())
    {
        return false;
    }
    let host = host.strip_prefix("*.").unwrap_or(host);
    !host.is_empty()
        && host.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= 63
                && !label.starts_with('-')
                && !label.ends_with('-')
                && label
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        })
}

fn valid_header_name(header: &str) -> bool {
    const PUNCTUATION: &[u8] = b"!#$%&'*+-.^_`|~";
    !header.is_empty()
        && header
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || PUNCTUATION.contains(&byte))
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
