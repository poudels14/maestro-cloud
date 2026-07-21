use kernel_api::{
    BuiltinKind, FirewallDirection, FirewallPolicy, FirewallPolicyId, FirewallSubject, Generation,
    IngressRoute, IngressRouteId, IngressRouteSpec, IngressRouteStatus, Object, ObjectMeta,
    OwnerReference, Ownership, ResourceId, ResourceKind, ResourceName, ResourceRevision, ServiceId,
    ServiceRolloutSpec,
};
use sha2::{Digest, Sha256};

use super::firewall_policies;
use crate::{ApiError, AppState, resource};

pub(super) async fn validate_desired(
    state: &AppState,
    service_id: &ServiceId,
    mut desired: ServiceRolloutSpec,
) -> Result<ServiceRolloutSpec, ApiError> {
    desired
        .service
        .validate()
        .map_err(|error| ApiError::bad_request(error.to_string()))?;
    if let Some(route) = &mut desired.ingress {
        route.hosts.sort();
        ingress::validate_route_spec(
            &managed_route_id(service_id)?,
            service_id,
            &desired.service,
            route,
        )
        .map_err(|error| ApiError::bad_request(error.to_string()))?;
        validate_route_claims(state, service_id, route).await?;
    }
    if let Some(policy) = &mut desired.egress {
        if policy.direction != FirewallDirection::Egress
            || policy.subject != FirewallSubject::Service(service_id.clone())
        {
            return Err(ApiError::bad_request(
                "managed egress policy must target the rollout service",
            ));
        }
        firewall_policies::normalize_spec(policy)?;
        validate_policy_claims(state, service_id).await?;
    }
    Ok(desired)
}

async fn validate_route_claims(
    state: &AppState,
    service_id: &ServiceId,
    desired: &IngressRouteSpec,
) -> Result<(), ApiError> {
    let managed_id = managed_route_id(service_id)?;
    let routes: Vec<IngressRoute> = resource::list(state, BuiltinKind::IngressRoute).await?;
    if routes.into_iter().any(|route| {
        route.meta.id != managed_id
            && route.meta.deletion_timestamp.is_none()
            && route.spec.path_prefix == desired.path_prefix
            && route
                .spec
                .hosts
                .iter()
                .any(|host| desired.hosts.contains(host))
    }) {
        return Err(ApiError::conflict(
            "routeClaimConflict",
            "another IngressRoute already claims a desired host and path",
        ));
    }
    Ok(())
}

async fn validate_policy_claims(state: &AppState, service_id: &ServiceId) -> Result<(), ApiError> {
    let managed_id = managed_policy_id(service_id)?;
    let policies: Vec<FirewallPolicy> = resource::list(state, BuiltinKind::FirewallPolicy).await?;
    if policies.into_iter().any(|policy| {
        policy.meta.id != managed_id
            && policy.meta.deletion_timestamp.is_none()
            && policy.spec.direction == FirewallDirection::Egress
            && policy.spec.subject == FirewallSubject::Service(service_id.clone())
    }) {
        return Err(ApiError::conflict(
            "policyScopeConflict",
            "another FirewallPolicy already owns this service egress scope",
        ));
    }
    Ok(())
}

pub(super) fn owner(service_id: &ServiceId) -> Result<OwnerReference, ApiError> {
    Ok(OwnerReference {
        resource: ResourceId::new(
            kind(BuiltinKind::Service)?,
            ResourceName::from(service_id.clone()),
        ),
        ownership: Ownership::Controller,
    })
}

pub(super) fn new_route(
    route_id: IngressRouteId,
    service_id: &ServiceId,
    spec: IngressRouteSpec,
) -> Result<IngressRoute, ApiError> {
    Ok(Object {
        meta: ObjectMeta {
            id: route_id,
            labels: Default::default(),
            annotations: Default::default(),
            revision: ResourceRevision::default(),
            generation: Generation(1),
            owner_refs: vec![owner(service_id)?],
            finalizers: Default::default(),
            deletion_timestamp: None,
        },
        spec,
        status: IngressRouteStatus {
            applied_generation: Generation::default(),
            conditions: Vec::new(),
        },
    })
}

pub(super) fn ensure_managed_owner(
    owners: &[OwnerReference],
    expected: &OwnerReference,
    kind: &str,
) -> Result<(), ApiError> {
    if owners.contains(expected) {
        return Ok(());
    }
    Err(ApiError::conflict(
        "managedResourceCollision",
        format!("reserved managed {kind} identity is owned by another resource"),
    ))
}

pub(super) fn managed_route_id(service_id: &ServiceId) -> Result<IngressRouteId, ApiError> {
    let id = managed_id(service_id, "ingress");
    IngressRouteId::new(id).map_err(|error| ApiError::internal(error.to_string()))
}

pub(super) fn managed_policy_id(service_id: &ServiceId) -> Result<FirewallPolicyId, ApiError> {
    let id = managed_id(service_id, "egress");
    FirewallPolicyId::new(id).map_err(|error| ApiError::internal(error.to_string()))
}

fn managed_id(service_id: &ServiceId, suffix: &str) -> String {
    let candidate = format!("{service_id}-{suffix}");
    if candidate.len() <= 253 {
        candidate
    } else {
        let digest = format!("{:x}", Sha256::digest(service_id.as_str().as_bytes()));
        format!(
            "managed-{suffix}-{}",
            digest.chars().take(32).collect::<String>()
        )
    }
}

fn kind(builtin: BuiltinKind) -> Result<ResourceKind, ApiError> {
    ResourceKind::new(builtin.as_str()).map_err(|error| ApiError::internal(error.to_string()))
}
