use axum::extract::rejection::JsonRejection;
use axum::extract::{DefaultBodyLimit, Extension, Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::post;
use axum::{Json, Router};
use kernel_api::{
    BuiltinKind, FirewallPolicy, FirewallPolicyId, FirewallPolicySpec, IngressRoute,
    IngressRouteId, IngressRouteSpec, Object, ResourceKind, ResourceName, ResourceRevision,
    Service, ServiceDiffChange, ServiceDiffStatus, ServiceId, ServiceRolloutDiffRequest,
    ServiceRolloutDiffResponse, ServiceRolloutRequest, ServiceRolloutResponse,
    ServiceRolloutRevisions, ServiceRolloutSpec,
};
use kernel_store::{Compare, ExpectedVersion, Keyspace, Mutation, StoredValue, Transaction};
use serde::Serialize;

use super::firewall_policies;
use super::service_commands::next_generation;
use super::service_rollout_validation::{
    ensure_managed_owner, managed_policy_id, managed_route_id, new_route, owner, validate_desired,
};
use super::write_plan::WritePlan;
use super::{service_diff, services};
use crate::mutation::{MAXIMUM_REQUEST_BYTES, MutationRequest};
use crate::system_resources::ensure_user_resource_id;
use crate::{ApiError, AppState, OperatorIdentity, mutation, resource};

pub(super) fn router() -> Router<AppState> {
    Router::new()
        .route("/api/services/{service_id}/rollout", post(apply))
        .route("/api/services/{service_id}/rollout/diff", post(diff))
        .layer(DefaultBodyLimit::max(MAXIMUM_REQUEST_BYTES))
}

async fn diff(
    State(state): State<AppState>,
    Path(service_id): Path<String>,
    payload: Result<Json<ServiceRolloutDiffRequest>, JsonRejection>,
) -> Result<Json<ServiceRolloutDiffResponse>, ApiError> {
    let service_id = parse_service_id(service_id)?;
    let desired = validate_desired(
        &state,
        &service_id,
        payload
            .map_err(|rejection| mutation::json_rejection(rejection, "service rollout diff"))?
            .0
            .desired,
    )
    .await?;
    let managed = ManagedResources::load(&state, &service_id).await?;
    let current = managed.decode()?;
    let mut changes = match &current.service {
        Some(service) => service_diff::changes(&service.spec, &desired.service)?,
        None => Vec::new(),
    };
    push_auxiliary_change(
        &mut changes,
        "ingress",
        current.ingress.as_ref().map(|route| &route.spec),
        desired.ingress.as_ref(),
    )?;
    push_auxiliary_change(
        &mut changes,
        "egress",
        current.egress.as_ref().map(|policy| &policy.spec),
        desired.egress.as_ref(),
    )?;
    let unchanged = current
        .service
        .as_ref()
        .is_some_and(|service| service.spec == desired.service)
        && current.ingress.as_ref().map(|route| &route.spec) == desired.ingress.as_ref()
        && current.egress.as_ref().map(|policy| &policy.spec) == desired.egress.as_ref();
    Ok(Json(ServiceRolloutDiffResponse {
        service_id,
        expected_revisions: managed.revisions(),
        status: if current.service.is_none() {
            ServiceDiffStatus::New
        } else if unchanged {
            ServiceDiffStatus::Unchanged
        } else {
            ServiceDiffStatus::Changed
        },
        changes,
    }))
}

async fn apply(
    State(state): State<AppState>,
    Path(service_id): Path<String>,
    Extension(operator): Extension<OperatorIdentity>,
    headers: HeaderMap,
    payload: Result<Json<ServiceRolloutRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<ServiceRolloutResponse>), ApiError> {
    let service_id = parse_service_id(service_id)?;
    ensure_user_resource_id("Service", service_id.as_str())?;
    let payload = payload
        .map_err(|rejection| mutation::json_rejection(rejection, "service rollout"))?
        .0;
    let desired = validate_desired(&state, &service_id, payload.desired).await?;
    let payload = ServiceRolloutRequest {
        expected_revisions: payload.expected_revisions,
        force: payload.force,
        desired,
    };
    let request = MutationRequest::new(
        &state,
        &headers,
        &operator,
        "POST /api/services/{serviceId}/rollout",
        &[service_id.as_str()],
        &payload,
    )?;
    if let Some(response) = request.replay(&state).await? {
        return Ok((StatusCode::ACCEPTED, Json(response)));
    }
    let frozen_rollout = if payload.force {
        FrozenRolloutPolicy::BypassNextGeneration
    } else {
        FrozenRolloutPolicy::HonorFreeze
    };
    let managed = ManagedResources::load(&state, &service_id).await?;
    let (compares, mutations, response) = managed.plan(
        service_id,
        payload.expected_revisions,
        frozen_rollout,
        payload.desired,
    )?;
    let response = request
        .commit(
            &state,
            response,
            Transaction {
                compares,
                mutations,
            },
            "revisionConflict",
            "a managed service resource changed after the rollout preview",
        )
        .await?;
    Ok((StatusCode::ACCEPTED, Json(response)))
}

struct ManagedResources {
    keys: Keyspace,
    service_id: ServiceId,
    route_id: IngressRouteId,
    policy_id: FirewallPolicyId,
    service: Option<StoredValue>,
    ingress: Option<StoredValue>,
    egress: Option<StoredValue>,
}

struct DecodedResources {
    service: Option<Service>,
    ingress: Option<IngressRoute>,
    egress: Option<FirewallPolicy>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FrozenRolloutPolicy {
    HonorFreeze,
    BypassNextGeneration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WriteDecision {
    Skip,
    Apply,
}

impl ManagedResources {
    async fn load(state: &AppState, service_id: &ServiceId) -> Result<Self, ApiError> {
        let keys = Keyspace::new(&state.cluster_id);
        let route_id = managed_route_id(service_id)?;
        let policy_id = managed_policy_id(service_id)?;
        let service_key = resource_key(&keys, BuiltinKind::Service, service_id.clone().into())?;
        let route_key = resource_key(&keys, BuiltinKind::IngressRoute, route_id.clone().into())?;
        let policy_key =
            resource_key(&keys, BuiltinKind::FirewallPolicy, policy_id.clone().into())?;
        let service = read(state, &service_key, "Service").await?;
        let ingress = read(state, &route_key, "IngressRoute").await?;
        let egress = read(state, &policy_key, "FirewallPolicy").await?;
        let managed = Self {
            keys,
            service_id: service_id.clone(),
            route_id,
            policy_id,
            service,
            ingress,
            egress,
        };
        managed.verify_ownership()?;
        Ok(managed)
    }

    fn revisions(&self) -> ServiceRolloutRevisions {
        ServiceRolloutRevisions {
            service: revision(&self.service),
            ingress: revision(&self.ingress),
            egress: revision(&self.egress),
        }
    }

    fn decode(&self) -> Result<DecodedResources, ApiError> {
        Ok(DecodedResources {
            service: decode_optional(self.service.as_ref(), &self.keys, BuiltinKind::Service)?,
            ingress: decode_optional(self.ingress.as_ref(), &self.keys, BuiltinKind::IngressRoute)?,
            egress: decode_optional(
                self.egress.as_ref(),
                &self.keys,
                BuiltinKind::FirewallPolicy,
            )?,
        })
    }

    fn verify_ownership(&self) -> Result<(), ApiError> {
        let expected = owner(&self.service_id)?;
        let ingress: Option<IngressRoute> =
            decode_optional(self.ingress.as_ref(), &self.keys, BuiltinKind::IngressRoute)?;
        let egress: Option<FirewallPolicy> = decode_optional(
            self.egress.as_ref(),
            &self.keys,
            BuiltinKind::FirewallPolicy,
        )?;
        if let Some(route) = ingress {
            ensure_managed_owner(&route.meta.owner_refs, &expected, "IngressRoute")?;
        }
        if let Some(policy) = egress {
            ensure_managed_owner(&policy.meta.owner_refs, &expected, "FirewallPolicy")?;
        }
        Ok(())
    }

    fn plan(
        self,
        service_id: ServiceId,
        expected: ServiceRolloutRevisions,
        frozen_rollout: FrozenRolloutPolicy,
        desired: ServiceRolloutSpec,
    ) -> Result<(Vec<Compare>, Vec<Mutation>, ServiceRolloutResponse), ApiError> {
        let service_kind = kind(BuiltinKind::Service)?;
        let service_key = self
            .keys
            .resource(&service_kind, &self.service_id.clone().into());
        let route_kind = kind(BuiltinKind::IngressRoute)?;
        let route_key = self
            .keys
            .resource(&route_kind, &self.route_id.clone().into());
        let policy_kind = kind(BuiltinKind::FirewallPolicy)?;
        let policy_key = self
            .keys
            .resource(&policy_kind, &self.policy_id.clone().into());
        let service_expected = exact_expected(&self.service, expected.service, "Service")?;
        let route_expected = exact_expected(&self.ingress, expected.ingress, "IngressRoute")?;
        let policy_expected = exact_expected(&self.egress, expected.egress, "FirewallPolicy")?;
        let service_plan = services::plan_service_write(
            self.service.as_ref(),
            &self.keys,
            &service_kind,
            service_id.clone(),
            kernel_api::ServiceWriteRequest {
                expected_revision: expected.service,
                spec: desired.service,
            },
        )?;
        let (mut service, service_write) = match service_plan {
            WritePlan::Retain {
                resource,
                expected: _,
            } => (resource, WriteDecision::Skip),
            WritePlan::Put {
                resource,
                expected: _,
            } => (resource, WriteDecision::Apply),
        };
        if service_write == WriteDecision::Apply {
            service.status.rollout_bypass_generation = (frozen_rollout
                == FrozenRolloutPolicy::BypassNextGeneration
                && service.status.rollout == kernel_api::RolloutState::Frozen)
                .then_some(service.meta.generation);
        }
        let (route, route_write) = plan_route(
            self.ingress.as_ref(),
            &self.keys,
            &route_kind,
            self.route_id,
            &service_id,
            desired.ingress,
        )?;
        let (policy, policy_write) = plan_policy(
            self.egress.as_ref(),
            &self.keys,
            &policy_kind,
            self.policy_id,
            &service_id,
            desired.egress,
        )?;
        let mut mutations = Vec::new();
        if service_write == WriteDecision::Apply {
            mutations.push(put(service_key.clone(), &service, "Service")?);
        }
        push_optional_mutation(
            &mut mutations,
            route_key.clone(),
            route.as_ref(),
            route_write,
        )?;
        push_optional_mutation(
            &mut mutations,
            policy_key.clone(),
            policy.as_ref(),
            policy_write,
        )?;
        let response = ServiceRolloutResponse {
            service_id,
            service_generation: service.meta.generation,
            ingress_generation: route.map(|route| route.meta.generation),
            egress_generation: policy.map(|policy| policy.meta.generation),
        };
        Ok((
            vec![
                Compare {
                    key: service_key,
                    expected: service_expected,
                },
                Compare {
                    key: route_key,
                    expected: route_expected,
                },
                Compare {
                    key: policy_key,
                    expected: policy_expected,
                },
            ],
            mutations,
            response,
        ))
    }
}

fn plan_route(
    current: Option<&StoredValue>,
    keys: &Keyspace,
    kind: &ResourceKind,
    route_id: IngressRouteId,
    service_id: &ServiceId,
    desired: Option<IngressRouteSpec>,
) -> Result<(Option<IngressRoute>, WriteDecision), ApiError> {
    let Some(spec) = desired else {
        return Ok((
            None,
            if current.is_some() {
                WriteDecision::Apply
            } else {
                WriteDecision::Skip
            },
        ));
    };
    let route = match current {
        None => new_route(route_id, service_id, spec)?,
        Some(stored) => {
            let mut route: IngressRoute =
                resource::decode(stored, keys, kind, BuiltinKind::IngressRoute)?;
            if route.meta.deletion_timestamp.is_some() {
                return Err(ApiError::conflict(
                    "deletionInProgress",
                    "managed IngressRoute deletion is in progress",
                ));
            }
            if route.spec == spec {
                return Ok((Some(route), WriteDecision::Skip));
            }
            route.meta.generation = next_generation(route.meta.generation, "IngressRoute")?;
            route.spec = spec;
            route
        }
    };
    Ok((Some(route), WriteDecision::Apply))
}

fn plan_policy(
    current: Option<&StoredValue>,
    keys: &Keyspace,
    kind: &ResourceKind,
    policy_id: FirewallPolicyId,
    service_id: &ServiceId,
    desired: Option<FirewallPolicySpec>,
) -> Result<(Option<FirewallPolicy>, WriteDecision), ApiError> {
    let Some(spec) = desired else {
        return Ok((
            None,
            if current.is_some() {
                WriteDecision::Apply
            } else {
                WriteDecision::Skip
            },
        ));
    };
    let policy = match current {
        None => {
            let mut policy = firewall_policies::new_policy(policy_id, spec);
            policy.meta.owner_refs = vec![owner(service_id)?];
            policy
        }
        Some(stored) => {
            let mut policy: FirewallPolicy =
                resource::decode(stored, keys, kind, BuiltinKind::FirewallPolicy)?;
            if policy.meta.deletion_timestamp.is_some() {
                return Err(ApiError::conflict(
                    "deletionInProgress",
                    "managed FirewallPolicy deletion is in progress",
                ));
            }
            if policy.spec == spec {
                return Ok((Some(policy), WriteDecision::Skip));
            }
            policy.meta.generation = next_generation(policy.meta.generation, "FirewallPolicy")?;
            policy.spec = spec;
            policy
        }
    };
    Ok((Some(policy), WriteDecision::Apply))
}

fn exact_expected(
    current: &Option<StoredValue>,
    expected: Option<ResourceRevision>,
    kind: &str,
) -> Result<ExpectedVersion, ApiError> {
    match (current, expected) {
        (None, None) => Ok(ExpectedVersion::Missing),
        (Some(stored), Some(expected)) if stored.version.resource_revision() == expected => {
            Ok(ExpectedVersion::Exact(stored.version))
        }
        _ => Err(ApiError::conflict(
            "revisionConflict",
            format!("{kind} is not at the rollout preview revision"),
        )),
    }
}

fn push_optional_mutation<Resource: Serialize>(
    mutations: &mut Vec<Mutation>,
    key: kernel_store::StoreKey,
    resource: Option<&Resource>,
    decision: WriteDecision,
) -> Result<(), ApiError> {
    if decision == WriteDecision::Skip {
        return Ok(());
    }
    match resource {
        Some(resource) => mutations.push(put(key, resource, "managed resource")?),
        None => mutations.push(Mutation::Delete { key }),
    }
    Ok(())
}

fn put<Resource: Serialize>(
    key: kernel_store::StoreKey,
    resource: &Resource,
    kind: &str,
) -> Result<Mutation, ApiError> {
    Ok(Mutation::Put {
        key,
        value: serde_json::to_vec(resource)
            .map_err(|error| ApiError::internal(format!("failed to encode {kind}: {error}")))?,
        session: None,
    })
}

fn push_auxiliary_change<Value: Serialize + PartialEq>(
    changes: &mut Vec<ServiceDiffChange>,
    field: &str,
    current: Option<&Value>,
    desired: Option<&Value>,
) -> Result<(), ApiError> {
    if current == desired {
        return Ok(());
    }
    changes.push(ServiceDiffChange {
        field: field.to_string(),
        from: display(current)?,
        to: display(desired)?,
    });
    Ok(())
}

fn display<Value: Serialize>(value: Option<&Value>) -> Result<Option<String>, ApiError> {
    value
        .map(|value| {
            serde_json::to_string(value).map_err(|error| {
                ApiError::internal(format!("failed to encode rollout diff: {error}"))
            })
        })
        .transpose()
}

fn decode_optional<Id, Spec, Status>(
    stored: Option<&StoredValue>,
    keys: &Keyspace,
    builtin: BuiltinKind,
) -> Result<Option<Object<Id, Spec, Status>>, ApiError>
where
    Id: Clone + std::fmt::Display + Into<ResourceName> + serde::de::DeserializeOwned,
    Spec: serde::de::DeserializeOwned,
    Status: serde::de::DeserializeOwned,
{
    let kind = kind(builtin)?;
    stored
        .map(|stored| resource::decode(stored, keys, &kind, builtin))
        .transpose()
}

fn revision(stored: &Option<StoredValue>) -> Option<ResourceRevision> {
    stored
        .as_ref()
        .map(|stored| stored.version.resource_revision())
}

async fn read(
    state: &AppState,
    key: &kernel_store::StoreKey,
    kind: &str,
) -> Result<Option<StoredValue>, ApiError> {
    state
        .store
        .get(key)
        .await
        .map_err(|error| ApiError::internal(format!("failed to read {kind}: {error}")))
}

fn resource_key(
    keys: &Keyspace,
    builtin: BuiltinKind,
    name: ResourceName,
) -> Result<kernel_store::StoreKey, ApiError> {
    Ok(keys.resource(&kind(builtin)?, &name))
}

fn kind(builtin: BuiltinKind) -> Result<ResourceKind, ApiError> {
    ResourceKind::new(builtin.as_str()).map_err(|error| ApiError::internal(error.to_string()))
}

fn parse_service_id(value: String) -> Result<ServiceId, ApiError> {
    ServiceId::new(value).map_err(|error| ApiError::bad_request(error.to_string()))
}
