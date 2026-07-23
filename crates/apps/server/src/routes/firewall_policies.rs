use std::collections::{BTreeMap, BTreeSet};

use axum::extract::rejection::JsonRejection;
use axum::extract::{DefaultBodyLimit, Extension, Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::{delete, put};
use axum::{Json, Router};
use ipnet::IpNet;
use kernel_api::{
    BuiltinKind, FirewallDirection, FirewallPolicy, FirewallPolicyId, FirewallPolicySpec,
    FirewallPolicyStatus, FirewallSubject, Generation, Node, Object, ObjectMeta, ResourceKind,
    ResourceRevision, Service, Timestamp,
};
use kernel_store::{Compare, ExpectedVersion, Keyspace, Mutation, Transaction};
use serde::{Deserialize, Serialize};

use super::service_commands::next_generation;
use crate::mutation::{MAXIMUM_REQUEST_BYTES, MutationRequest};
use crate::system_resources::ensure_user_resource_id;
use crate::{ApiError, AppState, OperatorIdentity, mutation, resource};

pub(super) fn router() -> Router<AppState> {
    Router::new()
        .route(
            "/api/firewall/policies/{policy_id}",
            put(write).merge(delete(delete_policy)),
        )
        .layer(DefaultBodyLimit::max(MAXIMUM_REQUEST_BYTES))
}

async fn write(
    State(state): State<AppState>,
    Path(policy_id): Path<String>,
    Extension(operator): Extension<OperatorIdentity>,
    headers: HeaderMap,
    payload: Result<Json<FirewallPolicyWriteRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<FirewallPolicyCommandResponse>), ApiError> {
    let policy_id = parse_id(policy_id)?;
    ensure_user_resource_id("FirewallPolicy", policy_id.as_str())?;
    let mut payload = payload
        .map_err(|rejection| mutation::json_rejection(rejection, "firewall policy"))?
        .0;
    normalize_spec(&mut payload.spec)?;
    let request = MutationRequest::new(
        &state,
        &headers,
        &operator,
        "PUT /api/firewall/policies/{policyId}",
        &[policy_id.as_str()],
        &payload,
    )?;
    if let Some(response) = request.replay(&state).await? {
        return Ok((StatusCode::ACCEPTED, Json(response)));
    }
    validate_policy_state(&state, &policy_id, &payload.spec).await?;
    let keys = Keyspace::new(&state.cluster_id);
    let kind = policy_kind()?;
    let key = keys.resource(&kind, &policy_id.clone().into());
    let current =
        state.store.get(&key).await.map_err(|error| {
            ApiError::internal(format!("failed to read FirewallPolicy: {error}"))
        })?;
    let (policy, expected, write) = plan_write(current.as_ref(), &keys, &kind, policy_id, payload)?;
    let response = FirewallPolicyCommandResponse::from(&policy);
    let mutations = if write {
        vec![Mutation::Put {
            key: key.clone(),
            value: encode(&policy)?,
            session: None,
        }]
    } else {
        Vec::new()
    };
    let response = request
        .commit(
            &state,
            response,
            Transaction {
                compares: vec![Compare { key, expected }],
                mutations,
            },
            "revisionConflict",
            "FirewallPolicy changed after its expected revision was read",
        )
        .await?;
    Ok((StatusCode::ACCEPTED, Json(response)))
}

async fn delete_policy(
    State(state): State<AppState>,
    Path(policy_id): Path<String>,
    Extension(operator): Extension<OperatorIdentity>,
    headers: HeaderMap,
    payload: Result<Json<FirewallPolicyDeleteRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<FirewallPolicyCommandResponse>), ApiError> {
    let policy_id = parse_id(policy_id)?;
    ensure_user_resource_id("FirewallPolicy", policy_id.as_str())?;
    let payload = payload
        .map_err(|rejection| mutation::json_rejection(rejection, "firewall policy command"))?
        .0;
    let request = MutationRequest::new(
        &state,
        &headers,
        &operator,
        "DELETE /api/firewall/policies/{policyId}",
        &[policy_id.as_str()],
        &payload,
    )?;
    if let Some(response) = request.replay(&state).await? {
        return Ok((StatusCode::ACCEPTED, Json(response)));
    }
    let keys = Keyspace::new(&state.cluster_id);
    let kind = policy_kind()?;
    let key = keys.resource(&kind, &policy_id.clone().into());
    let stored = state
        .store
        .get(&key)
        .await
        .map_err(|error| ApiError::internal(format!("failed to read FirewallPolicy: {error}")))?
        .ok_or_else(|| {
            ApiError::not_found(format!("FirewallPolicy `{policy_id}` does not exist"))
        })?;
    let mut policy: FirewallPolicy =
        resource::decode(&stored, &keys, &kind, BuiltinKind::FirewallPolicy)?;
    if policy.meta.revision != payload.expected_revision {
        return Err(ApiError::conflict(
            "revisionConflict",
            "FirewallPolicy is not at the expected revision",
        ));
    }
    let write = policy.meta.deletion_timestamp.is_none();
    if write {
        policy.meta.deletion_timestamp = Some(state.timestamp_clock.now());
    }
    let response = FirewallPolicyCommandResponse::from(&policy);
    let mutations = if write {
        vec![Mutation::Put {
            key: key.clone(),
            value: encode(&policy)?,
            session: None,
        }]
    } else {
        Vec::new()
    };
    let response = request
        .commit(
            &state,
            response,
            Transaction {
                compares: vec![Compare {
                    key,
                    expected: ExpectedVersion::Exact(stored.version),
                }],
                mutations,
            },
            "revisionConflict",
            "FirewallPolicy changed after its expected revision was read",
        )
        .await?;
    Ok((StatusCode::ACCEPTED, Json(response)))
}

fn plan_write(
    current: Option<&kernel_store::StoredValue>,
    keys: &Keyspace,
    kind: &ResourceKind,
    policy_id: FirewallPolicyId,
    payload: FirewallPolicyWriteRequest,
) -> Result<(FirewallPolicy, ExpectedVersion, bool), ApiError> {
    match (current, payload.expected_revision) {
        (None, None) => Ok((
            new_policy(policy_id, payload.spec),
            ExpectedVersion::Missing,
            true,
        )),
        (None, Some(_)) => Err(ApiError::conflict(
            "revisionConflict",
            "FirewallPolicy does not exist at the expected revision",
        )),
        (Some(stored), expected_revision) => {
            let mut current: FirewallPolicy =
                resource::decode(stored, keys, kind, BuiltinKind::FirewallPolicy)?;
            if current.meta.deletion_timestamp.is_some() {
                return Err(ApiError::conflict(
                    "deletionInProgress",
                    "FirewallPolicy deletion is already in progress",
                ));
            }
            if expected_revision != Some(current.meta.revision) {
                return Err(ApiError::conflict(
                    "revisionConflict",
                    "FirewallPolicy is not at the expected revision",
                ));
            }
            if current.spec == payload.spec {
                Ok((current, ExpectedVersion::Exact(stored.version), false))
            } else {
                current.meta.generation =
                    next_generation(current.meta.generation, "FirewallPolicy")?;
                current.spec = payload.spec;
                Ok((current, ExpectedVersion::Exact(stored.version), true))
            }
        }
    }
}

pub(super) fn normalize_spec(spec: &mut FirewallPolicySpec) -> Result<(), ApiError> {
    match (&spec.direction, &spec.subject) {
        (FirewallDirection::Egress, FirewallSubject::Global)
        | (FirewallDirection::HostInput, FirewallSubject::Global)
        | (FirewallDirection::Egress, FirewallSubject::Service(_))
        | (FirewallDirection::HostInput, FirewallSubject::Node(_)) => {}
        _ => {
            return Err(ApiError::bad_request(
                "egress policies support global or service subjects; hostInput policies support global or node subjects",
            ));
        }
    }
    for (rule_index, rule) in spec.rules.iter_mut().enumerate() {
        let network = rule.cidr.parse::<IpNet>().map_err(|error| {
            ApiError::bad_request(format!(
                "rules[{rule_index}].cidr `{}` is invalid: {error}",
                rule.cidr
            ))
        })?;
        if network.addr() != network.network() {
            return Err(ApiError::bad_request(format!(
                "rules[{rule_index}].cidr is not canonical; use {}",
                network.trunc()
            )));
        }
        for range in &rule.ports {
            if range.start == 0 || range.end == 0 || range.start > range.end {
                return Err(ApiError::bad_request(format!(
                    "rules[{rule_index}] has invalid port range {}-{}",
                    range.start, range.end
                )));
            }
        }
        rule.ports.sort_by_key(|range| (range.start, range.end));
        rule.ports.dedup();
    }
    Ok(())
}

async fn validate_policy_state(
    state: &AppState,
    policy_id: &FirewallPolicyId,
    spec: &FirewallPolicySpec,
) -> Result<(), ApiError> {
    match &spec.subject {
        FirewallSubject::Service(service_id) => {
            let _: Service = resource::get(state, BuiltinKind::Service, service_id.clone()).await?;
        }
        FirewallSubject::Node(node_id) => {
            let _: Node = resource::get(state, BuiltinKind::Node, node_id.clone()).await?;
        }
        FirewallSubject::Global => {}
    }
    let policies: Vec<FirewallPolicy> = resource::list(state, BuiltinKind::FirewallPolicy).await?;
    if policies.into_iter().any(|policy| {
        policy.meta.id != *policy_id
            && policy.meta.deletion_timestamp.is_none()
            && policy.spec.direction == spec.direction
            && policy.spec.subject == spec.subject
    }) {
        return Err(ApiError::conflict(
            "policyScopeConflict",
            "another FirewallPolicy already owns this direction and subject",
        ));
    }
    Ok(())
}

pub(super) fn new_policy(policy_id: FirewallPolicyId, spec: FirewallPolicySpec) -> FirewallPolicy {
    Object {
        meta: ObjectMeta {
            id: policy_id,
            labels: BTreeMap::new(),
            annotations: BTreeMap::new(),
            revision: ResourceRevision::default(),
            generation: Generation(1),
            owner_refs: Vec::new(),
            finalizers: BTreeSet::new(),
            deletion_timestamp: None,
        },
        spec,
        status: FirewallPolicyStatus {
            applied_generation: Generation::default(),
            ruleset_digest: None,
            conditions: Vec::new(),
        },
    }
}

fn encode(policy: &FirewallPolicy) -> Result<Vec<u8>, ApiError> {
    serde_json::to_vec(policy)
        .map_err(|error| ApiError::internal(format!("failed to encode FirewallPolicy: {error}")))
}

fn parse_id(value: String) -> Result<FirewallPolicyId, ApiError> {
    FirewallPolicyId::new(value).map_err(|error| ApiError::bad_request(error.to_string()))
}

fn policy_kind() -> Result<ResourceKind, ApiError> {
    ResourceKind::new(BuiltinKind::FirewallPolicy.as_str())
        .map_err(|error| ApiError::internal(error.to_string()))
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct FirewallPolicyWriteRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    expected_revision: Option<ResourceRevision>,
    spec: FirewallPolicySpec,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct FirewallPolicyDeleteRequest {
    expected_revision: ResourceRevision,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct FirewallPolicyCommandResponse {
    policy_id: FirewallPolicyId,
    generation: Generation,
    #[serde(skip_serializing_if = "Option::is_none")]
    deletion_timestamp: Option<Timestamp>,
}

impl From<&FirewallPolicy> for FirewallPolicyCommandResponse {
    fn from(policy: &FirewallPolicy) -> Self {
        Self {
            policy_id: policy.meta.id.clone(),
            generation: policy.meta.generation,
            deletion_timestamp: policy.meta.deletion_timestamp,
        }
    }
}
