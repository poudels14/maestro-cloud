use axum::extract::rejection::JsonRejection;
use axum::extract::{DefaultBodyLimit, Path, State};
use axum::routing::post;
use axum::{Json, Router};
use firewall::{FirewallInput, FirewallRuleset};
use kernel_api::{BuiltinKind, FirewallPolicy, FirewallPolicyId, FirewallPolicySpec, ResourceKind};
use kernel_store::Keyspace;
use serde::{Deserialize, Serialize};

use super::firewall_policies::normalize_spec;
use crate::mutation::MAXIMUM_REQUEST_BYTES;
use crate::{ApiError, AppState, mutation, resource};

pub(super) fn router() -> Router<AppState> {
    Router::new()
        .route("/api/firewall/policies/{policy_id}/dry-run", post(dry_run))
        .layer(DefaultBodyLimit::max(MAXIMUM_REQUEST_BYTES))
}

async fn dry_run(
    State(state): State<AppState>,
    Path(policy_id): Path<String>,
    payload: Result<Json<FirewallDryRunRequest>, JsonRejection>,
) -> Result<Json<FirewallDryRunResponse>, ApiError> {
    let policy_id = FirewallPolicyId::new(policy_id)
        .map_err(|error| ApiError::bad_request(error.to_string()))?;
    let mut payload = payload
        .map_err(|rejection| mutation::json_rejection(rejection, "firewall dry-run"))?
        .0;
    normalize_spec(&mut payload.spec)?;
    let settings = state.firewall_settings.clone().ok_or_else(|| {
        ApiError::service_unavailable("firewall planning is not configured on this API server")
    })?;
    let keys = Keyspace::new(&state.cluster_id);
    let snapshot = state.store.list(&keys.resources()).await.map_err(|error| {
        ApiError::internal(format!("failed to read firewall snapshot: {error}"))
    })?;
    let mut policies: Vec<FirewallPolicy> =
        decode_kind(&snapshot.values, &keys, BuiltinKind::FirewallPolicy)?;
    let services = decode_kind(&snapshot.values, &keys, BuiltinKind::Service)?;
    let assignments = decode_kind(&snapshot.values, &keys, BuiltinKind::Assignment)?;
    let node_networks = decode_kind(&snapshot.values, &keys, BuiltinKind::NodeNetwork)?;
    project_policy(&mut policies, policy_id, payload.spec)?;
    let plan = firewall::plan(FirewallInput {
        settings,
        policies,
        services,
        assignments,
        node_networks,
    })
    .map_err(|error| ApiError::conflict("firewallPlanConflict", error.to_string()))?;
    Ok(Json(FirewallDryRunResponse {
        bundle_digest: plan.bundle_digest,
        rulesets: plan.rulesets.into_iter().map(Into::into).collect(),
    }))
}

fn decode_kind<Id, Spec, Status>(
    values: &[kernel_store::StoredValue],
    keys: &Keyspace,
    kind: BuiltinKind,
) -> Result<Vec<kernel_api::Object<Id, Spec, Status>>, ApiError>
where
    Id: Clone + std::fmt::Display + Into<kernel_api::ResourceName> + serde::de::DeserializeOwned,
    Spec: serde::de::DeserializeOwned,
    Status: serde::de::DeserializeOwned,
{
    let resource_kind =
        ResourceKind::new(kind.as_str()).map_err(|error| ApiError::internal(error.to_string()))?;
    resource::decode_list(values, keys, &resource_kind, kind)
}

fn project_policy(
    policies: &mut Vec<FirewallPolicy>,
    policy_id: FirewallPolicyId,
    spec: FirewallPolicySpec,
) -> Result<(), ApiError> {
    if let Some(policy) = policies
        .iter_mut()
        .find(|policy| policy.meta.id == policy_id)
    {
        if policy.meta.deletion_timestamp.is_some() {
            return Err(ApiError::conflict(
                "deletionInProgress",
                "FirewallPolicy deletion is already in progress",
            ));
        }
        policy.spec = spec;
    } else {
        policies.push(super::firewall_policies::new_policy(policy_id, spec));
    }
    Ok(())
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct FirewallDryRunRequest {
    spec: FirewallPolicySpec,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct FirewallDryRunResponse {
    bundle_digest: String,
    rulesets: Vec<FirewallDryRunRuleset>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct FirewallDryRunRuleset {
    node_id: kernel_api::NodeId,
    table_name: String,
    script: String,
    digest: String,
}

impl From<FirewallRuleset> for FirewallDryRunRuleset {
    fn from(ruleset: FirewallRuleset) -> Self {
        Self {
            node_id: ruleset.node_id,
            table_name: ruleset.table_name,
            script: ruleset.script,
            digest: ruleset.digest,
        }
    }
}
