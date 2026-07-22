use std::collections::{BTreeMap, BTreeSet};

use axum::extract::rejection::JsonRejection;
use axum::extract::{DefaultBodyLimit, Extension, Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::get;
use axum::{Json, Router};
use kernel_api::{
    BuiltinKind, CommandRequest, Generation, Object, ObjectMeta, ResourceKind, ResourceRevision,
    UpgradeCommandResponse, UpgradeCreateRequest, UpgradePhase, UpgradeRun, UpgradeRunId,
    UpgradeRunSpec, UpgradeRunStatus,
};
use kernel_store::{Compare, ExpectedVersion, Keyspace, Mutation, Transaction};
use semver::Version;

use crate::mutation::{MAXIMUM_REQUEST_BYTES, MutationRequest};
use crate::{ApiError, AppState, OperatorIdentity, mutation, resource};

pub(super) fn router() -> Router<AppState> {
    Router::new()
        .route("/api/cluster/upgrades", get(list).post(create))
        .route(
            "/api/cluster/upgrades/{upgrade_run_id}",
            get(get_upgrade).delete(cancel),
        )
        .layer(DefaultBodyLimit::max(MAXIMUM_REQUEST_BYTES))
}

async fn list(State(state): State<AppState>) -> Result<Json<Vec<UpgradeRun>>, ApiError> {
    Ok(Json(resource::list(&state, BuiltinKind::UpgradeRun).await?))
}

async fn get_upgrade(
    State(state): State<AppState>,
    Path(upgrade_run_id): Path<String>,
) -> Result<Json<UpgradeRun>, ApiError> {
    let upgrade_run_id = parse_id(upgrade_run_id)?;
    Ok(Json(
        resource::get(&state, BuiltinKind::UpgradeRun, upgrade_run_id).await?,
    ))
}

async fn create(
    State(state): State<AppState>,
    Extension(operator): Extension<OperatorIdentity>,
    headers: HeaderMap,
    payload: Result<Json<UpgradeCreateRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<UpgradeCommandResponse>), ApiError> {
    let mut payload = payload
        .map_err(|rejection| mutation::json_rejection(rejection, "upgrade"))?
        .0;
    validate_spec(&mut payload.spec)?;
    let request = MutationRequest::new(
        &state,
        &headers,
        &operator,
        "POST /api/cluster/upgrades",
        &[payload.upgrade_run_id.as_str()],
        &payload,
    )?;
    if let Some(response) = request.replay(&state).await? {
        return Ok((StatusCode::ACCEPTED, Json(response)));
    }
    let keys = Keyspace::new(&state.cluster_id);
    let kind = upgrade_kind()?;
    let key = keys.resource(&kind, &payload.upgrade_run_id.clone().into());
    let run = new_run(payload.upgrade_run_id, payload.spec);
    let response = UpgradeCommandResponse::from(&run);
    let response = request
        .commit(
            &state,
            response,
            Transaction {
                compares: vec![Compare {
                    key: key.clone(),
                    expected: ExpectedVersion::Missing,
                }],
                mutations: vec![Mutation::Put {
                    key,
                    value: serde_json::to_vec(&run).map_err(|error| {
                        ApiError::internal(format!("failed to encode UpgradeRun: {error}"))
                    })?,
                    session: None,
                }],
            },
            "upgradeRunExists",
            "UpgradeRun id already exists",
        )
        .await?;
    Ok((StatusCode::ACCEPTED, Json(response)))
}

async fn cancel(
    State(state): State<AppState>,
    Path(upgrade_run_id): Path<String>,
    Extension(operator): Extension<OperatorIdentity>,
    headers: HeaderMap,
    payload: Result<Json<CommandRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<UpgradeCommandResponse>), ApiError> {
    let upgrade_run_id = parse_id(upgrade_run_id)?;
    let payload = payload
        .map_err(|rejection| mutation::json_rejection(rejection, "upgrade command"))?
        .0;
    let request = MutationRequest::new(
        &state,
        &headers,
        &operator,
        "DELETE /api/cluster/upgrades/{upgradeRunId}",
        &[upgrade_run_id.as_str()],
        &payload,
    )?;
    if let Some(response) = request.replay(&state).await? {
        return Ok((StatusCode::ACCEPTED, Json(response)));
    }
    let keys = Keyspace::new(&state.cluster_id);
    let kind = upgrade_kind()?;
    let key = keys.resource(&kind, &upgrade_run_id.clone().into());
    let stored = state
        .store
        .get(&key)
        .await
        .map_err(|error| ApiError::internal(format!("failed to read UpgradeRun: {error}")))?
        .ok_or_else(|| {
            ApiError::not_found(format!("UpgradeRun `{upgrade_run_id}` does not exist"))
        })?;
    let mut run: UpgradeRun = resource::decode(&stored, &keys, &kind, BuiltinKind::UpgradeRun)?;
    if run.meta.revision != payload.expected_revision {
        return Err(ApiError::conflict(
            "revisionConflict",
            "UpgradeRun is not at the expected revision",
        ));
    }
    let write = run.meta.deletion_timestamp.is_none();
    if write {
        run.meta.deletion_timestamp = Some(state.timestamp_clock.now());
    }
    let response = UpgradeCommandResponse::from(&run);
    let mutations = if write {
        vec![Mutation::Put {
            key: key.clone(),
            value: serde_json::to_vec(&run).map_err(|error| {
                ApiError::internal(format!("failed to encode UpgradeRun: {error}"))
            })?,
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
            "UpgradeRun changed after its expected revision was read",
        )
        .await?;
    Ok((StatusCode::ACCEPTED, Json(response)))
}

fn validate_spec(spec: &mut UpgradeRunSpec) -> Result<(), ApiError> {
    spec.target_version = spec.target_version.trim().to_string();
    Version::parse(&spec.target_version).map_err(|error| {
        ApiError::bad_request(format!(
            "invalid targetVersion `{}`: {error}",
            spec.target_version
        ))
    })?;
    let unique = spec.node_ids.iter().collect::<BTreeSet<_>>();
    if unique.len() != spec.node_ids.len() {
        return Err(ApiError::bad_request(
            "upgrade nodeIds must not contain duplicates",
        ));
    }
    Ok(())
}

fn new_run(upgrade_run_id: UpgradeRunId, spec: UpgradeRunSpec) -> UpgradeRun {
    Object {
        meta: ObjectMeta {
            id: upgrade_run_id,
            labels: BTreeMap::new(),
            annotations: BTreeMap::new(),
            revision: ResourceRevision::default(),
            generation: Generation(1),
            owner_refs: Vec::new(),
            finalizers: BTreeSet::new(),
            deletion_timestamp: None,
        },
        spec,
        status: UpgradeRunStatus {
            phase: UpgradePhase::Pending,
            nodes: Vec::new(),
            conditions: Vec::new(),
        },
    }
}

fn parse_id(value: String) -> Result<UpgradeRunId, ApiError> {
    UpgradeRunId::new(value).map_err(|error| ApiError::bad_request(error.to_string()))
}

fn upgrade_kind() -> Result<ResourceKind, ApiError> {
    ResourceKind::new(BuiltinKind::UpgradeRun.as_str())
        .map_err(|error| ApiError::internal(error.to_string()))
}
