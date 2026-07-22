use axum::extract::rejection::JsonRejection;
use axum::extract::{DefaultBodyLimit, Extension, Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::post;
use axum::{Json, Router};
use kernel_api::{
    BuiltinKind, CommandRequest, Condition, ConditionReason, ConditionState, ConditionType, Node,
    NodeCommandResponse, NodeId, ResourceKind, Timestamp,
};
use kernel_store::{Compare, ExpectedVersion, Keyspace, Mutation, Transaction};

use crate::mutation::{MAXIMUM_REQUEST_BYTES, MutationRequest};
use crate::{ApiError, AppState, OperatorIdentity, mutation, resource};

const DRAINING_CONDITION: &str = "Draining";

pub(super) fn router() -> Router<AppState> {
    Router::new()
        .route("/api/cluster/nodes/{node_id}/drain", post(drain))
        .route("/api/cluster/nodes/{node_id}/restore", post(restore))
        .layer(DefaultBodyLimit::max(MAXIMUM_REQUEST_BYTES))
}

async fn drain(
    state: State<AppState>,
    path: Path<String>,
    operator: Extension<OperatorIdentity>,
    headers: HeaderMap,
    payload: Result<Json<CommandRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<NodeCommandResponse>), ApiError> {
    command(
        state,
        path,
        operator,
        headers,
        payload,
        NodeCommandKind {
            operation: "POST /api/cluster/nodes/{nodeId}/drain",
            draining: true,
        },
    )
    .await
}

async fn restore(
    state: State<AppState>,
    path: Path<String>,
    operator: Extension<OperatorIdentity>,
    headers: HeaderMap,
    payload: Result<Json<CommandRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<NodeCommandResponse>), ApiError> {
    command(
        state,
        path,
        operator,
        headers,
        payload,
        NodeCommandKind {
            operation: "POST /api/cluster/nodes/{nodeId}/restore",
            draining: false,
        },
    )
    .await
}

async fn command(
    State(state): State<AppState>,
    Path(node_id): Path<String>,
    Extension(operator): Extension<OperatorIdentity>,
    headers: HeaderMap,
    payload: Result<Json<CommandRequest>, JsonRejection>,
    command: NodeCommandKind,
) -> Result<(StatusCode, Json<NodeCommandResponse>), ApiError> {
    let node_id = NodeId::new(node_id).map_err(|error| ApiError::bad_request(error.to_string()))?;
    let payload = payload
        .map_err(|rejection| mutation::json_rejection(rejection, "node command"))?
        .0;
    let request = MutationRequest::new(
        &state,
        &headers,
        &operator,
        command.operation,
        &[node_id.as_str()],
        &payload,
    )?;
    if let Some(response) = request.replay(&state).await? {
        return Ok((StatusCode::ACCEPTED, Json(response)));
    }
    let keys = Keyspace::new(&state.cluster_id);
    let kind = ResourceKind::new(BuiltinKind::Node.as_str())
        .map_err(|error| ApiError::internal(error.to_string()))?;
    let key = keys.resource(&kind, &node_id.clone().into());
    let stored = state
        .store
        .get(&key)
        .await
        .map_err(|error| ApiError::internal(format!("failed to read Node: {error}")))?
        .ok_or_else(|| ApiError::not_found(format!("Node `{node_id}` does not exist")))?;
    let mut node: Node = resource::decode(&stored, &keys, &kind, BuiltinKind::Node)?;
    if node.meta.revision != payload.expected_revision {
        return Err(ApiError::conflict(
            "revisionConflict",
            "Node is not at the expected revision",
        ));
    }
    let write = set_draining(&mut node, command.draining, state.timestamp_clock.now())?;
    let response = NodeCommandResponse {
        node_id,
        draining: command.draining,
    };
    let mutations = if write {
        vec![Mutation::Put {
            key: key.clone(),
            value: serde_json::to_vec(&node).map_err(|error| {
                ApiError::internal(format!("failed to encode Node resource: {error}"))
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
            "Node changed after its expected revision was read",
        )
        .await?;
    Ok((StatusCode::ACCEPTED, Json(response)))
}

fn set_draining(node: &mut Node, draining: bool, now: Timestamp) -> Result<bool, ApiError> {
    if node.meta.deletion_timestamp.is_some() {
        return Err(ApiError::conflict(
            "deletionInProgress",
            "Node deletion is already in progress",
        ));
    }
    let desired = if draining {
        ConditionState::True
    } else {
        ConditionState::False
    };
    let mut existing = node
        .status
        .conditions
        .iter()
        .filter(|condition| condition.condition_type.0 == DRAINING_CONDITION);
    let canonical = existing
        .next()
        .is_some_and(|condition| condition.state == desired)
        && existing.next().is_none();
    if canonical {
        return Ok(false);
    }
    node.status
        .conditions
        .retain(|condition| condition.condition_type.0 != DRAINING_CONDITION);
    node.status.conditions.push(Condition {
        condition_type: ConditionType(DRAINING_CONDITION.to_string()),
        state: desired,
        reason: ConditionReason(if draining { "Requested" } else { "Restored" }.to_string()),
        message: if draining {
            "node drain requested".to_string()
        } else {
            "node restored to scheduling".to_string()
        },
        observed_generation: node.meta.generation,
        last_transition_time: now,
    });
    Ok(true)
}

struct NodeCommandKind {
    operation: &'static str,
    draining: bool,
}
