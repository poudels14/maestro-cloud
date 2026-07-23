use axum::extract::rejection::JsonRejection;
use axum::extract::{DefaultBodyLimit, Extension, Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::post;
use axum::{Json, Router};
use cluster::NodeSchedulingAction;
use kernel_api::{BuiltinKind, CommandRequest, Node, NodeCommandResponse, NodeId, ResourceKind};
use kernel_store::{Compare, ExpectedVersion, Keyspace, Mutation, Transaction};

use crate::mutation::{MAXIMUM_REQUEST_BYTES, MutationRequest};
use crate::{ApiError, AppState, OperatorIdentity, mutation, resource};

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
        NodeSchedulingAction::Drain,
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
        NodeSchedulingAction::Restore,
    )
    .await
}

async fn command(
    State(state): State<AppState>,
    Path(node_id): Path<String>,
    Extension(operator): Extension<OperatorIdentity>,
    headers: HeaderMap,
    payload: Result<Json<CommandRequest>, JsonRejection>,
    action: NodeSchedulingAction,
) -> Result<(StatusCode, Json<NodeCommandResponse>), ApiError> {
    let node_id = NodeId::new(node_id).map_err(|error| ApiError::bad_request(error.to_string()))?;
    let payload = payload
        .map_err(|rejection| mutation::json_rejection(rejection, "node command"))?
        .0;
    let request = MutationRequest::new(
        &state,
        &headers,
        &operator,
        operation(action),
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
    if node.meta.deletion_timestamp.is_some() {
        return Err(ApiError::conflict(
            "deletionInProgress",
            "Node deletion is already in progress",
        ));
    }
    if action == NodeSchedulingAction::Restore
        && state
            .store
            .get(&keys.node_removal(&node_id))
            .await
            .map_err(|error| {
                ApiError::internal(format!("failed to read node removal intent: {error}"))
            })?
            .is_some()
    {
        return Err(ApiError::conflict(
            "removalInProgress",
            "Node restore is blocked by permanent removal",
        ));
    }
    let write = cluster::set_node_scheduling(&mut node, action, state.timestamp_clock.now());
    let response = NodeCommandResponse {
        node_id: node_id.clone(),
        draining: action == NodeSchedulingAction::Drain,
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
    let mut compares = vec![Compare {
        key,
        expected: ExpectedVersion::Exact(stored.version),
    }];
    if action == NodeSchedulingAction::Restore {
        compares.push(Compare {
            key: keys.node_removal(&node_id),
            expected: ExpectedVersion::Missing,
        });
    }
    let response = request
        .commit(
            &state,
            response,
            Transaction {
                compares,
                mutations,
            },
            "revisionConflict",
            "Node changed after its expected revision was read",
        )
        .await?;
    Ok((StatusCode::ACCEPTED, Json(response)))
}

const fn operation(action: NodeSchedulingAction) -> &'static str {
    match action {
        NodeSchedulingAction::Drain => "POST /api/cluster/nodes/{nodeId}/drain",
        NodeSchedulingAction::Restore => "POST /api/cluster/nodes/{nodeId}/restore",
    }
}
