use axum::extract::rejection::JsonRejection;
use axum::extract::{DefaultBodyLimit, Extension, Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::delete;
use axum::{Json, Router};
use cluster::{NodeRemovalCoordinator, NodeRemovalError, StoreProviderError};
use kernel_api::{NodeId, NodeRemovalRequest, NodeRemovalResponse};

use crate::mutation::{MAXIMUM_REQUEST_BYTES, MutationRequest};
use crate::{ApiError, AppState, OperatorIdentity, mutation};

pub(super) fn router() -> Router<AppState> {
    Router::new()
        .route("/api/cluster/nodes/{node_id}", delete(remove))
        .layer(DefaultBodyLimit::max(MAXIMUM_REQUEST_BYTES))
}

async fn remove(
    State(state): State<AppState>,
    Path(node_id): Path<String>,
    Extension(operator): Extension<OperatorIdentity>,
    headers: HeaderMap,
    payload: Result<Json<NodeRemovalRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<NodeRemovalResponse>), ApiError> {
    let node_id = NodeId::new(node_id).map_err(|error| ApiError::bad_request(error.to_string()))?;
    let payload = payload
        .map_err(|rejection| mutation::json_rejection(rejection, "node removal"))?
        .0;
    if payload.node_id != node_id {
        return Err(ApiError::bad_request(
            "node removal confirmation does not match the request path",
        ));
    }
    let request = MutationRequest::new(
        &state,
        &headers,
        &operator,
        "DELETE /api/cluster/nodes/{nodeId}",
        &[node_id.as_str()],
        &payload,
    )?;
    if let Some(response) = request.replay(&state).await? {
        return Ok((StatusCode::ACCEPTED, Json(response)));
    }

    let coordinator = NodeRemovalCoordinator::new(
        &state.cluster_id,
        state.store_provider.clone(),
        state.store.clone(),
    )
    .map_err(removal_error)?;
    let plan = coordinator
        .progress(&node_id, state.timestamp_clock.now())
        .await
        .map_err(removal_error)?;
    let (response, transaction) = plan.into_parts();
    let response = request
        .commit(
            &state,
            response,
            transaction,
            "removalConflict",
            "Node or workload placement changed while advancing removal",
        )
        .await?;
    Ok((StatusCode::ACCEPTED, Json(response)))
}

fn removal_error(error: NodeRemovalError) -> ApiError {
    match error {
        NodeRemovalError::UnknownNode { .. } => ApiError::not_found(error.to_string()),
        NodeRemovalError::DeletionInProgress { .. } => {
            ApiError::conflict("deletionInProgress", error.to_string())
        }
        NodeRemovalError::IntentIdentityConflict => {
            ApiError::conflict("removalIdentityConflict", error.to_string())
        }
        NodeRemovalError::Provider(StoreProviderError::MembershipConflict { .. }) => {
            ApiError::conflict("membershipConflict", error.to_string())
        }
        NodeRemovalError::Provider(_)
        | NodeRemovalError::ProviderUnavailable
        | NodeRemovalError::Store(_) => ApiError::service_unavailable(error.to_string()),
        _ => ApiError::internal(error.to_string()),
    }
}
