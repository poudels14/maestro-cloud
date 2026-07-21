use axum::Json;
use axum::Router;
use axum::extract::{Path, State};
use axum::routing::get;
use kernel_api::{BuiltinKind, Node, NodeId};

use crate::{ApiError, AppState, resource};

pub(super) fn router() -> Router<AppState> {
    Router::new()
        .route("/api/cluster/nodes", get(list_nodes))
        .route("/api/cluster/nodes/{node_id}", get(get_node))
}

async fn list_nodes(State(state): State<AppState>) -> Result<Json<Vec<Node>>, ApiError> {
    Ok(Json(resource::list(&state, BuiltinKind::Node).await?))
}

async fn get_node(
    State(state): State<AppState>,
    Path(node_id): Path<String>,
) -> Result<Json<Node>, ApiError> {
    let node_id = NodeId::new(node_id).map_err(|error| ApiError::bad_request(error.to_string()))?;
    Ok(Json(
        resource::get(&state, BuiltinKind::Node, node_id).await?,
    ))
}
