use axum::Json;
use axum::Router;
use axum::routing::get;
use serde_json::{Value, json};

use crate::AppState;

pub(super) fn router() -> Router<AppState> {
    Router::new()
        .route("/healthz", get(health))
        .route("/openapi.json", get(openapi))
}

async fn health() -> Json<Value> {
    Json(json!({ "status": "ok" }))
}

async fn openapi() -> Json<Value> {
    Json(crate::openapi_document())
}
