use axum::extract::{Path, State};
use axum::routing::get;
use axum::{Json, Router};
use kernel_api::{BuiltinKind, Preview, PreviewId, Webhook, WebhookId};

use crate::{ApiError, AppState, mask, resource};

pub(super) fn router() -> Router<AppState> {
    Router::new()
        .route("/api/previews", get(list_previews))
        .route("/api/previews/{preview_id}", get(get_preview))
        .route("/api/webhooks", get(list_webhooks))
        .route("/api/webhooks/{webhook_id}", get(get_webhook))
}

async fn list_previews(State(state): State<AppState>) -> Result<Json<Vec<Preview>>, ApiError> {
    Ok(Json(resource::list(&state, BuiltinKind::Preview).await?))
}

async fn get_preview(
    State(state): State<AppState>,
    Path(preview_id): Path<String>,
) -> Result<Json<Preview>, ApiError> {
    let preview_id =
        PreviewId::new(preview_id).map_err(|error| ApiError::bad_request(error.to_string()))?;
    Ok(Json(
        resource::get(&state, BuiltinKind::Preview, preview_id).await?,
    ))
}

async fn list_webhooks(State(state): State<AppState>) -> Result<Json<Vec<Webhook>>, ApiError> {
    Ok(Json(
        resource::list(&state, BuiltinKind::Webhook)
            .await?
            .into_iter()
            .map(mask::webhook)
            .collect(),
    ))
}

async fn get_webhook(
    State(state): State<AppState>,
    Path(webhook_id): Path<String>,
) -> Result<Json<Webhook>, ApiError> {
    let webhook_id =
        WebhookId::new(webhook_id).map_err(|error| ApiError::bad_request(error.to_string()))?;
    Ok(Json(mask::webhook(
        resource::get(&state, BuiltinKind::Webhook, webhook_id).await?,
    )))
}
