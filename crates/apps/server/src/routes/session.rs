use axum::Router;
use axum::extract::{Extension, Request};
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::post;

use crate::auth::{AuthPolicy, clear_browser_session};
use crate::{ApiError, AppState};

pub(super) fn router(auth: AuthPolicy) -> Router<AppState> {
    Router::new()
        .route(
            "/api/auth/session",
            post(create_session).delete(delete_session),
        )
        .layer(Extension(auth))
}

async fn create_session(
    Extension(auth): Extension<AuthPolicy>,
    request: Request,
) -> Result<Response, ApiError> {
    let cookie = auth.create_browser_session(&request)?;
    let cookie = HeaderValue::from_str(&cookie)
        .map_err(|_| ApiError::internal("failed to encode browser session cookie"))?;
    Ok((StatusCode::NO_CONTENT, [(header::SET_COOKIE, cookie)]).into_response())
}

async fn delete_session() -> Result<Response, ApiError> {
    let cookie = HeaderValue::from_str(&clear_browser_session())
        .map_err(|_| ApiError::internal("failed to encode browser session cookie"))?;
    Ok((StatusCode::NO_CONTENT, [(header::SET_COOKIE, cookie)]).into_response())
}
