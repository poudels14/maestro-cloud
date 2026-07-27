use std::path::Path;

use axum::http::StatusCode;
use axum::routing::any;
use axum::{Router, middleware};
use tower_http::services::{ServeDir, ServeFile};

use crate::auth::{AuthPolicy, require_operator_source};

pub(crate) fn serve(router: Router, directory: Option<&Path>, auth: AuthPolicy) -> Router {
    let Some(directory) = directory else {
        return router;
    };
    let files = ServeDir::new(directory).fallback(ServeFile::new(directory.join("index.html")));
    let panel = Router::new()
        .fallback_service(files)
        .layer(middleware::from_fn_with_state(auth, require_operator_source));
    router
        .route("/api", any(api_not_found))
        .route("/api/{*path}", any(api_not_found))
        .fallback_service(panel)
}

async fn api_not_found() -> StatusCode {
    StatusCode::NOT_FOUND
}
