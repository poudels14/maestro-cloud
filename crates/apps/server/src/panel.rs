use std::path::Path;

use axum::Router;
use axum::http::StatusCode;
use axum::routing::any;
use tower_http::services::{ServeDir, ServeFile};

pub(crate) fn serve(router: Router, directory: Option<&Path>) -> Router {
    let Some(directory) = directory else {
        return router;
    };
    let files = ServeDir::new(directory).fallback(ServeFile::new(directory.join("index.html")));
    router
        .route("/api", any(api_not_found))
        .route("/api/{*path}", any(api_not_found))
        .fallback_service(files)
}

async fn api_not_found() -> StatusCode {
    StatusCode::NOT_FOUND
}
