use std::path::Path;

use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use axum::middleware::Next;
use axum::response::Response;
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
        .nest_service("/assets", ServeDir::new(directory.join("assets")))
        .fallback_service(files)
        .layer(middleware::from_fn(panel_cache_policy))
        .layer(middleware::from_fn_with_state(
            auth,
            require_operator_source,
        ));
    router
        .route("/api", any(api_not_found))
        .route("/api/{*path}", any(api_not_found))
        .fallback_service(panel)
}

async fn api_not_found() -> StatusCode {
    StatusCode::NOT_FOUND
}

async fn panel_cache_policy(mut request: Request<Body>, next: Next) -> Response {
    let immutable_asset = request.uri().path().starts_with("/assets/");
    if !immutable_asset {
        request.headers_mut().remove(header::IF_MODIFIED_SINCE);
        request.headers_mut().remove(header::IF_UNMODIFIED_SINCE);
        request.headers_mut().remove(header::IF_NONE_MATCH);
    }
    let mut response = next.run(request).await;
    if response.status().is_success() {
        response.headers_mut().insert(
            header::CACHE_CONTROL,
            if immutable_asset {
                header::HeaderValue::from_static("public, max-age=31536000, immutable")
            } else {
                header::HeaderValue::from_static("no-store")
            },
        );
    }
    response
}
