use std::io;

use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::{Query, State};
use axum::http::{StatusCode, header};
use axum::response::Response;
use axum::routing::get;
use futures_util::stream;
use runtime::{ArtifactDigest, ArtifactStoreError};
use serde::Deserialize;

use crate::{ApiError, AppState};

pub(super) fn router() -> Router<AppState> {
    Router::new().route("/api/node/artifacts", get(export))
}

#[derive(Debug, Deserialize)]
struct ExportParameters {
    digest: String,
}

async fn export(
    State(state): State<AppState>,
    Query(parameters): Query<ExportParameters>,
) -> Result<Response<Body>, ApiError> {
    let digest = ArtifactDigest::new(parameters.digest)
        .map_err(|error| ApiError::bad_request(error.to_string()))?;
    let artifacts = state.artifacts.ok_or_else(|| {
        ApiError::service_unavailable("artifact storage is not configured on this node")
    })?;
    let source = artifacts.export(&digest).await.map_err(artifact_error)?;
    let chunks = stream::unfold(Some(source), |source| async move {
        let mut source = source?;
        match source.next().await {
            Ok(Some(chunk)) => Some((Ok::<Bytes, io::Error>(Bytes::from(chunk)), Some(source))),
            Ok(None) => None,
            Err(error) => Some((Err(io::Error::other(error.to_string())), None)),
        }
    });
    let mut response = Response::new(Body::from_stream(chunks));
    *response.status_mut() = StatusCode::OK;
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        header::HeaderValue::from_static("application/x-tar"),
    );
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        header::HeaderValue::from_static("no-store"),
    );
    Ok(response)
}

fn artifact_error(error: ArtifactStoreError) -> ApiError {
    match error {
        ArtifactStoreError::InvalidDigest | ArtifactStoreError::InvalidReference => {
            ApiError::bad_request(error.to_string())
        }
        ArtifactStoreError::NotFound { .. } => ApiError::not_found(error.to_string()),
        ArtifactStoreError::Rejected { .. } => ApiError::bad_request(error.to_string()),
        ArtifactStoreError::Unavailable { .. } | ArtifactStoreError::Stream { .. } => {
            ApiError::service_unavailable(error.to_string())
        }
    }
}
