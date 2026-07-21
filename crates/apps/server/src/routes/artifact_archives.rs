use axum::body::Bytes;
use axum::extract::rejection::BytesRejection;
use axum::extract::{DefaultBodyLimit, Path, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::routing::put;
use axum::{Json, Router};
use build::{ArtifactArchiveWrite, BuildSourceError};
use kernel_api::{ArtifactArchiveId, ArtifactArchiveUploadResponse};
use sha2::{Digest, Sha256};

use crate::{ApiError, AppState};

pub(super) const MAXIMUM_ARCHIVE_BYTES: usize = 64 * 1_024 * 1_024;

pub(super) fn router() -> Router<AppState> {
    Router::new()
        .route("/api/artifact-archives/{archive_id}", put(upload))
        .layer(DefaultBodyLimit::max(MAXIMUM_ARCHIVE_BYTES))
}

async fn upload(
    State(state): State<AppState>,
    Path(archive_id): Path<String>,
    headers: HeaderMap,
    payload: Result<Bytes, BytesRejection>,
) -> Result<(StatusCode, Json<ArtifactArchiveUploadResponse>), ApiError> {
    if headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        != Some("application/gzip")
    {
        return Err(ApiError::bad_request(
            "artifact archive Content-Type must be application/gzip",
        ));
    }
    let content = payload.map_err(|rejection| {
        if rejection.status() == StatusCode::PAYLOAD_TOO_LARGE {
            ApiError::payload_too_large(format!(
                "artifact archive exceeds {MAXIMUM_ARCHIVE_BYTES} bytes"
            ))
        } else {
            ApiError::bad_request(rejection.body_text())
        }
    })?;
    if !content.starts_with(&[0x1f, 0x8b]) {
        return Err(ApiError::bad_request(
            "artifact archive is not a gzip stream",
        ));
    }
    let archive_id = ArtifactArchiveId::new(archive_id)
        .map_err(|error| ApiError::bad_request(error.to_string()))?;
    let expected = ArtifactArchiveId::from_sha256(Sha256::digest(&content).into());
    if archive_id != expected {
        return Err(ApiError::bad_request(
            "artifact archive ID does not match its SHA-256 digest",
        ));
    }
    let store = state.artifact_archives.ok_or_else(|| {
        ApiError::service_unavailable("artifact archive storage is not configured on this node")
    })?;
    let outcome = store
        .put(&archive_id, &content)
        .await
        .map_err(archive_error)?;
    let status = match outcome {
        ArtifactArchiveWrite::Created => StatusCode::CREATED,
        ArtifactArchiveWrite::Existing => StatusCode::OK,
    };
    Ok((
        status,
        Json(ArtifactArchiveUploadResponse {
            archive_id,
            size_bytes: u64::try_from(content.len()).unwrap_or(u64::MAX),
        }),
    ))
}

fn archive_error(error: BuildSourceError) -> ApiError {
    match error {
        BuildSourceError::Rejected { message } => ApiError::bad_request(message),
        BuildSourceError::Unavailable { message } => ApiError::service_unavailable(message),
    }
}
