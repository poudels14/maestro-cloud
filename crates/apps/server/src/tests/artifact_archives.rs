use std::sync::Arc;

use axum::body::Body;
use axum::http::{Method, Request, StatusCode, header};
use build::LocalBuildSourceProvider;
use http_body_util::BodyExt;
use kernel_api::{ArtifactArchiveId, ArtifactArchiveUploadResponse, ClusterId};
use kernel_store::{InMemoryStore, TokioClock};
use sha2::{Digest, Sha256};
use tower::ServiceExt;

use crate::{ApiServer, ServerSettings};

#[tokio::test]
async fn artifact_archives_are_bounded_content_addressed_and_replayable()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let root = std::fs::canonicalize(directory.path())?;
    let content = b"\x1f\x8bmaestro-build-context";
    let archive_id = ArtifactArchiveId::from_sha256(Sha256::digest(content).into());
    let server = ApiServer::new(
        Arc::new(InMemoryStore::new(Arc::new(TokioClock::new()))),
        ClusterId::new("archive-test")?,
        ServerSettings::new("127.0.0.1:3000".parse()?, None),
    )?
    .with_artifact_archive_store(Arc::new(LocalBuildSourceProvider::new(
        root.join("workspaces"),
        root.join("archives"),
    )?));

    let created = upload(&server, archive_id.as_str(), content, "application/gzip").await?;
    assert_eq!(created.status(), StatusCode::CREATED);
    let response: ArtifactArchiveUploadResponse =
        serde_json::from_slice(&created.into_body().collect().await?.to_bytes())?;
    assert_eq!(response.archive_id, archive_id);
    assert_eq!(response.size_bytes, content.len() as u64);

    assert_eq!(
        upload(&server, archive_id.as_str(), content, "application/gzip")
            .await?
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        upload(&server, "sha256-wrong", content, "application/gzip")
            .await?
            .status(),
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        upload(
            &server,
            archive_id.as_str(),
            content,
            "application/octet-stream"
        )
        .await?
        .status(),
        StatusCode::BAD_REQUEST
    );
    Ok(())
}

async fn upload(
    server: &ApiServer,
    archive_id: &str,
    content: &[u8],
    content_type: &str,
) -> Result<axum::response::Response, Box<dyn std::error::Error>> {
    Ok(server
        .router()
        .oneshot(
            Request::builder()
                .method(Method::PUT)
                .uri(format!("/api/artifact-archives/{archive_id}"))
                .header(header::CONTENT_TYPE, content_type)
                .body(Body::from(content.to_vec()))?,
        )
        .await?)
}
