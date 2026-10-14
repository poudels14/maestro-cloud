use std::path::{Path, PathBuf};
use std::sync::Arc;

use axum::Json;
use axum::extract::{Multipart, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use maestro_control::new_deployment;
use maestro_core::{GitCommit, ServiceId, ServiceSpec};
use maestro_proto::pb::UploadChunk;
use maestro_store::{Consensus, Mutation, SecretKey};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tonic::Status;

use crate::node_service::NodeClients;

const ARCHIVE_EXTENSION: &str = "tar.gz";
const UPLOAD_CHUNK: usize = 1 << 20;
const UPLOAD_NAME_PREFIX: &str = "[up] ";

#[derive(Clone)]
pub struct UploadState {
    pub consensus: Arc<Consensus>,
    pub clients: NodeClients,
    pub key: Arc<SecretKey>,
    pub directory: PathBuf,
    pub allowed: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct UploadSpec {
    pub id: ServiceId,
    #[serde(flatten)]
    pub spec: ServiceSpec,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UploadAccepted {
    pub service_id: ServiceId,
    pub deployment_id: String,
    pub name: String,
}

pub async fn upload_service(State(state): State<UploadState>, multipart: Multipart) -> Response {
    let result = match state.allowed {
        false => Err((
            StatusCode::FORBIDDEN,
            "cli deployment is disabled on this cluster; set allowCliDeployment: true in maestro.jsonc to enable".to_string(),
        )),
        true => receive(&state, multipart).await,
    };
    match result {
        Ok(accepted) => (StatusCode::ACCEPTED, Json(accepted)).into_response(),
        Err((status, message)) => (status, Json(json!({ "error": message }))).into_response(),
    }
}

pub async fn accept_upload(
    consensus: &Consensus,
    key: &SecretKey,
    directory: &Path,
    upload: UploadSpec,
    archive: &Path,
) -> Result<UploadAccepted, Status> {
    let UploadSpec { id, mut spec } = upload;
    spec.validate(&id)
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
    let frozen = consensus
        .state()
        .cluster_freeze()
        .map_err(|error| Status::internal(error.to_string()))?;
    let build_from_upload = spec
        .build
        .as_ref()
        .is_some_and(|build| build.repo.is_none());
    match (frozen, build_from_upload) {
        (Some(freeze), _) => Err(Status::failed_precondition(format!(
            "cluster deploys are frozen: {}",
            freeze.reason
        ))),
        (None, false) => Err(Status::invalid_argument(
            "services up needs a build section without a repo",
        )),
        (None, true) => {
            spec.name = format!("{UPLOAD_NAME_PREFIX}{}", spec.name);
            let sealed = key.seal_spec(&spec);
            let deployment = new_deployment(&id, sealed.clone());
            let deployment_id = deployment.id.to_string();
            tokio::fs::rename(archive, archive_path(directory, &id, &deployment_id))
                .await
                .map_err(|error| Status::internal(format!("failed to store upload: {error}")))?;
            consensus
                .write(vec![
                    Mutation::PutService {
                        id: id.clone(),
                        spec: sealed,
                    },
                    Mutation::CreateDeployment { deployment },
                    Mutation::SetDeploymentBuild {
                        service_id: id.clone(),
                        deployment_id: deployment_id.as_str().into(),
                        commit: Some(GitCommit {
                            reference: "upload".to_string(),
                            message: "maestro services up".to_string(),
                        }),
                        built_on: None,
                    },
                ])
                .await
                .map_err(|error| Status::failed_precondition(error.to_string()))?;
            tracing::info!(service = %id, deployment = %deployment_id, "cli upload queued");
            Ok(UploadAccepted {
                service_id: id,
                deployment_id,
                name: spec.name,
            })
        }
    }
}

pub fn archive_path(directory: &Path, service_id: &ServiceId, deployment_id: &str) -> PathBuf {
    directory.join(format!("{service_id}-{deployment_id}.{ARCHIVE_EXTENSION}"))
}

pub fn incoming_path(directory: &Path) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_nanos());
    directory.join(format!(".incoming-{nanos}.{ARCHIVE_EXTENSION}"))
}

pub fn pack(directory: &Path) -> anyhow::Result<Vec<u8>> {
    let encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    let mut archive = tar::Builder::new(encoder);
    archive.follow_symlinks(false);
    let walker = ignore::WalkBuilder::new(directory)
        .hidden(false)
        .git_ignore(true)
        .git_exclude(true)
        .git_global(true)
        .require_git(false)
        .add_custom_ignore_filename(".dockerignore")
        .filter_entry(|entry| entry.file_name() != ".git")
        .build();
    for entry in walker {
        let entry = entry?;
        let relative = entry.path().strip_prefix(directory)?;
        if !relative.as_os_str().is_empty() {
            archive.append_path_with_name(entry.path(), relative)?;
        }
    }
    Ok(archive.into_inner()?.finish()?)
}

pub async fn unpack(archive: PathBuf, destination: PathBuf) -> Result<(), String> {
    tokio::task::spawn_blocking(move || {
        let file = std::fs::File::open(&archive).map_err(|error| {
            format!("uploaded context {} is missing: {error}", archive.display())
        })?;
        std::fs::create_dir_all(&destination).map_err(|error| error.to_string())?;
        tar::Archive::new(flate2::read::GzDecoder::new(file))
            .unpack(&destination)
            .map_err(|error| format!("failed to unpack uploaded context: {error}"))
    })
    .await
    .map_err(|error| error.to_string())?
}

pub async fn stream_chunks(
    upload: &UploadSpec,
    archive: &Path,
) -> Result<tokio::sync::mpsc::Receiver<UploadChunk>, Status> {
    let metadata =
        serde_json::to_vec(upload).map_err(|error| Status::internal(error.to_string()))?;
    let mut file = tokio::fs::File::open(archive)
        .await
        .map_err(|error| Status::internal(error.to_string()))?;
    let (sender, receiver) = tokio::sync::mpsc::channel(4);
    tokio::spawn(async move {
        let mut next = Some(UploadChunk {
            metadata,
            data: Vec::new(),
        });
        while let Some(chunk) = next.take() {
            if sender.send(chunk).await.is_ok() {
                let mut buffer = vec![0u8; UPLOAD_CHUNK];
                next = match file.read(&mut buffer).await {
                    Ok(0) | Err(_) => None,
                    Ok(read) => {
                        buffer.truncate(read);
                        Some(UploadChunk {
                            metadata: Vec::new(),
                            data: buffer,
                        })
                    }
                };
            }
        }
    });
    Ok(receiver)
}

async fn receive(
    state: &UploadState,
    mut multipart: Multipart,
) -> Result<UploadAccepted, (StatusCode, String)> {
    tokio::fs::create_dir_all(&state.directory)
        .await
        .map_err(internal)?;
    let incoming = incoming_path(&state.directory);
    let mut upload = None;
    let mut received_context = false;
    while let Some(mut field) = multipart.next_field().await.map_err(bad_request)? {
        match field.name() {
            Some("spec") => {
                let bytes = field.bytes().await.map_err(bad_request)?;
                upload = Some(
                    serde_json::from_slice::<UploadSpec>(&bytes).map_err(|error| {
                        (StatusCode::BAD_REQUEST, format!("invalid spec: {error}"))
                    })?,
                );
            }
            Some("context") => {
                let mut file = tokio::fs::File::create(&incoming).await.map_err(internal)?;
                while let Some(chunk) = field.chunk().await.map_err(bad_request)? {
                    file.write_all(&chunk).await.map_err(internal)?;
                }
                file.flush().await.map_err(internal)?;
                received_context = true;
            }
            _ => {}
        }
    }
    let result = match (upload, received_context) {
        (Some(upload), true) => forward_or_accept(state, upload, &incoming).await,
        _ => Err((
            StatusCode::BAD_REQUEST,
            "the upload needs a spec part and a context part".to_string(),
        )),
    };
    let _ = tokio::fs::remove_file(&incoming).await;
    result
}

async fn forward_or_accept(
    state: &UploadState,
    upload: UploadSpec,
    incoming: &Path,
) -> Result<UploadAccepted, (StatusCode, String)> {
    let metrics = state.consensus.metrics();
    let accepted = match state.consensus.current_leader() {
        Some((leader, _)) if leader == metrics.node_id => {
            accept_upload(
                &state.consensus,
                &state.key,
                &state.directory,
                upload,
                incoming,
            )
            .await
        }
        Some((_, address)) => state.clients.upload(&address, &upload, incoming).await,
        None => Err(Status::unavailable("the cluster has no leader")),
    };
    accepted.map_err(|status| {
        let code = match status.code() {
            tonic::Code::InvalidArgument => StatusCode::BAD_REQUEST,
            tonic::Code::FailedPrecondition => StatusCode::CONFLICT,
            tonic::Code::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        (code, status.message().to_string())
    })
}

fn internal(error: impl std::fmt::Display) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, error.to_string())
}

fn bad_request(error: impl std::fmt::Display) -> (StatusCode, String) {
    (StatusCode::BAD_REQUEST, error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn packs_respecting_ignore_files_and_unpacks() {
        let source = tempfile::tempdir().unwrap();
        std::fs::write(source.path().join("Dockerfile"), "FROM scratch\n").unwrap();
        std::fs::write(source.path().join(".gitignore"), "target/\n").unwrap();
        std::fs::write(source.path().join(".dockerignore"), "secret.txt\n").unwrap();
        std::fs::write(source.path().join("secret.txt"), "no").unwrap();
        std::fs::write(source.path().join(".env.example"), "A=1").unwrap();
        std::fs::create_dir_all(source.path().join("target")).unwrap();
        std::fs::write(source.path().join("target/big"), "x").unwrap();
        std::fs::create_dir_all(source.path().join(".git")).unwrap();
        std::fs::write(source.path().join(".git/HEAD"), "ref").unwrap();

        let bytes = pack(source.path()).unwrap();
        let output = tempfile::tempdir().unwrap();
        let archive = output.path().join("context.tar.gz");
        std::fs::write(&archive, bytes).unwrap();
        let destination = output.path().join("context");
        unpack(archive, destination.clone()).await.unwrap();

        assert!(destination.join("Dockerfile").exists());
        assert!(destination.join(".env.example").exists());
        assert!(!destination.join("secret.txt").exists());
        assert!(!destination.join("target").exists());
        assert!(!destination.join(".git").exists());
    }
}
