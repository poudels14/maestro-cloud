use std::io::ErrorKind;

use async_trait::async_trait;
use kernel_api::ArtifactArchiveId;
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;

use crate::local_fs::{ensure_root, hash_file_digest, io_unavailable};
use crate::{BuildSourceError, LocalBuildSourceProvider};

/// Whether a content-addressed archive write created new durable content.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArtifactArchiveWrite {
    /// The archive did not exist and was committed atomically.
    Created,
    /// Identical content was already present under the requested address.
    Existing,
}

/// Durable uploaded-source boundary shared by the API and build reconciler.
#[async_trait]
pub trait ArtifactArchiveStore: Send + Sync {
    /// Atomically stores bytes only when their SHA-256 address matches `archive_id`.
    async fn put(
        &self,
        archive_id: &ArtifactArchiveId,
        content: &[u8],
    ) -> Result<ArtifactArchiveWrite, BuildSourceError>;
}

#[async_trait]
impl ArtifactArchiveStore for LocalBuildSourceProvider {
    async fn put(
        &self,
        archive_id: &ArtifactArchiveId,
        content: &[u8],
    ) -> Result<ArtifactArchiveWrite, BuildSourceError> {
        let digest: [u8; 32] = Sha256::digest(content).into();
        if archive_id != &ArtifactArchiveId::from_sha256(digest) {
            return Err(BuildSourceError::rejected(
                "archive ID does not match the uploaded SHA-256 digest",
            ));
        }
        let root = ensure_root(&self.archive_root).await?;
        secure_directory(&root).await?;
        let target = root.join(archive_id.as_str());
        match existing_archive(&target, digest).await? {
            Some(outcome) => Ok(outcome),
            None => write_archive(&root, &target, content, digest).await,
        }
    }
}

async fn write_archive(
    root: &std::path::Path,
    target: &std::path::Path,
    content: &[u8],
    digest: [u8; 32],
) -> Result<ArtifactArchiveWrite, BuildSourceError> {
    let temporary = root.join(format!(".upload-{}", uuid::Uuid::new_v4().simple()));
    let mut options = tokio::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    options.mode(0o600);
    let mut file = options
        .open(&temporary)
        .await
        .map_err(|error| io_unavailable("create temporary build archive", &temporary, error))?;
    let result = async {
        file.write_all(content)
            .await
            .map_err(|error| io_unavailable("write temporary build archive", &temporary, error))?;
        file.sync_all()
            .await
            .map_err(|error| io_unavailable("sync temporary build archive", &temporary, error))?;
        drop(file);
        match tokio::fs::hard_link(&temporary, target).await {
            Ok(()) => Ok(ArtifactArchiveWrite::Created),
            Err(error) if error.kind() == ErrorKind::AlreadyExists => {
                existing_archive(target, digest).await?.ok_or_else(|| {
                    BuildSourceError::unavailable("archive disappeared during a concurrent upload")
                })
            }
            Err(error) => Err(io_unavailable("commit build archive", target, error)),
        }
    }
    .await;
    let cleanup = tokio::fs::remove_file(&temporary).await;
    match (result, cleanup) {
        (Ok(outcome), Ok(())) => Ok(outcome),
        (Ok(outcome), Err(error)) if error.kind() == ErrorKind::NotFound => Ok(outcome),
        (Ok(_), Err(error)) => Err(io_unavailable(
            "remove temporary build archive",
            &temporary,
            error,
        )),
        (Err(error), _) => Err(error),
    }
}

async fn existing_archive(
    target: &std::path::Path,
    digest: [u8; 32],
) -> Result<Option<ArtifactArchiveWrite>, BuildSourceError> {
    match tokio::fs::symlink_metadata(target).await {
        Ok(metadata) if metadata.file_type().is_file() => {
            if hash_file_digest(target).await? == digest {
                Ok(Some(ArtifactArchiveWrite::Existing))
            } else {
                Err(BuildSourceError::rejected(format!(
                    "content-addressed archive `{}` is corrupted",
                    target.display()
                )))
            }
        }
        Ok(_) => Err(BuildSourceError::rejected(format!(
            "archive path `{}` must be a regular file",
            target.display()
        ))),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
        Err(error) => Err(io_unavailable("inspect build archive", target, error)),
    }
}

async fn secure_directory(path: &std::path::Path) -> Result<(), BuildSourceError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        tokio::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))
            .await
            .map_err(|error| io_unavailable("protect build archive root", path, error))?;
    }
    Ok(())
}
