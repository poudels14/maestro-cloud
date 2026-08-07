use std::collections::{HashMap, HashSet};
use std::path::{Component, Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use docker::errors::Error as DockerError;
use docker::models::BuildInfo;
use docker::query_parameters::{
    CreateImageOptionsBuilder, ImportImageOptionsBuilder, ListImagesOptionsBuilder,
    PushImageOptionsBuilder, RemoveImageOptionsBuilder, TagImageOptionsBuilder,
};
use futures_util::{Stream, StreamExt};

use crate::docker::DockerRuntime;
use crate::docker_artifact_context::{file_stream, finish_archive_task, stream_directory_archive};
use crate::docker_artifact_support::{
    MANAGED_IMAGE_LABEL, MANAGED_IMAGE_VALUE, TEMPORARY_TAG_PREFIX, build_options, definition_text,
    imported_candidates, inspect_digest, operation_error, prunable_image_ids, split_tag,
    stream_error,
};
use crate::docker_support::is_not_found;
use crate::{
    ArtifactBuildOutputSink, ArtifactBuildOutputStream, ArtifactBuildRequest, ArtifactByteStream,
    ArtifactDigest, ArtifactPrunePolicy, ArtifactPruneReport, ArtifactReference, ArtifactSource,
    ArtifactStore, ArtifactStoreError, DiscardArtifactBuildOutput,
};

static BUILD_SEQUENCE: AtomicU64 = AtomicU64::new(1);
const TRANSFER_CHUNK_BYTES: usize = 64 * 1_024;

#[async_trait]
impl ArtifactStore for DockerRuntime {
    async fn build(
        &self,
        request: &ArtifactBuildRequest,
    ) -> Result<ArtifactDigest, ArtifactStoreError> {
        self.build_artifact(request, &DiscardArtifactBuildOutput)
            .await
    }

    async fn build_with_output(
        &self,
        request: &ArtifactBuildRequest,
        output: &dyn ArtifactBuildOutputSink,
    ) -> Result<ArtifactDigest, ArtifactStoreError> {
        self.build_artifact(request, output).await
    }

    async fn pull(
        &self,
        reference: &ArtifactReference,
    ) -> Result<ArtifactDigest, ArtifactStoreError> {
        let options = CreateImageOptionsBuilder::default()
            .from_image(reference.as_str())
            .build();
        consume_operation(
            "pull",
            Some(reference.as_str()),
            self.client.create_image(
                Some(options),
                None,
                self.registry_credential(reference).await?,
            ),
        )
        .await?;
        self.local_digest(reference.as_str()).await
    }

    async fn push(
        &self,
        digest: &ArtifactDigest,
        destination: &ArtifactReference,
    ) -> Result<(), ArtifactStoreError> {
        self.tag(digest, destination).await?;
        let (repository, tag) = split_tag(destination.as_str())?;
        let options = PushImageOptionsBuilder::default().tag(&tag).build();
        consume_operation(
            "push",
            Some(destination.as_str()),
            self.client.push_image(
                &repository,
                Some(options),
                self.registry_credential(destination).await?,
            ),
        )
        .await
    }

    async fn publish(
        &self,
        digest: &ArtifactDigest,
        destination: &ArtifactReference,
    ) -> Result<ArtifactDigest, ArtifactStoreError> {
        self.push(digest, destination).await?;
        let remote = self
            .client
            .inspect_registry_image(
                destination.as_str(),
                self.registry_credential(destination).await?,
            )
            .await
            .map_err(|error| {
                operation_error("resolve pushed", Some(destination.as_str()), error)
            })?;
        let digest = remote
            .descriptor
            .digest
            .filter(|digest| !digest.trim().is_empty())
            .ok_or_else(|| ArtifactStoreError::Unavailable {
                message: format!(
                    "Docker registry inspection for `{}` omitted the pushed digest",
                    destination.as_str()
                ),
            })?;
        ArtifactDigest::new(digest)?.for_reference(destination)
    }

    async fn resolve_digest(
        &self,
        reference: &ArtifactReference,
    ) -> Result<ArtifactDigest, ArtifactStoreError> {
        match self.client.inspect_image(reference.as_str()).await {
            Ok(inspect) => inspect_digest(&inspect, reference.as_str()),
            Err(error) if is_not_found(&error) => {
                let remote = self
                    .client
                    .inspect_registry_image(
                        reference.as_str(),
                        self.registry_credential(reference).await?,
                    )
                    .await
                    .map_err(|error| operation_error("resolve", Some(reference.as_str()), error))?;
                let digest =
                    remote
                        .descriptor
                        .digest
                        .ok_or_else(|| ArtifactStoreError::Unavailable {
                            message: format!(
                                "Docker registry inspection for `{}` omitted the digest",
                                reference.as_str()
                            ),
                        })?;
                ArtifactDigest::new(digest)
            }
            Err(error) => Err(operation_error("resolve", Some(reference.as_str()), error)),
        }
    }

    async fn ensure_local(
        &self,
        reference: &ArtifactReference,
    ) -> Result<ArtifactDigest, ArtifactStoreError> {
        match self.client.inspect_image(reference.as_str()).await {
            Ok(inspect) => inspect_digest(&inspect, reference.as_str()),
            Err(error) if is_not_found(&error) => self.pull(reference).await,
            Err(error) => Err(operation_error("resolve", Some(reference.as_str()), error)),
        }
    }

    async fn contains(&self, digest: &ArtifactDigest) -> Result<bool, ArtifactStoreError> {
        match self.client.inspect_image(digest.as_str()).await {
            Ok(_) => Ok(true),
            Err(error) if is_not_found(&error) => Ok(false),
            Err(error) => Err(operation_error("inspect", Some(digest.as_str()), error)),
        }
    }

    async fn export(
        &self,
        digest: &ArtifactDigest,
    ) -> Result<Box<dyn ArtifactByteStream>, ArtifactStoreError> {
        self.local_digest(digest.as_str()).await?;
        let stream = self.client.export_image(digest.as_str());
        Ok(Box::new(DockerArtifactStream {
            stream: Box::pin(stream),
            pending: None,
        }))
    }

    async fn import(
        &self,
        source: Box<dyn ArtifactByteStream>,
    ) -> Result<ArtifactDigest, ArtifactStoreError> {
        let source = futures_util::stream::unfold(Some(source), |state| async move {
            let mut source = state?;
            match source.next().await {
                Ok(Some(chunk)) if chunk.len() <= TRANSFER_CHUNK_BYTES => {
                    Some((Ok(Bytes::from(chunk)), Some(source)))
                }
                Ok(Some(chunk)) => Some((
                    Err(std::io::Error::other(format!(
                        "artifact source emitted {} bytes; maximum chunk is {TRANSFER_CHUNK_BYTES}",
                        chunk.len()
                    ))),
                    None,
                )),
                Ok(None) => None,
                Err(error) => Some((Err(std::io::Error::other(error)), None)),
            }
        });
        let options = ImportImageOptionsBuilder::default().build();
        let mut responses = self.client.import_image_stream(options, source, None);
        let mut candidates = Vec::new();
        while let Some(response) = responses.next().await {
            let response = response.map_err(|error| stream_error("import", error))?;
            candidates.extend(imported_candidates(&response));
        }
        candidates.reverse();
        candidates.sort_by_key(|candidate| !candidate.starts_with("sha256:"));
        let mut seen = HashSet::new();
        for candidate in candidates
            .into_iter()
            .filter(|candidate| seen.insert(candidate.clone()))
        {
            match self.local_digest(&candidate).await {
                Ok(digest) => return Ok(digest),
                Err(ArtifactStoreError::NotFound { .. }) => {}
                Err(error) => return Err(error),
            }
        }
        Err(ArtifactStoreError::Rejected {
            message: "Docker import completed without identifying a loaded image".to_owned(),
        })
    }

    async fn prune(
        &self,
        policy: &ArtifactPrunePolicy,
    ) -> Result<ArtifactPruneReport, ArtifactStoreError> {
        let ArtifactPrunePolicy::Preserve(preserved) = policy;
        let filters = HashMap::from([(
            "label".to_owned(),
            vec![format!("{MANAGED_IMAGE_LABEL}={MANAGED_IMAGE_VALUE}")],
        )]);
        let options = ListImagesOptionsBuilder::default()
            .all(true)
            .filters(&filters)
            .build();
        let images = self
            .client
            .list_images(Some(options))
            .await
            .map_err(|error| operation_error("list images for prune", None, error))?;
        let candidates = prunable_image_ids(&images, preserved);
        let mut removed = Vec::new();
        for candidate in candidates {
            let options = RemoveImageOptionsBuilder::default().noprune(true).build();
            match self
                .client
                .remove_image(&candidate, Some(options), None)
                .await
            {
                Ok(_) => match self.client.inspect_image(&candidate).await {
                    Err(error) if is_not_found(&error) => {
                        removed.push(ArtifactDigest::new(candidate)?);
                    }
                    Ok(_) => {}
                    Err(error) => {
                        return Err(operation_error(
                            "verify pruned image",
                            Some(&candidate),
                            error,
                        ));
                    }
                },
                Err(error) if is_not_found(&error) => {}
                Err(error) => {
                    return Err(operation_error("prune image", Some(&candidate), error));
                }
            }
        }
        Ok(ArtifactPruneReport { removed })
    }
}

impl DockerRuntime {
    async fn registry_credential(
        &self,
        reference: &ArtifactReference,
    ) -> Result<Option<docker::auth::DockerCredentials>, ArtifactStoreError> {
        let parsed = reference.parsed()?;
        let registry = parsed.registry();
        Ok(self
            .registry_credentials
            .credential(registry)
            .await?
            .map(|credential| docker::auth::DockerCredentials {
                username: Some(credential.username().to_owned()),
                password: Some(credential.secret().expose().to_owned()),
                serveraddress: Some(registry.to_owned()),
                ..Default::default()
            }))
    }
}

impl DockerRuntime {
    async fn build_artifact(
        &self,
        request: &ArtifactBuildRequest,
        output: &dyn ArtifactBuildOutputSink,
    ) -> Result<ArtifactDigest, ArtifactStoreError> {
        let temporary_tag = temporary_build_tag();
        let options = build_options(request, &temporary_tag)?;
        let definition = validate_source(&request.source).await?;
        match &request.source {
            ArtifactSource::Directory { root, .. } => {
                let (context, task) = stream_directory_archive(root.clone(), definition);
                let result = consume_build_operation(
                    "build",
                    self.client
                        .build_image(options, None, Some(docker::body_try_stream(context))),
                    output,
                )
                .await;
                let archive_result = finish_archive_task(task).await;
                result?;
                archive_result?;
            }
            ArtifactSource::Archive { path, .. } => {
                let archive = tokio::fs::File::open(path).await.map_err(|error| {
                    ArtifactStoreError::Rejected {
                        message: format!("open Docker build archive `{}`: {error}", path.display()),
                    }
                })?;
                consume_build_operation(
                    "build",
                    self.client.build_image(
                        options,
                        None,
                        Some(docker::body_try_stream(file_stream(archive))),
                    ),
                    output,
                )
                .await?;
            }
        }

        let digest = self.local_digest(&temporary_tag).await?;
        for tag in &request.tags {
            self.tag(&digest, tag).await?;
        }
        if !request.tags.is_empty() {
            let cleanup = RemoveImageOptionsBuilder::default().noprune(true).build();
            let _ignored = self
                .client
                .remove_image(&temporary_tag, Some(cleanup), None)
                .await;
        }
        Ok(digest)
    }
    async fn local_digest(&self, reference: &str) -> Result<ArtifactDigest, ArtifactStoreError> {
        let inspect = self
            .client
            .inspect_image(reference)
            .await
            .map_err(|error| operation_error("inspect", Some(reference), error))?;
        inspect_digest(&inspect, reference)
    }

    async fn tag(
        &self,
        digest: &ArtifactDigest,
        destination: &ArtifactReference,
    ) -> Result<(), ArtifactStoreError> {
        let (repository, tag) = split_tag(destination.as_str())?;
        let options = TagImageOptionsBuilder::default()
            .repo(&repository)
            .tag(&tag)
            .build();
        self.client
            .tag_image(digest.as_str(), Some(options))
            .await
            .map_err(|error| operation_error("tag", Some(digest.as_str()), error))
    }
}

struct DockerArtifactStream {
    stream: Pin<Box<dyn Stream<Item = Result<Bytes, DockerError>> + Send>>,
    pending: Option<Bytes>,
}

#[async_trait]
impl ArtifactByteStream for DockerArtifactStream {
    async fn next(&mut self) -> Result<Option<Vec<u8>>, ArtifactStoreError> {
        loop {
            if let Some(chunk) = bounded_chunk(&mut self.pending) {
                return Ok(Some(chunk));
            }
            match self.stream.next().await.transpose() {
                Ok(Some(chunk)) if chunk.is_empty() => {}
                Ok(Some(chunk)) => self.pending = Some(chunk),
                Ok(None) => return Ok(None),
                Err(error) => return Err(stream_error("export", error)),
            }
        }
    }
}

pub(crate) fn bounded_chunk(pending: &mut Option<Bytes>) -> Option<Vec<u8>> {
    let mut bytes = pending.take()?;
    let count = bytes.len().min(TRANSFER_CHUNK_BYTES);
    let chunk = bytes.split_to(count).to_vec();
    if !bytes.is_empty() {
        *pending = Some(bytes);
    }
    Some(chunk)
}

async fn consume_operation<T>(
    operation: &str,
    reference: Option<&str>,
    stream: impl Stream<Item = Result<T, DockerError>>,
) -> Result<(), ArtifactStoreError> {
    futures_util::pin_mut!(stream);
    while let Some(item) = stream.next().await {
        item.map_err(|error| operation_error(operation, reference, error))?;
    }
    Ok(())
}

pub(crate) async fn consume_build_operation(
    operation: &str,
    stream: impl Stream<Item = Result<BuildInfo, DockerError>>,
    output: &dyn ArtifactBuildOutputSink,
) -> Result<(), ArtifactStoreError> {
    futures_util::pin_mut!(stream);
    while let Some(item) = stream.next().await {
        let info = item.map_err(|error| operation_error(operation, None, error))?;
        let wrote_stream = if let Some(message) = info.stream.as_deref() {
            write_docker_output(output, ArtifactBuildOutputStream::Stdout, message).await;
            true
        } else {
            false
        };
        if let Some(message) = info
            .error_detail
            .as_ref()
            .and_then(|detail| detail.message.as_deref())
        {
            write_docker_output(output, ArtifactBuildOutputStream::Stderr, message).await;
            return Err(ArtifactStoreError::Rejected {
                message: format!("Docker {operation}: {message}"),
            });
        }
        if !wrote_stream && let Some(status) = info.status.as_deref() {
            write_docker_output(output, ArtifactBuildOutputStream::Stdout, status).await;
        }
    }
    Ok(())
}

async fn write_docker_output(
    output: &dyn ArtifactBuildOutputSink,
    stream: ArtifactBuildOutputStream,
    message: &str,
) {
    for line in message.split_inclusive('\n') {
        let line = line.strip_suffix('\n').unwrap_or(line);
        let line = line.strip_suffix('\r').unwrap_or(line);
        output.write(stream, line.as_bytes().to_vec()).await;
    }
}

async fn validate_source(source: &ArtifactSource) -> Result<PathBuf, ArtifactStoreError> {
    let (path, definition, directory) = match source {
        ArtifactSource::Directory { root, definition } => (root, definition, true),
        ArtifactSource::Archive { path, definition } => (path, definition, false),
    };
    validate_absolute_path(path)?;
    let metadata = tokio::fs::symlink_metadata(path)
        .await
        .map_err(|error| rejected_path("inspect", path, error))?;
    if (directory && !metadata.file_type().is_dir())
        || (!directory && !metadata.file_type().is_file())
    {
        return Err(ArtifactStoreError::Rejected {
            message: format!(
                "Docker build source `{}` must be a regular {}",
                path.display(),
                if directory { "directory" } else { "file" }
            ),
        });
    }
    let canonical = tokio::fs::canonicalize(path)
        .await
        .map_err(|error| rejected_path("canonicalize", path, error))?;
    if canonical != *path {
        return Err(ArtifactStoreError::Rejected {
            message: format!(
                "Docker build source `{}` traverses a symbolic link",
                path.display()
            ),
        });
    }
    let definition = PathBuf::from(definition_text(definition)?);
    if directory {
        let definition_path = path.join(&definition);
        let canonical_definition = tokio::fs::canonicalize(&definition_path)
            .await
            .map_err(|error| rejected_path("resolve build definition", &definition_path, error))?;
        let metadata = tokio::fs::metadata(&canonical_definition)
            .await
            .map_err(|error| rejected_path("inspect build definition", &definition_path, error))?;
        if !canonical_definition.starts_with(path) || !metadata.is_file() {
            return Err(ArtifactStoreError::Rejected {
                message: format!(
                    "Docker build definition `{}` must resolve to a file inside the context",
                    definition.display()
                ),
            });
        }
    }
    Ok(definition)
}

fn validate_absolute_path(path: &Path) -> Result<(), ArtifactStoreError> {
    if !path.is_absolute()
        || !path
            .components()
            .all(|component| matches!(component, Component::RootDir | Component::Normal(_)))
    {
        Err(ArtifactStoreError::Rejected {
            message: "Docker build source must be a normalized absolute path".to_owned(),
        })
    } else {
        Ok(())
    }
}

fn rejected_path(operation: &str, path: &Path, error: std::io::Error) -> ArtifactStoreError {
    ArtifactStoreError::Rejected {
        message: format!("{operation} `{}`: {error}", path.display()),
    }
}

fn temporary_build_tag() -> String {
    let sequence = BUILD_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    format!("{TEMPORARY_TAG_PREFIX}{}-{sequence}", std::process::id())
}
