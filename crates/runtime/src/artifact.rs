use std::collections::BTreeMap;
use std::path::PathBuf;

use async_trait::async_trait;
use kernel_api::SecretValue;
use serde::{Deserialize, Serialize};

/// Image name, tag, or digest accepted by an artifact backend.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct ArtifactReference(String);

impl ArtifactReference {
    /// Constructs a non-empty artifact reference.
    pub fn new(value: impl Into<String>) -> Result<Self, ArtifactStoreError> {
        let value = value.into();
        if value.trim().is_empty() {
            Err(ArtifactStoreError::InvalidReference)
        } else {
            Ok(Self(value))
        }
    }

    /// Returns the backend artifact reference.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for ArtifactReference {
    type Error = ArtifactStoreError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<ArtifactReference> for String {
    fn from(value: ArtifactReference) -> Self {
        value.0
    }
}

/// Immutable content digest resolved by an artifact backend.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct ArtifactDigest(String);

impl ArtifactDigest {
    /// Constructs a non-empty backend digest.
    pub fn new(value: impl Into<String>) -> Result<Self, ArtifactStoreError> {
        let value = value.into();
        if value.trim().is_empty() {
            Err(ArtifactStoreError::InvalidDigest)
        } else {
            Ok(Self(value))
        }
    }

    /// Returns the immutable backend digest.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for ArtifactDigest {
    type Error = ArtifactStoreError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<ArtifactDigest> for String {
    fn from(value: ArtifactDigest) -> Self {
        value.0
    }
}

/// Source tree supplied to an artifact build.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArtifactSource {
    /// Existing source directory and definition path relative to it.
    Directory {
        /// Absolute source directory.
        root: PathBuf,
        /// Relative Dockerfile or backend definition path.
        definition: PathBuf,
    },
    /// Existing source archive and definition path within it.
    Archive {
        /// Absolute archive path.
        path: PathBuf,
        /// Definition path inside the archive.
        definition: PathBuf,
    },
}

/// Immutable artifact build request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactBuildRequest {
    /// Directory or archive source.
    pub source: ArtifactSource,
    /// Non-secret build arguments.
    pub arguments: BTreeMap<String, String>,
    /// Secret build arguments passed through the backend's protected secret mechanism.
    pub secrets: BTreeMap<String, SecretValue>,
    /// References assigned only after a successful build.
    pub tags: Vec<ArtifactReference>,
}

/// Pull-based byte stream used for bounded artifact transfer.
#[async_trait]
pub trait ArtifactByteStream: Send {
    /// Returns the next byte chunk, or `None` at a clean archive boundary.
    async fn next(&mut self) -> Result<Option<Vec<u8>>, ArtifactStoreError>;
}

/// Artifact retention selection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArtifactPrunePolicy {
    /// Remove unreferenced artifacts while preserving the provided immutable digests.
    Preserve(Vec<ArtifactDigest>),
}

/// Result of one artifact prune operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactPruneReport {
    /// Digests removed by the backend in stable order.
    pub removed: Vec<ArtifactDigest>,
}

/// Matchable artifact backend failure.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ArtifactStoreError {
    /// An empty image reference is never meaningful.
    #[error("artifact reference cannot be empty")]
    InvalidReference,
    /// An empty immutable digest is never meaningful.
    #[error("artifact digest cannot be empty")]
    InvalidDigest,
    /// Requested artifact does not exist locally or remotely.
    #[error("artifact `{reference}` does not exist")]
    NotFound {
        /// Missing reference or digest.
        reference: String,
    },
    /// Backend is temporarily unavailable and the caller may retry.
    #[error("artifact backend is unavailable: {message}")]
    Unavailable {
        /// Backend detail safe to log.
        message: String,
    },
    /// Backend rejected a request that retrying cannot repair.
    #[error("artifact backend rejected the operation: {message}")]
    Rejected {
        /// Backend detail safe to surface.
        message: String,
    },
    /// Streaming transfer failed and must resume from a higher-level checkpoint.
    #[error("artifact transfer failed: {message}")]
    Stream {
        /// Backend detail safe to log.
        message: String,
    },
}

/// Runtime-native image and artifact operations.
#[async_trait]
pub trait ArtifactStore: Send + Sync {
    /// Builds source and returns its immutable digest only after backend completion.
    async fn build(
        &self,
        request: &ArtifactBuildRequest,
    ) -> Result<ArtifactDigest, ArtifactStoreError>;

    /// Pulls a reference into local backend storage and returns its immutable digest.
    async fn pull(
        &self,
        reference: &ArtifactReference,
    ) -> Result<ArtifactDigest, ArtifactStoreError>;

    /// Pushes an immutable local artifact to a remote reference.
    async fn push(
        &self,
        digest: &ArtifactDigest,
        destination: &ArtifactReference,
    ) -> Result<(), ArtifactStoreError>;

    /// Resolves a local or remote reference without changing workload state.
    async fn resolve_digest(
        &self,
        reference: &ArtifactReference,
    ) -> Result<ArtifactDigest, ArtifactStoreError>;

    /// Ensures a referenced artifact is present in the local runtime store.
    ///
    /// Backends may avoid a remote transfer when the reference already resolves
    /// locally. The default preserves compatibility for stores whose resolver
    /// already materializes missing content.
    async fn ensure_local(
        &self,
        reference: &ArtifactReference,
    ) -> Result<ArtifactDigest, ArtifactStoreError> {
        self.resolve_digest(reference).await
    }

    /// Opens a bounded byte stream for one immutable local artifact.
    async fn export(
        &self,
        digest: &ArtifactDigest,
    ) -> Result<Box<dyn ArtifactByteStream>, ArtifactStoreError>;

    /// Imports a complete byte stream and commits the artifact only at clean end-of-stream.
    async fn import(
        &self,
        source: Box<dyn ArtifactByteStream>,
    ) -> Result<ArtifactDigest, ArtifactStoreError>;

    /// Removes only artifacts selected by the explicit retention policy.
    async fn prune(
        &self,
        policy: &ArtifactPrunePolicy,
    ) -> Result<ArtifactPruneReport, ArtifactStoreError>;
}

#[cfg(any(feature = "containerd", feature = "docker"))]
pub(crate) fn workload_artifact_error(
    reference: &ArtifactReference,
    error: ArtifactStoreError,
) -> crate::RuntimeError {
    match error {
        ArtifactStoreError::Unavailable { message } | ArtifactStoreError::Stream { message } => {
            crate::RuntimeError::Unavailable {
                message: format!(
                    "failed to materialize workload artifact `{}`: {message}",
                    reference.as_str()
                ),
            }
        }
        error => crate::RuntimeError::Rejected {
            message: format!(
                "failed to materialize workload artifact `{}`: {error}",
                reference.as_str()
            ),
        },
    }
}
