use async_trait::async_trait;
use kernel_api::{BuildId, BuildSource, SecretValue};
use runtime::ArtifactSource;

/// Materialized source tree and the immutable revision it represents.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedBuildSource {
    /// Directory or archive ready for the artifact backend.
    pub artifact_source: ArtifactSource,
    /// Immutable source identity resolved by the source backend.
    pub revision: String,
}

/// Matchable source preparation failure.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum BuildSourceError {
    /// Source storage or source control is temporarily unavailable.
    #[error("build source is temporarily unavailable: {message}")]
    Unavailable {
        /// Detail safe to persist in a status condition.
        message: String,
    },
    /// The requested source cannot produce a valid build on retry.
    #[error("build source was rejected: {message}")]
    Rejected {
        /// Detail safe to persist in a status condition.
        message: String,
    },
}

impl BuildSourceError {
    /// Constructs a transient source error with status-safe detail.
    pub fn unavailable(message: impl Into<String>) -> Self {
        Self::Unavailable {
            message: message.into(),
        }
    }

    /// Constructs a permanent source error with status-safe detail.
    pub fn rejected(message: impl Into<String>) -> Self {
        Self::Rejected {
            message: message.into(),
        }
    }
}

/// Source-control and uploaded-archive materialization boundary.
#[async_trait]
pub trait BuildSourceProvider: Send + Sync {
    /// Materializes source, resolving the requested revision on the first call.
    ///
    /// Once `resolved_revision` is present, the returned source must represent
    /// that exact immutable revision. This makes retries immune to a moving
    /// branch or tag.
    async fn prepare(
        &self,
        build_id: &BuildId,
        source: &BuildSource,
        resolved_revision: Option<&str>,
        github_token: Option<&SecretValue>,
    ) -> Result<PreparedBuildSource, BuildSourceError>;

    /// Removes source material owned only by one Build resource.
    async fn cleanup(&self, build_id: &BuildId) -> Result<(), BuildSourceError>;
}

/// Remote immutable-revision lookup used by the service build watcher.
#[async_trait]
pub trait BuildRevisionResolver: Send + Sync {
    /// Resolves a watched Git source to its current immutable revision.
    async fn resolve_revision(
        &self,
        source: &BuildSource,
        github_token: Option<&SecretValue>,
    ) -> Result<Option<String>, BuildSourceError>;
}
