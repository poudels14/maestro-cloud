use std::time::Duration;

use async_trait::async_trait;
use kernel_api::Timestamp;

/// Whether an open pull request is eligible for a preview.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PullRequestReadiness {
    /// The pull request is ready for review and preview deployment.
    Ready,
    /// The pull request remains a draft and must not receive a preview.
    Draft,
}

/// Pull-request state returned by the source-control seam.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PullRequest {
    /// Repository-local pull-request number.
    pub number: u64,
    /// Human-readable pull-request title.
    pub title: String,
    /// Draft or ready state.
    pub readiness: PullRequestReadiness,
    /// Creation time used for global oldest-first quota selection.
    pub created_at: Timestamp,
    /// Mutable source branch name.
    pub head_reference: String,
    /// Immutable current head revision.
    pub head_revision: String,
    /// Full owner/name identity of the head repository.
    pub head_repository: Option<String>,
}

/// Matchable source-control transport failures.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PullRequestApiError {
    /// A temporary transport or server failure.
    #[error("pull-request API is unavailable: {message}")]
    Unavailable {
        /// Sanitized failure detail.
        message: String,
    },
    /// The repository is rate-limited until a known retry delay.
    #[error("pull-request API rate limited for {}s", retry_after.as_secs())]
    RateLimited {
        /// Minimum delay before another request.
        retry_after: Duration,
    },
    /// Authentication or repository configuration was permanently rejected.
    #[error("pull-request API rejected the request: {message}")]
    Rejected {
        /// Sanitized failure detail.
        message: String,
    },
}

/// Source-control boundary used by preview discovery and sticky feedback.
#[async_trait]
pub trait PullRequestApi: Send + Sync {
    /// Lists every open pull request, including drafts and forks.
    async fn list_open(
        &self,
        owner: &str,
        repository: &str,
    ) -> Result<Vec<PullRequest>, PullRequestApiError>;

    /// Creates or updates the native GitHub deployment for one preview revision.
    async fn publish_deployment(
        &self,
        owner: &str,
        repository: &str,
        deployment: &PullRequestDeployment,
    ) -> Result<(), PullRequestApiError>;
}

/// Native GitHub deployment state projected from the Maestro preview lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PullRequestDeploymentState {
    /// The preview is waiting for cluster capacity.
    Queued,
    /// The preview is building or rolling out.
    InProgress,
    /// The current pull-request revision is serving traffic.
    Success,
    /// The current pull-request revision failed to deploy.
    Failure,
    /// The transient preview environment no longer exists.
    Inactive,
}

impl PullRequestDeploymentState {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::InProgress => "in_progress",
            Self::Success => "success",
            Self::Failure => "failure",
            Self::Inactive => "inactive",
        }
    }
}

/// Desired native GitHub deployment and latest status for one preview revision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PullRequestDeployment {
    /// Immutable pull-request head revision deployed by Maestro.
    pub head_revision: String,
    /// Stable GitHub environment identity shared by revisions of one preview.
    pub environment: String,
    /// Latest deployment state reported to GitHub.
    pub state: PullRequestDeploymentState,
    /// Short human-readable status rendered by GitHub.
    pub description: String,
    /// Public preview URL once the environment is ready.
    pub environment_url: Option<String>,
}
