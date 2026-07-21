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

    /// Creates or replaces one marker-keyed pull-request comment.
    async fn upsert_comment(
        &self,
        owner: &str,
        repository: &str,
        pull_request_number: u64,
        comment_key: &str,
        body: &str,
    ) -> Result<(), PullRequestApiError>;
}
