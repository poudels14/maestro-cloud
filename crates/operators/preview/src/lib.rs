//! Pull-request preview derivation for Maestro services.
//!
//! This crate reads typed Preview, Service, and IngressRoute resources and
//! writes only fenced derived resources. GitHub transport and daemon
//! composition remain outside the derivation core.

mod github;
mod github_client;
mod reconciler;
mod repository;
mod resource;
mod snapshot;
mod source_plan;
mod source_reconciler;
mod source_snapshot;
mod source_writer;
mod writer;

pub use github::{PullRequest, PullRequestApi, PullRequestApiError, PullRequestReadiness};
pub use github_client::{GithubClientError, GithubPullRequestClient};
pub use reconciler::{PreviewError, PreviewReconciler, PreviewSettings};
pub use source_plan::{
    PreviewFeedback, PreviewFeedbackKind, PreviewSourceDiagnostic, PreviewSourcePlan,
    PreviewSourcePlanError, RepositoryPullRequests, plan_preview_sources,
};
pub use source_reconciler::{PreviewSourceError, PreviewSourceReconciler, PreviewSourceSettings};

#[cfg(test)]
mod tests;
