//! Pull-request preview derivation for Maestro services.
//!
//! This crate reads typed Preview, Service, and IngressRoute resources and
//! writes only fenced derived resources. GitHub transport and daemon
//! composition remain outside the derivation core.

mod github;
mod reconciler;
mod resource;
mod snapshot;
mod source_plan;
mod writer;

pub use github::{PullRequest, PullRequestApi, PullRequestApiError, PullRequestReadiness};
pub use reconciler::{PreviewError, PreviewReconciler, PreviewSettings};
pub use source_plan::{
    PreviewFeedback, PreviewFeedbackKind, PreviewSourceDiagnostic, PreviewSourcePlan,
    PreviewSourcePlanError, RepositoryPullRequests, plan_preview_sources,
};

#[cfg(test)]
mod tests;
