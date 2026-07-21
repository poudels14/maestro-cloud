use std::fmt::Debug;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::{FixtureName, FixtureSourceRevision};

/// A preview-enabled build service and pull request used by acceptance tests.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreviewServiceFixture {
    /// Stable base service name.
    pub name: FixtureName,
    /// GitHub repository cloned for the base and preview builds.
    pub repository: String,
    /// Mutable base branch.
    pub branch: String,
    /// Repository-local pull-request number.
    pub pull_request_number: u64,
    /// Grace period retained after the pull request closes.
    pub close_grace_period_secs: u64,
}

/// Observable preview lifecycle phase after a quiet reconciliation window.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PreviewCompletion {
    /// Derived resources are still converging.
    Pending,
    /// The derived service is serving the current pull-request revision.
    Active,
    /// The pull request closed and its grace period has not elapsed.
    Closing,
    /// Derived resources were removed after grace or lifetime expiry.
    Expired,
}

/// Preview state visible through implementation-independent resource projections.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreviewRolloutSnapshot {
    /// Stable Preview resource identity, absent after final collection.
    pub preview_id: Option<FixtureName>,
    /// Current Preview lifecycle phase, absent after final collection.
    pub completion: Option<PreviewCompletion>,
    /// Stable derived Service identity, absent after final collection.
    pub service_id: Option<FixtureName>,
    /// Derived Service generation.
    pub service_generation: Option<u64>,
    /// Git revision desired by the derived Service.
    pub desired_revision: Option<FixtureSourceRevision>,
    /// Git revision consumed by the active derived deployment.
    pub active_revision: Option<FixtureSourceRevision>,
    /// Public preview hostname, absent after route collection.
    pub public_host: Option<String>,
}

/// Drives pull-request previews through reusable acceptance scenarios.
#[async_trait]
pub trait PreviewCluster: Send {
    /// A matchable error returned by preview driver operations.
    type Error: Debug + std::fmt::Display + Send + Sync + 'static;

    /// Applies a preview-enabled base service through the public configuration path.
    async fn apply_preview_service(
        &mut self,
        fixture: PreviewServiceFixture,
    ) -> Result<(), Self::Error>;

    /// Opens a same-repository pull request at an immutable head revision.
    async fn open_pull_request(
        &mut self,
        revision: FixtureSourceRevision,
    ) -> Result<(), Self::Error>;

    /// Advances the open pull request to a new immutable head revision.
    async fn push_pull_request(
        &mut self,
        revision: FixtureSourceRevision,
    ) -> Result<(), Self::Error>;

    /// Removes the pull request from the source provider's open set.
    async fn close_pull_request(&mut self) -> Result<(), Self::Error>;

    /// Advances the implementation clock beyond the configured close grace period.
    async fn elapse_close_grace(&mut self) -> Result<(), Self::Error>;

    /// Waits for a quiet reconciliation window and returns projected preview state.
    async fn await_preview_converged(&mut self) -> Result<PreviewRolloutSnapshot, Self::Error>;
}
