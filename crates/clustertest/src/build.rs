use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::FixtureName;

/// An immutable source revision observed by a build scenario.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct FixtureSourceRevision(String);

impl FixtureSourceRevision {
    /// Creates a scenario-local source revision.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Returns the source revision as text for driver translation.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A build-backed service applied through the implementation's public path.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BuildServiceFixture {
    /// Stable service name.
    pub name: FixtureName,
    /// Git repository cloned by the build source adapter.
    pub repository: String,
    /// Mutable Git branch watched for new revisions.
    pub branch: String,
    /// Build definition path relative to the source root.
    pub dockerfile: String,
    /// Whether the service watches its branch for changes.
    pub watch: bool,
    /// Public build arguments.
    pub arguments: BTreeMap<String, String>,
    /// Secret names supplied to the build without exposing their values.
    pub secret_names: BTreeSet<String>,
}

/// Terminal or in-flight state of one persisted build.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BuildCompletion {
    /// Source preparation or artifact execution is still in progress.
    Running,
    /// An immutable artifact was produced.
    Succeeded,
    /// A terminal build error was persisted.
    Failed,
    /// The build was canceled before producing an artifact.
    Canceled,
}

/// Persisted state relevant to one build attempt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BuildSnapshot {
    /// Immutable source revision persisted before artifact execution.
    pub source_revision: Option<FixtureSourceRevision>,
    /// Current build completion state.
    pub completion: BuildCompletion,
    /// Immutable image digest produced by the artifact backend.
    pub image_digest: Option<String>,
}

/// Sanitized artifact-backend request evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactBuildSnapshot {
    /// Public build arguments delivered to the backend.
    pub arguments: BTreeMap<String, String>,
    /// Secret names delivered to the backend; values are intentionally absent.
    pub secret_names: BTreeSet<String>,
}

/// Observable build and rollout state after a quiet reconciliation window.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BuildRolloutSnapshot {
    /// Revision most recently persisted by Git watch.
    pub watched_revision: Option<FixtureSourceRevision>,
    /// Source revision consumed by the active deployment.
    pub active_revision: Option<FixtureSourceRevision>,
    /// Builds retained by the control plane.
    pub builds: Vec<BuildSnapshot>,
    /// Sanitized artifact requests made during this scenario.
    pub artifact_builds: Vec<ArtifactBuildSnapshot>,
}

/// Drives build-backed services through reusable acceptance scenarios.
#[async_trait]
pub trait BuildCluster: Send {
    /// A matchable error returned by build driver operations.
    type Error: Debug + std::fmt::Display + Send + Sync + 'static;

    /// Changes the Git revision returned by the source-control seam.
    async fn set_remote_revision(
        &mut self,
        revision: FixtureSourceRevision,
    ) -> Result<(), Self::Error>;

    /// Applies a build-backed service through the implementation's public path.
    async fn apply_build_service(
        &mut self,
        fixture: BuildServiceFixture,
    ) -> Result<(), Self::Error>;

    /// Waits for a quiet reconciliation window and returns sanitized build state.
    async fn await_build_converged(&mut self) -> Result<BuildRolloutSnapshot, Self::Error>;
}
