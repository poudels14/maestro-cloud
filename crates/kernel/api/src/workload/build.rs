use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::BuildTemplate;
use crate::{BuildId, Condition, DeploymentId, Object, ServiceId};

/// Persisted phase of an artifact build.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum BuildPhase {
    /// Waiting for a build worker.
    Queued,
    /// Fetching and preparing source material.
    Preparing,
    /// Running the configured build backend.
    Building,
    /// The immutable artifact is available.
    Succeeded,
    /// The build ended with a terminal error.
    Failed,
    /// The build was canceled before completion.
    Canceled,
}

impl BuildPhase {
    /// Whether the build state machine permits a transition.
    pub fn can_transition_to(self, target: Self) -> bool {
        self == target
            || matches!(
                (self, target),
                (Self::Queued, Self::Preparing | Self::Canceled)
                    | (
                        Self::Preparing,
                        Self::Building | Self::Failed | Self::Canceled
                    )
                    | (
                        Self::Building,
                        Self::Succeeded | Self::Failed | Self::Canceled
                    )
            )
    }
}

/// Immutable Git commit selected for a deployment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct GitCommit {
    /// Full commit object identifier.
    pub revision: String,
    /// First line of the commit message.
    pub title: String,
}

/// Desired build source and service association.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct BuildSpec {
    /// Service requesting the build.
    pub service_id: ServiceId,
    /// Deployment that will consume the build.
    pub deployment_id: DeploymentId,
    /// Immutable build template copied from the service.
    pub template: BuildTemplate,
}

/// Observed build phase and immutable artifact identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct BuildStatus {
    /// Current build phase.
    pub phase: BuildPhase,
    /// Immutable image digest produced by a successful build.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub image_digest: Option<String>,
    /// Source revision resolved by the build backend.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_revision: Option<String>,
    /// Git commit title resolved with `source_revision`, when the source is Git.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_title: Option<String>,
    /// Generic progress and failure evidence.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<Condition>,
}

/// An artifact build resource.
pub type Build = Object<BuildId, BuildSpec, BuildStatus>;
