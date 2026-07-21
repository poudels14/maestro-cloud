use std::sync::Arc;
use std::time::Duration;

use kernel_api::SecretValue;
use preview::{GithubPullRequestClient, PreviewSettings, PreviewSourceSettings, PullRequestApi};
use serde::{Deserialize, Serialize};

use crate::PreviewOperatorSettings;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(15);

/// Optional cluster-wide pull-request preview integration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PreviewLaunchConfig {
    /// DNS suffix used for stable preview hostnames.
    pub domain: String,
    /// GitHub token with pull-request read and issue-comment write access.
    pub github_token: SecretValue,
    /// Maximum previews retained cluster-wide, including close grace periods.
    pub max_concurrent_previews: usize,
}

impl PreviewLaunchConfig {
    /// Validates DNS, credential, and quota bounds without contacting GitHub.
    pub fn validate(&self) -> Result<(), PreviewLaunchError> {
        self.configure().map(|_| ())
    }

    pub(crate) fn configure(&self) -> Result<ConfiguredPreview, PreviewLaunchError> {
        if self.max_concurrent_previews == 0 {
            return Err(PreviewLaunchError::EmptyQuota);
        }
        let derivation = PreviewSettings::new(self.domain.clone())?;
        let pull_requests = Arc::new(GithubPullRequestClient::new(
            self.github_token.clone(),
            REQUEST_TIMEOUT,
        )?);
        Ok(ConfiguredPreview {
            settings: PreviewOperatorSettings {
                source: PreviewSourceSettings {
                    poll_interval: Duration::from_secs(60),
                    max_concurrent_previews: self.max_concurrent_previews,
                    initial_backoff: Duration::from_secs(30),
                    max_backoff: Duration::from_secs(5 * 60),
                },
                derivation,
            },
            pull_requests,
        })
    }
}

pub(crate) struct ConfiguredPreview {
    pub(crate) settings: PreviewOperatorSettings,
    pub(crate) pull_requests: Arc<dyn PullRequestApi>,
}

/// Invalid preview integration configuration.
#[derive(Debug, thiserror::Error)]
pub enum PreviewLaunchError {
    /// A zero quota can never admit a preview.
    #[error("preview maximum concurrent previews must be greater than zero")]
    EmptyQuota,
    /// The preview domain was not a valid DNS suffix.
    #[error(transparent)]
    Preview(#[from] preview::PreviewError),
    /// GitHub authentication or HTTP client construction was invalid.
    #[error(transparent)]
    Github(#[from] preview::GithubClientError),
}
