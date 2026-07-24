use std::sync::Arc;
use std::time::Duration;

use cluster::PreviewLaunchConfig;
use preview::{GithubPullRequestClient, PreviewSettings, PreviewSourceSettings, PullRequestApi};

use crate::PreviewOperatorSettings;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(15);

/// Validates DNS, credential, and quota bounds without contacting GitHub.
pub(crate) fn validate_preview(config: &PreviewLaunchConfig) -> Result<(), PreviewLaunchError> {
    configure_preview(config).map(|_| ())
}

pub(crate) fn configure_preview(
    config: &PreviewLaunchConfig,
) -> Result<ConfiguredPreview, PreviewLaunchError> {
    if config.max_concurrent_previews == 0 {
        return Err(PreviewLaunchError::EmptyQuota);
    }
    let derivation = PreviewSettings::new(config.domain.clone())?;
    let pull_requests = Arc::new(GithubPullRequestClient::new(
        config.github_token.clone(),
        REQUEST_TIMEOUT,
    )?);
    Ok(ConfiguredPreview {
        settings: PreviewOperatorSettings {
            source: PreviewSourceSettings {
                poll_interval: Duration::from_secs(60),
                max_concurrent_previews: config.max_concurrent_previews,
                initial_backoff: Duration::from_secs(30),
                max_backoff: Duration::from_secs(5 * 60),
            },
            derivation,
        },
        pull_requests,
    })
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
