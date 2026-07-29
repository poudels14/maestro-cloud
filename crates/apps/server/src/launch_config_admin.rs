use cluster::PreviewLaunchConfig;
use kernel_api::{ClusterId, NodeId};

/// Node-local protected launch-document mutation boundary.
#[async_trait::async_trait]
pub trait LaunchConfigAdmin: Send + Sync {
    /// Cluster identity embedded in the protected launch document.
    fn cluster_id(&self) -> &ClusterId;

    /// Local node identity embedded in the protected launch document.
    fn node_id(&self) -> &NodeId;

    /// Replaces the preview integration and reports whether persistence changed.
    async fn replace_preview(
        &self,
        preview: PreviewLaunchConfig,
    ) -> Result<bool, LaunchConfigAdminError>;
}

/// Sanitized node-local launch-document update failure.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{message}")]
pub struct LaunchConfigAdminError {
    message: String,
}

impl LaunchConfigAdminError {
    /// Creates one failure safe to return to an authenticated operator.
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}
