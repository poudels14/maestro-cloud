use async_trait::async_trait;

use crate::BackendChange;

/// Side-effect boundary for publishing one service's ingress configuration.
#[async_trait]
pub trait IngressBackend: Send + Sync {
    /// Idempotently converges the active and removable traffic generations.
    ///
    /// Implementations must stage all backend services before switching routers,
    /// and must tolerate replay after cancellation or a later store conflict.
    async fn apply(&self, change: &BackendChange) -> Result<(), IngressBackendError>;
}

/// Matchable ingress publication failure retried by the controller runtime.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("ingress backend publication failed: {message}")]
pub struct IngressBackendError {
    message: String,
}

impl IngressBackendError {
    /// Creates an error without exposing backend-specific types to the operator.
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    /// Returns operator-facing backend failure detail.
    pub fn message(&self) -> &str {
        &self.message
    }
}
