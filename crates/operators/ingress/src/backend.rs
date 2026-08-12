use async_trait::async_trait;

use crate::{BackendChange, IngressBlocklistChange};

/// Side-effect boundary for publishing one service's ingress configuration.
#[async_trait]
pub trait IngressBackend: Send + Sync {
    /// Idempotently converges the active and removable traffic generations.
    ///
    /// Implementations must stage all backend services before switching routers,
    /// and must tolerate replay after cancellation or a later store conflict.
    async fn apply(&self, change: &BackendChange) -> Result<(), IngressBackendError>;

    /// Idempotently replaces the cluster-wide client-address rejection policy.
    async fn apply_blocklist(
        &self,
        change: &IngressBlocklistChange,
    ) -> Result<(), IngressBackendError>;
}

/// Matchable ingress publication failure retried by the controller runtime.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("ingress backend publication failed: {message}")]
pub struct IngressBackendError {
    message: String,
    terminal_reason: Option<&'static str>,
}

impl IngressBackendError {
    /// Creates an error without exposing backend-specific types to the operator.
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            terminal_reason: None,
        }
    }

    /// Creates a non-retryable backend error caused by desired configuration.
    pub fn terminal(reason: &'static str, message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            terminal_reason: Some(reason),
        }
    }

    /// Returns operator-facing backend failure detail.
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Returns the stable reason when retrying cannot change the outcome.
    pub fn terminal_reason(&self) -> Option<&'static str> {
        self.terminal_reason
    }
}
