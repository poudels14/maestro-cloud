use async_trait::async_trait;

use crate::FirewallBundle;

/// Side-effect boundary for atomically publishing complete per-node rulesets.
#[async_trait]
pub trait FirewallBackend: Send + Sync {
    /// Idempotently applies the exact complete bundle before policy acknowledgement.
    ///
    /// Implementations must tolerate replay after cancellation or a later store
    /// conflict. Each node ruleset is a complete owned-table transaction, never
    /// a sequence of delete-then-create mutations.
    async fn apply(&self, bundle: &FirewallBundle) -> Result<(), FirewallBackendError>;
}

/// Matchable backend failure retried by the shared controller runtime.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("firewall backend application failed: {message}")]
pub struct FirewallBackendError {
    message: String,
}

impl FirewallBackendError {
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
