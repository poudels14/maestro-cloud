use std::collections::BTreeMap;

use async_trait::async_trait;
use kernel_api::SecretValue;

/// Resolves an external key/value source at the execution boundary that consumes it.
#[async_trait]
pub trait ValueSourceResolver: Send + Sync {
    /// Fetches and parses the current values referenced by `source`.
    async fn resolve(
        &self,
        source: &str,
    ) -> Result<BTreeMap<String, SecretValue>, ValueSourceError>;
}

/// Failure to fetch or decode an external value source.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ValueSourceError {
    /// The source may become available on a later reconciliation attempt.
    #[error("external value source is unavailable: {message}")]
    Unavailable { message: String },
    /// The reference or its current contents cannot be consumed safely.
    #[error("external value source was rejected: {message}")]
    Rejected { message: String },
}
