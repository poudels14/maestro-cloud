use std::io;

/// User-facing CLI failure with secret-safe context.
#[derive(Debug, thiserror::Error)]
pub enum CliError {
    #[error("invalid input: {message}")]
    InvalidInput { message: String },
    #[error("invalid contexts file: {message}")]
    InvalidContexts { message: String },
    #[error("not found: {message}")]
    NotFound { message: String },
    #[error("{action}: {source}")]
    Io {
        action: String,
        #[source]
        source: io::Error,
    },
    #[error("{action}: {source}")]
    Json {
        action: String,
        #[source]
        source: serde_json::Error,
    },
    #[error("failed to sign operator token")]
    Token(#[source] jsonwebtoken::errors::Error),
}

impl CliError {
    pub(crate) fn invalid_input(message: impl Into<String>) -> Self {
        Self::InvalidInput {
            message: message.into(),
        }
    }

    pub(crate) fn invalid_contexts(message: impl Into<String>) -> Self {
        Self::InvalidContexts {
            message: message.into(),
        }
    }

    pub(crate) fn not_found(message: impl Into<String>) -> Self {
        Self::NotFound {
            message: message.into(),
        }
    }

    pub(crate) fn io(action: impl Into<String>, source: io::Error) -> Self {
        Self::Io {
            action: action.into(),
            source,
        }
    }

    pub(crate) fn json(action: impl Into<String>, source: serde_json::Error) -> Self {
        Self::Json {
            action: action.into(),
            source,
        }
    }
}
