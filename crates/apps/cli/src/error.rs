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
    #[error("{action}: {source}")]
    Transport {
        action: String,
        #[source]
        source: reqwest::Error,
    },
    #[error("API request failed with HTTP {status} ({code}): {message}")]
    Api {
        status: u16,
        code: String,
        message: String,
    },
    #[error("API response exceeded the {limit_bytes}-byte client limit")]
    ResponseTooLarge { limit_bytes: usize },
    #[error("invalid API response: {message}")]
    InvalidApiResponse { message: String },
    #[error("{action}: {message}")]
    Cluster { action: String, message: String },
    #[error("interactive exec failed: {message}")]
    Exec { message: String },
    #[error("remote command exited with status {code}")]
    ExecExit { code: i32 },
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

    pub(crate) fn transport(action: impl Into<String>, source: reqwest::Error) -> Self {
        Self::Transport {
            action: action.into(),
            source,
        }
    }

    pub(crate) fn invalid_api_response(message: impl Into<String>) -> Self {
        Self::InvalidApiResponse {
            message: message.into(),
        }
    }

    pub(crate) fn cluster(action: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Cluster {
            action: action.into(),
            message: message.into(),
        }
    }

    pub(crate) fn exec(message: impl Into<String>) -> Self {
        Self::Exec {
            message: message.into(),
        }
    }

    /// Process status the CLI executable should return for this failure.
    pub fn process_exit_code(&self) -> i32 {
        match self {
            Self::ExecExit { code } => (*code).clamp(1, 255),
            _ => 1,
        }
    }

    /// Returns whether the executable should print this error before exiting.
    pub fn should_report(&self) -> bool {
        !matches!(self, Self::ExecExit { .. })
    }
}
