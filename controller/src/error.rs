use std::fmt::{Display, Formatter};

#[derive(Debug)]
pub enum Error {
    Io(std::io::Error),
    Http(reqwest::Error),
    Json(serde_json::Error),
    Json5(json5::Error),
    InvalidInput(String),
    InvalidConfig(String),
    NotFound(String),
    Conflict(String),
    External(String),
    Internal(String),
    Message(String),
}

impl Error {
    pub fn invalid_input(message: impl Into<String>) -> Self {
        Self::InvalidInput(message.into())
    }

    pub fn invalid_config(message: impl Into<String>) -> Self {
        Self::InvalidConfig(message.into())
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        Self::NotFound(message.into())
    }

    pub fn conflict(message: impl Into<String>) -> Self {
        Self::Conflict(message.into())
    }

    pub fn external(message: impl Into<String>) -> Self {
        Self::External(message.into())
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::Internal(message.into())
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(err) => write!(f, "io error: {err}"),
            Self::Http(err) => write!(f, "http error: {err}"),
            Self::Json(err) => write!(f, "json error: {err}"),
            Self::Json5(err) => write!(f, "json5 error: {err}"),
            Self::InvalidInput(message) => write!(f, "invalid input: {message}"),
            Self::InvalidConfig(message) => write!(f, "invalid config: {message}"),
            Self::NotFound(message) => write!(f, "not found: {message}"),
            Self::Conflict(message) => write!(f, "conflict: {message}"),
            Self::External(message) => write!(f, "external error: {message}"),
            Self::Internal(message) => write!(f, "internal error: {message}"),
            Self::Message(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(err) => Some(err),
            Self::Http(err) => Some(err),
            Self::Json(err) => Some(err),
            Self::Json5(err) => Some(err),
            Self::InvalidInput(_)
            | Self::InvalidConfig(_)
            | Self::NotFound(_)
            | Self::Conflict(_)
            | Self::External(_)
            | Self::Internal(_)
            | Self::Message(_) => None,
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<reqwest::Error> for Error {
    fn from(value: reqwest::Error) -> Self {
        Self::Http(value)
    }
}

impl From<serde_json::Error> for Error {
    fn from(value: serde_json::Error) -> Self {
        Self::Json(value)
    }
}

impl From<json5::Error> for Error {
    fn from(value: json5::Error) -> Self {
        Self::Json5(value)
    }
}

impl From<String> for Error {
    fn from(value: String) -> Self {
        Self::Message(value)
    }
}

impl From<&str> for Error {
    fn from(value: &str) -> Self {
        Self::Message(value.to_string())
    }
}

impl From<anyhow::Error> for Error {
    fn from(value: anyhow::Error) -> Self {
        Self::Message(value.to_string())
    }
}

pub type Result<T> = std::result::Result<T, Error>;
