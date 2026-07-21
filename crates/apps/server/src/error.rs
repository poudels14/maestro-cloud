use std::net::SocketAddr;

use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::Serialize;

/// Stable machine-readable API failure body.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ApiErrorBody {
    /// Stable error class suitable for client branching.
    pub code: &'static str,
    /// Human-readable context that must not be parsed by clients.
    pub message: String,
}

/// One request rejection mapped to an HTTP status and JSON contract.
#[derive(Debug)]
pub struct ApiError {
    status: StatusCode,
    body: ApiErrorBody,
}

impl ApiError {
    pub(crate) fn bad_request(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "invalidRequest", message)
    }

    pub(crate) fn unauthorized(message: impl Into<String>) -> Self {
        Self::new(StatusCode::UNAUTHORIZED, "unauthorized", message)
    }

    pub(crate) fn forbidden(message: impl Into<String>) -> Self {
        Self::new(StatusCode::FORBIDDEN, "forbidden", message)
    }

    pub(crate) fn not_found(message: impl Into<String>) -> Self {
        Self::new(StatusCode::NOT_FOUND, "notFound", message)
    }

    pub(crate) fn conflict(code: &'static str, message: impl Into<String>) -> Self {
        Self::new(StatusCode::CONFLICT, code, message)
    }

    pub(crate) fn payload_too_large(message: impl Into<String>) -> Self {
        Self::new(StatusCode::PAYLOAD_TOO_LARGE, "payloadTooLarge", message)
    }

    pub(crate) fn internal(message: impl Into<String>) -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, "internal", message)
    }

    fn new(status: StatusCode, code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            body: ApiErrorBody {
                code,
                message: message.into(),
            },
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.status, Json(self.body)).into_response()
    }
}

/// API construction, listener, or serving failure.
#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    /// Static listener or authentication settings were unsafe.
    #[error(transparent)]
    Settings(#[from] crate::ServerSettingsError),
    /// The configured TCP listener could not be claimed.
    #[error("failed to bind API listener `{address}`: {source}")]
    Bind {
        address: SocketAddr,
        #[source]
        source: std::io::Error,
    },
    /// The operating system did not report the bound address.
    #[error("failed to inspect bound API listener: {0}")]
    LocalAddress(std::io::Error),
    /// PEM certificate or private-key material could not configure HTTPS.
    #[error("failed to configure API TLS identity: {0}")]
    TlsConfiguration(std::io::Error),
    /// A bound listener could not be transferred to the HTTP runtime.
    #[error("failed to configure bound API listener: {0}")]
    ListenerConfiguration(std::io::Error),
    /// The HTTP server stopped unexpectedly.
    #[error("API server failed: {0}")]
    Serve(std::io::Error),
}
