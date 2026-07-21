use std::collections::BTreeMap;
use std::time::Duration;

use async_trait::async_trait;

const MAX_RESPONSE_BODY_BYTES: usize = 4_096;

/// Complete outbound request passed to an injected sink transport.
///
/// This type intentionally does not implement `Debug` because headers may contain credentials.
pub struct HttpRequest {
    /// Absolute destination URL.
    pub url: String,
    /// Exact outbound header names and values.
    pub headers: BTreeMap<String, String>,
    /// Exact transport body.
    pub body: Vec<u8>,
}

/// Bounded HTTP response required by log sinks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpResponse {
    /// Numeric HTTP status code.
    pub status: u16,
    /// Response body truncated to a safe diagnostic bound.
    pub body: String,
}

/// Injected sink HTTP boundary used by deterministic contract tests.
#[async_trait]
pub trait HttpTransport: Send + Sync {
    /// Sends one complete request.
    async fn send(&self, request: HttpRequest) -> Result<HttpResponse, HttpTransportError>;
}

/// Production rustls-backed HTTP transport with a fixed request timeout.
pub struct ReqwestHttpTransport {
    client: reqwest::Client,
}

impl ReqwestHttpTransport {
    /// Constructs a bounded HTTP/1 transport.
    pub fn new(timeout: Duration) -> Result<Self, ReqwestHttpTransportError> {
        if timeout.is_zero() {
            return Err(ReqwestHttpTransportError::ZeroTimeout);
        }
        let client = reqwest::Client::builder()
            .http1_only()
            .timeout(timeout)
            .build()
            .map_err(ReqwestHttpTransportError::Build)?;
        Ok(Self { client })
    }
}

#[async_trait]
impl HttpTransport for ReqwestHttpTransport {
    async fn send(&self, request: HttpRequest) -> Result<HttpResponse, HttpTransportError> {
        let mut builder = self.client.post(&request.url).body(request.body);
        for (name, value) in request.headers {
            builder = builder.header(name, value);
        }
        let mut response =
            builder
                .send()
                .await
                .map_err(|error| HttpTransportError::Unavailable {
                    message: error.to_string(),
                })?;
        let status = response.status().as_u16();
        let mut body = Vec::new();
        while body.len() < MAX_RESPONSE_BODY_BYTES {
            let Some(chunk) =
                response
                    .chunk()
                    .await
                    .map_err(|error| HttpTransportError::Unavailable {
                        message: error.to_string(),
                    })?
            else {
                break;
            };
            let remaining = MAX_RESPONSE_BODY_BYTES.saturating_sub(body.len());
            body.extend_from_slice(chunk.get(..remaining).unwrap_or(&chunk));
        }
        Ok(HttpResponse {
            status,
            body: String::from_utf8_lossy(&body).into_owned(),
        })
    }
}

/// A production HTTP client could not be built safely.
#[derive(Debug, thiserror::Error)]
pub enum ReqwestHttpTransportError {
    /// A zero timeout would disable the required request bound.
    #[error("HTTP transport timeout must be non-zero")]
    ZeroTimeout,
    /// Reqwest rejected the client configuration.
    #[error("failed to build HTTP transport: {0}")]
    Build(reqwest::Error),
}

/// An outbound HTTP request could not be completed.
#[derive(Debug, thiserror::Error)]
pub enum HttpTransportError {
    /// The remote transport is temporarily unavailable.
    #[error("HTTP transport is unavailable: {message}")]
    Unavailable {
        /// Safe transport diagnostic.
        message: String,
    },
}
