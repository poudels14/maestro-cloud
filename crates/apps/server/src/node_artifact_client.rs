use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::time::Duration;

use async_trait::async_trait;
use axum::body::Bytes;
use axum::http::header;
use kernel_api::{NodeId, SecretValue};
use runtime::{ArtifactByteStream, ArtifactDigest, ArtifactStoreError};

use crate::TlsIdentity;
use crate::node_http_client::{
    NodeHttpClient, NodeHttpClientError, NodeHttpRequestError, decode_response,
};

const STREAM_IDLE_TIMEOUT: Duration = Duration::from_secs(30);
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
const TRANSFER_CHUNK_BYTES: usize = 64 * 1_024;

/// Mutual-TLS client for runtime-native artifact streams exported by cluster peers.
pub struct HttpNodeArtifactClient {
    transport: NodeHttpClient,
}

impl HttpNodeArtifactClient {
    /// Builds a peer client over the validated static node topology.
    pub fn new(
        local_node_id: NodeId,
        endpoints: BTreeMap<NodeId, SocketAddr>,
        trust_root_pem: &str,
        identity: &TlsIdentity,
        jwt_secret_key: &SecretValue,
    ) -> Result<Self, NodeHttpClientError> {
        Ok(Self {
            transport: NodeHttpClient::new(
                local_node_id,
                endpoints,
                trust_root_pem,
                identity,
                jwt_secret_key,
            )?,
        })
    }

    /// Opens one authenticated peer stream without buffering the artifact in memory.
    pub async fn export(
        &self,
        node_id: &NodeId,
        digest: &ArtifactDigest,
    ) -> Result<Box<dyn ArtifactByteStream>, NodeArtifactTransferError> {
        let response = self
            .transport
            .get_response(
                node_id,
                "/api/node/artifacts",
                &[("digest".to_owned(), digest.as_str().to_owned())],
            )
            .await
            .map_err(map_request_error)?;
        if !response.status().is_success() {
            return match tokio::time::timeout(
                HANDSHAKE_TIMEOUT,
                decode_response::<serde_json::Value>(response),
            )
            .await
            {
                Ok(Err(error)) => Err(map_request_error(error)),
                Ok(Ok(_)) => Err(NodeArtifactTransferError::Unavailable {
                    message: "peer returned an invalid successful error response".to_owned(),
                }),
                Err(_) => Err(NodeArtifactTransferError::Unavailable {
                    message: "peer artifact error response timed out".to_owned(),
                }),
            };
        }
        if response
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            != Some("application/x-tar")
        {
            return Err(NodeArtifactTransferError::Rejected {
                message: "peer artifact response has an invalid Content-Type".to_owned(),
            });
        }
        Ok(Box::new(HttpArtifactStream {
            response,
            pending: None,
        }))
    }
}

struct HttpArtifactStream {
    response: reqwest::Response,
    pending: Option<Bytes>,
}

#[async_trait]
impl ArtifactByteStream for HttpArtifactStream {
    async fn next(&mut self) -> Result<Option<Vec<u8>>, ArtifactStoreError> {
        loop {
            if let Some(mut pending) = self.pending.take() {
                let count = pending.len().min(TRANSFER_CHUNK_BYTES);
                let chunk = pending.split_to(count).to_vec();
                if !pending.is_empty() {
                    self.pending = Some(pending);
                }
                return Ok(Some(chunk));
            }
            self.pending = tokio::time::timeout(STREAM_IDLE_TIMEOUT, self.response.chunk())
                .await
                .map_err(|_| ArtifactStoreError::Stream {
                    message: "peer artifact stream was idle for 30 seconds".to_owned(),
                })?
                .map_err(|error| ArtifactStoreError::Stream {
                    message: format!("failed to read peer artifact stream: {error}"),
                })?;
            if self.pending.is_none() {
                return Ok(None);
            }
        }
    }
}

fn map_request_error(error: NodeHttpRequestError) -> NodeArtifactTransferError {
    match error {
        NodeHttpRequestError::Rejected { message } => {
            NodeArtifactTransferError::Rejected { message }
        }
        NodeHttpRequestError::Unavailable { message } => {
            NodeArtifactTransferError::Unavailable { message }
        }
    }
}

/// Peer artifact handshake failure before a byte stream is established.
#[derive(Debug, thiserror::Error)]
pub enum NodeArtifactTransferError {
    /// The peer rejected the requested digest.
    #[error("peer artifact request was rejected: {message}")]
    Rejected { message: String },
    /// The peer or its runtime artifact backend could not open the stream.
    #[error("peer artifact is unavailable: {message}")]
    Unavailable { message: String },
}
