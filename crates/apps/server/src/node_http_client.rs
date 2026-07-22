use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::time::{Duration, SystemTime};

use jsonwebtoken::{Algorithm, EncodingKey, Header};
use kernel_api::{NodeId, SecretValue};
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::TlsIdentity;

const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const MAXIMUM_RESPONSE_BYTES: usize = 32 * 1_024 * 1_024;
const NODE_TOKEN_LIFETIME: Duration = Duration::from_secs(60);

/// Shared bounded mutual-TLS transport for node-selected JSON reads.
pub(crate) struct NodeHttpClient {
    local_node_id: NodeId,
    endpoints: BTreeMap<NodeId, reqwest::Url>,
    client: reqwest::Client,
    token_subject: String,
    token_key: EncodingKey,
}

impl NodeHttpClient {
    pub(crate) fn new(
        local_node_id: NodeId,
        endpoints: BTreeMap<NodeId, SocketAddr>,
        trust_root_pem: &str,
        identity: &TlsIdentity,
        jwt_secret_key: &SecretValue,
    ) -> Result<Self, NodeHttpClientError> {
        if !endpoints.contains_key(&local_node_id) {
            return Err(NodeHttpClientError::MissingLocalNode {
                node_id: local_node_id,
            });
        }
        if let Some((node_id, _)) = endpoints.iter().find(|(_, endpoint)| endpoint.port() == 0) {
            return Err(NodeHttpClientError::ZeroPort {
                node_id: node_id.clone(),
            });
        }
        if jwt_secret_key.expose().len() < 32 {
            return Err(NodeHttpClientError::WeakJwtSecret);
        }
        let root = reqwest::Certificate::from_pem(trust_root_pem.as_bytes())
            .map_err(NodeHttpClientError::TrustRoot)?;
        let mut identity_pem = identity.certificate_pem.as_bytes().to_vec();
        if !identity_pem.ends_with(b"\n") {
            identity_pem.push(b'\n');
        }
        identity_pem.extend_from_slice(identity.private_key_pem.expose().as_bytes());
        let client_identity = reqwest::Identity::from_pem(&identity_pem)
            .map_err(NodeHttpClientError::ClientIdentity)?;
        let client = reqwest::Client::builder()
            .https_only(true)
            .http1_only()
            .connect_timeout(DEFAULT_REQUEST_TIMEOUT)
            .add_root_certificate(root)
            .identity(client_identity)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(NodeHttpClientError::BuildClient)?;
        let endpoints = endpoints
            .into_iter()
            .map(|(node_id, endpoint)| {
                let url = reqwest::Url::parse(&format!("https://{endpoint}")).map_err(|_| {
                    NodeHttpClientError::Endpoint {
                        node_id: node_id.clone(),
                    }
                })?;
                Ok((node_id, url))
            })
            .collect::<Result<_, _>>()?;
        let token_subject = format!("maestro-node:{local_node_id}");
        let token_key = EncodingKey::from_secret(jwt_secret_key.expose().as_bytes());
        Ok(Self {
            local_node_id,
            endpoints,
            client,
            token_subject,
            token_key,
        })
    }

    pub(crate) fn local_node_id(&self) -> &NodeId {
        &self.local_node_id
    }

    pub(crate) async fn get_json<Response>(
        &self,
        node_id: &NodeId,
        path: &str,
        parameters: &[(String, String)],
    ) -> Result<Response, NodeHttpRequestError>
    where
        Response: DeserializeOwned,
    {
        let response = self.get_response(node_id, path, parameters).await?;
        tokio::time::timeout(DEFAULT_REQUEST_TIMEOUT, decode_response(response))
            .await
            .map_err(|_| NodeHttpRequestError::unavailable("node response timed out"))?
    }

    pub(crate) async fn get_response(
        &self,
        node_id: &NodeId,
        path: &str,
        parameters: &[(String, String)],
    ) -> Result<reqwest::Response, NodeHttpRequestError> {
        let mut endpoint = self.endpoints.get(node_id).cloned().ok_or_else(|| {
            NodeHttpRequestError::unavailable(format!("node `{node_id}` has no query endpoint"))
        })?;
        endpoint.set_path(path);
        endpoint
            .query_pairs_mut()
            .extend_pairs(parameters.iter().map(|(name, value)| (name, value)));
        tokio::time::timeout(
            DEFAULT_REQUEST_TIMEOUT,
            self.client
                .get(endpoint)
                .bearer_auth(self.node_token()?)
                .send(),
        )
        .await
        .map_err(|_| NodeHttpRequestError::unavailable("node request timed out"))?
        .map_err(|error| NodeHttpRequestError::unavailable(format!("node request failed: {error}")))
    }

    fn node_token(&self) -> Result<String, NodeHttpRequestError> {
        let issued_at = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_err(|_| {
                NodeHttpRequestError::unavailable("system clock is earlier than the Unix epoch")
            })?
            .as_secs();
        jsonwebtoken::encode(
            &Header::new(Algorithm::HS256),
            &NodeClaims {
                sub: &self.token_subject,
                scope: "node",
                iat: issued_at,
                exp: issued_at.saturating_add(NODE_TOKEN_LIFETIME.as_secs()),
            },
            &self.token_key,
        )
        .map_err(|error| {
            NodeHttpRequestError::unavailable(format!("failed to sign node request: {error}"))
        })
    }
}

#[derive(Serialize)]
struct NodeClaims<'a> {
    sub: &'a str,
    scope: &'static str,
    iat: u64,
    exp: u64,
}

pub(crate) async fn decode_response<Response>(
    mut response: reqwest::Response,
) -> Result<Response, NodeHttpRequestError>
where
    Response: DeserializeOwned,
{
    let status = response.status();
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|error| {
        NodeHttpRequestError::unavailable(format!("failed to read node response: {error}"))
    })? {
        if body.len().saturating_add(chunk.len()) > MAXIMUM_RESPONSE_BYTES {
            return Err(NodeHttpRequestError::unavailable(
                "node response exceeded 32 MiB",
            ));
        }
        body.extend_from_slice(&chunk);
    }
    if !status.is_success() {
        let message = serde_json::from_slice::<serde_json::Value>(&body)
            .ok()
            .and_then(|value| {
                value
                    .get("message")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned)
            })
            .unwrap_or_else(|| format!("node endpoint returned HTTP {status}"));
        return if status == reqwest::StatusCode::BAD_REQUEST {
            Err(NodeHttpRequestError::Rejected { message })
        } else {
            Err(NodeHttpRequestError::Unavailable { message })
        };
    }
    serde_json::from_slice(&body).map_err(|error| {
        NodeHttpRequestError::unavailable(format!("node response was invalid: {error}"))
    })
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum NodeHttpRequestError {
    #[error("node query was rejected: {message}")]
    Rejected { message: String },
    #[error("node query is unavailable: {message}")]
    Unavailable { message: String },
}

impl NodeHttpRequestError {
    fn unavailable(message: impl Into<String>) -> Self {
        Self::Unavailable {
            message: message.into(),
        }
    }
}

/// Invalid topology, TLS material, or credentials for a node query client.
#[derive(Debug, thiserror::Error)]
pub enum NodeHttpClientError {
    #[error("cluster query endpoints omit local node `{node_id}`")]
    MissingLocalNode { node_id: NodeId },
    #[error("cluster query endpoint for node `{node_id}` uses port zero")]
    ZeroPort { node_id: NodeId },
    #[error("cluster query JWT secret must contain at least 32 bytes")]
    WeakJwtSecret,
    #[error("cluster query trust root is invalid: {0}")]
    TrustRoot(reqwest::Error),
    #[error("cluster query client identity is invalid: {0}")]
    ClientIdentity(reqwest::Error),
    #[error("failed to build cluster query client: {0}")]
    BuildClient(reqwest::Error),
    #[error("cluster query endpoint for node `{node_id}` is invalid")]
    Endpoint { node_id: NodeId },
}
