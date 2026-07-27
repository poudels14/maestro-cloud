use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use kernel_api::{AssignmentId, ExecStreamFrame, NodeId, SecretValue};
use node_agent::{NodeExecError, NodeExecService};
use runtime::{ExecInput, ExecOutput, ExecRequest, ExecSession, ExecSessionKiller, RuntimeError};
use serde::Serialize;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::{Message, protocol::WebSocketConfig};
use tokio_tungstenite::{Connector, MaybeTlsStream, WebSocketStream};

use crate::TlsIdentity;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const NODE_TOKEN_LIFETIME: Duration = Duration::from_secs(60);
const MAXIMUM_FRAME_BYTES: usize = 16 * 1_024 * 1_024;

type RemoteSocket = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

/// Cluster-aware source of policy-checked runtime exec sessions.
#[async_trait]
pub trait ClusterExecSessions: Send + Sync {
    /// Opens an assignment on its already-resolved owning node.
    async fn open(
        &self,
        node_id: &NodeId,
        assignment_id: &AssignmentId,
        request: ExecRequest,
    ) -> Result<Box<dyn ExecSession>, ExecSessionOpenError>;

    /// Opens an assignment only through the node-local policy bridge.
    async fn open_local(
        &self,
        assignment_id: &AssignmentId,
        request: ExecRequest,
    ) -> Result<Box<dyn ExecSession>, ExecSessionOpenError>;
}

/// Mutual-TLS node client with a direct path to the local node exec bridge.
pub struct HttpClusterExecSessions {
    local_node_id: NodeId,
    local: Arc<NodeExecService>,
    endpoints: BTreeMap<NodeId, reqwest::Url>,
    connector: Connector,
    token_subject: String,
    token_key: EncodingKey,
}

impl HttpClusterExecSessions {
    /// Builds one bounded static-topology exec client.
    pub fn new(
        local_node_id: NodeId,
        endpoints: BTreeMap<NodeId, SocketAddr>,
        trust_root_pem: &str,
        identity: &TlsIdentity,
        jwt_secret_key: &SecretValue,
        local: Arc<NodeExecService>,
    ) -> Result<Self, HttpExecClientError> {
        validate_endpoints(&local_node_id, &endpoints)?;
        if jwt_secret_key.expose().len() < 32 {
            return Err(HttpExecClientError::WeakJwtSecret);
        }
        let connector = Connector::Rustls(Arc::new(client_tls(trust_root_pem, identity)?));
        let endpoints = endpoints
            .into_iter()
            .map(|(node_id, endpoint)| {
                reqwest::Url::parse(&format!("wss://{endpoint}"))
                    .map(|endpoint| (node_id.clone(), endpoint))
                    .map_err(|_| HttpExecClientError::Endpoint { node_id })
            })
            .collect::<Result<_, _>>()?;
        let token_subject = format!("maestro-node:{local_node_id}");
        let token_key = EncodingKey::from_secret(jwt_secret_key.expose().as_bytes());
        Ok(Self {
            local_node_id,
            local,
            endpoints,
            connector,
            token_subject,
            token_key,
        })
    }

    async fn open_remote(
        &self,
        node_id: &NodeId,
        assignment_id: &AssignmentId,
        request: ExecRequest,
    ) -> Result<Box<dyn ExecSession>, ExecSessionOpenError> {
        let endpoint = self.endpoint(node_id, assignment_id, &request)?;
        let mut websocket_request = endpoint
            .as_str()
            .into_client_request()
            .map_err(|error| unavailable(format!("failed to build node exec request: {error}")))?;
        let token = self.node_token()?;
        websocket_request.headers_mut().insert(
            tokio_tungstenite::tungstenite::http::header::AUTHORIZATION,
            format!("Bearer {token}")
                .parse()
                .map_err(|_| unavailable("failed to encode node exec authorization"))?,
        );
        let configuration = WebSocketConfig::default()
            .max_message_size(Some(MAXIMUM_FRAME_BYTES))
            .max_frame_size(Some(MAXIMUM_FRAME_BYTES));
        let connected = tokio::time::timeout(
            CONNECT_TIMEOUT,
            tokio_tungstenite::connect_async_tls_with_config(
                websocket_request,
                Some(configuration),
                true,
                Some(self.connector.clone()),
            ),
        )
        .await
        .map_err(|_| unavailable(format!("node `{node_id}` exec connection timed out")))?
        .map_err(|error| {
            unavailable(format!("node `{node_id}` exec connection failed: {error}"))
        })?;
        Ok(Box::new(RemoteExecSession {
            socket: connected.0,
        }))
    }

    fn endpoint(
        &self,
        node_id: &NodeId,
        assignment_id: &AssignmentId,
        request: &ExecRequest,
    ) -> Result<reqwest::Url, ExecSessionOpenError> {
        let mut endpoint = self
            .endpoints
            .get(node_id)
            .cloned()
            .ok_or_else(|| unavailable(format!("node `{node_id}` has no exec endpoint")))?;
        endpoint.set_path(&format!("/api/node/assignments/{assignment_id}/exec"));
        let command = command_arguments(request);
        let encoded = serde_json::to_string(&command)
            .map_err(|error| unavailable(format!("failed to encode exec command: {error}")))?;
        let mut query = endpoint.query_pairs_mut();
        query.append_pair("command", &encoded);
        match request.mode {
            runtime::ExecMode::Pipes => {
                query.append_pair("tty", "false");
            }
            runtime::ExecMode::Terminal { columns, rows } => {
                query.append_pair("tty", "true");
                query.append_pair("columns", &columns.to_string());
                query.append_pair("rows", &rows.to_string());
            }
        }
        drop(query);
        Ok(endpoint)
    }

    fn node_token(&self) -> Result<String, ExecSessionOpenError> {
        let issued_at = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_err(|_| unavailable("system clock is earlier than the Unix epoch"))?
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
        .map_err(|error| unavailable(format!("failed to sign node exec request: {error}")))
    }
}

#[async_trait]
impl ClusterExecSessions for HttpClusterExecSessions {
    async fn open(
        &self,
        node_id: &NodeId,
        assignment_id: &AssignmentId,
        request: ExecRequest,
    ) -> Result<Box<dyn ExecSession>, ExecSessionOpenError> {
        if node_id == &self.local_node_id {
            self.open_local(assignment_id, request).await
        } else {
            self.open_remote(node_id, assignment_id, request).await
        }
    }

    async fn open_local(
        &self,
        assignment_id: &AssignmentId,
        request: ExecRequest,
    ) -> Result<Box<dyn ExecSession>, ExecSessionOpenError> {
        self.local
            .open(assignment_id, request)
            .await
            .map_err(node_error)
    }
}

struct RemoteExecSession {
    socket: RemoteSocket,
}

#[async_trait]
impl ExecSession for RemoteExecSession {
    async fn send(&mut self, input: ExecInput) -> Result<(), RuntimeError> {
        let frame = match input {
            ExecInput::Stdin(bytes) => ExecStreamFrame::Stdin(bytes),
            ExecInput::Resize { columns, rows } => ExecStreamFrame::Resize { columns, rows },
            ExecInput::CloseStdin => ExecStreamFrame::CloseStdin,
        };
        send_remote(&mut self.socket, frame).await
    }

    async fn next(&mut self) -> Result<Option<ExecOutput>, RuntimeError> {
        loop {
            match self.socket.next().await {
                Some(Ok(Message::Binary(encoded))) => {
                    let frame = ExecStreamFrame::decode(&encoded).map_err(protocol_error)?;
                    return match frame {
                        ExecStreamFrame::Stdout(bytes) => Ok(Some(ExecOutput::Stdout(bytes))),
                        ExecStreamFrame::Stderr(bytes) => Ok(Some(ExecOutput::Stderr(bytes))),
                        ExecStreamFrame::Exited { code } => Ok(Some(ExecOutput::Exited { code })),
                        ExecStreamFrame::Error(message) => Err(RuntimeError::Stream { message }),
                        _ => Err(protocol_error("node sent a client-input exec frame")),
                    };
                }
                Some(Ok(Message::Ping(payload))) => self
                    .socket
                    .send(Message::Pong(payload))
                    .await
                    .map_err(remote_error)?,
                Some(Ok(Message::Pong(_))) => {}
                Some(Ok(Message::Close(_))) | None => return Ok(None),
                Some(Ok(Message::Text(_) | Message::Frame(_))) => {
                    return Err(protocol_error("node sent a non-binary exec message"));
                }
                Some(Err(error)) => return Err(remote_error(error)),
            }
        }
    }

    fn killer(&mut self) -> Option<&mut dyn ExecSessionKiller> {
        Some(self)
    }
}

#[async_trait]
impl ExecSessionKiller for RemoteExecSession {
    async fn kill(&mut self) -> Result<(), RuntimeError> {
        send_remote(&mut self.socket, ExecStreamFrame::Kill).await
    }
}

async fn send_remote(
    socket: &mut RemoteSocket,
    frame: ExecStreamFrame,
) -> Result<(), RuntimeError> {
    let encoded = frame.encode().map_err(protocol_error)?;
    socket
        .send(Message::Binary(encoded.into()))
        .await
        .map_err(remote_error)
}

fn command_arguments(request: &ExecRequest) -> Vec<&str> {
    let mut command = Vec::with_capacity(request.command.arguments.len().saturating_add(1));
    command.push(request.command.executable.as_str());
    command.extend(request.command.arguments.iter().map(String::as_str));
    command
}

fn validate_endpoints(
    local_node_id: &NodeId,
    endpoints: &BTreeMap<NodeId, SocketAddr>,
) -> Result<(), HttpExecClientError> {
    if !endpoints.contains_key(local_node_id) {
        return Err(HttpExecClientError::MissingLocalNode {
            node_id: local_node_id.clone(),
        });
    }
    if let Some((node_id, _)) = endpoints.iter().find(|(_, endpoint)| endpoint.port() == 0) {
        return Err(HttpExecClientError::ZeroPort {
            node_id: node_id.clone(),
        });
    }
    let unique = endpoints.values().collect::<BTreeSet<_>>();
    if unique.len() != endpoints.len() {
        return Err(HttpExecClientError::DuplicateEndpoint);
    }
    Ok(())
}

fn client_tls(
    trust_root_pem: &str,
    identity: &TlsIdentity,
) -> Result<rustls::ClientConfig, HttpExecClientError> {
    use rustls::pki_types::pem::PemObject;
    use rustls::pki_types::{CertificateDer, PrivateKeyDer};

    let _ = rustls::crypto::ring::default_provider().install_default();
    let roots = CertificateDer::pem_slice_iter(trust_root_pem.as_bytes())
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| HttpExecClientError::TrustRoot)?;
    let mut root_store = rustls::RootCertStore::empty();
    let (accepted, _) = root_store.add_parsable_certificates(roots);
    if accepted == 0 {
        return Err(HttpExecClientError::TrustRoot);
    }
    let certificates = CertificateDer::pem_slice_iter(identity.certificate_pem.as_bytes())
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| HttpExecClientError::ClientIdentity)?;
    let private_key = PrivateKeyDer::from_pem_slice(identity.private_key_pem.expose().as_bytes())
        .map_err(|_| HttpExecClientError::ClientIdentity)?;
    let mut config = rustls::ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_client_auth_cert(certificates, private_key)
        .map_err(|_| HttpExecClientError::ClientIdentity)?;
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(config)
}

fn node_error(error: NodeExecError) -> ExecSessionOpenError {
    let message = error.to_string();
    match error {
        NodeExecError::SessionLimitReached { .. }
        | NodeExecError::Store(_)
        | NodeExecError::Runtime(RuntimeError::Unavailable { .. })
        | NodeExecError::Runtime(RuntimeError::Stream { .. })
        | NodeExecError::Runtime(RuntimeError::Timeout { .. }) => unavailable(message),
        _ => ExecSessionOpenError::Rejected { message },
    }
}

fn unavailable(message: impl Into<String>) -> ExecSessionOpenError {
    ExecSessionOpenError::Unavailable {
        message: message.into(),
    }
}

fn remote_error(error: impl std::fmt::Display) -> RuntimeError {
    RuntimeError::Stream {
        message: format!("node exec stream failed: {error}"),
    }
}

fn protocol_error(error: impl std::fmt::Display) -> RuntimeError {
    RuntimeError::Stream {
        message: format!("invalid node exec protocol: {error}"),
    }
}

#[derive(Serialize)]
struct NodeClaims<'a> {
    sub: &'a str,
    scope: &'static str,
    iat: u64,
    exp: u64,
}

/// Session rejection safe to return inside an authenticated exec stream.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ExecSessionOpenError {
    /// Policy, identity, request, or capability made this session permanently invalid.
    #[error("{message}")]
    Rejected { message: String },
    /// Capacity, storage, runtime, or peer transport is temporarily unavailable.
    #[error("{message}")]
    Unavailable { message: String },
}

/// Invalid node topology or mutual-TLS client material.
#[derive(Debug, thiserror::Error)]
pub enum HttpExecClientError {
    #[error("cluster exec endpoints omit local node `{node_id}`")]
    MissingLocalNode { node_id: NodeId },
    #[error("cluster exec endpoint for node `{node_id}` uses port zero")]
    ZeroPort { node_id: NodeId },
    #[error("cluster exec endpoints must be unique")]
    DuplicateEndpoint,
    #[error("cluster exec JWT secret must contain at least 32 bytes")]
    WeakJwtSecret,
    #[error("cluster exec trust root is invalid")]
    TrustRoot,
    #[error("cluster exec client identity is invalid")]
    ClientIdentity,
    #[error("cluster exec endpoint for node `{node_id}` is invalid")]
    Endpoint { node_id: NodeId },
}
