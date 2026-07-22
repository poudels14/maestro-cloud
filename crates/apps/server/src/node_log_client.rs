use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use kernel_api::{NodeId, SecretValue};
use logs::{
    LogHistogramBucket, LogHistogramGroupBy, LogHistogramQuery, LogQueryScope, LogQueryStore,
    LogQueryStoreError, LogReadCursor, LogReadOrder, LogReadQuery, NodeLogQueryStore,
    SequencedLogEntry,
};
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::TlsIdentity;

const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const MAXIMUM_RESPONSE_BYTES: usize = 32 * 1_024 * 1_024;
const NODE_TOKEN_LIFETIME: Duration = Duration::from_secs(60);

/// HTTPS node-local query proxy using cluster trust and one node certificate.
pub struct HttpNodeLogQueryStore {
    local_node_id: NodeId,
    local: Arc<dyn LogQueryStore>,
    endpoints: BTreeMap<NodeId, reqwest::Url>,
    client: reqwest::Client,
    token_subject: String,
    token_key: EncodingKey,
}

impl HttpNodeLogQueryStore {
    /// Builds a bounded mutual-TLS client over one validated static topology.
    pub fn new(
        local_node_id: NodeId,
        endpoints: BTreeMap<NodeId, SocketAddr>,
        trust_root_pem: &str,
        identity: &TlsIdentity,
        jwt_secret_key: &SecretValue,
        local: Arc<dyn LogQueryStore>,
    ) -> Result<Self, NodeLogClientError> {
        if !endpoints.contains_key(&local_node_id) {
            return Err(NodeLogClientError::MissingLocalNode {
                node_id: local_node_id,
            });
        }
        if let Some((node_id, _)) = endpoints.iter().find(|(_, endpoint)| endpoint.port() == 0) {
            return Err(NodeLogClientError::ZeroPort {
                node_id: node_id.clone(),
            });
        }
        if jwt_secret_key.expose().len() < 32 {
            return Err(NodeLogClientError::WeakJwtSecret);
        }
        let root = reqwest::Certificate::from_pem(trust_root_pem.as_bytes())
            .map_err(NodeLogClientError::TrustRoot)?;
        let mut identity_pem = identity.certificate_pem.as_bytes().to_vec();
        if !identity_pem.ends_with(b"\n") {
            identity_pem.push(b'\n');
        }
        identity_pem.extend_from_slice(identity.private_key_pem.expose().as_bytes());
        let client_identity = reqwest::Identity::from_pem(&identity_pem)
            .map_err(NodeLogClientError::ClientIdentity)?;
        let client = reqwest::Client::builder()
            .https_only(true)
            .http1_only()
            .connect_timeout(DEFAULT_REQUEST_TIMEOUT)
            .timeout(DEFAULT_REQUEST_TIMEOUT)
            .add_root_certificate(root)
            .identity(client_identity)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(NodeLogClientError::BuildClient)?;
        let endpoints = endpoints
            .into_iter()
            .map(|(node_id, endpoint)| {
                let url = reqwest::Url::parse(&format!("https://{endpoint}")).map_err(|_| {
                    NodeLogClientError::Endpoint {
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
            local,
            endpoints,
            client,
            token_subject,
            token_key,
        })
    }

    async fn get<Response>(
        &self,
        node_id: &NodeId,
        path: &str,
        parameters: &[(String, String)],
    ) -> Result<Response, LogQueryStoreError>
    where
        Response: DeserializeOwned,
    {
        let mut endpoint =
            self.endpoints.get(node_id).cloned().ok_or_else(|| {
                unavailable(format!("node `{node_id}` has no log query endpoint"))
            })?;
        endpoint.set_path(path);
        endpoint
            .query_pairs_mut()
            .extend_pairs(parameters.iter().map(|(name, value)| (name, value)));
        let token = self.node_token()?;
        let response = self
            .client
            .get(endpoint)
            .bearer_auth(token)
            .send()
            .await
            .map_err(|error| unavailable(format!("node log request failed: {error}")))?;
        decode_response(response).await
    }

    fn node_token(&self) -> Result<String, LogQueryStoreError> {
        let issued_at = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_err(|_| unavailable("system clock is earlier than the Unix epoch"))?
            .as_secs();
        let expires_at = issued_at.saturating_add(NODE_TOKEN_LIFETIME.as_secs());
        jsonwebtoken::encode(
            &Header::new(Algorithm::HS256),
            &NodeClaims {
                sub: &self.token_subject,
                scope: "node",
                iat: issued_at,
                exp: expires_at,
            },
            &self.token_key,
        )
        .map_err(|error| unavailable(format!("failed to sign node log request: {error}")))
    }
}

#[async_trait]
impl NodeLogQueryStore for HttpNodeLogQueryStore {
    async fn query_node_logs(
        &self,
        node_id: &NodeId,
        query: &LogReadQuery,
    ) -> Result<Vec<SequencedLogEntry>, LogQueryStoreError> {
        if node_id == &self.local_node_id {
            return self.local.query_logs(query).await;
        }
        self.get(node_id, "/api/node/logs", &read_parameters(query))
            .await
    }

    async fn query_node_histogram(
        &self,
        node_id: &NodeId,
        query: &LogHistogramQuery,
    ) -> Result<Vec<LogHistogramBucket>, LogQueryStoreError> {
        if node_id == &self.local_node_id {
            return self.local.query_log_histogram(query).await;
        }
        self.get(
            node_id,
            "/api/node/logs/histogram",
            &histogram_parameters(query),
        )
        .await
    }
}

#[derive(Serialize)]
struct NodeClaims<'a> {
    sub: &'a str,
    scope: &'static str,
    iat: u64,
    exp: u64,
}

fn read_parameters(query: &LogReadQuery) -> Vec<(String, String)> {
    let mut parameters = scope_parameters(query.scope());
    parameters.push(("tail".to_owned(), query.limit().to_string()));
    parameters.push((
        "order".to_owned(),
        match query.order() {
            LogReadOrder::OldestFirst => "oldest",
            LogReadOrder::NewestFirst => "newest",
        }
        .to_owned(),
    ));
    if let Some(cursor) = query.cursor() {
        let (name, sequence) = match cursor {
            LogReadCursor::After(sequence) => ("after", sequence),
            LogReadCursor::Before(sequence) => ("before", sequence),
        };
        parameters.push((name.to_owned(), sequence.0.to_string()));
    }
    push_common_parameters(&mut parameters, query.search(), query.from(), query.to());
    parameters
}

fn histogram_parameters(query: &LogHistogramQuery) -> Vec<(String, String)> {
    let mut parameters = scope_parameters(query.scope());
    parameters.extend([
        ("from".to_owned(), query.from().0.to_string()),
        ("to".to_owned(), query.to().0.to_string()),
        ("bucketMs".to_owned(), query.bucket_ms().to_string()),
        (
            "groupBy".to_owned(),
            match query.group_by() {
                LogHistogramGroupBy::Level => "level",
                LogHistogramGroupBy::HttpStatusClass => "status",
            }
            .to_owned(),
        ),
    ]);
    if let Some(search) = query.search() {
        parameters.push(("query".to_owned(), search.as_str().to_owned()));
    }
    parameters
}

fn scope_parameters(scope: &LogQueryScope) -> Vec<(String, String)> {
    let (scope, scope_id) = match scope {
        LogQueryScope::All => ("all", None),
        LogQueryScope::Service(service_id) => ("service", Some(service_id.as_str())),
        LogQueryScope::Deployment(deployment_id) => ("deployment", Some(deployment_id.as_str())),
        LogQueryScope::System => ("system", None),
        LogQueryScope::SystemComponent(component) => ("systemComponent", Some(component.as_str())),
        LogQueryScope::Build(build_id) => ("build", Some(build_id.as_str())),
    };
    let mut parameters = vec![("scope".to_owned(), scope.to_owned())];
    if let Some(scope_id) = scope_id {
        parameters.push(("scopeId".to_owned(), scope_id.to_owned()));
    }
    parameters
}

fn push_common_parameters(
    parameters: &mut Vec<(String, String)>,
    search: Option<&logs::LogQuery>,
    from: Option<kernel_api::Timestamp>,
    to: Option<kernel_api::Timestamp>,
) {
    if let Some(search) = search {
        parameters.push(("query".to_owned(), search.as_str().to_owned()));
    }
    if let Some(from) = from {
        parameters.push(("from".to_owned(), from.0.to_string()));
    }
    if let Some(to) = to {
        parameters.push(("to".to_owned(), to.0.to_string()));
    }
}

async fn decode_response<Response>(
    mut response: reqwest::Response,
) -> Result<Response, LogQueryStoreError>
where
    Response: DeserializeOwned,
{
    let status = response.status();
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|error| unavailable(format!("failed to read node log response: {error}")))?
    {
        if body.len().saturating_add(chunk.len()) > MAXIMUM_RESPONSE_BYTES {
            return Err(unavailable("node log response exceeded 32 MiB"));
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
            .unwrap_or_else(|| format!("node log endpoint returned HTTP {status}"));
        return if status == reqwest::StatusCode::BAD_REQUEST {
            Err(LogQueryStoreError::Rejected { message })
        } else {
            Err(unavailable(message))
        };
    }
    serde_json::from_slice(&body)
        .map_err(|error| unavailable(format!("node log response was invalid: {error}")))
}

fn unavailable(message: impl Into<String>) -> LogQueryStoreError {
    LogQueryStoreError::Unavailable {
        message: message.into(),
    }
}

/// Invalid static topology or TLS material for cluster log proxying.
#[derive(Debug, thiserror::Error)]
pub enum NodeLogClientError {
    /// The transport could not route its own node directly to local storage.
    #[error("cluster log endpoints omit local node `{node_id}`")]
    MissingLocalNode { node_id: NodeId },
    /// A static node API endpoint cannot use an ephemeral port.
    #[error("cluster log endpoint for node `{node_id}` uses port zero")]
    ZeroPort { node_id: NodeId },
    /// Cluster peers require the same HS256 security margin as operator requests.
    #[error("cluster log JWT secret must contain at least 32 bytes")]
    WeakJwtSecret,
    /// The configured cluster CA was not a PEM certificate.
    #[error("cluster log trust root is invalid: {0}")]
    TrustRoot(reqwest::Error),
    /// The node certificate and key could not form a client identity.
    #[error("cluster log client identity is invalid: {0}")]
    ClientIdentity(reqwest::Error),
    /// Reqwest rejected the bounded TLS client settings.
    #[error("failed to build cluster log client: {0}")]
    BuildClient(reqwest::Error),
    /// A validated socket address unexpectedly failed URL construction.
    #[error("cluster log endpoint for node `{node_id}` is invalid")]
    Endpoint { node_id: NodeId },
}
