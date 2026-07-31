use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use cluster::{
    CaDiscoveryRequest, CaDiscoveryResponse, ClusterConfig, EncryptedJoinResponse, JoinPayload,
    JoinPrivateKey, JoinRequest, JoinResponseStatus, SignedJoinRequest, decrypt_join_response,
    load_or_create_join_key, sign_join_request, verify_ca_discovery_response,
    verify_join_request_signature,
};
use kernel_api::{NodeId, NodeRole};

use crate::CliError;
use crate::api_client::decode_response_with_limit;
use crate::config::load_cluster;
use crate::config_source::ConfigSourceReader;
use crate::launch_document::{DaemonLaunchDocument, validate_etcd_binary};
use crate::private_document::{Persisted, persist_private_new, read_private};

const ADMISSION_RESPONSE_LIMIT_BYTES: usize = 64 * 1_024;
const DEFAULT_CONTAINERD_SOCKET: &str = "/run/containerd/containerd.sock";

pub(crate) struct JoinOptions {
    pub(crate) leader: String,
    pub(crate) config_source: String,
    pub(crate) data_directory: PathBuf,
    pub(crate) containerd_socket: PathBuf,
    pub(crate) etcd_binary: Option<PathBuf>,
    pub(crate) output: Option<PathBuf>,
}

impl JoinOptions {
    pub(crate) fn new(leader: String, config_source: String, data_directory: PathBuf) -> Self {
        Self {
            leader,
            config_source,
            data_directory,
            containerd_socket: PathBuf::from(DEFAULT_CONTAINERD_SOCKET),
            etcd_binary: None,
            output: None,
        }
    }
}

pub(crate) async fn join(
    options: JoinOptions,
    output: &mut dyn Write,
    reader: &impl ConfigSourceReader,
) -> Result<(), CliError> {
    join_with_transport(options, output, reader, &ReqwestJoinTransport).await
}

pub(crate) async fn join_with_transport(
    options: JoinOptions,
    output: &mut dyn Write,
    reader: &impl ConfigSourceReader,
    transport: &impl JoinTransport,
) -> Result<(), CliError> {
    validate_paths(&options)?;
    let origin = parse_leader_origin(&options.leader)?;
    let loaded = load_cluster(&options.config_source, reader).await?;
    let node =
        loaded.cluster.nodes.get(&loaded.node_id).ok_or_else(|| {
            CliError::invalid_input("selected node disappeared from the topology")
        })?;
    if node.role == NodeRole::Master {
        return Err(CliError::invalid_input(
            "the declared master initializes the cluster and cannot join it",
        ));
    }
    validate_etcd_binary(node.role, options.etcd_binary.as_deref())?;

    let key_path = options.data_directory.join("security").join("join.key");
    let join_key = load_or_create_join_key(&key_path)
        .map_err(|error| cluster_error("failed to load node join key", error))?;
    let discovery_request = CaDiscoveryRequest::new(loaded.cluster.name.clone());
    let discovery = transport.discover(&origin, &discovery_request).await?;
    verify_ca_discovery_response(
        &loaded.cluster.join_secret,
        &loaded.cluster.name,
        &discovery_request,
        &discovery,
    )
    .map_err(|error| cluster_error("failed to authenticate cluster CA discovery", error))?;
    if discovery.cluster_id != loaded.cluster.cluster_id {
        return Err(CliError::invalid_api_response(
            "discovered cluster identity does not match the configured cluster",
        ));
    }
    reqwest::Certificate::from_pem(discovery.ca_certificate_pem.as_bytes()).map_err(|_| {
        CliError::invalid_api_response("discovered cluster CA is not a valid PEM certificate")
    })?;

    let request_path = options
        .data_directory
        .join("security")
        .join("join-request.json");
    let signed = load_or_create_request(
        &request_path,
        &join_key,
        &loaded.cluster,
        &loaded.node_id,
        now_unix_ms()?,
    )?;
    let admission = transport
        .admit(&origin, &discovery.ca_certificate_pem, &signed)
        .await?;
    let payload = decrypt_join_response(
        &loaded.cluster.join_secret,
        &join_key,
        &signed.request,
        &admission.envelope,
        admission.status,
    )
    .map_err(|error| cluster_error("failed to authenticate cluster join grant", error))?;
    validate_grant_trust(&discovery, &payload)?;
    let launch = DaemonLaunchDocument::joined(
        loaded.node_id,
        node.role,
        options.data_directory.clone(),
        options.containerd_socket.clone(),
        options.etcd_binary.clone(),
        &loaded.encryption_key,
        payload,
    )?;
    let destination = options
        .output
        .unwrap_or_else(|| options.data_directory.join("launch.json"));
    let (launch, persisted) = match read_private(&destination, "daemon launch document") {
        Ok(encoded) => {
            let existing: DaemonLaunchDocument =
                serde_json::from_slice(&encoded).map_err(|error| {
                    CliError::json("failed to decode daemon launch document", error)
                })?;
            existing.validate()?;
            if !existing.equivalent(&launch, &loaded.encryption_key)? {
                return Err(CliError::invalid_input(format!(
                    "refusing to overwrite a different daemon launch document `{}`",
                    destination.display()
                )));
            }
            (existing, Persisted::Reused)
        }
        Err(CliError::NotFound { .. }) => {
            persist_private_new(&destination, &launch, "daemon launch document")?;
            (launch, Persisted::Created)
        }
        Err(error) => return Err(error),
    };
    writeln!(
        output,
        "[maestro]: {} joined launch document for node `{}`",
        persisted.verb(),
        launch.node_id(),
    )
    .map_err(output_error)?;
    writeln!(output, "Launch config: {}", destination.display()).map_err(output_error)?;
    writeln!(
        output,
        "Start with: maestro-daemon start --config {} {}",
        options.config_source,
        destination.display()
    )
    .map_err(output_error)
}

pub(crate) trait JoinTransport {
    async fn discover(
        &self,
        origin: &reqwest::Url,
        request: &CaDiscoveryRequest,
    ) -> Result<CaDiscoveryResponse, CliError>;

    async fn admit(
        &self,
        origin: &reqwest::Url,
        ca_certificate_pem: &str,
        request: &SignedJoinRequest,
    ) -> Result<AdmissionResponse, CliError>;
}

pub(crate) struct AdmissionResponse {
    pub(crate) status: JoinResponseStatus,
    pub(crate) envelope: EncryptedJoinResponse,
}

struct ReqwestJoinTransport;

impl JoinTransport for ReqwestJoinTransport {
    async fn discover(
        &self,
        origin: &reqwest::Url,
        request: &CaDiscoveryRequest,
    ) -> Result<CaDiscoveryResponse, CliError> {
        // This client is deliberately confined to the HMAC-authenticated discovery exchange.
        let client = client_builder()
            .danger_accept_invalid_certs(true)
            .build()
            .map_err(|source| {
                CliError::transport("failed to construct CA discovery client", source)
            })?;
        let response = client
            .post(endpoint(origin, "/api/cluster/ca")?)
            .json(request)
            .send()
            .await
            .map_err(|source| CliError::transport("cluster CA discovery failed", source))?;
        decode_response_with_limit(response, ADMISSION_RESPONSE_LIMIT_BYTES).await
    }

    async fn admit(
        &self,
        origin: &reqwest::Url,
        ca_certificate_pem: &str,
        request: &SignedJoinRequest,
    ) -> Result<AdmissionResponse, CliError> {
        let certificate =
            reqwest::Certificate::from_pem(ca_certificate_pem.as_bytes()).map_err(|_| {
                CliError::invalid_api_response("discovered cluster CA is not a valid certificate")
            })?;
        let client = client_builder()
            .tls_built_in_root_certs(false)
            .add_root_certificate(certificate)
            .local_address(std::net::IpAddr::V4(request.request.endpoint.host_address))
            .build()
            .map_err(|source| {
                CliError::transport("failed to construct trusted join client", source)
            })?;
        let response = client
            .post(endpoint(origin, "/api/cluster/join")?)
            .json(request)
            .send()
            .await
            .map_err(|source| CliError::transport("authenticated cluster join failed", source))?;
        let status = JoinResponseStatus::new(response.status().as_u16()).map_err(|error| {
            CliError::invalid_api_response(format!("join response status is invalid: {error}"))
        })?;
        let envelope = decode_response_with_limit(response, ADMISSION_RESPONSE_LIMIT_BYTES).await?;
        Ok(AdmissionResponse { status, envelope })
    }
}

fn client_builder() -> reqwest::ClientBuilder {
    reqwest::Client::builder()
        .https_only(true)
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(30))
        .redirect(reqwest::redirect::Policy::none())
        .user_agent(concat!("maestro/", env!("CARGO_PKG_VERSION")))
}

fn parse_leader_origin(value: &str) -> Result<reqwest::Url, CliError> {
    let origin = reqwest::Url::parse(value)
        .map_err(|error| CliError::invalid_input(format!("invalid leader origin: {error}")))?;
    if origin.scheme() != "https"
        || origin.host().is_none()
        || !origin.username().is_empty()
        || origin.password().is_some()
        || origin.query().is_some()
        || origin.fragment().is_some()
        || origin.path() != "/"
    {
        return Err(CliError::invalid_input(
            "leader must be an HTTPS origin without credentials, path, query, or fragment",
        ));
    }
    Ok(origin)
}

fn endpoint(origin: &reqwest::Url, path: &str) -> Result<reqwest::Url, CliError> {
    origin
        .join(path)
        .map_err(|error| CliError::invalid_input(format!("invalid admission endpoint: {error}")))
}

fn now_unix_ms() -> Result<i64, CliError> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| {
            CliError::invalid_input(format!("system clock is before epoch: {error}"))
        })?;
    i64::try_from(elapsed.as_millis())
        .map_err(|_| CliError::invalid_input("system time is outside the supported range"))
}

fn validate_paths(options: &JoinOptions) -> Result<(), CliError> {
    if !options.data_directory.is_absolute() || !options.containerd_socket.is_absolute() {
        return Err(CliError::invalid_input(
            "data directory and containerd socket must be absolute paths",
        ));
    }
    Ok(())
}

fn load_or_create_request(
    path: &Path,
    key: &JoinPrivateKey,
    config: &ClusterConfig,
    node_id: &NodeId,
    now_unix_ms: i64,
) -> Result<SignedJoinRequest, CliError> {
    match read_private(path, "persisted join request") {
        Ok(encoded) => {
            let signed =
                serde_json::from_slice::<SignedJoinRequest>(&encoded).map_err(|source| {
                    CliError::json("failed to decode persisted join request", source)
                })?;
            validate_persisted_request(&signed, key, config, node_id)?;
            Ok(signed)
        }
        Err(CliError::NotFound { .. }) => {
            let request =
                JoinRequest::from_config(key, config, node_id, now_unix_ms).map_err(|error| {
                    cluster_error("failed to construct cluster join request", error)
                })?;
            let signature = sign_join_request(&config.join_secret, &request)
                .map_err(|error| cluster_error("failed to sign cluster join request", error))?;
            let signed = SignedJoinRequest { request, signature };
            persist_private_new(path, &signed, "join request")?;
            Ok(signed)
        }
        Err(error) => Err(error),
    }
}

fn validate_persisted_request(
    signed: &SignedJoinRequest,
    key: &JoinPrivateKey,
    config: &ClusterConfig,
    node_id: &NodeId,
) -> Result<(), CliError> {
    signed
        .request
        .validate_wire_shape(signed.request.timestamp_unix_ms)
        .map_err(|error| cluster_error("persisted join request is invalid", error))?;
    verify_join_request_signature(&config.join_secret, &signed.request, &signed.signature)
        .map_err(|error| cluster_error("persisted join request authentication failed", error))?;
    let mut expected =
        JoinRequest::from_config(key, config, node_id, signed.request.timestamp_unix_ms)
            .map_err(|error| cluster_error("failed to validate persisted join request", error))?;
    expected.nonce.clone_from(&signed.request.nonce);
    if signed.request != expected {
        return Err(CliError::invalid_input(
            "persisted join request does not match the current node key and cluster config",
        ));
    }
    Ok(())
}

fn validate_grant_trust(
    discovery: &CaDiscoveryResponse,
    payload: &JoinPayload,
) -> Result<(), CliError> {
    if payload.certificates.trust_root_pem != discovery.ca_certificate_pem {
        return Err(CliError::invalid_api_response(
            "join grant trust root does not match authenticated CA discovery",
        ));
    }
    Ok(())
}

fn cluster_error(action: &str, error: impl std::fmt::Display) -> CliError {
    CliError::cluster(action, error.to_string())
}

fn output_error(source: std::io::Error) -> CliError {
    CliError::io("failed to write command output", source)
}
