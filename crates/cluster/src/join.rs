use std::fmt::{Debug, Formatter};

use hmac::{Hmac, Mac};
use kernel_api::{ClusterId, NodeId, NodeRole, SecretValue};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use x25519_dalek::{PublicKey, StaticSecret};

use crate::{
    CertificateError, ClusterConfig, ClusterPorts, ClusterPreflightError, Ipv4Cidr, NodeEndpoint,
    certificate_fingerprint,
};

type HmacSha256 = Hmac<Sha256>;

const CA_DISCOVERY_CONTEXT: &[u8] = b"maestro-cluster-ca-discovery-v1";
const JOIN_REQUEST_MAX_SKEW_MILLIS: u64 = 5 * 60 * 1_000;

/// Ephemeral or persisted X25519 private key held only by a joining node.
pub struct JoinPrivateKey(StaticSecret);

impl JoinPrivateKey {
    /// Generates a fresh cryptographic join key.
    pub fn generate() -> Self {
        Self(StaticSecret::random())
    }

    /// Restores an exact 32-byte private key from protected local storage.
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(StaticSecret::from(bytes))
    }

    /// Returns the public key encoded for a join request.
    pub fn public_key_hex(&self) -> String {
        hex::encode(PublicKey::from(&self.0).as_bytes())
    }

    pub(crate) fn secret(&self) -> &StaticSecret {
        &self.0
    }
}

impl Debug for JoinPrivateKey {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("JoinPrivateKey([REDACTED])")
    }
}

/// Untrusted wire request sent by a prospective cluster member.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct JoinRequest {
    /// Cluster identity authenticated during CA discovery.
    pub cluster_id: ClusterId,
    /// Human-readable cluster name authenticated during CA discovery.
    pub cluster_name: String,
    /// Stable node identity requested by the joining host.
    pub node_id: NodeId,
    /// Operator-facing hostname.
    pub hostname: String,
    /// Requested public cluster role.
    pub role: NodeRole,
    /// Source address and public API listener.
    pub endpoint: NodeEndpoint,
    /// Requested private workload allocation.
    pub workload_subnet: Ipv4Cidr,
    /// Persisted cluster-wide service ports observed by the joiner.
    pub ports: ClusterPorts,
    /// X25519 public key used to encrypt the admission response.
    pub joiner_public_key: String,
    /// Wall-clock request time used only for replay-window validation.
    pub timestamp_unix_ms: i64,
    /// Fresh 16-byte random value encoded as lowercase hexadecimal.
    pub nonce: String,
}

impl JoinRequest {
    /// Builds a request from an already parsed cluster configuration.
    pub fn from_config(
        private_key: &JoinPrivateKey,
        config: &ClusterConfig,
        node_id: &NodeId,
        timestamp_unix_ms: i64,
    ) -> Result<Self, JoinProtocolError> {
        config.preflight()?;
        let node = config
            .nodes
            .get(node_id)
            .ok_or_else(|| JoinProtocolError::UnknownNode {
                node_id: node_id.clone(),
            })?;
        let nonce_entropy = StaticSecret::random().to_bytes();
        Ok(Self {
            cluster_id: config.cluster_id.clone(),
            cluster_name: config.name.clone(),
            node_id: node_id.clone(),
            hostname: node.hostname.clone(),
            role: node.role,
            endpoint: node.endpoint,
            workload_subnet: node.workload_subnet,
            ports: config.ports,
            joiner_public_key: private_key.public_key_hex(),
            timestamp_unix_ms,
            nonce: hex::encode(&nonce_entropy[..16]),
        })
    }

    /// Validates cryptographic field shape and the request replay window.
    pub fn validate_wire_shape(&self, now_unix_ms: i64) -> Result<(), JoinProtocolError> {
        decode_hex_array::<32>(&self.joiner_public_key, HexField::JoinerPublicKey)?;
        decode_hex_array::<16>(&self.nonce, HexField::JoinNonce)?;
        if now_unix_ms.abs_diff(self.timestamp_unix_ms) > JOIN_REQUEST_MAX_SKEW_MILLIS {
            return Err(JoinProtocolError::TimestampOutsideWindow {
                request_unix_ms: self.timestamp_unix_ms,
                now_unix_ms,
            });
        }
        Ok(())
    }
}

/// Constant-time verifiable HMAC over a canonical join request body.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RequestSignature(String);

impl RequestSignature {
    /// Returns the lowercase hexadecimal wire value.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Complete signed admission request submitted by a prospective node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SignedJoinRequest {
    /// Topology-bound node identity and one-time encryption key.
    pub request: JoinRequest,
    /// HMAC over the canonical request body using the bootstrap secret.
    pub signature: RequestSignature,
}

/// Request used to discover a cluster trust root before sending node details.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CaDiscoveryRequest {
    /// Expected cluster name from operator configuration.
    pub cluster_name: String,
    /// Fresh 32-byte random value encoded as lowercase hexadecimal.
    pub nonce: String,
}

impl CaDiscoveryRequest {
    /// Creates a discovery challenge with fresh cryptographic entropy.
    pub fn new(cluster_name: impl Into<String>) -> Self {
        Self {
            cluster_name: cluster_name.into(),
            nonce: hex::encode(StaticSecret::random().to_bytes()),
        }
    }
}

/// Authenticated cluster trust root returned during discovery.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CaDiscoveryResponse {
    /// Stable identity bound into the proof.
    pub cluster_id: ClusterId,
    /// Public trust root bound into the proof by its DER fingerprint.
    pub ca_certificate_pem: String,
    /// HMAC proof over the challenge, cluster identity, and certificate.
    pub proof: RequestSignature,
}

/// Signs a canonical request with the bootstrap secret.
pub fn sign_join_request(
    secret: &SecretValue,
    request: &JoinRequest,
) -> Result<RequestSignature, JoinProtocolError> {
    Ok(RequestSignature(sign_bytes(
        secret,
        &canonical_body(request)?,
    )?))
}

/// Authenticates a canonical request in constant time.
pub fn verify_join_request_signature(
    secret: &SecretValue,
    request: &JoinRequest,
    signature: &RequestSignature,
) -> Result<(), JoinProtocolError> {
    verify_bytes(secret, &canonical_body(request)?, signature)
}

/// Creates an authenticated response to a CA discovery challenge.
pub fn create_ca_discovery_response(
    secret: &SecretValue,
    cluster_name: &str,
    cluster_id: &ClusterId,
    ca_certificate_pem: &str,
    request: &CaDiscoveryRequest,
) -> Result<CaDiscoveryResponse, JoinProtocolError> {
    let message = ca_discovery_message(cluster_name, cluster_id, ca_certificate_pem, request)?;
    Ok(CaDiscoveryResponse {
        cluster_id: cluster_id.clone(),
        ca_certificate_pem: ca_certificate_pem.to_owned(),
        proof: RequestSignature(sign_bytes(secret, &message)?),
    })
}

/// Verifies that discovery data belongs to the requested named cluster.
pub fn verify_ca_discovery_response(
    secret: &SecretValue,
    expected_cluster_name: &str,
    request: &CaDiscoveryRequest,
    response: &CaDiscoveryResponse,
) -> Result<(), JoinProtocolError> {
    let message = ca_discovery_message(
        expected_cluster_name,
        &response.cluster_id,
        &response.ca_certificate_pem,
        request,
    )?;
    verify_bytes(secret, &message, &response.proof)
        .map_err(|_| JoinProtocolError::DiscoveryAuthenticationFailed)
}

/// Returns a stable fingerprint for an encoded join public key.
pub fn public_key_fingerprint(public_key: &str) -> Result<String, JoinProtocolError> {
    let public = decode_hex_array::<32>(public_key, HexField::JoinerPublicKey)?;
    Ok(hex::encode(Sha256::digest(public)))
}

pub(crate) fn canonical_body(request: &JoinRequest) -> Result<Vec<u8>, JoinProtocolError> {
    serde_json_canonicalizer::to_vec(request).map_err(JoinProtocolError::Serialization)
}

pub(crate) fn decode_public_key(value: &str) -> Result<[u8; 32], JoinProtocolError> {
    decode_hex_array::<32>(value, HexField::JoinerPublicKey)
}

pub(crate) fn decode_leader_public_key(value: &str) -> Result<[u8; 32], JoinProtocolError> {
    decode_hex_array::<32>(value, HexField::LeaderPublicKey)
}

pub(crate) fn validate_shared_secret(secret: &SecretValue) -> Result<(), JoinProtocolError> {
    if secret.expose().chars().count() < 32 {
        return Err(JoinProtocolError::WeakSharedSecret);
    }
    Ok(())
}

/// Why a signed join or CA discovery message was rejected.
#[derive(Debug, thiserror::Error)]
pub enum JoinProtocolError {
    /// The cluster topology used to construct the request was invalid.
    #[error("cluster preflight failed: {0}")]
    InvalidTopology(#[from] ClusterPreflightError),
    /// A requested node does not exist in the declared topology.
    #[error("node `{node_id}` is absent from the cluster topology")]
    UnknownNode { node_id: NodeId },
    /// A hexadecimal field could not be decoded.
    #[error("{field} is not valid hexadecimal")]
    InvalidHex { field: &'static str },
    /// A decoded cryptographic field had the wrong byte length.
    #[error("{field} must contain exactly {expected} bytes, found {observed}")]
    InvalidLength {
        field: &'static str,
        expected: usize,
        observed: usize,
    },
    /// The request timestamp was too far from the admission server's clock.
    #[error(
        "join request timestamp {request_unix_ms} is outside the allowed window around {now_unix_ms}"
    )]
    TimestampOutsideWindow {
        request_unix_ms: i64,
        now_unix_ms: i64,
    },
    /// The HMAC did not authenticate the canonical request bytes.
    #[error("join request signature mismatch")]
    SignatureMismatch,
    /// A discovery response was for a differently named cluster.
    #[error("CA discovery response belongs to a different cluster name")]
    DiscoveryClusterNameMismatch,
    /// The HMAC did not authenticate the returned trust root.
    #[error("cluster CA discovery authentication failed")]
    DiscoveryAuthenticationFailed,
    /// Certificate parsing failed while binding a discovery proof.
    #[error("invalid discovered certificate: {0}")]
    InvalidCertificate(#[from] CertificateError),
    /// Canonical JSON serialization failed.
    #[error("failed to serialize join protocol message: {0}")]
    Serialization(serde_json::Error),
    /// The HMAC implementation rejected key material.
    #[error("failed to initialize join message authentication")]
    AuthenticationInitialization,
    /// A bootstrap secret was too short to authenticate join traffic safely.
    #[error("cluster join secret must contain at least 32 characters")]
    WeakSharedSecret,
    /// Encryption or authentication of a join response failed.
    #[error("join response encryption failed")]
    ResponseEncryption,
    /// An encrypted response could not be authenticated.
    #[error("join response authentication failed")]
    ResponseAuthentication,
    /// A response nonce did not have the required wire shape.
    #[error("join response nonce must contain exactly 12 bytes")]
    InvalidResponseNonce,
    /// Key derivation failed for the response context.
    #[error("failed to derive the join response key")]
    KeyDerivation,
    /// The decrypted payload was bound to a different cluster.
    #[error("join response belongs to a different cluster")]
    ResponseClusterMismatch,
    /// Decrypted topology or ports differed from the signed request.
    #[error("join response configuration differs from the signed request")]
    ResponseConfigurationMismatch,
    /// CA signing material was missing or granted to an ineligible role.
    #[error("join response certificate issuer grant does not match the requested role")]
    ResponseIssuerGrantMismatch,
    /// Cluster-wide operator or storage secrets were too weak to start safely.
    #[error("join response granted invalid cluster-wide secrets")]
    ResponseSecretGrantMismatch,
    /// An HTTP response status was outside the valid range.
    #[error("invalid join response HTTP status {status}")]
    InvalidResponseStatus { status: u16 },
}

#[derive(Debug, Clone, Copy)]
enum HexField {
    JoinerPublicKey,
    LeaderPublicKey,
    JoinNonce,
    RequestSignature,
}

impl HexField {
    fn name(self) -> &'static str {
        match self {
            Self::JoinerPublicKey => "joiner public key",
            Self::LeaderPublicKey => "leader public key",
            Self::JoinNonce => "join nonce",
            Self::RequestSignature => "request signature",
        }
    }
}

fn sign_bytes(secret: &SecretValue, message: &[u8]) -> Result<String, JoinProtocolError> {
    validate_shared_secret(secret)?;
    let mut mac = HmacSha256::new_from_slice(secret.expose().as_bytes())
        .map_err(|_| JoinProtocolError::AuthenticationInitialization)?;
    mac.update(message);
    Ok(hex::encode(mac.finalize().into_bytes()))
}

fn verify_bytes(
    secret: &SecretValue,
    message: &[u8],
    signature: &RequestSignature,
) -> Result<(), JoinProtocolError> {
    validate_shared_secret(secret)?;
    let signature = decode_hex_array::<32>(&signature.0, HexField::RequestSignature)?;
    let mut mac = HmacSha256::new_from_slice(secret.expose().as_bytes())
        .map_err(|_| JoinProtocolError::AuthenticationInitialization)?;
    mac.update(message);
    mac.verify_slice(&signature)
        .map_err(|_| JoinProtocolError::SignatureMismatch)
}

fn ca_discovery_message(
    cluster_name: &str,
    cluster_id: &ClusterId,
    ca_certificate_pem: &str,
    request: &CaDiscoveryRequest,
) -> Result<Vec<u8>, JoinProtocolError> {
    if request.cluster_name != cluster_name {
        return Err(JoinProtocolError::DiscoveryClusterNameMismatch);
    }
    let nonce = decode_hex_array::<32>(&request.nonce, HexField::JoinNonce)?;
    let ca_sha256 = certificate_fingerprint(ca_certificate_pem)?;
    let mut message = Vec::with_capacity(
        CA_DISCOVERY_CONTEXT.len()
            + cluster_name.len()
            + cluster_id.as_str().len()
            + nonce.len()
            + ca_sha256.len()
            + 5,
    );
    for field in [
        CA_DISCOVERY_CONTEXT,
        cluster_name.as_bytes(),
        cluster_id.as_str().as_bytes(),
        nonce.as_slice(),
        ca_sha256.as_bytes(),
    ] {
        message.extend_from_slice(field);
        message.push(0);
    }
    Ok(message)
}

fn decode_hex_array<const SIZE: usize>(
    value: &str,
    field: HexField,
) -> Result<[u8; SIZE], JoinProtocolError> {
    let decoded = hex::decode(value).map_err(|_| JoinProtocolError::InvalidHex {
        field: field.name(),
    })?;
    let observed = decoded.len();
    decoded
        .try_into()
        .map_err(|_| JoinProtocolError::InvalidLength {
            field: field.name(),
            expected: SIZE,
            observed,
        })
}
