use std::collections::BTreeMap;

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use chacha20poly1305::{
    ChaCha20Poly1305, Key, Nonce,
    aead::{Aead, Generate, KeyInit, Payload},
};
use hkdf::Hkdf;
use hmac::{Hmac, Mac};
use kernel_api::{ClusterId, NodeId, SecretValue};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use x25519_dalek::{EphemeralSecret, PublicKey};
use zeroize::Zeroizing;

use crate::{
    CloudflareTunnelConfig, ClusterCertificateAuthority, ClusterLaunchPolicy, ClusterPorts,
    Ipv4Cidr, JoinPrivateKey, JoinProtocolError, JoinRequest, NodeCertificateBundle,
    NodeDefinition, TailscaleGatewayConfig,
    join::{canonical_body, decode_leader_public_key, decode_public_key, validate_shared_secret},
};

type HmacSha256 = Hmac<Sha256>;

const RESPONSE_CONTEXT: &[u8] = b"maestro-join-response-v1";

/// Successful cluster state delivered only to the authenticated joiner.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct JoinPayload {
    /// Stable identity of the joined cluster.
    pub cluster_id: ClusterId,
    /// Operator-facing cluster name.
    pub cluster_name: String,
    /// Authoritative node topology at admission time.
    pub nodes: BTreeMap<NodeId, NodeDefinition>,
    /// Authoritative private networks allowed to initiate control traffic.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub control_allow_cidrs: Vec<Ipv4Cidr>,
    /// Persisted cluster service ports.
    pub ports: ClusterPorts,
    /// Optional managed Tailscale subnet-router fleet.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tailscale: Option<TailscaleGatewayConfig>,
    /// Optional remotely managed Cloudflare Tunnel connector fleet.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cloudflare: Option<CloudflareTunnelConfig>,
    /// Optional production integrations copied into the joining node's protected launch document.
    #[serde(default)]
    pub launch_policy: ClusterLaunchPolicy,
    /// Node-specific mutual-authentication identity.
    pub certificates: NodeCertificateBundle,
    /// Cluster-wide key used to encrypt persisted internal values.
    pub store_encryption_secret: SecretValue,
    /// Opaque store membership data granted to control-plane joiners.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub store_join_ticket: Option<crate::StoreJoinTicket>,
    /// Trust-root signing material granted only to control-plane nodes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub certificate_issuer: Option<ClusterCertificateAuthority>,
}

/// Validated HTTP status bound into encrypted response authentication.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JoinResponseStatus(u16);

impl JoinResponseStatus {
    /// Status used for an admitted join request.
    pub const ACCEPTED: Self = Self(200);

    /// Accepts only assigned HTTP response codes.
    pub fn new(status: u16) -> Result<Self, JoinProtocolError> {
        if !(100..=599).contains(&status) {
            return Err(JoinProtocolError::InvalidResponseStatus { status });
        }
        Ok(Self(status))
    }

    /// Returns the HTTP wire value.
    pub fn as_u16(self) -> u16 {
        self.0
    }
}

/// Authenticated ciphertext returned by the current cluster leader.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EncryptedJoinResponse {
    /// Cluster identity used as the HKDF salt.
    pub cluster_id: ClusterId,
    /// Ephemeral X25519 public key of the admitting leader.
    pub leader_public_key: String,
    /// ChaCha20-Poly1305 nonce encoded as Base64.
    pub nonce: String,
    /// Authenticated payload ciphertext encoded as Base64.
    pub ciphertext: String,
}

/// Encrypts a payload to the public key authenticated by the signed request.
pub fn encrypt_join_response(
    secret: &SecretValue,
    request: &JoinRequest,
    payload: &JoinPayload,
    status: JoinResponseStatus,
) -> Result<EncryptedJoinResponse, JoinProtocolError> {
    validate_payload_binding(request, payload)?;
    let joiner_public = PublicKey::from(decode_public_key(&request.joiner_public_key)?);
    let leader_secret = EphemeralSecret::random();
    let leader_public = PublicKey::from(&leader_secret);
    let shared = leader_secret.diffie_hellman(&joiner_public);
    let key = Zeroizing::new(derive_response_key(
        secret,
        request,
        &payload.cluster_id,
        shared.as_bytes(),
    )?);
    let cipher = ChaCha20Poly1305::new(&Key::from(*key));
    let nonce = Nonce::generate();
    let plaintext = serde_json::to_vec(payload).map_err(JoinProtocolError::Serialization)?;
    let aad = response_aad(request, status)?;
    let ciphertext = cipher
        .encrypt(
            &nonce,
            Payload {
                msg: &plaintext,
                aad: &aad,
            },
        )
        .map_err(|_| JoinProtocolError::ResponseEncryption)?;

    Ok(EncryptedJoinResponse {
        cluster_id: payload.cluster_id.clone(),
        leader_public_key: hex::encode(leader_public.as_bytes()),
        nonce: BASE64.encode(nonce.as_slice()),
        ciphertext: BASE64.encode(ciphertext),
    })
}

/// Decrypts and authenticates a response against the complete signed request.
pub fn decrypt_join_response(
    secret: &SecretValue,
    private_key: &JoinPrivateKey,
    request: &JoinRequest,
    envelope: &EncryptedJoinResponse,
    status: JoinResponseStatus,
) -> Result<JoinPayload, JoinProtocolError> {
    if envelope.cluster_id != request.cluster_id {
        return Err(JoinProtocolError::ResponseClusterMismatch);
    }
    let leader_public = PublicKey::from(decode_leader_public_key(&envelope.leader_public_key)?);
    let shared = private_key.secret().diffie_hellman(&leader_public);
    let key = Zeroizing::new(derive_response_key(
        secret,
        request,
        &envelope.cluster_id,
        shared.as_bytes(),
    )?);
    let cipher = ChaCha20Poly1305::new(&Key::from(*key));
    let nonce = BASE64
        .decode(&envelope.nonce)
        .map_err(|_| JoinProtocolError::InvalidResponseNonce)?;
    let nonce: [u8; 12] = nonce
        .try_into()
        .map_err(|_| JoinProtocolError::InvalidResponseNonce)?;
    let ciphertext = BASE64
        .decode(&envelope.ciphertext)
        .map_err(|_| JoinProtocolError::ResponseAuthentication)?;
    let aad = response_aad(request, status)?;
    let plaintext = cipher
        .decrypt(
            &Nonce::from(nonce),
            Payload {
                msg: &ciphertext,
                aad: &aad,
            },
        )
        .map_err(|_| JoinProtocolError::ResponseAuthentication)?;
    let payload: JoinPayload =
        serde_json::from_slice(&plaintext).map_err(JoinProtocolError::Serialization)?;
    validate_payload_binding(request, &payload)?;
    Ok(payload)
}

fn derive_response_key(
    secret: &SecretValue,
    request: &JoinRequest,
    cluster_id: &ClusterId,
    shared_secret: &[u8; 32],
) -> Result<[u8; 32], JoinProtocolError> {
    validate_shared_secret(secret)?;
    let request_nonce = hex::decode(&request.nonce).map_err(|_| JoinProtocolError::InvalidHex {
        field: "join nonce",
    })?;
    let mut nonce_mac = HmacSha256::new_from_slice(secret.expose().as_bytes())
        .map_err(|_| JoinProtocolError::AuthenticationInitialization)?;
    nonce_mac.update(&request_nonce);
    let nonce_proof = nonce_mac.finalize().into_bytes();
    let mut input = Zeroizing::new(Vec::with_capacity(shared_secret.len() + nonce_proof.len()));
    input.extend_from_slice(shared_secret);
    input.extend_from_slice(&nonce_proof);
    let hkdf = Hkdf::<Sha256>::new(Some(cluster_id.as_str().as_bytes()), &input);
    let mut key = [0_u8; 32];
    hkdf.expand(RESPONSE_CONTEXT, &mut key)
        .map_err(|_| JoinProtocolError::KeyDerivation)?;
    Ok(key)
}

fn validate_payload_binding(
    request: &JoinRequest,
    payload: &JoinPayload,
) -> Result<(), JoinProtocolError> {
    if request.cluster_id != payload.cluster_id {
        return Err(JoinProtocolError::ResponseClusterMismatch);
    }
    if request.cluster_name != payload.cluster_name || request.ports != payload.ports {
        return Err(JoinProtocolError::ResponseConfigurationMismatch);
    }
    let requested_node = NodeDefinition {
        hostname: request.hostname.clone(),
        endpoint: request.endpoint,
        workload_subnet: request.workload_subnet,
        role: request.role,
    };
    if payload.nodes.get(&request.node_id) != Some(&requested_node) {
        return Err(JoinProtocolError::ResponseConfigurationMismatch);
    }
    if request.role.is_control_plane() != payload.certificate_issuer.is_some()
        || request.role.is_control_plane() != payload.store_join_ticket.is_some()
    {
        return Err(JoinProtocolError::ResponseIssuerGrantMismatch);
    }
    if payload.store_encryption_secret.expose().chars().count() < 32 {
        return Err(JoinProtocolError::ResponseStoreSecretGrantMismatch);
    }
    Ok(())
}

fn response_aad(
    request: &JoinRequest,
    status: JoinResponseStatus,
) -> Result<Vec<u8>, JoinProtocolError> {
    let request_digest = Sha256::digest(canonical_body(request)?);
    let mut aad = Vec::with_capacity(RESPONSE_CONTEXT.len() + request_digest.len() + 3);
    aad.extend_from_slice(RESPONSE_CONTEXT);
    aad.push(0);
    aad.extend_from_slice(&request_digest);
    aad.extend_from_slice(&status.as_u16().to_be_bytes());
    Ok(aad)
}
