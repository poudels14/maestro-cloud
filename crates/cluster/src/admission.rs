use std::net::Ipv4Addr;

use kernel_api::{NodeId, NodeRole};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    ClusterConfig, ClusterPreflightError, Ipv4Cidr, JoinProtocolError, JoinRequest, NodeDefinition,
    NodeEndpoint, RequestSignature, join::canonical_body, public_key_fingerprint,
    verify_join_request_signature,
};

/// Immutable evidence that an authenticated request matched declared topology.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct JoinAdmission {
    /// Stable node identity reserved by this admission.
    pub node_id: NodeId,
    /// Public cluster role authorized by configuration.
    pub role: NodeRole,
    /// Private source endpoint authorized by configuration.
    pub endpoint: NodeEndpoint,
    /// Workload network reserved for the node.
    pub workload_subnet: Ipv4Cidr,
    /// Fingerprint of the encryption key bound to the signed request.
    pub public_key_sha256: String,
    /// Digest used to make repeat admission attempts idempotently comparable.
    pub request_sha256: String,
    /// Nonce persisted by the admission coordinator to prevent replay.
    pub request_nonce: String,
    /// Admission time supplied by the composition root's clock.
    pub admitted_at_unix_ms: i64,
}

/// Authenticates and evaluates a join without performing external mutation.
pub fn admit_join_request(
    config: &ClusterConfig,
    request: &JoinRequest,
    signature: &RequestSignature,
    source_address: Ipv4Addr,
    now_unix_ms: i64,
) -> Result<JoinAdmission, AdmissionError> {
    request.validate_wire_shape(now_unix_ms)?;
    verify_join_request_signature(&config.join_secret, request, signature)?;
    config.preflight()?;

    if request.cluster_id != config.cluster_id || request.cluster_name != config.name {
        return Err(AdmissionError::ClusterIdentityMismatch);
    }
    if request.ports != config.ports {
        return Err(AdmissionError::PortAllocationMismatch);
    }
    if request.endpoint.host_address != source_address {
        return Err(AdmissionError::SourceAddressMismatch {
            requested: request.endpoint.host_address,
            observed: source_address,
        });
    }
    if !config.control_allow_cidrs.is_empty()
        && !config
            .control_allow_cidrs
            .iter()
            .any(|network| network.contains(source_address))
    {
        return Err(AdmissionError::SourceOutsideControlNetworks {
            address: source_address,
        });
    }

    let declared =
        config
            .nodes
            .get(&request.node_id)
            .ok_or_else(|| AdmissionError::UnknownNode {
                node_id: request.node_id.clone(),
            })?;
    let requested = NodeDefinition {
        hostname: request.hostname.clone(),
        endpoint: request.endpoint,
        workload_subnet: request.workload_subnet,
        role: request.role,
    };
    if declared != &requested {
        return Err(AdmissionError::DeclaredNodeMismatch {
            node_id: request.node_id.clone(),
        });
    }

    let canonical = canonical_body(request)?;
    Ok(JoinAdmission {
        node_id: request.node_id.clone(),
        role: request.role,
        endpoint: request.endpoint,
        workload_subnet: request.workload_subnet,
        public_key_sha256: public_key_fingerprint(&request.joiner_public_key)?,
        request_sha256: hex::encode(Sha256::digest(canonical)),
        request_nonce: request.nonce.clone(),
        admitted_at_unix_ms: now_unix_ms,
    })
}

/// Why a signed request was denied before any membership mutation.
#[derive(Debug, thiserror::Error)]
pub enum AdmissionError {
    /// Cryptographic shape, freshness, or authentication failed.
    #[error("join protocol validation failed: {0}")]
    Protocol(#[from] JoinProtocolError),
    /// The authoritative cluster configuration was unsafe.
    #[error("cluster preflight failed: {0}")]
    InvalidTopology(#[from] ClusterPreflightError),
    /// Cluster name or stable identity differed from the admission server.
    #[error("join request targets a different cluster")]
    ClusterIdentityMismatch,
    /// The joiner did not use the persisted cluster port allocation.
    #[error("join request cluster ports differ from the persisted allocation")]
    PortAllocationMismatch,
    /// The transport source did not match the signed control address.
    #[error("join source `{observed}` does not match requested address `{requested}`")]
    SourceAddressMismatch {
        requested: Ipv4Addr,
        observed: Ipv4Addr,
    },
    /// The source was not admitted by a configured private allowlist.
    #[error("join source `{address}` is outside the control allowlist")]
    SourceOutsideControlNetworks { address: Ipv4Addr },
    /// No node with the signed stable identity was declared.
    #[error("node `{node_id}` is absent from the cluster topology")]
    UnknownNode { node_id: NodeId },
    /// Role, hostname, endpoint, or workload network differed from declaration.
    #[error("join request for node `{node_id}` differs from the declared node definition")]
    DeclaredNodeMismatch { node_id: NodeId },
}
