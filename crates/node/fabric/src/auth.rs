use std::collections::BTreeMap;

use kernel_api::{AssignmentId, DeploymentId, NodeId, ServiceId, WorkloadId};
use subtle::ConstantTimeEq;
use zeroize::{Zeroize, ZeroizeOnDrop};

const TOKEN_LENGTH: usize = 32;

/// Per-workload bearer token mounted with that workload's private node socket.
#[derive(Clone, Zeroize, ZeroizeOnDrop)]
pub struct WorkloadToken([u8; TOKEN_LENGTH]);

impl WorkloadToken {
    /// Constructs a token from cryptographically random agent-minted bytes.
    pub fn from_bytes(bytes: [u8; TOKEN_LENGTH]) -> Self {
        Self(bytes)
    }

    /// Returns token bytes for mounting into the authorized workload only.
    pub fn expose(&self) -> &[u8; TOKEN_LENGTH] {
        &self.0
    }
}

impl std::fmt::Debug for WorkloadToken {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("WorkloadToken([REDACTED])")
    }
}

/// Unix peer credentials captured from `SO_PEERCRED` for one accepted stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SocketPeer {
    /// Host process identity reported by the kernel.
    pub process_id: u32,
    /// Host user identity reported by the kernel.
    pub user_id: u32,
    /// Host group identity reported by the kernel.
    pub group_id: u32,
}

/// Typed downward-API identity bound to one workload credential.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkloadClaims {
    /// Runtime workload receiving the mounted socket.
    pub workload_id: WorkloadId,
    /// Assignment that created the workload.
    pub assignment_id: AssignmentId,
    /// Node hosting the workload.
    pub node_id: NodeId,
    /// Service owning the workload.
    pub service_id: ServiceId,
    /// Immutable deployment owning the workload.
    pub deployment_id: DeploymentId,
    /// Resource labels exposed through the downward API.
    pub labels: BTreeMap<String, String>,
}

/// Token, peer-credential, and claims binding for one private workload socket.
pub struct WorkloadAuthorization {
    token: WorkloadToken,
    expected_user_id: u32,
    claims: WorkloadClaims,
}

impl WorkloadAuthorization {
    /// Binds agent-minted credentials and claims to the workload's host user.
    pub fn new(token: WorkloadToken, expected_user_id: u32, claims: WorkloadClaims) -> Self {
        Self {
            token,
            expected_user_id,
            claims,
        }
    }

    /// Authenticates one request using constant-time token comparison and kernel peer identity.
    pub fn authorize(
        &self,
        presented_token: &[u8],
        peer: SocketPeer,
    ) -> Result<&WorkloadClaims, AuthorizationError> {
        if peer.user_id != self.expected_user_id {
            Err(AuthorizationError::PeerUserMismatch {
                expected: self.expected_user_id,
                actual: peer.user_id,
            })
        } else if presented_token.len() != TOKEN_LENGTH
            || !bool::from(self.token.0.ct_eq(presented_token))
        {
            Err(AuthorizationError::InvalidToken)
        } else {
            Ok(&self.claims)
        }
    }
}

impl std::fmt::Debug for WorkloadAuthorization {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WorkloadAuthorization")
            .field("token", &self.token)
            .field("expected_user_id", &self.expected_user_id)
            .field("claims", &self.claims)
            .finish()
    }
}

/// Matchable per-request workload authentication failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum AuthorizationError {
    /// The bearer token was missing, malformed, or did not match.
    #[error("workload token is invalid")]
    InvalidToken,
    /// Kernel peer credentials did not match the workload's configured host user.
    #[error("workload peer user {actual} does not match expected user {expected}")]
    PeerUserMismatch {
        /// Host user configured for the workload.
        expected: u32,
        /// Host user reported by `SO_PEERCRED`.
        actual: u32,
    },
}
