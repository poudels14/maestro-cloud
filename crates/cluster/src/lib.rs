//! Cluster formation, membership, and private network contracts.
//!
//! This crate owns the boundary between operator-facing cluster topology and
//! the internal store and mesh providers that realize it. It may depend on
//! kernel contracts, but never on node agents, runtimes, operators, or apps.

mod admission;
mod admission_coordinator;
mod certificates;
mod embedded_etcd;
mod embedded_etcd_files;
mod embedded_etcd_membership;
mod embedded_etcd_plan;
mod embedded_etcd_process;
mod join;
mod join_crypto;
mod join_key;
mod network;
mod ports;
mod provider;
mod topology;

pub use admission::{AdmissionError, JoinAdmission, admit_join_request};
pub use admission_coordinator::{
    AdmissionCoordinator, AdmissionCoordinatorError, NodeJoinApproval, NodeJoinApprovalRequest,
    NodeJoinApprovalState,
};
pub use certificates::{
    CertificateError, CertificateKeyPair, CertificateValidity, ClusterCertificateAuthority,
    NodeCertificateBundle, certificate_fingerprint,
};
pub use embedded_etcd::{EmbeddedEtcdProvider, EmbeddedEtcdSettings};
pub use join::{
    CaDiscoveryRequest, CaDiscoveryResponse, JoinPrivateKey, JoinProtocolError, JoinRequest,
    RequestSignature, SignedJoinRequest, create_ca_discovery_response, public_key_fingerprint,
    sign_join_request, verify_ca_discovery_response, verify_join_request_signature,
};
pub use join_crypto::{
    EncryptedJoinResponse, JoinPayload, JoinResponseStatus, decrypt_join_response,
    encrypt_join_response,
};
pub use join_key::{JoinKeyError, load_or_create_join_key};
pub use network::{CidrError, Ipv4Cidr};
pub use ports::{ClusterPorts, ClusterPortsError, DEFAULT_WIREGUARD_PORT};
pub use provider::{
    MemberActivation, MemberState, StoreJoinTicket, StoreMember, StoreProvider,
    StoreProviderConfig, StoreProviderError, StoreRecovery, StoreRecoveryPermit,
    StoreRecoveryReport, StoreRuntime, StoreShutdown, StoreStartMode,
};
pub use topology::{
    ClusterConfig, ClusterPreflightError, NodeDefinition, NodeEndpoint, ValidatedTopology,
    WIREGUARD_MTU_BYTES,
};

#[cfg(test)]
mod tests;
