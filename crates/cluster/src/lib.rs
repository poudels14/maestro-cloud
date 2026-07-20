//! Cluster formation, membership, and private network contracts.
//!
//! This crate owns the boundary between operator-facing cluster topology and
//! the internal store and mesh providers that realize it.

mod certificates;
mod network;
mod ports;
mod topology;

pub use certificates::{
    CertificateError, CertificateKeyPair, CertificateValidity, ClusterCertificateAuthority,
    NodeCertificateBundle, certificate_fingerprint,
};
pub use network::{CidrError, Ipv4Cidr};
pub use ports::{ClusterPorts, ClusterPortsError, DEFAULT_WIREGUARD_PORT};
pub use topology::{
    ClusterConfig, ClusterPreflightError, NodeDefinition, NodeEndpoint, ValidatedTopology,
    WIREGUARD_MTU_BYTES,
};

#[cfg(test)]
mod tests;
