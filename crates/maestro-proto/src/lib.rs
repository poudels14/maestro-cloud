mod join_client;
mod pki;

pub mod pb {
    tonic::include_proto!("maestro.v1");
}

pub use join_client::connect_pinned;
pub use pki::{
    CertRole, ClusterCa, NODE_SERVER_NAME, NodeCredentials, PeerIdentity, PkiError,
    VOTER_SERVER_NAME, fingerprint_of_pem, install_crypto_provider, peer_identity,
    pinned_client_config,
};
