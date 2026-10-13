mod join_client;
mod pki;

pub mod pb {
    tonic::include_proto!("maestro.v1");
}

pub use join_client::connect_pinned;
pub use pki::{
    ClusterCa, NODE_SERVER_NAME, NodeCredentials, PkiError, fingerprint_of_pem,
    install_crypto_provider, pinned_client_config,
};
