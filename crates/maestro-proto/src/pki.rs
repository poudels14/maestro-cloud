use std::net::IpAddr;
use std::path::Path;
use std::sync::Arc;

use rcgen::{
    BasicConstraints, CertificateParams, DistinguishedName, DnType, ExtendedKeyUsagePurpose, IsCa,
    Issuer, KeyPair, KeyUsagePurpose,
};
use rustls::client::WebPkiServerVerifier;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::CryptoProvider;
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{DigitallySignedStruct, RootCertStore, SignatureScheme};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tonic::transport::{Certificate, ClientTlsConfig, Identity, ServerTlsConfig};

pub const NODE_SERVER_NAME: &str = "node.maestro.internal";

const CA_CERT_FILE: &str = "ca.crt";
const CA_KEY_FILE: &str = "ca.key";
const NODE_CERT_FILE: &str = "node.crt";
const NODE_KEY_FILE: &str = "node.key";

#[derive(Debug, Error)]
pub enum PkiError {
    #[error("certificate generation failed: {0}")]
    Generate(#[from] rcgen::Error),
    #[error("failed to access PKI files: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid certificate: {0}")]
    Invalid(String),
}

pub struct ClusterCa {
    cert_pem: String,
    key_pem: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeCredentials {
    pub cert_pem: String,
    pub key_pem: String,
    pub ca_pem: String,
}

#[derive(Debug)]
struct PinnedCaVerifier {
    fingerprint: String,
    provider: Arc<CryptoProvider>,
}

impl ClusterCa {
    pub fn generate(cluster_name: &str) -> Result<Self, PkiError> {
        let key = KeyPair::generate()?;
        let mut params = CertificateParams::new(Vec::<String>::new())?;
        params.distinguished_name = distinguished_name(&format!("maestro {cluster_name} CA"));
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        let cert = params.self_signed(&key)?;
        Ok(Self {
            cert_pem: cert.pem(),
            key_pem: key.serialize_pem(),
        })
    }

    pub fn load(directory: &Path) -> Result<Self, PkiError> {
        Ok(Self {
            cert_pem: std::fs::read_to_string(directory.join(CA_CERT_FILE))?,
            key_pem: std::fs::read_to_string(directory.join(CA_KEY_FILE))?,
        })
    }

    pub fn from_pem(cert_pem: String, key_pem: String) -> Self {
        Self { cert_pem, key_pem }
    }

    pub fn key_pem(&self) -> &str {
        &self.key_pem
    }

    pub fn save(&self, directory: &Path) -> Result<(), PkiError> {
        std::fs::create_dir_all(directory)?;
        std::fs::write(directory.join(CA_CERT_FILE), &self.cert_pem)?;
        write_private(&directory.join(CA_KEY_FILE), &self.key_pem)
    }

    pub fn cert_pem(&self) -> &str {
        &self.cert_pem
    }

    pub fn fingerprint(&self) -> Result<String, PkiError> {
        fingerprint_of_pem(&self.cert_pem)
    }

    pub fn issue_node(
        &self,
        node_id: u64,
        addresses: &[IpAddr],
    ) -> Result<NodeCredentials, PkiError> {
        let ca_key = KeyPair::from_pem(&self.key_pem)?;
        let issuer = Issuer::from_ca_cert_pem(&self.cert_pem, ca_key)?;
        let node_key = KeyPair::generate()?;
        let names = [NODE_SERVER_NAME.to_string()]
            .into_iter()
            .chain(addresses.iter().map(ToString::to_string))
            .collect::<Vec<_>>();
        let mut params = CertificateParams::new(names)?;
        params.distinguished_name = distinguished_name(&format!("maestro-node-{node_id}"));
        params.key_usages = vec![
            KeyUsagePurpose::DigitalSignature,
            KeyUsagePurpose::KeyEncipherment,
        ];
        params.extended_key_usages = vec![
            ExtendedKeyUsagePurpose::ServerAuth,
            ExtendedKeyUsagePurpose::ClientAuth,
        ];
        let cert = params.signed_by(&node_key, &issuer)?;
        Ok(NodeCredentials {
            cert_pem: cert.pem(),
            key_pem: node_key.serialize_pem(),
            ca_pem: self.cert_pem.clone(),
        })
    }
}

pub fn install_crypto_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

impl NodeCredentials {
    pub fn load(directory: &Path) -> Result<Self, PkiError> {
        Ok(Self {
            cert_pem: std::fs::read_to_string(directory.join(NODE_CERT_FILE))?,
            key_pem: std::fs::read_to_string(directory.join(NODE_KEY_FILE))?,
            ca_pem: std::fs::read_to_string(directory.join(CA_CERT_FILE))?,
        })
    }

    pub fn save(&self, directory: &Path) -> Result<(), PkiError> {
        std::fs::create_dir_all(directory)?;
        std::fs::write(directory.join(NODE_CERT_FILE), &self.cert_pem)?;
        std::fs::write(directory.join(CA_CERT_FILE), &self.ca_pem)?;
        write_private(&directory.join(NODE_KEY_FILE), &self.key_pem)
    }

    pub fn server_tls(&self) -> ServerTlsConfig {
        install_crypto_provider();
        let chain = format!("{}{}", self.cert_pem, self.ca_pem);
        ServerTlsConfig::new()
            .identity(Identity::from_pem(chain, &self.key_pem))
            .client_ca_root(Certificate::from_pem(&self.ca_pem))
            .client_auth_optional(true)
    }

    pub fn client_tls(&self) -> ClientTlsConfig {
        install_crypto_provider();
        ClientTlsConfig::new()
            .ca_certificate(Certificate::from_pem(&self.ca_pem))
            .identity(Identity::from_pem(&self.cert_pem, &self.key_pem))
            .domain_name(NODE_SERVER_NAME)
    }
}

pub fn fingerprint_of_pem(pem: &str) -> Result<String, PkiError> {
    let der = rustls::pki_types::pem::PemObject::from_pem_slice(pem.as_bytes())
        .map(|cert: CertificateDer<'static>| cert.to_vec())
        .map_err(|error| PkiError::Invalid(error.to_string()))?;
    Ok(hex::encode(Sha256::digest(der)))
}

pub fn pinned_client_config(ca_fingerprint: &str) -> rustls::ClientConfig {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let verifier = PinnedCaVerifier {
        fingerprint: ca_fingerprint.to_ascii_lowercase(),
        provider: provider.clone(),
    };
    let mut config = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .unwrap_or_else(|_| unreachable!("ring supports the default protocol versions"))
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(verifier))
        .with_no_client_auth();
    config.alpn_protocols = vec![b"h2".to_vec()];
    config
}

impl ServerCertVerifier for PinnedCaVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        let pinned = intermediates
            .iter()
            .find(|cert| hex::encode(Sha256::digest(cert.as_ref())) == self.fingerprint)
            .ok_or_else(|| {
                rustls::Error::General("server did not present the pinned cluster CA".to_string())
            })?;
        let mut roots = RootCertStore::empty();
        roots.add(pinned.clone().into_owned())?;
        let verifier =
            WebPkiServerVerifier::builder_with_provider(Arc::new(roots), self.provider.clone())
                .build()
                .map_err(|error| rustls::Error::General(error.to_string()))?;
        verifier.verify_server_cert(end_entity, &[], server_name, ocsp_response, now)
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        signature: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            signature,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        signature: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            signature,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

fn distinguished_name(common_name: &str) -> DistinguishedName {
    let mut name = DistinguishedName::new();
    name.push(DnType::CommonName, common_name);
    name
}

fn write_private(path: &Path, contents: &str) -> Result<(), PkiError> {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(path)?;
    file.write_all(contents.as_bytes())?;
    Ok(())
}
