use std::fmt::{Debug, Formatter};
use std::io::Write;
use std::net::Ipv4Addr;
use std::path::{Path, PathBuf};

use kernel_api::{NodeId, NodeRole, SecretValue};
use rcgen::{
    BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose, SanType,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use time::OffsetDateTime;
use x509_parser::{parse_x509_certificate, pem::parse_x509_pem};

use crate::NodeDefinition;

/// Explicit validity interval supplied by the composition root's clock.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CertificateValidity {
    not_before: OffsetDateTime,
    not_after: OffsetDateTime,
}

impl CertificateValidity {
    /// Creates a non-empty certificate validity interval.
    pub fn new(
        not_before: OffsetDateTime,
        not_after: OffsetDateTime,
    ) -> Result<Self, CertificateError> {
        if not_after <= not_before {
            return Err(CertificateError::InvalidValidity);
        }
        Ok(Self {
            not_before,
            not_after,
        })
    }
}

/// A certificate and its secret private key.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CertificateKeyPair {
    /// PEM-encoded X.509 certificate.
    pub certificate_pem: String,
    /// PEM-encoded PKCS#8 private key with redacted debug output.
    pub private_key_pem: SecretValue,
}

impl Debug for CertificateKeyPair {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CertificateKeyPair")
            .field("certificate_pem", &"[PEM]")
            .field("private_key_pem", &self.private_key_pem)
            .finish()
    }
}

/// Cluster trust root retained only by control-plane certificate issuers.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ClusterCertificateAuthority {
    /// Public PEM certificate distributed to every cluster member.
    pub certificate_pem: String,
    /// Secret PEM key distributed only to authorized control-plane issuers.
    pub private_key_pem: SecretValue,
}

impl Debug for ClusterCertificateAuthority {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ClusterCertificateAuthority")
            .field("certificate_pem", &"[PEM]")
            .field("private_key_pem", &self.private_key_pem)
            .finish()
    }
}

impl ClusterCertificateAuthority {
    /// Validates the certificate, signing capability, and private-key binding.
    pub fn validate(&self) -> Result<(), CertificateError> {
        validate_ca_material(&self.certificate_pem, self.private_key_pem.expose())
    }

    /// Generates a self-signed trust root for one named cluster.
    pub fn generate(
        cluster_name: &str,
        validity: CertificateValidity,
    ) -> Result<Self, CertificateError> {
        let key = KeyPair::generate()?;
        let params = ca_params(cluster_name, validity);
        let certificate = params.self_signed(&key)?;
        Ok(Self {
            certificate_pem: certificate.pem(),
            private_key_pem: SecretValue::new(key.serialize_pem()),
        })
    }

    /// Loads existing CA material or initializes it without overwriting files.
    pub fn load_or_initialize(
        directory: &Path,
        cluster_name: &str,
        validity: CertificateValidity,
    ) -> Result<Self, CertificateError> {
        let certificate_path = directory.join("ca.pem");
        let private_key_path = directory.join("ca-key.pem");
        let certificate_exists = certificate_path.exists();
        let private_key_exists = private_key_path.exists();

        match (certificate_exists, private_key_exists) {
            (true, true) => Self::load(directory),
            (true, false) | (false, true) => Err(CertificateError::IncompleteAuthority {
                directory: directory.to_path_buf(),
            }),
            (false, false) => {
                create_directory(directory)?;
                let authority = Self::generate(cluster_name, validity)?;
                write_new_file(
                    &private_key_path,
                    authority.private_key_pem.expose().as_bytes(),
                    FileSensitivity::Private,
                )?;
                write_new_file(
                    &certificate_path,
                    authority.certificate_pem.as_bytes(),
                    FileSensitivity::Public,
                )?;
                sync_directory(directory)?;
                Ok(authority)
            }
        }
    }

    /// Loads and validates persisted trust-root material.
    pub fn load(directory: &Path) -> Result<Self, CertificateError> {
        let certificate_path = directory.join("ca.pem");
        let private_key_path = directory.join("ca-key.pem");
        validate_private_permissions(&private_key_path)?;
        let certificate_pem = read_text(&certificate_path)?;
        let private_key_pem = read_text(&private_key_path)?;
        validate_ca_material(&certificate_pem, &private_key_pem)?;
        Ok(Self {
            certificate_pem,
            private_key_pem: SecretValue::new(private_key_pem),
        })
    }

    /// Issues one node identity usable for mutually authenticated cluster RPC.
    pub fn issue_node_certificate(
        &self,
        node_id: &NodeId,
        hostname: &str,
        host_address: Ipv4Addr,
        role: NodeRole,
        validity: CertificateValidity,
    ) -> Result<NodeCertificateBundle, CertificateError> {
        self.issue_node_certificate_with_ip_sans(
            node_id,
            hostname,
            host_address,
            &[],
            role,
            validity,
        )
    }

    /// Issues one node identity for both its control and workload-bridge addresses.
    pub fn issue_node_certificate_for_definition(
        &self,
        node_id: &NodeId,
        node: &NodeDefinition,
        validity: CertificateValidity,
    ) -> Result<NodeCertificateBundle, CertificateError> {
        let bridge_address = node.workload_subnet.gateway_address().ok_or(
            CertificateError::MissingWorkloadBridge {
                network: node.workload_subnet.to_string(),
            },
        )?;
        self.issue_node_certificate_with_ip_sans(
            node_id,
            &node.hostname,
            node.endpoint.host_address,
            &[bridge_address],
            node.role,
            validity,
        )
    }

    /// Issues one node identity with extra IP subject alternative names.
    ///
    /// The primary address remains required. Extra addresses support nodes
    /// whose authenticated cluster traffic can originate from another local
    /// interface.
    pub fn issue_node_certificate_with_ip_sans(
        &self,
        node_id: &NodeId,
        hostname: &str,
        host_address: Ipv4Addr,
        additional_host_addresses: &[Ipv4Addr],
        role: NodeRole,
        validity: CertificateValidity,
    ) -> Result<NodeCertificateBundle, CertificateError> {
        validate_ca_material(&self.certificate_pem, self.private_key_pem.expose())?;
        let issuer_key = KeyPair::from_pem(self.private_key_pem.expose())?;
        let issuer = Issuer::from_ca_cert_pem(&self.certificate_pem, &issuer_key)?;
        let node_key = KeyPair::generate()?;
        let params = node_params(
            node_id,
            hostname,
            host_address,
            additional_host_addresses,
            role,
            validity,
        )?;
        let certificate = params.signed_by(&node_key, &issuer)?;

        Ok(NodeCertificateBundle {
            trust_root_pem: self.certificate_pem.clone(),
            identity: CertificateKeyPair {
                certificate_pem: certificate.pem(),
                private_key_pem: SecretValue::new(node_key.serialize_pem()),
            },
        })
    }
}

/// Trust and leaf material delivered to an admitted node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NodeCertificateBundle {
    /// Public cluster root used to authenticate every peer.
    pub trust_root_pem: String,
    /// Node-specific certificate and private key.
    pub identity: CertificateKeyPair,
}

/// Returns the SHA-256 fingerprint of the DER certificate bytes.
pub fn certificate_fingerprint(certificate_pem: &str) -> Result<String, CertificateError> {
    let der = certificate_der(certificate_pem)?;
    Ok(hex::encode(Sha256::digest(der)))
}

/// Why cluster certificate generation, validation, or persistence failed.
#[derive(Debug, thiserror::Error)]
pub enum CertificateError {
    /// A certificate cannot have an empty or reversed lifetime.
    #[error("certificate expiration must be later than its activation time")]
    InvalidValidity,
    /// PEM or X.509 data was malformed or failed signature validation.
    #[error("invalid cluster certificate authority: {reason}")]
    InvalidAuthority { reason: String },
    /// A CA certificate was paired with a different private key.
    #[error("cluster CA certificate and private key do not match")]
    AuthorityKeyMismatch,
    /// Only one of the two persisted CA files was present.
    #[error("cluster certificate authority is incomplete in `{}`", directory.display())]
    IncompleteAuthority { directory: PathBuf },
    /// A persisted signing key was readable by users other than its owner.
    #[error("private key `{}` has insecure permissions {mode:#o}", path.display())]
    InsecurePermissions { path: PathBuf, mode: u32 },
    /// A DNS subject alternative name was not syntactically valid.
    #[error("invalid certificate hostname `{hostname}`")]
    InvalidHostname { hostname: String },
    /// A declared workload subnet could not supply a bridge address.
    #[error("workload subnet `{network}` has no certificate bridge address")]
    MissingWorkloadBridge { network: String },
    /// Filesystem work failed at a certificate persistence boundary.
    #[error("failed to {action} certificate file `{}`: {source}", path.display())]
    Io {
        action: &'static str,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    /// The certificate generator rejected a key or certificate profile.
    #[error("certificate generation failed: {0}")]
    Generator(#[from] rcgen::Error),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FileSensitivity {
    Public,
    Private,
}

fn ca_params(cluster_name: &str, validity: CertificateValidity) -> CertificateParams {
    let mut params = CertificateParams::default();
    params.not_before = validity.not_before;
    params.not_after = validity.not_after;
    params
        .distinguished_name
        .push(DnType::CommonName, format!("maestro-{cluster_name}-ca"));
    params
        .distinguished_name
        .push(DnType::OrganizationName, "maestro");
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    params
}

fn node_params(
    node_id: &NodeId,
    hostname: &str,
    host_address: Ipv4Addr,
    additional_host_addresses: &[Ipv4Addr],
    role: NodeRole,
    validity: CertificateValidity,
) -> Result<CertificateParams, CertificateError> {
    let dns_name =
        hostname
            .to_owned()
            .try_into()
            .map_err(|_| CertificateError::InvalidHostname {
                hostname: hostname.to_owned(),
            })?;
    let mut params = CertificateParams::default();
    params.not_before = validity.not_before;
    params.not_after = validity.not_after;
    params
        .distinguished_name
        .push(DnType::CommonName, format!("maestro-node-{node_id}"));
    params
        .distinguished_name
        .push(DnType::OrganizationName, "maestro");
    params
        .distinguished_name
        .push(DnType::OrganizationalUnitName, role_name(role));
    params.subject_alt_names = vec![
        SanType::DnsName(dns_name),
        SanType::IpAddress(host_address.into()),
    ];
    params.subject_alt_names.extend(
        additional_host_addresses
            .iter()
            .copied()
            .filter(|address| *address != host_address)
            .map(|address| SanType::IpAddress(address.into())),
    );
    params.key_usages = vec![
        KeyUsagePurpose::DigitalSignature,
        KeyUsagePurpose::KeyEncipherment,
    ];
    params.extended_key_usages = vec![
        ExtendedKeyUsagePurpose::ServerAuth,
        ExtendedKeyUsagePurpose::ClientAuth,
    ];
    Ok(params)
}

fn role_name(role: NodeRole) -> &'static str {
    match role {
        NodeRole::Master => "master",
        NodeRole::Hybrid => "hybrid",
        NodeRole::ControlPlane => "control-plane",
        NodeRole::Worker => "worker",
    }
}

fn validate_ca_material(
    certificate_pem: &str,
    private_key_pem: &str,
) -> Result<(), CertificateError> {
    let der = certificate_der(certificate_pem)?;
    let (_, certificate) =
        parse_x509_certificate(&der).map_err(|error| CertificateError::InvalidAuthority {
            reason: error.to_string(),
        })?;
    certificate
        .verify_signature(None)
        .map_err(|error| CertificateError::InvalidAuthority {
            reason: error.to_string(),
        })?;
    let constraints =
        certificate
            .basic_constraints()
            .map_err(|error| CertificateError::InvalidAuthority {
                reason: error.to_string(),
            })?;
    if !constraints.is_some_and(|constraints| constraints.value.ca) {
        return Err(CertificateError::InvalidAuthority {
            reason: "certificate is not authorized to sign certificates".to_owned(),
        });
    }

    let key = KeyPair::from_pem(private_key_pem)?;
    if certificate.public_key().subject_public_key.data.as_ref() != key.public_key_raw() {
        return Err(CertificateError::AuthorityKeyMismatch);
    }
    Ok(())
}

fn certificate_der(certificate_pem: &str) -> Result<Vec<u8>, CertificateError> {
    let (_, pem) = parse_x509_pem(certificate_pem.as_bytes()).map_err(|error| {
        CertificateError::InvalidAuthority {
            reason: error.to_string(),
        }
    })?;
    if pem.label != "CERTIFICATE" {
        return Err(CertificateError::InvalidAuthority {
            reason: format!("unexpected PEM label `{}`", pem.label),
        });
    }
    Ok(pem.contents)
}

fn create_directory(directory: &Path) -> Result<(), CertificateError> {
    std::fs::create_dir_all(directory).map_err(|source| CertificateError::Io {
        action: "create directory for",
        path: directory.to_path_buf(),
        source,
    })
}

fn read_text(path: &Path) -> Result<String, CertificateError> {
    std::fs::read_to_string(path).map_err(|source| CertificateError::Io {
        action: "read",
        path: path.to_path_buf(),
        source,
    })
}

fn write_new_file(
    path: &Path,
    contents: &[u8],
    sensitivity: FileSensitivity,
) -> Result<(), CertificateError> {
    let mut options = std::fs::OpenOptions::new();
    options.create_new(true).write(true);
    #[cfg(unix)]
    if sensitivity == FileSensitivity::Private {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(path).map_err(|source| CertificateError::Io {
        action: "create",
        path: path.to_path_buf(),
        source,
    })?;
    file.write_all(contents)
        .and_then(|()| file.sync_all())
        .map_err(|source| CertificateError::Io {
            action: "persist",
            path: path.to_path_buf(),
            source,
        })
}

fn sync_directory(directory: &Path) -> Result<(), CertificateError> {
    let file = std::fs::File::open(directory).map_err(|source| CertificateError::Io {
        action: "open directory for sync of",
        path: directory.to_path_buf(),
        source,
    })?;
    file.sync_all().map_err(|source| CertificateError::Io {
        action: "sync directory for",
        path: directory.to_path_buf(),
        source,
    })
}

fn validate_private_permissions(path: &Path) -> Result<(), CertificateError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let mode = std::fs::metadata(path)
            .map_err(|source| CertificateError::Io {
                action: "inspect permissions of",
                path: path.to_path_buf(),
                source,
            })?
            .permissions()
            .mode()
            & 0o777;
        if mode & 0o077 != 0 {
            return Err(CertificateError::InsecurePermissions {
                path: path.to_path_buf(),
                mode,
            });
        }
    }
    Ok(())
}
