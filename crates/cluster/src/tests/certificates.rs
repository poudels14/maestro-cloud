use std::net::Ipv4Addr;

use kernel_api::{NodeId, NodeRole};
use time::{Duration, OffsetDateTime};
use x509_parser::{extensions::GeneralName, parse_x509_certificate, pem::parse_x509_pem};

use crate::{
    CertificateValidity, ClusterCertificateAuthority, Ipv4Cidr, NodeDefinition, NodeEndpoint,
    certificate_fingerprint,
};

#[test]
fn initializes_and_reloads_one_cluster_authority() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let validity = validity()?;
    let first = ClusterCertificateAuthority::load_or_initialize(
        &directory.path().join("cluster-ca"),
        "test-cluster",
        validity,
    )?;
    let second = ClusterCertificateAuthority::load_or_initialize(
        &directory.path().join("cluster-ca"),
        "test-cluster",
        validity,
    )?;

    assert_eq!(
        certificate_fingerprint(&first.certificate_pem)?,
        certificate_fingerprint(&second.certificate_pem)?
    );
    assert_eq!(first, second);
    assert!(!format!("{first:?}").contains("PRIVATE KEY"));
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(directory.path().join("cluster-ca/ca-key.pem"))?
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600);
    }
    Ok(())
}

#[test]
fn issues_a_node_identity_signed_by_the_cluster_root() -> Result<(), Box<dyn std::error::Error>> {
    let authority = ClusterCertificateAuthority::generate("test-cluster", validity()?)?;
    let node = NodeDefinition {
        hostname: "node-1.internal".to_owned(),
        endpoint: NodeEndpoint {
            host_address: Ipv4Addr::new(10, 20, 0, 11),
            api_port: 3_000,
        },
        workload_subnet: Ipv4Cidr::new(Ipv4Addr::new(10, 42, 1, 0), 24)?,
        role: NodeRole::Master,
    };
    let bundle = authority.issue_node_certificate_for_definition(
        &NodeId::new("node-1")?,
        &node,
        validity()?,
    )?;

    let (_, root_pem) = parse_x509_pem(bundle.trust_root_pem.as_bytes())?;
    let (_, root) = parse_x509_certificate(&root_pem.contents)?;
    let (_, leaf_pem) = parse_x509_pem(bundle.identity.certificate_pem.as_bytes())?;
    let (_, leaf) = parse_x509_certificate(&leaf_pem.contents)?;
    leaf.verify_signature(Some(root.public_key()))?;

    assert!(
        leaf.validity()
            .is_valid_at((OffsetDateTime::UNIX_EPOCH + Duration::days(20_100)).into())
    );
    let ip_sans = leaf
        .subject_alternative_name()?
        .ok_or("node certificate SAN is missing")?
        .value
        .general_names
        .iter()
        .filter_map(|name| match name {
            GeneralName::IPAddress(address) => Some(*address),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert!(ip_sans.contains(&Ipv4Addr::new(10, 20, 0, 11).octets().as_slice()));
    assert!(ip_sans.contains(&Ipv4Addr::new(10, 42, 1, 1).octets().as_slice()));
    assert!(!format!("{:?}", bundle.identity).contains("PRIVATE KEY"));
    Ok(())
}

fn validity() -> Result<CertificateValidity, Box<dyn std::error::Error>> {
    let start = OffsetDateTime::UNIX_EPOCH + Duration::days(20_000);
    Ok(CertificateValidity::new(
        start,
        start + Duration::days(365),
    )?)
}
