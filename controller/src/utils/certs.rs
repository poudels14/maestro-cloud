use std::path::Path;

use anyhow::{Result, anyhow};
use rcgen::{
    BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose, SanType,
};

pub struct EtcdCerts {
    pub ca_pem: String,
    pub server_cert_pem: String,
    pub server_key_pem: String,
    pub client_cert_pem: String,
    pub client_key_pem: String,
}

pub fn generate_etcd_certs() -> Result<EtcdCerts> {
    let ca_key = KeyPair::generate()?;
    let mut ca_params = CertificateParams::default();
    ca_params
        .distinguished_name
        .push(DnType::CommonName, "maestro-etcd-ca");
    ca_params
        .distinguished_name
        .push(DnType::OrganizationName, "maestro");
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    let ca_cert = ca_params.self_signed(&ca_key)?;
    let ca_issuer = Issuer::from_params(&ca_params, &ca_key);

    let server_key = KeyPair::generate()?;
    let mut server_params = CertificateParams::default();
    server_params
        .distinguished_name
        .push(DnType::CommonName, "maestro-etcd");
    server_params
        .distinguished_name
        .push(DnType::OrganizationName, "maestro");
    server_params.subject_alt_names = vec![
        SanType::DnsName("maestro-etcd".try_into()?),
        SanType::DnsName("localhost".try_into()?),
        SanType::IpAddress("127.0.0.1".parse().map_err(|err| anyhow!("{err}"))?),
    ];
    server_params.key_usages = vec![
        KeyUsagePurpose::DigitalSignature,
        KeyUsagePurpose::KeyEncipherment,
    ];
    server_params.extended_key_usages = vec![
        ExtendedKeyUsagePurpose::ServerAuth,
        ExtendedKeyUsagePurpose::ClientAuth,
    ];
    let server_cert = server_params.signed_by(&server_key, &ca_issuer)?;

    let client_key = KeyPair::generate()?;
    let mut client_params = CertificateParams::default();
    client_params
        .distinguished_name
        .push(DnType::CommonName, "maestro-etcd-client");
    client_params
        .distinguished_name
        .push(DnType::OrganizationName, "maestro");
    client_params.key_usages = vec![
        KeyUsagePurpose::DigitalSignature,
        KeyUsagePurpose::KeyEncipherment,
    ];
    client_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
    let client_cert = client_params.signed_by(&client_key, &ca_issuer)?;

    Ok(EtcdCerts {
        ca_pem: ca_cert.pem(),
        server_cert_pem: server_cert.pem(),
        server_key_pem: server_key.serialize_pem(),
        client_cert_pem: client_cert.pem(),
        client_key_pem: client_key.serialize_pem(),
    })
}

pub fn write_etcd_certs(certs_dir: &Path, certs: &EtcdCerts) -> Result<()> {
    std::fs::create_dir_all(certs_dir)
        .map_err(|err| anyhow!("failed to create certs dir {}: {err}", certs_dir.display()))?;

    let files = [
        ("ca.pem", &certs.ca_pem),
        ("server.pem", &certs.server_cert_pem),
        ("server-key.pem", &certs.server_key_pem),
        ("client.pem", &certs.client_cert_pem),
        ("client-key.pem", &certs.client_key_pem),
    ];
    for (name, content) in &files {
        let path = certs_dir.join(name);
        std::fs::write(&path, content)
            .map_err(|err| anyhow!("failed to write {}: {err}", path.display()))?;
    }

    Ok(())
}
