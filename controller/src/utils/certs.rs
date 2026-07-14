use std::path::Path;

use anyhow::{Result, anyhow};
use base64::Engine as _;
use rcgen::{
    BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose, SanType,
};
use sha2::{Digest, Sha256};

#[derive(Clone)]
pub struct EtcdCerts {
    pub ca_pem: String,
    pub server_cert_pem: String,
    pub server_key_pem: String,
    pub peer_cert_pem: String,
    pub peer_key_pem: String,
    pub client_cert_pem: String,
    pub client_key_pem: String,
    pub probe_client_cert_pem: String,
    pub probe_client_key_pem: String,
    pub traefik_client_cert_pem: String,
    pub traefik_client_key_pem: String,
    pub api_cert_pem: String,
    pub api_key_pem: String,
}

pub struct ClusterCa {
    pub cert_pem: String,
    pub key_pem: String,
}

pub fn certificate_fingerprint(certificate_pem: &str) -> Result<String> {
    let encoded = certificate_pem
        .lines()
        .filter(|line| !line.starts_with("-----"))
        .collect::<String>();
    let certificate_der = base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .map_err(|error| anyhow!("invalid cluster CA certificate PEM: {error}"))?;
    Ok(Sha256::digest(certificate_der)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
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
        peer_cert_pem: server_cert.pem(),
        peer_key_pem: server_key.serialize_pem(),
        client_cert_pem: client_cert.pem(),
        client_key_pem: client_key.serialize_pem(),
        probe_client_cert_pem: client_cert.pem(),
        probe_client_key_pem: client_key.serialize_pem(),
        traefik_client_cert_pem: client_cert.pem(),
        traefik_client_key_pem: client_key.serialize_pem(),
        api_cert_pem: server_cert.pem(),
        api_key_pem: server_key.serialize_pem(),
    })
}

pub fn generate_cluster_ca() -> Result<ClusterCa> {
    let key = KeyPair::generate()?;
    let params = cluster_ca_params();
    let cert = params.self_signed(&key)?;
    Ok(ClusterCa {
        cert_pem: cert.pem(),
        key_pem: key.serialize_pem(),
    })
}

pub fn generate_cluster_node_certs(
    ca: &ClusterCa,
    host_ip: std::net::Ipv4Addr,
    role: crate::cluster::NodeRole,
) -> Result<EtcdCerts> {
    generate_cluster_node_certs_for_endpoint(ca, host_ip, None, role)
}

pub fn generate_cluster_node_certs_for_endpoint(
    ca: &ClusterCa,
    host_ip: std::net::Ipv4Addr,
    identity_api_port: Option<u16>,
    role: crate::cluster::NodeRole,
) -> Result<EtcdCerts> {
    let issuer_key = KeyPair::from_pem(&ca.key_pem)?;
    let issuer_params = cluster_ca_params();
    let issuer = Issuer::from_params(&issuer_params, &issuer_key);
    let suffix = identity_api_port.map_or_else(
        || format!("{:08x}", u32::from(host_ip)),
        |port| format!("{:08x}-{port:04x}", u32::from(host_ip)),
    );
    let member_name = format!("maestro-{suffix}");

    let server_key = KeyPair::generate()?;
    let server_cert = node_certificate_params(&member_name, host_ip, true, true)
        .signed_by(&server_key, &issuer)?;
    let peer_key = KeyPair::generate()?;
    let peer_cert =
        node_certificate_params(&member_name, host_ip, true, true).signed_by(&peer_key, &issuer)?;
    let client_key = KeyPair::generate()?;
    let client_cert = node_certificate_params(
        &format!(
            "maestro-{}-{suffix}",
            match role {
                crate::cluster::NodeRole::Voter => "voter",
                crate::cluster::NodeRole::Worker => "worker",
            },
        ),
        host_ip,
        false,
        true,
    )
    .signed_by(&client_key, &issuer)?;
    let probe_client_key = KeyPair::generate()?;
    let probe_client_cert =
        node_certificate_params(&format!("maestro-probe-{suffix}"), host_ip, false, true)
            .signed_by(&probe_client_key, &issuer)?;
    let traefik_client_key = KeyPair::generate()?;
    let traefik_client_cert =
        node_certificate_params(&format!("maestro-traefik-{suffix}"), host_ip, false, true)
            .signed_by(&traefik_client_key, &issuer)?;
    let api_key = KeyPair::generate()?;
    let api_cert = node_certificate_params(&format!("maestro-api-{suffix}"), host_ip, true, false)
        .signed_by(&api_key, &issuer)?;

    Ok(EtcdCerts {
        ca_pem: ca.cert_pem.clone(),
        server_cert_pem: server_cert.pem(),
        server_key_pem: server_key.serialize_pem(),
        peer_cert_pem: peer_cert.pem(),
        peer_key_pem: peer_key.serialize_pem(),
        client_cert_pem: client_cert.pem(),
        client_key_pem: client_key.serialize_pem(),
        probe_client_cert_pem: probe_client_cert.pem(),
        probe_client_key_pem: probe_client_key.serialize_pem(),
        traefik_client_cert_pem: traefik_client_cert.pem(),
        traefik_client_key_pem: traefik_client_key.serialize_pem(),
        // Include the public CA after the leaf so an unprovisioned joiner can
        // fingerprint the advertised root before it sends its signed request.
        api_cert_pem: format!("{}\n{}", api_cert.pem(), ca.cert_pem),
        api_key_pem: api_key.serialize_pem(),
    })
}

pub fn write_cluster_ca(ca_dir: &Path, ca: &ClusterCa) -> Result<()> {
    std::fs::create_dir_all(ca_dir).map_err(|err| {
        anyhow!(
            "failed to create cluster CA dir {}: {err}",
            ca_dir.display()
        )
    })?;
    write_private_file(&ca_dir.join("ca-key.pem"), &ca.key_pem)?;
    std::fs::write(ca_dir.join("ca.pem"), &ca.cert_pem)?;
    Ok(())
}

pub fn load_cluster_ca(ca_dir: &Path) -> Result<ClusterCa> {
    Ok(ClusterCa {
        cert_pem: std::fs::read_to_string(ca_dir.join("ca.pem"))?,
        key_pem: std::fs::read_to_string(ca_dir.join("ca-key.pem"))?,
    })
}

pub fn read_etcd_certs(certs_dir: &Path) -> Result<EtcdCerts> {
    Ok(EtcdCerts {
        ca_pem: std::fs::read_to_string(certs_dir.join("ca.pem"))?,
        server_cert_pem: std::fs::read_to_string(certs_dir.join("server.pem"))?,
        server_key_pem: std::fs::read_to_string(certs_dir.join("server-key.pem"))?,
        peer_cert_pem: std::fs::read_to_string(certs_dir.join("peer.pem"))?,
        peer_key_pem: std::fs::read_to_string(certs_dir.join("peer-key.pem"))?,
        client_cert_pem: std::fs::read_to_string(certs_dir.join("client.pem"))?,
        client_key_pem: std::fs::read_to_string(certs_dir.join("client-key.pem"))?,
        probe_client_cert_pem: std::fs::read_to_string(certs_dir.join("probe-client.pem"))?,
        probe_client_key_pem: std::fs::read_to_string(certs_dir.join("probe-client-key.pem"))?,
        traefik_client_cert_pem: std::fs::read_to_string(certs_dir.join("traefik-client.pem"))?,
        traefik_client_key_pem: std::fs::read_to_string(certs_dir.join("traefik-client-key.pem"))?,
        api_cert_pem: std::fs::read_to_string(certs_dir.join("api.pem"))?,
        api_key_pem: std::fs::read_to_string(certs_dir.join("api-key.pem"))?,
    })
}

pub fn write_etcd_certs(certs_dir: &Path, certs: &EtcdCerts) -> Result<()> {
    std::fs::create_dir_all(certs_dir)
        .map_err(|err| anyhow!("failed to create certs dir {}: {err}", certs_dir.display()))?;

    let files = [
        ("ca.pem", &certs.ca_pem),
        ("server.pem", &certs.server_cert_pem),
        ("server-key.pem", &certs.server_key_pem),
        ("peer.pem", &certs.peer_cert_pem),
        ("peer-key.pem", &certs.peer_key_pem),
        ("client.pem", &certs.client_cert_pem),
        ("client-key.pem", &certs.client_key_pem),
        ("probe-client.pem", &certs.probe_client_cert_pem),
        ("probe-client-key.pem", &certs.probe_client_key_pem),
        ("traefik-client.pem", &certs.traefik_client_cert_pem),
        ("traefik-client-key.pem", &certs.traefik_client_key_pem),
        ("api.pem", &certs.api_cert_pem),
        ("api-key.pem", &certs.api_key_pem),
    ];
    for (name, content) in &files {
        let path = certs_dir.join(name);
        if name.ends_with("-key.pem") {
            write_private_file(&path, content)?;
        } else {
            std::fs::write(&path, content)
                .map_err(|err| anyhow!("failed to write {}: {err}", path.display()))?;
        }
    }

    Ok(())
}

fn cluster_ca_params() -> CertificateParams {
    let mut params = CertificateParams::default();
    params
        .distinguished_name
        .push(DnType::CommonName, "maestro-cluster-ca");
    params
        .distinguished_name
        .push(DnType::OrganizationName, "maestro");
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    params
}

fn node_certificate_params(
    common_name: &str,
    host_ip: std::net::Ipv4Addr,
    server_auth: bool,
    client_auth: bool,
) -> CertificateParams {
    let mut params = CertificateParams::default();
    params
        .distinguished_name
        .push(DnType::CommonName, common_name);
    params
        .distinguished_name
        .push(DnType::OrganizationName, "maestro");
    params.subject_alt_names = vec![
        SanType::DnsName("maestro-etcd".try_into().expect("static DNS name")),
        SanType::DnsName("localhost".try_into().expect("static DNS name")),
        SanType::IpAddress(host_ip.into()),
        SanType::IpAddress(std::net::Ipv4Addr::LOCALHOST.into()),
    ];
    params.key_usages = vec![
        KeyUsagePurpose::DigitalSignature,
        KeyUsagePurpose::KeyEncipherment,
    ];
    if server_auth {
        params
            .extended_key_usages
            .push(ExtendedKeyUsagePurpose::ServerAuth);
    }
    if client_auth {
        params
            .extended_key_usages
            .push(ExtendedKeyUsagePurpose::ClientAuth);
    }
    params
}

fn write_private_file(path: &Path, contents: &str) -> Result<()> {
    use std::io::Write;

    let mut options = std::fs::OpenOptions::new();
    options.create_new(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options
        .open(path)
        .map_err(|err| anyhow!("failed to create private key {}: {err}", path.display()))?;
    file.write_all(contents.as_bytes())?;
    file.sync_all()?;
    Ok(())
}
