use std::{
    net::{IpAddr, Ipv4Addr},
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};

use rustls::{
    DigitallySignedStruct, SignatureScheme,
    client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
    pki_types::{CertificateDer, ServerName, UnixTime},
};
use sha2::{Digest, Sha256};

use crate::{
    cluster::{self, NodeRole},
    error::{Error, Result},
};

pub async fn approve_node(
    host: &str,
    node_id: String,
    role: NodeRole,
    cluster_host_ip: Ipv4Addr,
    cluster_api_port: Option<u16>,
    subnet: String,
    public_key_sha256: String,
) -> Result<()> {
    let admission = cluster::join::JoinAdmission {
        node_id,
        role,
        cluster_host_ip,
        cluster_api_port: cluster_api_port.unwrap_or_default(),
        identity_api_port: cluster_api_port,
        subnet,
        public_key_sha256,
        created_at_ms: crate::cluster_stats::now_ms(),
    };
    let base = crate::cli::contexts::normalize_base_url(host)?;
    let response = crate::cli::idempotent(
        crate::cli::contexts::build_http_client()?
            .post(format!("{base}/api/cluster/admissions"))
            .json(&admission),
    )
    .send()
    .await
    .map_err(|error| Error::external(format!("failed to approve voter: {error}")))?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(Error::external(format!(
            "voter approval failed ({status}): {body}"
        )));
    }
    println!("[maestro]: voter admission created");
    Ok(())
}

pub async fn remove_node(host: &str, node_id: &str) -> Result<()> {
    #[derive(serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct RemoveResponse {
        state: cluster::join::RemoveNodeOutcome,
    }

    let base = crate::cli::contexts::normalize_base_url(host)?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
    loop {
        let response = crate::cli::idempotent(
            crate::cli::contexts::build_http_client()?
                .delete(format!("{base}/api/cluster/nodes/{node_id}")),
        )
        .send()
        .await
        .map_err(|error| Error::external(format!("failed to remove node: {error}")))?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(Error::external(format!(
                "node removal failed ({status}): {body}"
            )));
        }
        let outcome: RemoveResponse = response
            .json()
            .await
            .map_err(|error| Error::external(format!("invalid node removal response: {error}")))?;
        match outcome.state {
            cluster::join::RemoveNodeOutcome::Removed => {
                println!("[maestro]: node `{node_id}` removed");
                println!("rotate the join secret and cluster CA if this node may be compromised");
                return Ok(());
            }
            cluster::join::RemoveNodeOutcome::Draining => {
                println!("[maestro]: waiting for node `{node_id}` to drain");
            }
            cluster::join::RemoveNodeOutcome::LeadershipTransferRequired => {
                println!("[maestro]: leadership transferred; waiting for the new leader");
            }
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(Error::external(
                "timed out waiting for the node to drain and leave the cluster",
            ));
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

pub async fn prepare_join(config_source: &str, base_data_dir: &Path) -> Result<()> {
    let (config, data_dir, host_ip) = load_join_config(config_source, base_data_dir).await?;
    let node_id = cluster::identity::load_or_create_node_id(&data_dir)
        .map_err(|error| Error::invalid_config(error.to_string()))?;
    let private_key = cluster::join::load_or_create_join_key(&data_dir)
        .map_err(|error| Error::internal(error.to_string()))?;
    let subnet = config
        .subnet
        .as_deref()
        .ok_or_else(|| Error::invalid_config("local subnet is required for cluster join"))?;
    println!("node id: {node_id}");
    println!("role: {}", config.node.role);
    println!("host IP: {host_ip}");
    println!("API port: {}", config.cluster.api_port);
    println!("subnet: {subnet}");
    println!(
        "public key SHA-256: {}",
        cluster::join::join_key_fingerprint(&private_key)
    );
    if config.node.role.is_voter() {
        println!("approve this exact identity on the current leader before joining");
    }
    Ok(())
}

pub async fn join_cluster(
    leader_address: &str,
    config_source: &str,
    base_data_dir: &Path,
) -> Result<()> {
    let (config, data_dir, host_ip) = load_join_config(config_source, base_data_dir).await?;
    if data_dir.join("system/certs").exists() || data_dir.join("system/cluster-id").exists() {
        return Err(Error::conflict(
            "cluster identity already exists; refusing to replace joined node material",
        ));
    }
    let join_secret = config
        .cluster
        .join_secret
        .as_deref()
        .ok_or_else(|| Error::invalid_config("cluster.join-secret is required"))?;
    let expected_ca = normalize_fingerprint(
        config
            .cluster
            .ca_sha256
            .as_deref()
            .ok_or_else(|| Error::invalid_config("cluster.ca-sha256 is required"))?,
    )?;
    let subnet = config
        .subnet
        .clone()
        .ok_or_else(|| Error::invalid_config("local subnet is required for cluster join"))?;
    let node_id = cluster::identity::load_or_create_node_id(&data_dir)
        .map_err(|error| Error::invalid_config(error.to_string()))?;
    let private_key = cluster::join::load_or_create_join_key(&data_dir)
        .map_err(|error| Error::internal(error.to_string()))?;
    let timestamp_ms = crate::cluster_stats::now_ms();
    let endpoint = config
        .cluster
        .local_endpoint(host_ip, config.node.role)
        .map_err(|error| Error::invalid_config(error.to_string()))?;
    let request = cluster::join::create_join_request(
        &private_key,
        node_id,
        cluster::identity::local_hostname(),
        config.node.role,
        endpoint,
        subnet,
        None,
        timestamp_ms,
    );
    let signature = cluster::join::sign_request(join_secret, &request)
        .map_err(|error| Error::internal(error.to_string()))?;
    let seed_api_port = config
        .cluster
        .resolved_nodes()
        .map_err(|error| Error::invalid_config(error.to_string()))?
        .first()
        .map(|node| node.api_port)
        .ok_or_else(|| Error::invalid_config("cluster.nodes is empty"))?;
    let base = normalize_leader_address(leader_address, seed_api_port)?;
    let client = pinned_join_client(&base, host_ip, &expected_ca).await?;
    let response = client
        .post(format!("{base}/api/cluster/join"))
        .header("X-Maestro-Join-Signature", signature)
        .json(&request)
        .send()
        .await
        .map_err(|error| Error::external(format!("cluster join request failed: {error}")))?;
    let status = response.status();
    if !status.is_success() {
        return Err(Error::external(format!("cluster join rejected ({status})")));
    }
    let envelope: cluster::join::JoinEnvelope = response
        .json()
        .await
        .map_err(|error| Error::external(format!("invalid cluster join response: {error}")))?;
    let payload = cluster::join::decrypt_response(
        join_secret,
        &private_key,
        &request,
        &envelope,
        status.as_u16(),
    )
    .map_err(|error| Error::external(error.to_string()))?;
    if payload.cluster_id != envelope.cluster_id {
        return Err(Error::external(
            "cluster join response identity was not authenticated",
        ));
    }
    install_join_payload(&data_dir, config.node.role, &payload)?;
    println!("[maestro]: joined cluster `{}`", payload.display_name);
    println!("cluster id: {}", payload.cluster_id);
    println!(
        "certificate bundle: {}",
        data_dir.join("system/certs").display()
    );
    Ok(())
}

async fn load_join_config(
    config_source: &str,
    base_data_dir: &Path,
) -> Result<(crate::config::StartConfig, std::path::PathBuf, Ipv4Addr)> {
    let config = crate::config::load_config(config_source)
        .await
        .map_err(|error| Error::invalid_config(error.to_string()))?;
    cluster::network::validate_cluster_config(
        &config.cluster,
        config.subnet.as_deref(),
        config.node.role,
    )
    .map_err(|error| Error::invalid_config(error.to_string()))?;
    if config.cluster.nodes.is_empty() {
        return Err(Error::invalid_config(
            "cluster.nodes must contain at least the bootstrap voter for join",
        ));
    }
    let data_dir = base_data_dir.join(config.cluster.name.to_lowercase());
    std::fs::create_dir_all(&data_dir)?;
    let host_ip =
        cluster::network::resolve_cluster_host_ip(&config.cluster, &data_dir, config.node.role)
            .map_err(|error| Error::invalid_config(error.to_string()))?
            .ok_or_else(|| Error::invalid_config("failed to resolve cluster host IP"))?;
    Ok((config, data_dir, host_ip))
}

fn install_join_payload(
    data_dir: &Path,
    role: NodeRole,
    payload: &cluster::join::JoinPayload,
) -> Result<()> {
    let system_dir = data_dir.join("system");
    std::fs::create_dir_all(&system_dir)?;
    let temporary = system_dir.join("certs.joining");
    if temporary.exists() {
        std::fs::remove_dir_all(&temporary)?;
    }
    crate::utils::certs::write_etcd_certs(&temporary, &payload.certificates.clone().into())
        .map_err(|error| Error::internal(error.to_string()))?;
    if role.is_voter() {
        let ca = payload
            .voter_ca
            .clone()
            .ok_or_else(|| Error::external("voter join response omitted cluster CA material"))?;
        crate::utils::certs::write_cluster_ca(&temporary.join("cluster-ca"), &ca.into())
            .map_err(|error| Error::internal(error.to_string()))?;
    }
    let certs_dir = system_dir.join("certs");
    std::fs::rename(&temporary, &certs_dir)?;
    if let Err(error) = write_new_private(
        &system_dir.join("cluster-id"),
        payload.cluster_id.as_bytes(),
    ) {
        let _ = std::fs::remove_dir_all(&certs_dir);
        return Err(error);
    }
    if role.is_voter() {
        let join_info = payload
            .join_info
            .as_ref()
            .ok_or_else(|| Error::external("voter join response omitted etcd membership"))?;
        if let Err(error) = cluster::bootstrap::persist_join_info(data_dir, join_info) {
            let _ = std::fs::remove_dir_all(&certs_dir);
            let _ = std::fs::remove_file(system_dir.join("cluster-id"));
            return Err(Error::internal(error.to_string()));
        }
    }
    cluster::join::persist_voter_cache(
        data_dir,
        &cluster::join::ClusterVoterCache {
            cluster_id: payload.cluster_id.clone(),
            voter_host_ips: payload.voter_host_ips.clone(),
            voter_endpoints: payload.voter_endpoints.clone(),
            subnets: payload.subnets.clone(),
            initial_voter_host_ips: payload.initial_voter_host_ips.clone(),
            initial_voter_endpoints: payload.initial_voter_endpoints.clone(),
            api_port: payload.api_port,
            etcd_client_port: payload.etcd_client_port,
            etcd_peer_port: payload.etcd_peer_port,
        },
    )
    .map_err(|error| Error::internal(error.to_string()))?;
    let _ = std::fs::remove_file(system_dir.join("join-key"));
    Ok(())
}

async fn pinned_join_client(
    base: &str,
    local_ip: Ipv4Addr,
    expected_fingerprint: &str,
) -> Result<reqwest::Client> {
    let captured = Arc::new(Mutex::new(None));
    let verifier = Arc::new(CaCaptureVerifier {
        expected_fingerprint: expected_fingerprint.to_string(),
        captured: captured.clone(),
    });
    let tls = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(verifier)
        .with_no_client_auth();
    let discovery = reqwest::Client::builder()
        .use_preconfigured_tls(tls)
        .https_only(true)
        .local_address(IpAddr::V4(local_ip))
        .timeout(Duration::from_secs(10))
        .build()
        .map_err(|error| Error::internal(error.to_string()))?;
    discovery
        .get(format!("{base}/_healthy"))
        .send()
        .await
        .map_err(|error| Error::external(format!("failed to pin cluster CA: {error}")))?;
    let ca = captured
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .clone()
        .ok_or_else(|| Error::external("server did not present the configured cluster CA"))?;
    reqwest::Client::builder()
        .add_root_certificate(
            reqwest::Certificate::from_der(ca.as_ref())
                .map_err(|error| Error::internal(error.to_string()))?,
        )
        .https_only(true)
        .local_address(IpAddr::V4(local_ip))
        .timeout(Duration::from_secs(30))
        .build()
        .map_err(|error| Error::internal(error.to_string()))
}

#[derive(Debug)]
struct CaCaptureVerifier {
    expected_fingerprint: String,
    captured: Arc<Mutex<Option<CertificateDer<'static>>>>,
}

impl ServerCertVerifier for CaCaptureVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> std::result::Result<ServerCertVerified, rustls::Error> {
        let matching = std::iter::once(end_entity)
            .chain(intermediates.iter())
            .find(|certificate| {
                format!("{:x}", Sha256::digest(certificate.as_ref())) == self.expected_fingerprint
            })
            .cloned();
        let Some(matching) = matching else {
            return Err(rustls::Error::General(
                "presented chain does not contain the pinned cluster CA".to_string(),
            ));
        };
        *self
            .captured
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(matching.into_owned());
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        rustls::crypto::ring::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

fn normalize_fingerprint(value: &str) -> Result<String> {
    let value = value
        .trim()
        .strip_prefix("sha256:")
        .unwrap_or(value.trim())
        .to_ascii_lowercase();
    if value.len() != 64 || !value.chars().all(|character| character.is_ascii_hexdigit()) {
        return Err(Error::invalid_config(
            "cluster.ca-sha256 must be a 64-character SHA-256 fingerprint",
        ));
    }
    Ok(value)
}

fn normalize_leader_address(value: &str, default_port: u16) -> Result<String> {
    let value = value.trim().trim_end_matches('/');
    let with_scheme = if value.contains("://") {
        value.to_string()
    } else if value.rsplit_once(':').is_some() {
        format!("https://{value}")
    } else {
        format!("https://{value}:{default_port}")
    };
    let parsed = reqwest::Url::parse(&with_scheme)
        .map_err(|error| Error::invalid_input(format!("invalid leader address: {error}")))?;
    if parsed.scheme() != "https" || parsed.host_str().is_none() {
        return Err(Error::invalid_input(
            "leader address must be a private HTTPS host",
        ));
    }
    Ok(with_scheme)
}

fn write_new_private(path: &Path, contents: &[u8]) -> Result<()> {
    use std::io::Write;

    let mut options = std::fs::OpenOptions::new();
    options.create_new(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(path)?;
    file.write_all(contents)?;
    file.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn leader_address_requires_https_and_adds_default_port() {
        assert_eq!(
            normalize_leader_address("10.20.0.11", 3001).unwrap(),
            "https://10.20.0.11:3001"
        );
        assert!(normalize_leader_address("http://10.20.0.11:3001", 3001).is_err());
    }

    #[test]
    fn ca_fingerprint_is_normalized() {
        let fingerprint = "AB".repeat(32);
        assert_eq!(
            normalize_fingerprint(&format!("sha256:{fingerprint}")).unwrap(),
            "ab".repeat(32)
        );
    }
}
