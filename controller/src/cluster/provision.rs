use std::{net::IpAddr, path::Path, time::Duration};

use crate::{
    cluster::{self, NodeRole},
    config::{ClusterConfig, StartConfig},
    error::{Error, Result},
    signal::ShutdownEvent,
    utils::certs,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeedIdentity {
    pub cluster_id: String,
    pub created: bool,
}

pub fn identity_installed(data_dir: &Path) -> Result<bool> {
    let cluster_id = data_dir.join("system/cluster-id");
    let certs_dir = data_dir.join("system/certs");
    match (cluster_id.exists(), certs_dir.exists()) {
        (false, false) => Ok(false),
        (true, true) => {
            cluster::identity::load_cluster_id(data_dir)
                .map_err(|error| Error::invalid_config(error.to_string()))?;
            certs::read_etcd_certs(&certs_dir)
                .map_err(|error| Error::invalid_config(error.to_string()))?;
            Ok(true)
        }
        _ => Err(Error::invalid_config(
            "partial cluster identity found; refusing to replace cluster certificates or identity",
        )),
    }
}

pub fn ensure_seed_identity(
    config: &ClusterConfig,
    role: NodeRole,
    data_dir: &Path,
    host_ip: std::net::Ipv4Addr,
) -> Result<SeedIdentity> {
    if !role.is_voter() {
        return Err(Error::invalid_config(
            "the first configured cluster node must be hybrid or voter",
        ));
    }
    let local_endpoint = config
        .local_endpoint(host_ip, role)
        .map_err(|error| Error::invalid_config(error.to_string()))?;
    let initial_voters = config
        .resolved_nodes()
        .map_err(|error| Error::invalid_config(error.to_string()))?;
    if initial_voters.first() != Some(&local_endpoint) {
        return Err(Error::invalid_config(
            "automatic cluster initialization is restricted to cluster.nodes[0]",
        ));
    }
    let member_exists = data_dir.join("system/etcd/data/member").exists();
    if identity_installed(data_dir).unwrap_or(false) {
        let cluster_id = cluster::identity::load_cluster_id(data_dir)
            .map_err(|error| Error::invalid_config(error.to_string()))?;
        if !member_exists
            && cluster::bootstrap::seed_is_armed(data_dir)
                .map_err(|error| Error::invalid_config(error.to_string()))?
        {
            cluster::bootstrap::ensure_seed_armed(data_dir, &cluster_id, host_ip)
                .map_err(|error| Error::invalid_config(error.to_string()))?;
        }
        return Ok(SeedIdentity {
            cluster_id,
            created: false,
        });
    }
    if member_exists {
        return Err(Error::invalid_config(
            "etcd member data exists without a cluster identity; refusing fresh bootstrap",
        ));
    }

    let cluster_id = if data_dir.join("system/cluster-id").exists() {
        cluster::identity::load_cluster_id(data_dir)
            .map_err(|error| Error::invalid_config(error.to_string()))?
    } else {
        cluster::identity::create_cluster_id(data_dir)
            .map_err(|error| Error::internal(error.to_string()))?
    };
    cluster::bootstrap::ensure_seed_armed(data_dir, &cluster_id, host_ip)
        .map_err(|error| Error::invalid_config(error.to_string()))?;
    let ca_dir = data_dir.join("system/certs/cluster-ca");
    let ca = if ca_dir.exists() {
        match certs::load_cluster_ca(&ca_dir) {
            Ok(ca) => ca,
            Err(_) => {
                std::fs::remove_dir_all(&ca_dir).map_err(|error| {
                    Error::internal(format!(
                        "failed to resume incomplete automatic cluster CA: {error}"
                    ))
                })?;
                let ca = certs::generate_cluster_ca().map_err(|error| {
                    Error::internal(format!("failed to regenerate cluster CA: {error}"))
                })?;
                certs::write_cluster_ca(&ca_dir, &ca).map_err(|error| {
                    Error::internal(format!("failed to persist cluster CA: {error}"))
                })?;
                ca
            }
        }
    } else {
        let ca = certs::generate_cluster_ca()
            .map_err(|error| Error::internal(format!("failed to generate cluster CA: {error}")))?;
        certs::write_cluster_ca(&ca_dir, &ca)
            .map_err(|error| Error::internal(format!("failed to persist cluster CA: {error}")))?;
        ca
    };
    let node_certs = certs::generate_cluster_node_certs_for_endpoint(
        &ca,
        host_ip,
        local_endpoint.identity_api_port,
        role,
    )
    .map_err(|error| Error::internal(format!("failed to issue seed certificates: {error}")))?;
    certs::write_etcd_certs(&data_dir.join("system/certs"), &node_certs).map_err(|error| {
        Error::internal(format!("failed to install seed certificates: {error}"))
    })?;

    let voter_host_ips = initial_voters.iter().map(|node| node.host_ip).collect();
    let endpoint_identity = config.uses_node_ports();
    cluster::join::persist_voter_cache(
        data_dir,
        &cluster::join::ClusterVoterCache {
            cluster_id: cluster_id.clone(),
            voter_host_ips,
            voter_endpoints: if endpoint_identity {
                initial_voters.clone()
            } else {
                Vec::new()
            },
            initial_voter_host_ips: initial_voters.iter().map(|node| node.host_ip).collect(),
            initial_voter_endpoints: if endpoint_identity {
                initial_voters
            } else {
                Vec::new()
            },
            api_port: config.api_port,
            etcd_client_port: config.etcd_client_port,
            etcd_peer_port: config.etcd_peer_port,
        },
    )
    .map_err(|error| Error::internal(error.to_string()))?;
    cluster::bootstrap::ensure_seed_armed(data_dir, &cluster_id, host_ip)
        .map_err(|error| Error::internal(format!("failed to arm cluster seed: {error}")))?;
    Ok(SeedIdentity {
        cluster_id,
        created: true,
    })
}

pub async fn auto_join(
    config: &StartConfig,
    data_dir: &Path,
    host_ip: std::net::Ipv4Addr,
    mut shutdown: tokio::sync::broadcast::Receiver<ShutdownEvent>,
) -> Result<()> {
    let mut delay = Duration::from_secs(1);
    loop {
        match join_once(config, data_dir, host_ip).await {
            Ok(()) => return Ok(()),
            Err(error) => {
                eprintln!(
                    "[maestro]: waiting to join cluster `{}`: {error}; retrying in {}s",
                    config.cluster.name,
                    delay.as_secs()
                );
            }
        }
        tokio::select! {
            _ = tokio::time::sleep(delay) => {}
            _ = shutdown.recv() => {
                return Err(Error::external("cluster join interrupted by shutdown"));
            }
        }
        delay = (delay * 2).min(Duration::from_secs(30));
    }
}

pub async fn join_once(
    config: &StartConfig,
    data_dir: &Path,
    host_ip: std::net::Ipv4Addr,
) -> Result<()> {
    join_once_via(config, data_dir, host_ip, None).await
}

pub async fn join_once_via(
    config: &StartConfig,
    data_dir: &Path,
    host_ip: std::net::Ipv4Addr,
    preferred_address: Option<&str>,
) -> Result<()> {
    repair_incomplete_join(data_dir)?;
    if identity_installed(data_dir)? {
        return Ok(());
    }
    let join_secret = config
        .cluster
        .join_secret
        .as_deref()
        .ok_or_else(|| Error::invalid_config("cluster.join-secret is required"))?;
    let subnet = config
        .subnet
        .as_deref()
        .ok_or_else(|| Error::invalid_config("local subnet is required for cluster join"))?;
    let local_endpoint = config
        .cluster
        .local_endpoint(host_ip, config.node.role)
        .map_err(|error| Error::invalid_config(error.to_string()))?;
    let mut bases = config
        .cluster
        .resolved_nodes()
        .map_err(|error| Error::invalid_config(error.to_string()))?
        .into_iter()
        .map(|node| format!("https://{}", node.api_address()))
        .collect::<Vec<_>>();
    if let Some(address) = preferred_address {
        let default_port = config
            .cluster
            .resolved_nodes()
            .map_err(|error| Error::invalid_config(error.to_string()))?
            .first()
            .map(|node| node.api_port)
            .ok_or_else(|| Error::invalid_config("cluster.nodes is empty"))?;
        let preferred = normalize_join_base(address, default_port)?;
        bases.retain(|base| base != &preferred);
        bases.insert(0, preferred);
    }
    let cluster_name = config.cluster.name.to_lowercase();
    let discovery = discover_ca(&bases, host_ip, &cluster_name, join_secret).await?;
    let ca = reqwest::Certificate::from_pem(discovery.ca_pem.as_bytes())
        .map_err(|error| Error::external(format!("cluster returned an invalid CA: {error}")))?;
    let client = reqwest::Client::builder()
        .add_root_certificate(ca)
        .tls_built_in_root_certs(false)
        .https_only(true)
        .no_proxy()
        .local_address(IpAddr::V4(host_ip))
        .timeout(Duration::from_secs(30))
        .build()
        .map_err(|error| Error::internal(error.to_string()))?;
    let node_id = cluster::identity::load_or_create_node_id(data_dir)
        .map_err(|error| Error::invalid_config(error.to_string()))?;
    let private_key = cluster::join::load_or_create_join_key(data_dir)
        .map_err(|error| Error::internal(error.to_string()))?;

    let mut failures = Vec::new();
    for base in &bases {
        let request = cluster::join::create_join_request(
            &private_key,
            node_id.clone(),
            cluster::identity::local_hostname(),
            config.node.role,
            local_endpoint,
            subnet.to_string(),
            None,
            crate::cluster_stats::now_ms(),
        );
        let signature = cluster::join::sign_request(join_secret, &request)
            .map_err(|error| Error::internal(error.to_string()))?;
        let response = match client
            .post(format!("{base}/api/cluster/join"))
            .header("X-Maestro-Join-Signature", signature)
            .json(&request)
            .send()
            .await
        {
            Ok(response) => response,
            Err(error) => {
                failures.push(format!("{base}: {error}"));
                continue;
            }
        };
        let status = response.status();
        if !status.is_success() {
            failures.push(format!("{base}: HTTP {status}"));
            continue;
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
        validate_join_payload(config, &cluster_name, &discovery, &envelope, &payload)?;
        install_join_payload(data_dir, config.node.role, &payload)?;
        eprintln!(
            "[maestro]: automatically joined cluster `{}` as {}",
            config.cluster.name, config.node.role
        );
        return Ok(());
    }
    Err(Error::external(format!(
        "no configured voter accepted the join request ({})",
        failures.join("; ")
    )))
}

fn repair_incomplete_join(data_dir: &Path) -> Result<()> {
    let system = data_dir.join("system");
    let certs_dir = system.join("certs");
    if certs_dir.exists()
        && !system.join("cluster-id").exists()
        && system.join("join-key").exists()
        && !system.join("etcd/data/member").exists()
    {
        std::fs::remove_dir_all(&certs_dir).map_err(|error| {
            Error::internal(format!(
                "failed to resume interrupted cluster join: {error}"
            ))
        })?;
    }
    Ok(())
}

fn normalize_join_base(value: &str, default_port: u16) -> Result<String> {
    let value = value.trim().trim_end_matches('/');
    let with_scheme = if value.contains("://") {
        value.to_string()
    } else if value.rsplit_once(':').is_some() {
        format!("https://{value}")
    } else {
        format!("https://{value}:{default_port}")
    };
    let parsed = reqwest::Url::parse(&with_scheme)
        .map_err(|error| Error::invalid_input(format!("invalid voter address: {error}")))?;
    if parsed.scheme() != "https" || parsed.host_str().is_none() {
        return Err(Error::invalid_input(
            "voter address must be a private HTTPS host",
        ));
    }
    Ok(with_scheme)
}

async fn discover_ca(
    bases: &[String],
    host_ip: std::net::Ipv4Addr,
    cluster_name: &str,
    join_secret: &str,
) -> Result<cluster::join::CaDiscoveryResponse> {
    let client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .https_only(true)
        .no_proxy()
        .local_address(IpAddr::V4(host_ip))
        .timeout(Duration::from_secs(10))
        .build()
        .map_err(|error| Error::internal(error.to_string()))?;
    let request = cluster::join::CaDiscoveryRequest {
        cluster_name: cluster_name.to_string(),
        nonce: hex::encode(x25519_dalek::StaticSecret::random().to_bytes()),
    };
    let mut failures = Vec::new();
    for base in bases {
        let response = match client
            .post(format!("{base}/api/cluster/ca"))
            .json(&request)
            .send()
            .await
        {
            Ok(response) => response,
            Err(error) => {
                failures.push(format!("{base}: {error}"));
                continue;
            }
        };
        if !response.status().is_success() {
            failures.push(format!("{base}: HTTP {}", response.status()));
            continue;
        }
        let response: cluster::join::CaDiscoveryResponse = response
            .json()
            .await
            .map_err(|error| Error::external(format!("invalid CA discovery response: {error}")))?;
        match cluster::join::verify_ca_discovery_response(
            join_secret,
            cluster_name,
            &request,
            &response,
        ) {
            Ok(()) => return Ok(response),
            Err(error) => failures.push(format!("{base}: {error}")),
        }
    }
    Err(Error::external(format!(
        "could not authenticate a cluster CA ({})",
        failures.join("; ")
    )))
}

fn validate_join_payload(
    config: &StartConfig,
    cluster_name: &str,
    discovery: &cluster::join::CaDiscoveryResponse,
    envelope: &cluster::join::JoinEnvelope,
    payload: &cluster::join::JoinPayload,
) -> Result<()> {
    if payload.cluster_id != envelope.cluster_id
        || payload.cluster_id != discovery.cluster_id
        || payload.display_name != cluster_name
    {
        return Err(Error::external(
            "cluster join response identity was not authenticated",
        ));
    }
    let discovered_ca = certs::certificate_fingerprint(&discovery.ca_pem)
        .map_err(|error| Error::external(error.to_string()))?;
    let issued_ca = certs::certificate_fingerprint(&payload.certificates.ca_pem)
        .map_err(|error| Error::external(error.to_string()))?;
    if discovered_ca != issued_ca {
        return Err(Error::external(
            "joined node certificates do not chain to the authenticated cluster CA",
        ));
    }
    if let Some(voter_ca) = &payload.voter_ca {
        let voter_ca = certs::certificate_fingerprint(&voter_ca.cert_pem)
            .map_err(|error| Error::external(error.to_string()))?;
        if voter_ca != discovered_ca {
            return Err(Error::external(
                "voter CA material does not match the authenticated cluster CA",
            ));
        }
    }
    let configured = config
        .cluster
        .resolved_nodes()
        .map_err(|error| Error::invalid_config(error.to_string()))?;
    let topology_matches = if config.cluster.uses_node_ports() {
        payload.initial_voter_endpoints == configured
    } else {
        payload.initial_voter_host_ips
            == configured
                .iter()
                .map(|node| node.host_ip)
                .collect::<Vec<_>>()
    };
    if !topology_matches {
        return Err(Error::external(
            "cluster join response does not match the configured initial voters",
        ));
    }
    Ok(())
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
    certs::write_etcd_certs(&temporary, &payload.certificates.clone().into())
        .map_err(|error| Error::internal(error.to_string()))?;
    if role.is_voter() {
        let ca = payload
            .voter_ca
            .clone()
            .ok_or_else(|| Error::external("voter join response omitted cluster CA material"))?;
        certs::write_cluster_ca(&temporary.join("cluster-ca"), &ca.into())
            .map_err(|error| Error::internal(error.to_string()))?;
    }
    let certs_dir = system_dir.join("certs");
    std::fs::rename(&temporary, &certs_dir)?;
    if let Err(error) = cluster::identity::persist_cluster_id(data_dir, &payload.cluster_id) {
        let _ = std::fs::remove_dir_all(&certs_dir);
        return Err(Error::internal(error.to_string()));
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[derive(Clone)]
    struct JoinServerState {
        secret: String,
        cluster_name: String,
        cluster_id: String,
        ca_pem: String,
        payload: cluster::join::JoinPayload,
    }

    async fn discover_test_ca(
        axum::extract::State(state): axum::extract::State<Arc<JoinServerState>>,
        axum::Json(request): axum::Json<cluster::join::CaDiscoveryRequest>,
    ) -> axum::Json<cluster::join::CaDiscoveryResponse> {
        axum::Json(
            cluster::join::create_ca_discovery_response(
                &state.secret,
                &state.cluster_name,
                &state.cluster_id,
                &state.ca_pem,
                &request,
            )
            .expect("create CA discovery proof"),
        )
    }

    async fn join_test_node(
        axum::extract::State(state): axum::extract::State<Arc<JoinServerState>>,
        headers: axum::http::HeaderMap,
        axum::Json(request): axum::Json<cluster::join::JoinRequest>,
    ) -> axum::Json<cluster::join::JoinEnvelope> {
        let signature = headers
            .get("x-maestro-join-signature")
            .and_then(|value| value.to_str().ok())
            .expect("signed join request");
        cluster::join::verify_request_signature(&state.secret, &request, signature)
            .expect("authenticate join request");
        axum::Json(
            cluster::join::encrypt_response(&state.secret, &request, &state.payload, 200)
                .expect("encrypt join response"),
        )
    }

    fn cluster_config() -> ClusterConfig {
        ClusterConfig {
            name: "test".to_string(),
            nodes: vec!["10.20.0.11:3101".parse().unwrap()],
            control_allow_cidrs: vec!["10.20.0.0/24".to_string()],
            api_port: 3101,
            shared_registry: Some("registry.example.com/maestro".to_string()),
            join_secret: Some("x".repeat(32)),
            ..ClusterConfig::default()
        }
    }

    #[test]
    fn seed_identity_is_created_once_without_operator_material() {
        let root = std::env::temp_dir().join(format!(
            "maestro-auto-seed-{}",
            crate::utils::nanoid::unique_id(10)
        ));
        let config = cluster_config();
        let first = ensure_seed_identity(
            &config,
            NodeRole::Hybrid,
            &root,
            "10.20.0.11".parse().unwrap(),
        )
        .expect("automatically initialize seed");
        assert!(first.created);
        assert!(identity_installed(&root).unwrap());
        assert_eq!(
            cluster::identity::load_cluster_id(&root).unwrap(),
            first.cluster_id
        );
        let cache = cluster::join::load_voter_cache(&root, &first.cluster_id)
            .unwrap()
            .expect("seed voter cache");
        assert_eq!(
            cache.initial_voter_endpoints,
            config.resolved_nodes().unwrap()
        );
        let second = ensure_seed_identity(
            &config,
            NodeRole::Hybrid,
            &root,
            "10.20.0.11".parse().unwrap(),
        )
        .expect("reuse initialized seed");
        assert!(!second.created);
        assert_eq!(second.cluster_id, first.cluster_id);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn seed_initialization_resumes_before_bootstrap_is_armed() {
        let root = std::env::temp_dir().join(format!(
            "maestro-auto-seed-resume-{}",
            crate::utils::nanoid::unique_id(10)
        ));
        let cluster_id = cluster::identity::create_cluster_id(&root).unwrap();
        let identity = ensure_seed_identity(
            &cluster_config(),
            NodeRole::Hybrid,
            &root,
            "10.20.0.11".parse().unwrap(),
        )
        .expect("resume automatic seed initialization");
        assert_eq!(identity.cluster_id, cluster_id);
        assert!(identity.created);
        assert!(identity_installed(&root).unwrap());
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn only_first_configured_voter_can_initialize_cluster() {
        let root = std::env::temp_dir().join(format!(
            "maestro-auto-seed-reject-{}",
            crate::utils::nanoid::unique_id(10)
        ));
        let mut config = cluster_config();
        config.nodes = vec![
            "10.20.0.11:3101".parse().unwrap(),
            "10.20.0.12:3201".parse().unwrap(),
            "10.20.0.13:3301".parse().unwrap(),
        ];
        assert!(
            ensure_seed_identity(
                &config,
                NodeRole::Hybrid,
                &root,
                "10.20.0.12".parse().unwrap(),
            )
            .is_err()
        );
        assert!(!root.join("system/cluster-id").exists());
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn automatic_join_authenticates_ca_then_uses_verified_tls() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let listener = std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        let seed: cluster::ClusterNodeEndpoint = format!("127.0.0.1:{port}").parse().unwrap();
        let ca = certs::generate_cluster_ca().expect("generate test CA");
        let server_certs = certs::generate_cluster_node_certs_for_endpoint(
            &ca,
            std::net::Ipv4Addr::LOCALHOST,
            Some(port),
            NodeRole::Hybrid,
        )
        .expect("generate server certificate");
        let worker_ip = "127.0.0.2".parse().unwrap();
        let worker_certs = certs::generate_cluster_node_certs_for_endpoint(
            &ca,
            worker_ip,
            Some(port),
            NodeRole::Worker,
        )
        .expect("generate worker certificate");
        let cluster_id = "0123456789abcdef0123456789abcdef".to_string();
        let secret = "a-random-automatic-join-test-secret".to_string();
        let state = Arc::new(JoinServerState {
            secret: secret.clone(),
            cluster_name: "test".to_string(),
            cluster_id: cluster_id.clone(),
            ca_pem: ca.cert_pem.clone(),
            payload: cluster::join::JoinPayload {
                cluster_id: cluster_id.clone(),
                display_name: "test".to_string(),
                voter_host_ips: vec![seed.host_ip],
                voter_endpoints: vec![seed],
                initial_voter_host_ips: vec![seed.host_ip],
                initial_voter_endpoints: vec![seed],
                api_port: seed.api_port,
                etcd_client_port: seed.etcd_client_port,
                etcd_peer_port: seed.etcd_peer_port,
                certificates: cluster::join::NodeCertificateBundle::from(&worker_certs),
                voter_ca: None,
                join_info: None,
            },
        });
        let app = axum::Router::new()
            .route("/api/cluster/ca", axum::routing::post(discover_test_ca))
            .route("/api/cluster/join", axum::routing::post(join_test_node))
            .with_state(state);
        let tls = axum_server::tls_rustls::RustlsConfig::from_pem(
            server_certs.api_cert_pem.into_bytes(),
            server_certs.api_key_pem.into_bytes(),
        )
        .await
        .expect("build test TLS server");
        let handle = axum_server::Handle::<std::net::SocketAddr>::new();
        let server_handle = handle.clone();
        let server = tokio::spawn(async move {
            axum_server::bind_rustls((std::net::Ipv4Addr::LOCALHOST, port).into(), tls)
                .handle(server_handle)
                .serve(app.into_make_service())
                .await
        });
        tokio::time::sleep(Duration::from_millis(100)).await;

        let root = std::env::temp_dir().join(format!(
            "maestro-auto-join-{}",
            crate::utils::nanoid::unique_id(10)
        ));
        let mut config = StartConfig {
            cluster: ClusterConfig {
                name: "test".to_string(),
                nodes: vec![format!("127.0.0.1:{port}").parse().unwrap()],
                api_port: port,
                control_allow_cidrs: vec!["127.0.0.0/8".to_string()],
                shared_registry: Some("registry.invalid/maestro".to_string()),
                join_secret: Some(secret),
                ..ClusterConfig::default()
            },
            node: crate::config::NodeConfig {
                role: NodeRole::Worker,
            },
            subnet: Some("172.22.2.0/24".to_string()),
            ingress: crate::config::IngressConfig {
                port: Some(8080),
                ports: Vec::new(),
            },
            egress: Default::default(),
            encryption_key: "test-encryption-key".to_string(),
            jwt_secret_key: Some("j".repeat(32)),
            tailscale: None,
            tags: Vec::new(),
            datadog: None,
            system: None,
            runtime: Default::default(),
            depot: Default::default(),
            cloudflare: None,
            slack: None,
            log_backup: None,
            disable_etcd_cert: false,
            allow_cli_deployment: false,
        };
        join_once(&config, &root, worker_ip)
            .await
            .expect("complete automatic authenticated join");
        assert_eq!(
            cluster::identity::load_cluster_id(&root).unwrap(),
            cluster_id
        );
        assert!(identity_installed(&root).unwrap());
        let installed = certs::read_etcd_certs(&root.join("system/certs")).unwrap();
        assert_eq!(
            certs::certificate_fingerprint(&installed.ca_pem).unwrap(),
            certs::certificate_fingerprint(&ca.cert_pem).unwrap()
        );

        let rejected = std::env::temp_dir().join(format!(
            "maestro-auto-join-rejected-{}",
            crate::utils::nanoid::unique_id(10)
        ));
        config.cluster.join_secret = Some("a-different-random-join-test-secret".to_string());
        assert!(join_once(&config, &rejected, worker_ip).await.is_err());
        assert!(!rejected.join("system/cluster-id").exists());
        assert!(!rejected.join("system/certs").exists());

        handle.shutdown();
        server.await.unwrap().unwrap();
        let _ = std::fs::remove_dir_all(root);
        let _ = std::fs::remove_dir_all(rejected);
    }
}
