use std::{
    collections::BTreeMap,
    net::{IpAddr, SocketAddr},
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, anyhow, bail};
use axum::{Json, Router, extract::State, routing::post};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use tokio::sync::broadcast;

use crate::{
    cluster::{ClusterNodeEndpoint, ClusterRuntime},
    signal::ShutdownEvent,
    utils::certs::EtcdCerts,
};

type HmacSha256 = Hmac<Sha256>;

const RECOVERY_CONTEXT: &[u8] = b"maestro-cluster-recovery-status-v1";
const RECOVERY_REQUEST_CONTEXT: &[u8] = b"maestro-cluster-recovery-request-v1";
const LOCAL_MEMBER_FAILURE_LIMIT: u8 = 6;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryDecision {
    Normal,
    WaitForExisting,
    ForceNewCluster,
    BootstrapClean,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) enum MemberDataState {
    Missing,
    Present,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RecoveryStatusRequest {
    pub(crate) cluster_name: String,
    pub(crate) cluster_id: String,
    pub(crate) nonce: String,
    proof: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RecoveryStatusResponse {
    cluster_id: String,
    endpoint: ClusterNodeEndpoint,
    member_data: MemberDataState,
    cluster_available: bool,
    proof: String,
}

#[derive(Clone)]
struct RecoveryServerState {
    cluster_name: String,
    cluster_id: String,
    endpoint: ClusterNodeEndpoint,
    member_data: MemberDataState,
    join_secret: String,
}

/// Coordinates recovery before the local etcd member starts. A missing voter may rejoin a live
/// quorum immediately. Reconfiguration without quorum is allowed only after every configured
/// voter has answered the authenticated rendezvous, so an unreachable voter is never mistaken
/// for an empty one.
pub async fn coordinate(
    runtime: &ClusterRuntime,
    cluster_name: &str,
    data_dir: &Path,
    join_secret: &str,
    certs: &EtcdCerts,
    runtime_cli: Option<&str>,
    mut shutdown: broadcast::Receiver<ShutdownEvent>,
) -> Result<RecoveryDecision> {
    if !runtime.role.is_voter() {
        return Ok(RecoveryDecision::Normal);
    }

    let local_state = inspect_local_member_state(data_dir, runtime_cli)?;
    let force_new_pending =
        crate::cluster::bootstrap::force_new_cluster_is_pending(data_dir, &runtime.cluster_id)?;
    if force_new_pending {
        if local_state == MemberDataState::Present {
            return Ok(RecoveryDecision::ForceNewCluster);
        }
        crate::cluster::bootstrap::complete_force_new_cluster(data_dir)?;
    }
    if local_state == MemberDataState::Missing
        && runtime.is_seed()
        && crate::cluster::bootstrap::seed_is_armed(data_dir)?
    {
        return Ok(RecoveryDecision::Normal);
    }

    let server_state = Arc::new(RecoveryServerState {
        cluster_name: cluster_name.to_string(),
        cluster_id: runtime.cluster_id.clone(),
        endpoint: runtime.local_endpoint(),
        member_data: local_state,
        join_secret: join_secret.to_string(),
    });
    let app = Router::new()
        .route("/api/cluster/recovery-status", post(recovery_status))
        .with_state(server_state);
    let tls = axum_server::tls_rustls::RustlsConfig::from_pem(
        certs.api_cert_pem.clone().into_bytes(),
        certs.api_key_pem.clone().into_bytes(),
    )
    .await
    .context("failed to configure cluster recovery TLS")?;
    let handle = axum_server::Handle::<SocketAddr>::new();
    let server_handle = handle.clone();
    let bind = SocketAddr::new(IpAddr::V4(runtime.host_ip), runtime.api_port);
    let server = tokio::spawn(async move {
        axum_server::bind_rustls(bind, tls)
            .handle(server_handle)
            .serve(app.into_make_service())
            .await
    });

    tokio::time::sleep(Duration::from_millis(50)).await;
    if server.is_finished() {
        return server
            .await
            .context("cluster recovery rendezvous task failed")?
            .context("failed to bind cluster recovery rendezvous")
            .map(|()| RecoveryDecision::Normal);
    }

    let result = coordinate_with_peers(
        runtime,
        cluster_name,
        data_dir,
        join_secret,
        local_state,
        &mut shutdown,
    )
    .await;
    if result.is_ok() {
        // Let peers observe this node's authenticated startup state before releasing the API port.
        // This closes the rendezvous race where a quorum can decide first while an empty voter is
        // still collecting the same responses.
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    handle.graceful_shutdown(Some(Duration::from_secs(2)));
    let _ = server.await;
    result
}

pub fn spawn_local_member_monitor(
    runtime: ClusterRuntime,
    tls: etcd_client::TlsOptions,
    restart: broadcast::Sender<ShutdownEvent>,
    mut shutdown: broadcast::Receiver<ShutdownEvent>,
    logger: crate::logs::Logger,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let endpoint = format!("https://{}:{}", runtime.host_ip, runtime.etcd_client_port);
        let mut failures = 0_u8;
        loop {
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(5)) => {}
                _ = shutdown.recv() => return,
            }
            let options = etcd_client::ConnectOptions::new()
                .with_tls(tls.clone())
                .with_connect_timeout(Duration::from_secs(2))
                .with_timeout(Duration::from_secs(2));
            let healthy =
                match etcd_client::Client::connect([endpoint.as_str()], Some(options)).await {
                    Ok(mut client) => client
                        .status()
                        .await
                        .is_ok_and(|status| status.leader() != 0 && status.errors().is_empty()),
                    Err(_) => false,
                };
            if healthy {
                if failures > 0 {
                    logger.emit("info", "local etcd member recovered before daemon restart");
                }
                failures = 0;
                continue;
            }
            failures = failures.saturating_add(1);
            if failures < LOCAL_MEMBER_FAILURE_LIMIT {
                continue;
            }
            logger.emit(
                "error",
                "local etcd member remained unhealthy; restarting the daemon for automatic member recovery",
            );
            let _ = restart.send(ShutdownEvent::Restart);
            return;
        }
    })
}

async fn coordinate_with_peers(
    runtime: &ClusterRuntime,
    cluster_name: &str,
    data_dir: &Path,
    join_secret: &str,
    local_state: MemberDataState,
    shutdown: &mut broadcast::Receiver<ShutdownEvent>,
) -> Result<RecoveryDecision> {
    let client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .https_only(true)
        .no_proxy()
        .local_address(IpAddr::V4(runtime.host_ip))
        .timeout(Duration::from_secs(2))
        .build()
        .context("failed to build cluster recovery client")?;
    let local_endpoint = runtime.local_endpoint();
    let mut statuses = BTreeMap::from([(local_endpoint, (local_state, Instant::now()))]);
    let quorum = runtime.voter_endpoints.len() / 2 + 1;
    let mut delay = Duration::from_millis(250);

    loop {
        for endpoint in &runtime.voter_endpoints {
            if *endpoint == runtime.local_endpoint() {
                continue;
            }
            match probe_peer(
                &client,
                endpoint,
                cluster_name,
                &runtime.cluster_id,
                join_secret,
            )
            .await
            {
                Ok(PeerProbe::LiveCluster) => {
                    return Ok(if local_state == MemberDataState::Present {
                        RecoveryDecision::Normal
                    } else {
                        RecoveryDecision::WaitForExisting
                    });
                }
                Ok(PeerProbe::Starting(state)) => {
                    statuses.insert(*endpoint, (state, Instant::now()));
                }
                Err(_) => {}
            }
        }

        statuses.retain(|endpoint, (_, observed_at)| {
            *endpoint == local_endpoint || observed_at.elapsed() <= Duration::from_secs(10)
        });

        let present = statuses
            .values()
            .filter(|(state, _)| *state == MemberDataState::Present)
            .count();
        if present >= quorum {
            return Ok(if local_state == MemberDataState::Present {
                RecoveryDecision::Normal
            } else {
                RecoveryDecision::WaitForExisting
            });
        }
        if statuses.len() == runtime.voter_endpoints.len() {
            if present == 0 {
                if runtime.is_seed() {
                    crate::cluster::bootstrap::rearm_seed_after_empty_consensus(
                        data_dir,
                        &runtime.cluster_id,
                        runtime.host_ip,
                    )?;
                    return Ok(RecoveryDecision::BootstrapClean);
                }
                return Ok(RecoveryDecision::WaitForExisting);
            }

            let survivor = runtime
                .voter_endpoints
                .iter()
                .find(|endpoint| {
                    statuses
                        .get(endpoint)
                        .is_some_and(|(state, _)| *state == MemberDataState::Present)
                })
                .ok_or_else(|| anyhow!("cluster recovery found no surviving voter"))?;
            if *survivor == runtime.local_endpoint() {
                crate::cluster::bootstrap::mark_force_new_cluster(data_dir, &runtime.cluster_id)?;
                return Ok(RecoveryDecision::ForceNewCluster);
            }
            return Ok(RecoveryDecision::WaitForExisting);
        }

        tokio::select! {
            _ = tokio::time::sleep(delay) => {}
            _ = shutdown.recv() => bail!("cluster recovery interrupted by shutdown"),
        }
        delay = (delay * 2).min(Duration::from_secs(5));
    }
}

enum PeerProbe {
    LiveCluster,
    Starting(MemberDataState),
}

async fn probe_peer(
    client: &reqwest::Client,
    endpoint: &ClusterNodeEndpoint,
    cluster_name: &str,
    cluster_id: &str,
    join_secret: &str,
) -> Result<PeerProbe> {
    let base = format!("https://{}:{}", endpoint.host_ip, endpoint.api_port);
    let request = create_request(join_secret, cluster_name, cluster_id)?;
    let response = client
        .post(format!("{base}/api/cluster/recovery-status"))
        .json(&request)
        .send()
        .await?;
    if !response.status().is_success() {
        bail!(
            "peer recovery rendezvous returned HTTP {}",
            response.status()
        );
    }
    let response: RecoveryStatusResponse = response.json().await?;
    verify_response(join_secret, &request, &response)?;
    if response.cluster_id != cluster_id || response.endpoint != *endpoint {
        bail!("peer recovery response identity does not match the configured voter");
    }
    Ok(if response.cluster_available {
        PeerProbe::LiveCluster
    } else {
        PeerProbe::Starting(response.member_data)
    })
}

async fn recovery_status(
    State(state): State<Arc<RecoveryServerState>>,
    Json(request): Json<RecoveryStatusRequest>,
) -> Result<Json<RecoveryStatusResponse>, axum::http::StatusCode> {
    if request.cluster_name != state.cluster_name || request.cluster_id != state.cluster_id {
        return Err(axum::http::StatusCode::FORBIDDEN);
    }
    verify_request(&state.join_secret, &request).map_err(|_| axum::http::StatusCode::FORBIDDEN)?;
    create_response(
        &state.join_secret,
        &request,
        &state.cluster_id,
        state.endpoint,
        state.member_data,
        false,
    )
    .map(Json)
    .map_err(|_| axum::http::StatusCode::FORBIDDEN)
}

pub(crate) fn create_response(
    secret: &str,
    request: &RecoveryStatusRequest,
    cluster_id: &str,
    endpoint: ClusterNodeEndpoint,
    member_data: MemberDataState,
    cluster_available: bool,
) -> Result<RecoveryStatusResponse> {
    let mut response = RecoveryStatusResponse {
        cluster_id: cluster_id.to_string(),
        endpoint,
        member_data,
        cluster_available,
        proof: String::new(),
    };
    let message = recovery_message(request, &response)?;
    let mut mac = <HmacSha256 as Mac>::new_from_slice(secret.as_bytes())?;
    mac.update(&message);
    response.proof = hex::encode(mac.finalize().into_bytes());
    Ok(response)
}

fn create_request(
    secret: &str,
    cluster_name: &str,
    cluster_id: &str,
) -> Result<RecoveryStatusRequest> {
    let mut request = RecoveryStatusRequest {
        cluster_name: cluster_name.to_string(),
        cluster_id: cluster_id.to_string(),
        nonce: hex::encode(x25519_dalek::StaticSecret::random().to_bytes()),
        proof: String::new(),
    };
    let message = recovery_request_message(&request)?;
    let mut mac = <HmacSha256 as Mac>::new_from_slice(secret.as_bytes())?;
    mac.update(&message);
    request.proof = hex::encode(mac.finalize().into_bytes());
    Ok(request)
}

pub(crate) fn verify_request(secret: &str, request: &RecoveryStatusRequest) -> Result<()> {
    let message = recovery_request_message(request)?;
    let proof = hex::decode(&request.proof).context("invalid recovery request proof")?;
    let mut mac = <HmacSha256 as Mac>::new_from_slice(secret.as_bytes())?;
    mac.update(&message);
    mac.verify_slice(&proof)
        .map_err(|_| anyhow!("cluster recovery request authentication failed"))
}

fn recovery_request_message(request: &RecoveryStatusRequest) -> Result<Vec<u8>> {
    let nonce = hex::decode(&request.nonce).context("invalid recovery status nonce")?;
    if nonce.len() != 32 {
        bail!("recovery status nonce must contain 32 bytes");
    }
    let payload = serde_json::to_vec(&(&request.cluster_name, &request.cluster_id, &nonce))?;
    let mut message = Vec::with_capacity(RECOVERY_REQUEST_CONTEXT.len() + payload.len());
    message.extend_from_slice(RECOVERY_REQUEST_CONTEXT);
    message.extend_from_slice(&payload);
    Ok(message)
}

fn verify_response(
    secret: &str,
    request: &RecoveryStatusRequest,
    response: &RecoveryStatusResponse,
) -> Result<()> {
    let message = recovery_message(request, response)?;
    let proof = hex::decode(&response.proof).context("invalid recovery status proof")?;
    let mut mac = <HmacSha256 as Mac>::new_from_slice(secret.as_bytes())?;
    mac.update(&message);
    mac.verify_slice(&proof)
        .map_err(|_| anyhow!("cluster recovery status authentication failed"))
}

fn recovery_message(
    request: &RecoveryStatusRequest,
    response: &RecoveryStatusResponse,
) -> Result<Vec<u8>> {
    let nonce = hex::decode(&request.nonce).context("invalid recovery status nonce")?;
    if nonce.len() != 32 {
        bail!("recovery status nonce must contain 32 bytes");
    }
    let payload = serde_json::to_vec(&(
        &request.cluster_name,
        &request.cluster_id,
        &nonce,
        &response.cluster_id,
        response.endpoint,
        response.member_data,
        response.cluster_available,
    ))?;
    let mut message = Vec::with_capacity(RECOVERY_CONTEXT.len() + payload.len());
    message.extend_from_slice(RECOVERY_CONTEXT);
    message.extend_from_slice(&payload);
    Ok(message)
}

fn inspect_local_member_state(
    data_dir: &Path,
    runtime_cli: Option<&str>,
) -> Result<MemberDataState> {
    let etcd_data = data_dir.join("system/etcd/data");
    if !etcd_data.join("member").is_dir() {
        return Ok(MemberDataState::Missing);
    }
    let Some(runtime_cli) = runtime_cli else {
        return Ok(MemberDataState::Present);
    };
    let database = etcd_data.join("member/snap/db");
    let mount = format!("{}:/etcd-data:ro", etcd_data.display());
    let output = std::process::Command::new(runtime_cli)
        .args([
            "run",
            "--rm",
            "--volume",
            &mount,
            crate::deployment::ETCD_IMAGE_TAG,
            "etcdutl",
            "snapshot",
            "status",
            "--write-out=json",
            "/etcd-data/member/snap/db",
        ])
        .output()
        .with_context(|| format!("failed to validate local etcd database with `{runtime_cli}`"))?;
    if output.status.success() {
        return Ok(MemberDataState::Present);
    }
    let diagnostic = String::from_utf8_lossy(&output.stderr).trim().to_string();
    if !database.exists() || diagnostic.starts_with("Error:") {
        let quarantine = data_dir.join(format!(
            "system/etcd/data.corrupt-{}",
            crate::utils::nanoid::unique_id(12).to_ascii_lowercase()
        ));
        std::fs::rename(&etcd_data, &quarantine).with_context(|| {
            format!(
                "local etcd data is corrupt ({diagnostic}); failed to quarantine it at {}",
                quarantine.display()
            )
        })?;
        eprintln!(
            "[maestro]: quarantined corrupt local etcd data at {}: {}",
            quarantine.display(),
            if diagnostic.is_empty() {
                "database validation failed"
            } else {
                diagnostic.as_str()
            }
        );
        return Ok(MemberDataState::Missing);
    }
    bail!(
        "could not validate local etcd data without risking automatic replacement: {}",
        if diagnostic.is_empty() {
            String::from_utf8_lossy(&output.stdout).trim().to_string()
        } else {
            diagnostic
        }
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    fn test_runtime(
        endpoint: ClusterNodeEndpoint,
        voters: &[ClusterNodeEndpoint],
        index: usize,
    ) -> ClusterRuntime {
        ClusterRuntime {
            cluster_id: "0123456789abcdef0123456789abcdef".to_string(),
            node_id: format!("node{index:08}"),
            instance_id: format!("instance-{index}"),
            host_ip: endpoint.host_ip,
            role: if index == 0 {
                crate::cluster::NodeRole::Master
            } else {
                crate::cluster::NodeRole::Hybrid
            },
            initial_voters: voters.to_vec(),
            voter_endpoints: voters.to_vec(),
            subnet: format!("172.22.{}.0/24", index + 1),
            control_allow_cidrs: vec!["127.0.0.0/8".to_string()],
            api_port: endpoint.api_port,
            gateway_port: endpoint.gateway_port,
            etcd_client_port: endpoint.etcd_client_port,
            etcd_peer_port: endpoint.etcd_peer_port,
            labels: BTreeMap::new(),
        }
    }

    fn test_endpoint(api_port: u16) -> ClusterNodeEndpoint {
        ClusterNodeEndpoint {
            host_ip: Ipv4Addr::LOCALHOST,
            api_port,
            gateway_port: api_port + 1,
            etcd_client_port: api_port + 2,
            etcd_peer_port: api_port + 3,
        }
    }

    fn reserve_endpoints() -> Vec<ClusterNodeEndpoint> {
        let mut endpoints = Vec::new();
        while endpoints.len() < 3 {
            let listener = std::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
            let port = listener.local_addr().unwrap().port();
            drop(listener);
            if port <= u16::MAX - 3
                && endpoints
                    .iter()
                    .all(|endpoint: &ClusterNodeEndpoint| endpoint.api_port.abs_diff(port) > 3)
            {
                endpoints.push(test_endpoint(port));
            }
        }
        endpoints
    }

    async fn coordinate_test_cluster(
        present: &[usize],
    ) -> (Vec<RecoveryDecision>, Vec<std::path::PathBuf>) {
        let endpoints = reserve_endpoints();
        let ca = crate::utils::certs::generate_cluster_ca().unwrap();
        let (shutdown, _) = broadcast::channel(2);
        let mut roots = Vec::new();
        let mut tasks = Vec::new();
        for (index, endpoint) in endpoints.iter().copied().enumerate() {
            let root = std::env::temp_dir().join(format!(
                "maestro-recovery-test-{index}-{}",
                crate::utils::nanoid::unique_id(10)
            ));
            std::fs::create_dir_all(root.join("system")).unwrap();
            if index == 0 {
                crate::cluster::bootstrap::arm_seed(
                    &root,
                    "0123456789abcdef0123456789abcdef",
                    Ipv4Addr::LOCALHOST,
                )
                .unwrap();
                crate::cluster::bootstrap::mark_seed_starting(&root).unwrap();
                crate::cluster::bootstrap::mark_seed_joined(&root).unwrap();
            }
            if present.contains(&index) {
                std::fs::create_dir_all(root.join("system/etcd/data/member")).unwrap();
            }
            let certs = crate::utils::certs::generate_cluster_node_certs_for_endpoint(
                &ca,
                endpoint.host_ip,
                endpoint.api_port,
                if index == 0 {
                    crate::cluster::NodeRole::Master
                } else {
                    crate::cluster::NodeRole::Hybrid
                },
            )
            .unwrap();
            let runtime = test_runtime(endpoint, &endpoints, index);
            let task_root = root.clone();
            let receiver = shutdown.subscribe();
            tasks.push(tokio::spawn(async move {
                coordinate(
                    &runtime,
                    "test",
                    &task_root,
                    "shared-recovery-secret",
                    &certs,
                    None,
                    receiver,
                )
                .await
            }));
            roots.push(root);
        }
        let mut decisions = Vec::new();
        for task in tasks {
            decisions.push(task.await.unwrap().unwrap());
        }
        (decisions, roots)
    }

    #[test]
    fn recovery_status_proof_binds_state_and_endpoint() {
        let request =
            create_request("shared-secret", "prod", "0123456789abcdef0123456789abcdef").unwrap();
        verify_request("shared-secret", &request).unwrap();
        assert!(verify_request("wrong-secret", &request).is_err());
        let endpoint = "10.20.0.11:3101".parse().unwrap();
        let response = create_response(
            "shared-secret",
            &request,
            &request.cluster_id,
            endpoint,
            MemberDataState::Missing,
            false,
        )
        .unwrap();
        verify_response("shared-secret", &request, &response).unwrap();

        let mut tampered = response.clone();
        tampered.member_data = MemberDataState::Present;
        assert!(verify_response("shared-secret", &request, &tampered).is_err());
        let mut tampered = response.clone();
        tampered.cluster_available = true;
        assert!(verify_response("shared-secret", &request, &tampered).is_err());
        assert!(verify_response("wrong-secret", &request, &response).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn corrupt_member_data_is_quarantined_before_recovery() {
        use std::os::unix::fs::PermissionsExt;

        let root = std::env::temp_dir().join(format!(
            "maestro-corrupt-etcd-{}",
            crate::utils::nanoid::unique_id(10)
        ));
        std::fs::create_dir_all(root.join("system/etcd/data/member/snap")).unwrap();
        std::fs::write(root.join("system/etcd/data/member/snap/db"), b"corrupt").unwrap();
        let validator = root.join("invalid-etcd-runtime");
        std::fs::write(
            &validator,
            "#!/bin/sh\necho 'Error: invalid database' >&2\nexit 1\n",
        )
        .unwrap();
        std::fs::set_permissions(&validator, std::fs::Permissions::from_mode(0o700)).unwrap();

        assert_eq!(
            inspect_local_member_state(&root, validator.to_str()).unwrap(),
            MemberDataState::Missing
        );
        assert!(!root.join("system/etcd/data").exists());
        assert!(
            std::fs::read_dir(root.join("system/etcd"))
                .unwrap()
                .filter_map(|entry| entry.ok())
                .any(|entry| entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with("data.corrupt-"))
        );
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn every_empty_voter_authorizes_only_the_seed_to_bootstrap_clean() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let (decisions, roots) = coordinate_test_cluster(&[]).await;
        assert_eq!(
            decisions,
            vec![
                RecoveryDecision::BootstrapClean,
                RecoveryDecision::WaitForExisting,
                RecoveryDecision::WaitForExisting,
            ]
        );
        assert!(crate::cluster::bootstrap::seed_is_armed(&roots[0]).unwrap());
        for root in roots {
            let _ = std::fs::remove_dir_all(root);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn one_surviving_voter_rebuilds_quorum_after_every_peer_answers() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let (decisions, roots) = coordinate_test_cluster(&[1]).await;
        assert_eq!(
            decisions,
            vec![
                RecoveryDecision::WaitForExisting,
                RecoveryDecision::ForceNewCluster,
                RecoveryDecision::WaitForExisting,
            ]
        );
        for root in roots {
            let _ = std::fs::remove_dir_all(root);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn surviving_quorum_starts_normally_and_empty_voter_waits_to_rejoin() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let (decisions, roots) = coordinate_test_cluster(&[0, 2]).await;
        assert_eq!(
            decisions,
            vec![
                RecoveryDecision::Normal,
                RecoveryDecision::WaitForExisting,
                RecoveryDecision::Normal,
            ]
        );
        for root in roots {
            let _ = std::fs::remove_dir_all(root);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn unreachable_voters_never_authorize_an_empty_bootstrap() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let endpoints = reserve_endpoints();
        let root = std::env::temp_dir().join(format!(
            "maestro-unreachable-recovery-{}",
            crate::utils::nanoid::unique_id(10)
        ));
        crate::cluster::bootstrap::arm_seed(
            &root,
            "0123456789abcdef0123456789abcdef",
            Ipv4Addr::LOCALHOST,
        )
        .unwrap();
        crate::cluster::bootstrap::mark_seed_starting(&root).unwrap();
        crate::cluster::bootstrap::mark_seed_joined(&root).unwrap();
        let ca = crate::utils::certs::generate_cluster_ca().unwrap();
        let certs = crate::utils::certs::generate_cluster_node_certs_for_endpoint(
            &ca,
            endpoints[0].host_ip,
            endpoints[0].api_port,
            crate::cluster::NodeRole::Hybrid,
        )
        .unwrap();
        let runtime = test_runtime(endpoints[0], &endpoints, 0);
        let (shutdown, _) = broadcast::channel(2);
        let receiver = shutdown.subscribe();
        let task_root = root.clone();
        let mut task = tokio::spawn(async move {
            coordinate(
                &runtime,
                "test",
                &task_root,
                "shared-recovery-secret",
                &certs,
                None,
                receiver,
            )
            .await
        });

        assert!(
            tokio::time::timeout(Duration::from_millis(750), &mut task)
                .await
                .is_err(),
            "the empty seed decided before every configured voter answered"
        );
        let _ = shutdown.send(ShutdownEvent::Force);
        assert!(task.await.unwrap().is_err());
        assert!(!crate::cluster::bootstrap::seed_is_armed(&root).unwrap());
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn durable_force_new_marker_resumes_without_peer_rendezvous() {
        let endpoints = reserve_endpoints();
        let root = std::env::temp_dir().join(format!(
            "maestro-force-recovery-resume-{}",
            crate::utils::nanoid::unique_id(10)
        ));
        std::fs::create_dir_all(root.join("system/etcd/data/member")).unwrap();
        crate::cluster::bootstrap::mark_force_new_cluster(
            &root,
            "0123456789abcdef0123456789abcdef",
        )
        .unwrap();
        let ca = crate::utils::certs::generate_cluster_ca().unwrap();
        let certs = crate::utils::certs::generate_cluster_node_certs_for_endpoint(
            &ca,
            endpoints[0].host_ip,
            endpoints[0].api_port,
            crate::cluster::NodeRole::Hybrid,
        )
        .unwrap();
        let runtime = test_runtime(endpoints[0], &endpoints, 0);
        let (_shutdown, receiver) = broadcast::channel(1);
        assert_eq!(
            coordinate(
                &runtime,
                "test",
                &root,
                "shared-recovery-secret",
                &certs,
                None,
                receiver,
            )
            .await
            .unwrap(),
            RecoveryDecision::ForceNewCluster
        );
        let _ = std::fs::remove_dir_all(root);
    }
}
