#![cfg(target_os = "linux")]

use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::net::Ipv4Addr;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::Duration;

use cluster::{
    CertificateValidity, ClusterCertificateAuthority, ClusterConfig, ClusterPorts,
    EmbeddedEtcdProvider, EmbeddedEtcdSettings, Ipv4Cidr, JoinPayload, JoinPrivateKey, JoinRequest,
    JoinResponseStatus, MemberState, NodeCertificateBundle, NodeDefinition, NodeEndpoint,
    StoreJoinTicket, StoreMember, StoreProvider, StoreProviderConfig, StoreProviderError,
    TailscaleGatewayConfig, admit_join_request, decrypt_join_response, encrypt_join_response,
    sign_join_request,
};
use clustertest::{FixtureNodeName, scenarios::cluster_bootstraps_joins_meshes_and_recovers};
use daemon::{DaemonLaunchDocument, StoreLaunchMode};
use kernel_api::{ClusterId, NodeId, NodeInstanceId, NodeRole, SecretValue};
use kernel_store::{EtcdStore, EtcdTlsConfig, Keyspace, Store, TokioClock, derive_key};
use time::{Duration as TimeDuration, OffsetDateTime};

#[path = "real_cluster/dns.rs"]
mod dns;
#[path = "real_cluster/ingress.rs"]
mod ingress;
#[path = "real_cluster/network.rs"]
mod network;
#[path = "real_cluster/scenario.rs"]
mod scenario;
#[path = "real_cluster/system_plane.rs"]
mod system_plane;
#[path = "real_cluster/workload.rs"]
mod workload;
#[path = "real_cluster/workload_fixture.rs"]
mod workload_fixture;

use dns::LocalDnsUpstream;
use network::{
    RealNode, kill_namespace_processes, node_namespace_diagnostics, shortened_interface_name,
    workload_namespace_diagnostics,
};

const SETUP_TIMEOUT: Duration = Duration::from_secs(30);
const RETRY_DELAY: Duration = Duration::from_millis(100);
const STORE_CLIENT_PORT: u16 = 34_379;
const STORE_PEER_PORT: u16 = 34_380;
const WIREGUARD_PORT: u16 = 34_820;
const GATEWAY_PORT: u16 = 34_001;
const API_PORT: u16 = 35_000;
const OPERATOR_JWT_SECRET: &str = "real-cluster-operator-secret-with-at-least-32-characters";
const ENCRYPTION_SECRET: &str = "real-cluster-encryption-secret-with-at-least-32-characters";

static NEXT_NETWORK: AtomicU16 = AtomicU16::new(1);

#[tokio::test]
#[ignore = "requires root, containerd, iproute2, ping, nftables, WireGuard, MAESTRO_ETCD_BIN, MAESTRO_CONTAINERD_SOCKET, and Linux network namespaces"]
async fn real_process_one_node_cluster_setup() -> Result<(), Box<dyn std::error::Error>> {
    let mut cluster = RealProcessCluster::new(1).await?;
    cluster_bootstraps_joins_meshes_and_recovers(&mut cluster).await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires root, containerd, iproute2, ping, nftables, WireGuard, MAESTRO_ETCD_BIN, MAESTRO_CONTAINERD_SOCKET, and Linux network namespaces"]
async fn real_process_three_node_cluster_setup() -> Result<(), Box<dyn std::error::Error>> {
    let mut cluster = RealProcessCluster::new(3).await?;
    cluster_bootstraps_joins_meshes_and_recovers(&mut cluster).await?;
    Ok(())
}

struct RealProcessCluster {
    root: tempfile::TempDir,
    daemon_binary: PathBuf,
    etcd_binary: PathBuf,
    containerd_socket: PathBuf,
    bridge: String,
    resolv_conf: PathBuf,
    dns_upstream: Option<LocalDnsUpstream>,
    cluster_config_path: PathBuf,
    cluster: ClusterConfig,
    authority: ClusterCertificateAuthority,
    nodes: Vec<RealNode>,
    securities: BTreeMap<NodeId, NodeCertificateBundle>,
    write_sequence: u32,
}

impl RealProcessCluster {
    async fn new(node_count: usize) -> Result<Self, Box<dyn std::error::Error>> {
        Self::new_with_operator_access(node_count, OperatorAccess::Disabled).await
    }

    async fn new_with_operator_access(
        node_count: usize,
        operator_access: OperatorAccess,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        if !matches!(node_count, 1 | 3) {
            return Err("real-process cluster requires one or three nodes".into());
        }
        require_command("ip")?;
        require_command("ping")?;
        let etcd_binary = PathBuf::from(std::env::var("MAESTRO_ETCD_BIN")?);
        let containerd_socket = PathBuf::from(std::env::var("MAESTRO_CONTAINERD_SOCKET")?);
        let daemon_binary = PathBuf::from(env!("CARGO_BIN_EXE_daemon"));
        let root = tempfile::tempdir()?;
        let allocation = NEXT_NETWORK.fetch_add(1, Ordering::Relaxed);
        let segment = u8::try_from((allocation % 200).saturating_add(20))?;
        let token = format!("{:x}{allocation:x}", std::process::id());
        let bridge = shortened_interface_name("mb", &token, 0);
        let cluster = topology(node_count, segment, &token, operator_access)?;
        let cluster_config_path = root.path().join("cluster.json");
        write_cluster_config(&cluster_config_path, &cluster)?;
        let resolv_conf = root.path().join("resolv.conf");
        std::fs::write(
            &resolv_conf,
            format!("nameserver 10.203.{segment}.1\noptions timeout:1 attempts:1\n"),
        )?;
        let authority = ClusterCertificateAuthority::generate(
            &cluster.name,
            CertificateValidity::new(
                OffsetDateTime::now_utc() - TimeDuration::hours(1),
                OffsetDateTime::now_utc() + TimeDuration::days(365),
            )?,
        )?;
        let validity = CertificateValidity::new(
            OffsetDateTime::now_utc() - TimeDuration::hours(1),
            OffsetDateTime::now_utc() + TimeDuration::days(365),
        )?;
        let securities = cluster
            .nodes
            .iter()
            .map(|(node_id, node)| {
                Ok((
                    node_id.clone(),
                    authority.issue_node_certificate(
                        node_id,
                        &node.hostname,
                        node.endpoint.host_address,
                        node.role,
                        validity,
                    )?,
                ))
            })
            .collect::<Result<BTreeMap<_, _>, cluster::CertificateError>>()?;
        let nodes = cluster
            .nodes
            .iter()
            .enumerate()
            .map(|(index, (node_id, node))| RealNode {
                fixture: FixtureNodeName::new(node_id.as_str()),
                node_id: node_id.clone(),
                namespace: format!("maestro-{token}-{index}"),
                workload_namespace: format!("maestro-wl-{token}-{index}"),
                host_veth: shortened_interface_name("mh", &token, index),
                namespace_veth: format!("mn{index}"),
                workload_host_veth: shortened_interface_name("mw", &token, index),
                workload_peer_veth: shortened_interface_name("mx", &token, index),
                host_address: node.endpoint.host_address,
                workload_gateway: Ipv4Addr::new(
                    node.workload_subnet.network_address().octets()[0],
                    node.workload_subnet.network_address().octets()[1],
                    node.workload_subnet.network_address().octets()[2],
                    1,
                ),
                workload_address: Ipv4Addr::new(
                    node.workload_subnet.network_address().octets()[0],
                    node.workload_subnet.network_address().octets()[1],
                    node.workload_subnet.network_address().octets()[2],
                    17,
                ),
                data_directory: root.path().join(node_id.as_str()),
                config_path: root.path().join(node_id.as_str()).join("launch.json"),
                log_path: root.path().join(format!("{node_id}.log")),
                child: None,
                launch_sequence: 0,
            })
            .collect();
        let mut real = Self {
            root,
            daemon_binary,
            etcd_binary,
            containerd_socket,
            bridge,
            resolv_conf,
            dns_upstream: None,
            cluster_config_path,
            cluster,
            authority,
            nodes,
            securities,
            write_sequence: 0,
        };
        real.create_network(segment)?;
        real.dns_upstream = Some(LocalDnsUpstream::bind(Ipv4Addr::new(10, 203, segment, 1))?);
        Ok(real)
    }

    async fn launch_node(
        &mut self,
        index: usize,
        store_mode: StoreLaunchMode,
    ) -> Result<(), RealClusterError> {
        if self.node(index)?.child.is_some() {
            return Err(RealClusterError::new("node process is already running"));
        }
        let launch_sequence = {
            let node = self.node_mut(index)?;
            node.launch_sequence = node.launch_sequence.saturating_add(1);
            node.launch_sequence
        };
        let node = self.node(index)?;
        let security = self
            .securities
            .get(&node.node_id)
            .cloned()
            .ok_or_else(|| RealClusterError::new("node security is missing"))?;
        let config = DaemonLaunchDocument::new(
            node.node_id.clone(),
            node.data_directory.clone(),
            self.containerd_socket.clone(),
            Some(self.etcd_binary.clone()),
            store_mode,
            security,
            Some(self.authority.clone()),
            &SecretValue::new(ENCRYPTION_SECRET),
            Some(
                NodeInstanceId::new(format!("{}-process-{launch_sequence}", node.node_id))
                    .map_err(RealClusterError::from_display)?,
            ),
        )
        .map_err(RealClusterError::from_display)?;
        std::fs::create_dir_all(&node.data_directory).map_err(RealClusterError::from_display)?;
        write_private_json(&node.config_path, &config)?;
        let log = append_file(&node.log_path)?;
        let error_log = log.try_clone().map_err(RealClusterError::from_display)?;
        let mut command = Command::new("unshare");
        command
            .args([
                "--mount",
                "--propagation",
                "private",
                "--",
                "bash",
                "-ceu",
                "mount --bind \"$1\" /etc/resolv.conf; shift; exec \"$@\"",
                "maestro-real-cluster",
            ])
            .arg(&self.resolv_conf)
            .args(["ip", "netns", "exec", &node.namespace])
            .env("AWS_ACCESS_KEY_ID", "maestro-real-cluster")
            .env(
                "AWS_SECRET_ACCESS_KEY",
                "maestro-real-cluster-secret-access-key",
            )
            .env("AWS_REGION", "us-east-1")
            .env("AWS_EC2_METADATA_DISABLED", "true")
            .arg(&self.daemon_binary)
            .arg("start")
            .arg("--config")
            .arg(&self.cluster_config_path)
            .arg("--data-dir")
            .arg(&node.data_directory)
            .arg("--containerd-socket")
            .arg(&self.containerd_socket)
            .arg("--etcd-binary")
            .arg(&self.etcd_binary)
            .stdin(Stdio::null())
            .stdout(Stdio::from(log))
            .stderr(Stdio::from(error_log))
            .process_group(0);
        let child = command.spawn().map_err(RealClusterError::from_display)?;
        self.node_mut(index)?.child = Some(child);
        Ok(())
    }

    async fn wait_store(&mut self) -> Result<EtcdStore, RealClusterError> {
        let deadline = tokio::time::Instant::now() + SETUP_TIMEOUT;
        loop {
            self.ensure_children_running()?;
            match self.connect_store().await {
                Ok(store)
                    if store
                        .list(&Keyspace::new(&self.cluster.cluster_id).resources())
                        .await
                        .is_ok() =>
                {
                    return Ok(store);
                }
                Ok(_) if tokio::time::Instant::now() < deadline => {
                    tokio::time::sleep(RETRY_DELAY).await;
                }
                Ok(_) => {
                    return Err(RealClusterError::new(format!(
                        "store readiness deadline elapsed before a linearizable list succeeded; log: {}",
                        read_log(&self.node(0)?.log_path),
                    )));
                }
                Err(error) if tokio::time::Instant::now() < deadline => {
                    let _detail = error;
                    tokio::time::sleep(RETRY_DELAY).await;
                }
                Err(error) => {
                    return Err(RealClusterError::new(format!(
                        "store readiness deadline elapsed: {error}; log: {}",
                        read_log(&self.node(0)?.log_path),
                    )));
                }
            }
        }
    }

    async fn connect_store(&self) -> Result<EtcdStore, kernel_store::StoreError> {
        let node = self
            .nodes
            .iter()
            .find(|node| node.child.is_some())
            .ok_or_else(|| kernel_store::StoreError::Unavailable {
                message: "no running node is available for store access".to_owned(),
            })?;
        let security = self.securities.get(&node.node_id).ok_or_else(|| {
            kernel_store::StoreError::Contract {
                message: "running node security is missing".to_owned(),
            }
        })?;
        let tls = EtcdTlsConfig::new(
            node.host_address.to_string(),
            security.trust_root_pem.as_bytes().to_vec(),
            security.identity.certificate_pem.as_bytes().to_vec(),
            security
                .identity
                .private_key_pem
                .expose()
                .as_bytes()
                .to_vec(),
        );
        let encryption_key =
            derive_key("real-cluster-store-secret-with-32-characters").map_err(|error| {
                kernel_store::StoreError::Contract {
                    message: format!("real-cluster encryption key is invalid: {error}"),
                }
            })?;
        EtcdStore::connect_with_tls_and_encryption(
            [format!(
                "https://{}:{}",
                node.host_address, STORE_CLIENT_PORT
            )],
            tls,
            encryption_key,
        )
        .await
    }

    fn leader_provider(&self) -> Result<EmbeddedEtcdProvider, RealClusterError> {
        let config = self.provider_config(0)?;
        EmbeddedEtcdProvider::new(
            config,
            self.etcd_binary.clone(),
            Arc::new(TokioClock::new()),
            EmbeddedEtcdSettings::default(),
        )
        .map_err(RealClusterError::from_display)
    }

    fn provider_config(&self, index: usize) -> Result<StoreProviderConfig, RealClusterError> {
        let node = self.node(index)?;
        let known = self
            .nodes
            .iter()
            .map(|member| {
                (
                    member.node_id.clone(),
                    StoreMember {
                        node_id: member.node_id.clone(),
                        host_address: member.host_address,
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();
        let local = known
            .get(&node.node_id)
            .cloned()
            .ok_or_else(|| RealClusterError::new("local provider member is missing"))?;
        let security = self
            .securities
            .get(&node.node_id)
            .cloned()
            .ok_or_else(|| RealClusterError::new("provider security is missing"))?;
        StoreProviderConfig::new(
            self.cluster.cluster_id.clone(),
            local,
            known,
            self.cluster.ports,
            node.data_directory.join("store"),
            SecretValue::new("real-cluster-store-secret-with-32-characters"),
            security,
        )
        .map_err(RealClusterError::from_display)
    }

    async fn stage_member(&mut self, index: usize) -> Result<StoreJoinTicket, RealClusterError> {
        let provider = self.leader_provider()?;
        let node = self.node(index)?;
        let member = StoreMember {
            node_id: node.node_id.clone(),
            host_address: node.host_address,
        };
        let deadline = tokio::time::Instant::now() + SETUP_TIMEOUT;
        loop {
            match provider.stage_member(member.clone()).await {
                Ok((ticket, activation)) if activation.state == MemberState::Staged => {
                    return Ok(ticket);
                }
                Ok(_) | Err(StoreProviderError::Unavailable { .. })
                    if tokio::time::Instant::now() < deadline =>
                {
                    tokio::time::sleep(RETRY_DELAY).await;
                }
                Err(error) => return Err(RealClusterError::from_display(error)),
                Ok(_) => return Err(RealClusterError::new("member staging did not converge")),
            }
        }
    }

    async fn admit_and_stage_member(
        &mut self,
        index: usize,
    ) -> Result<StoreJoinTicket, RealClusterError> {
        let node = self.node(index)?;
        let node_id = node.node_id.clone();
        let source_address = node.host_address;
        let certificates = self
            .securities
            .get(&node_id)
            .cloned()
            .ok_or_else(|| RealClusterError::new("joiner security is missing"))?;
        let join_key = JoinPrivateKey::generate();
        let now_unix_ms =
            i64::try_from(OffsetDateTime::now_utc().unix_timestamp_nanos() / i128::from(1_000_000))
                .map_err(RealClusterError::from_display)?;
        let request = JoinRequest::from_config(&join_key, &self.cluster, &node_id, now_unix_ms)
            .map_err(RealClusterError::from_display)?;
        let signature = sign_join_request(&self.cluster.join_secret, &request)
            .map_err(RealClusterError::from_display)?;
        let admission = admit_join_request(
            &self.cluster,
            &request,
            &signature,
            source_address,
            now_unix_ms,
        )
        .map_err(RealClusterError::from_display)?;
        if admission.node_id != node_id {
            return Err(RealClusterError::new(
                "join admission returned a different node identity",
            ));
        }

        let ticket = self.stage_member(index).await?;
        let payload = JoinPayload {
            cluster_id: self.cluster.cluster_id.clone(),
            cluster_name: self.cluster.name.clone(),
            nodes: self.cluster.nodes.clone(),
            ports: self.cluster.ports,
            certificates,
            certificate_issuer: Some(self.authority.clone()),
            store_join_ticket: Some(ticket),
        };
        let envelope = encrypt_join_response(
            &self.cluster.join_secret,
            &request,
            &payload,
            JoinResponseStatus::ACCEPTED,
        )
        .map_err(RealClusterError::from_display)?;
        let admitted = decrypt_join_response(
            &self.cluster.join_secret,
            &join_key,
            &request,
            &envelope,
            JoinResponseStatus::ACCEPTED,
        )
        .map_err(RealClusterError::from_display)?;
        self.securities.insert(node_id, admitted.certificates);
        admitted
            .store_join_ticket
            .ok_or_else(|| RealClusterError::new("join response omitted the store ticket"))
    }

    async fn activate_member(&mut self, ticket: &StoreJoinTicket) -> Result<(), RealClusterError> {
        let provider = self.leader_provider()?;
        let deadline = tokio::time::Instant::now() + SETUP_TIMEOUT;
        loop {
            match provider.activate_member(ticket).await {
                Ok(activation) if activation.state == MemberState::Active => return Ok(()),
                Ok(_) | Err(StoreProviderError::MemberNotReady { .. })
                    if tokio::time::Instant::now() < deadline =>
                {
                    tokio::time::sleep(RETRY_DELAY).await;
                }
                Err(error) => return Err(RealClusterError::from_display(error)),
                Ok(_) => return Err(RealClusterError::new("member activation did not converge")),
            }
        }
    }

    async fn stop_process(&mut self, index: usize) -> Result<(), RealClusterError> {
        let namespace = self.node(index)?.namespace.clone();
        let Some(mut child) = self.node_mut(index)?.child.take() else {
            return Err(RealClusterError::new("node process is not running"));
        };
        kill_process_group(child.id());
        kill_namespace_processes(&namespace);
        let deadline = tokio::time::Instant::now() + SETUP_TIMEOUT;
        loop {
            if child
                .try_wait()
                .map_err(RealClusterError::from_display)?
                .is_some()
            {
                kill_process_group(child.id());
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                child.kill().map_err(RealClusterError::from_display)?;
                child.wait().map_err(RealClusterError::from_display)?;
                return Err(RealClusterError::new(
                    "node processes did not stop before the loss deadline",
                ));
            }
            tokio::time::sleep(RETRY_DELAY).await;
        }
    }

    fn ensure_children_running(&mut self) -> Result<(), RealClusterError> {
        for node in &mut self.nodes {
            if let Some(child) = node.child.as_mut()
                && let Some(status) = child.try_wait().map_err(RealClusterError::from_display)?
            {
                return Err(RealClusterError::new(format!(
                    "daemon `{}` exited early with {status}; log: {}",
                    node.node_id,
                    read_log(&node.log_path)
                )));
            }
        }
        Ok(())
    }

    fn node(&self, index: usize) -> Result<&RealNode, RealClusterError> {
        self.nodes
            .get(index)
            .ok_or_else(|| RealClusterError::new(format!("node index {index} is out of range")))
    }

    fn node_mut(&mut self, index: usize) -> Result<&mut RealNode, RealClusterError> {
        self.nodes
            .get_mut(index)
            .ok_or_else(|| RealClusterError::new(format!("node index {index} is out of range")))
    }

    fn index_for(&self, fixture: &FixtureNodeName) -> Result<usize, RealClusterError> {
        self.nodes
            .iter()
            .position(|node| node.fixture == *fixture)
            .ok_or_else(|| RealClusterError::new(format!("unknown node `{}`", fixture.as_str())))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OperatorAccess {
    Disabled,
    Tailscale,
}

#[derive(Debug, thiserror::Error)]
#[error("real-process cluster failed: {detail}")]
struct RealClusterError {
    detail: String,
}

impl RealClusterError {
    fn new(detail: impl Into<String>) -> Self {
        Self {
            detail: detail.into(),
        }
    }

    fn from_display(error: impl std::fmt::Display) -> Self {
        Self::new(error.to_string())
    }
}

fn topology(
    node_count: usize,
    segment: u8,
    token: &str,
    operator_access: OperatorAccess,
) -> Result<ClusterConfig, Box<dyn std::error::Error>> {
    let roles = if node_count == 1 {
        vec![NodeRole::Master]
    } else {
        vec![NodeRole::Master, NodeRole::Hybrid, NodeRole::ControlPlane]
    };
    let nodes = roles
        .into_iter()
        .enumerate()
        .map(|(index, role)| {
            let number = u8::try_from(index)?.saturating_add(1);
            let node_id = NodeId::new(format!("node-{number}"))?;
            Ok((
                node_id.clone(),
                NodeDefinition {
                    hostname: format!("{node_id}.internal"),
                    endpoint: NodeEndpoint {
                        host_address: Ipv4Addr::new(10, 203, segment, number.saturating_add(10)),
                        api_port: API_PORT,
                    },
                    workload_subnet: format!("172.22.{number}.0/24").parse::<Ipv4Cidr>()?,
                    role,
                },
            ))
        })
        .collect::<Result<BTreeMap<_, _>, Box<dyn std::error::Error>>>()?;
    let name = format!("real-{}", token.to_ascii_lowercase());
    let tailscale = match operator_access {
        OperatorAccess::Disabled => None,
        OperatorAccess::Tailscale => Some(TailscaleGatewayConfig {
            auth_key: SecretValue::new("tskey-auth-real-cluster-fixture"),
            advertise_routes: None,
            tags: Vec::new(),
            cross_cluster_dns: Vec::new(),
        }),
    };
    Ok(ClusterConfig {
        cluster_id: ClusterId::new(name.clone())?,
        name,
        nodes,
        control_allow_cidrs: vec![format!("10.203.{segment}.0/24").parse()?],
        ports: ClusterPorts::new(
            GATEWAY_PORT,
            STORE_CLIENT_PORT,
            STORE_PEER_PORT,
            WIREGUARD_PORT,
        )?,
        join_secret: SecretValue::new("real-process-join-secret-with-at-least-32-characters"),
        tailscale,
        cloudflare: None,
    })
}

fn write_private_json(path: &Path, value: &impl serde::Serialize) -> Result<(), RealClusterError> {
    let mut options = OpenOptions::new();
    options.create(true).truncate(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let file = options.open(path).map_err(RealClusterError::from_display)?;
    serde_json::to_writer_pretty(file, value).map_err(RealClusterError::from_display)
}

fn write_cluster_config(path: &Path, cluster: &ClusterConfig) -> Result<(), RealClusterError> {
    let nodes = cluster
        .nodes
        .iter()
        .map(|(node_id, node)| {
            (
                node_id.to_string(),
                serde_json::json!({
                    "hostname": node.hostname,
                    "endpoint": format!("{}:{}", node.endpoint.host_address, node.endpoint.api_port),
                    "subnet": node.workload_subnet.to_string(),
                    "role": role_name(node.role),
                }),
            )
        })
        .collect::<serde_json::Map<_, _>>();
    let tailscale = cluster.tailscale.as_ref().map(|tailscale| {
        serde_json::json!({
            "auth-key": tailscale.auth_key.expose(),
            "advertise-routes": tailscale.advertise_routes.as_ref().map(|routes| {
                routes.iter().map(ToString::to_string).collect::<Vec<_>>()
            }),
            "tags": tailscale.tags,
            "cross-cluster-dns": tailscale.cross_cluster_dns,
        })
    });
    let document = serde_json::json!({
        "jwt-secret-key": OPERATOR_JWT_SECRET,
        "encryption-key": ENCRYPTION_SECRET,
        "cluster": {
            "cluster-id": cluster.cluster_id.to_string(),
            "name": cluster.name,
            "nodes": nodes,
            "control-allow-cidrs": cluster.control_allow_cidrs.iter().map(ToString::to_string).collect::<Vec<_>>(),
            "ports": {
                "gateway": cluster.ports.gateway,
                "store-client": cluster.ports.store_client,
                "store-peer": cluster.ports.store_peer,
                "wireguard": cluster.ports.wireguard,
            },
            "join-secret": cluster.join_secret.expose(),
        },
        "tailscale": tailscale,
    });
    write_private_json(path, &document)
}

fn role_name(role: NodeRole) -> &'static str {
    match role {
        NodeRole::Master => "master",
        NodeRole::Hybrid => "hybrid",
        NodeRole::ControlPlane => "control-plane",
        NodeRole::Worker => "worker",
    }
}

fn append_file(path: &Path) -> Result<File, RealClusterError> {
    OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(RealClusterError::from_display)
}

fn require_command(command: &str) -> Result<(), Box<dyn std::error::Error>> {
    let status = Command::new(command)
        .arg("-Version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()?;
    if status.success() || command == "ping" {
        Ok(())
    } else {
        Err(format!("required command `{command}` is unavailable").into())
    }
}

fn kill_process_group(process_id: u32) {
    let _ = Command::new("kill")
        .args(["-KILL", "--", &format!("-{process_id}")])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
}

fn read_log(path: &Path) -> String {
    std::fs::read_to_string(path).unwrap_or_else(|error| format!("[unavailable: {error}]"))
}
