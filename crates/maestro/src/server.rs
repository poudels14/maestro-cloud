use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use clap::Args;
use maestro_agent::{
    Attachment, DnsRecords, DnsServer, EgressFirewall, EgressPolicy, HealthMonitor,
    HostCommandRunner, IngressWriter, MeshConfig, PeerClusters, Reconciler, ReconcilerSettings,
    StatusBoard, SystemSupervisor, TailscaleNode, TunnelConnectors, WIREGUARD_INTERFACE,
    WIREGUARD_PORT, WireGuardMesh, apply_workload_slice_limits, detect_capacity,
    discover_peer_clusters, load_or_generate_key, publish_replica_manifest, serve_denied,
    system_reserve, tailscale_container, traefik_container, upstream_resolvers,
};
use maestro_control::ApiAuth;
use maestro_core::{ClusterConfig, NodeRecord, NodeRole, NodeState, WireGuardInfo};
use maestro_proto::pb::node_server::NodeServer;
use maestro_proto::{ClusterCa, NodeCredentials};
use maestro_runtime::{CniNetwork, ContainerdRuntime, ContainerdSettings};
use maestro_store::{Mutation, SecretKey};
use tokio::task::JoinSet;

use crate::control_link::GrpcControlLink;
use crate::control_plane::{self, ControlPlaneSettings};
use crate::identity::NodeIdentity;
use crate::join::join_secret;
use crate::log_driver;
use crate::node_service::{MeshImageFetcher, NodeClients, NodeService};
use crate::unix_http;

const DEFAULT_ADVERTISE: &str = "127.0.0.1:7400";
const LEADER_TIMEOUT: Duration = Duration::from_secs(30);
const DNS_PORT: u16 = 53;
const REPLICA_MANIFEST: &str = "/run/maestro/replicas.json";
const TAILSCALE_SOCKET_DIR: &str = "/run/maestro/tailscale";
const CONTAINERD_NAMESPACE: &str = "maestro";
const BRIDGE_NAME: &str = "maestro0";
const DEFAULT_CNI_DIRS: [&str; 3] = ["/usr/local/libexec/cni", "/opt/cni/bin", "/usr/lib/cni"];

#[derive(Args)]
pub struct InitArgs {
    #[arg(long, default_value = "/var/lib/maestro")]
    data_dir: PathBuf,
    #[arg(long, default_value = DEFAULT_ADVERTISE)]
    advertise: String,
    #[arg(long)]
    config: Option<String>,
}

#[derive(Args)]
pub struct AgentArgs {
    #[arg(long, default_value = "/var/lib/maestro")]
    data_dir: PathBuf,
    #[arg(long)]
    config: Option<String>,
    #[arg(long, default_value = "127.0.0.1:3000")]
    listen: SocketAddr,
    #[arg(long, default_value = "/run/maestro/api.sock")]
    api_socket: PathBuf,
    #[arg(long, default_value = "/run/containerd/containerd.sock")]
    containerd_socket: PathBuf,
    #[arg(long)]
    admin_source: Option<PathBuf>,
    #[arg(long, default_value = "/run/maestro/telemetry.sock")]
    telemetry_socket: PathBuf,
    #[arg(long)]
    grpc_listen: Option<SocketAddr>,
}

pub async fn init(args: InitArgs) -> anyhow::Result<()> {
    let config = load_config(args.config.as_deref())?;
    let identity =
        NodeIdentity::bootstrap(&args.data_dir, &args.advertise, config.cluster.clone())?;
    SecretKey::load_or_create(&encryption_key_path(&args.data_dir))?;
    api_secret(&args.data_dir)?;
    join_secret(&args.data_dir)?;
    let pki_dir = args.data_dir.join("pki");
    if !pki_dir.join("node").exists() {
        let ca = ClusterCa::generate(&identity.cluster.name)?;
        ca.save(&pki_dir)?;
        let addresses = advertise_ip(&identity.advertise)
            .into_iter()
            .collect::<Vec<_>>();
        ca.issue_node(identity.id.0, &addresses)?
            .save(&pki_dir.join("node"))?;
    }
    let credentials = NodeCredentials::load(&pki_dir.join("node"))?;
    let consensus = control_plane::open_consensus(&args.data_dir, &identity, credentials).await?;
    consensus.bootstrap_single_voter().await?;
    consensus.wait_until_leader(LEADER_TIMEOUT).await?;
    let existing = consensus.state().node(identity.id)?;
    let needs_registration = existing
        .as_ref()
        .is_none_or(|node| node.wireguard.is_none());
    if needs_registration {
        let capacity = detect_capacity()?;
        let wireguard_key = load_or_generate_key(&args.data_dir.join("keys")).await?;
        let wireguard = advertise_ip(&identity.advertise).map(|ip| WireGuardInfo {
            public_key: wireguard_key.public_key,
            endpoint: SocketAddr::new(ip, WIREGUARD_PORT),
        });
        let record = NodeRecord {
            id: identity.id,
            name: identity.name.clone(),
            role: NodeRole::Voter,
            endpoint: identity.advertise.clone(),
            subnet: Some(identity.subnet.clone()),
            runs_workloads: true,
            capacity,
            reserved: system_reserve(&capacity, &config.node.reserve),
            labels: config.node.labels.clone(),
            state: NodeState::Active,
            wireguard,
        };
        consensus
            .write(vec![Mutation::PutNode { node: record }])
            .await?;
    }
    consensus.shutdown().await?;
    println!(
        "initialized cluster {} on node {} ({}) in {}",
        identity.cluster.name,
        identity.name,
        identity.id,
        args.data_dir.display()
    );
    Ok(())
}

pub async fn run_agent(args: AgentArgs) -> anyhow::Result<()> {
    let config = load_config(args.config.as_deref())?;
    let identity = NodeIdentity::load(&args.data_dir)?;
    let credentials = NodeCredentials::load(&args.data_dir.join("pki").join("node"))?;
    let capacity = detect_capacity()?;
    let reserved = system_reserve(&capacity, &config.node.reserve);
    if let Err(error) = apply_workload_slice_limits(&capacity, &reserved).await {
        tracing::warn!(%error, "workload slice limits were not applied");
    }
    let gateway = subnet_gateway(&identity.subnet)?;
    let host_resolv = std::fs::read_to_string(host_resolv_conf()).unwrap_or_default();
    let container_resolv =
        write_container_resolv_conf(&args.data_dir, gateway, &identity.cluster.name)?;
    let log_uri = match log_driver::ensure_binary_link() {
        Some(binary) => Some(log_driver::uri(&binary, &args.data_dir.join("logs"))),
        None => {
            tracing::warn!("maestro-log-driver link is missing; container logs go to plain files");
            None
        }
    };
    let network = CniNetwork::bridge(
        BRIDGE_NAME,
        &identity.subnet,
        cni_dirs(),
        &args.data_dir.join("cni"),
    );
    let runtime = Arc::new(
        ContainerdRuntime::connect(
            ContainerdSettings {
                socket: args.containerd_socket.clone(),
                namespace: CONTAINERD_NAMESPACE.to_string(),
                snapshotter: "overlayfs".to_string(),
                state_dir: args.data_dir.join("runtime"),
                resolv_conf: container_resolv,
                log_uri,
            },
            network,
        )
        .await?,
    );
    let ingress = IngressWriter::new(args.data_dir.join("traefik"), &config.ingress.ports);
    let tailscale = config.tailscale.as_ref().map(|tailscale| TailscaleNode {
        auth_key: tailscale.auth_key.clone(),
        cluster: identity.cluster.name.clone(),
        node_id: identity.id.to_string(),
        routes: std::iter::once(identity.subnet.clone())
            .chain(tailscale.advertise_routes.iter().cloned())
            .collect(),
        state_dir: args.data_dir.join("tailscale"),
        socket_dir: PathBuf::from(TAILSCALE_SOCKET_DIR),
    });
    if let Some(node) = &tailscale {
        std::fs::create_dir_all(&node.state_dir)?;
        std::fs::create_dir_all(&node.socket_dir)?;
    }
    let system = SystemSupervisor::new(
        runtime.clone(),
        std::iter::once(traefik_container(
            ingress.directory(),
            &config.ingress.ports,
            &identity.cluster.cidr,
        ))
        .chain(tailscale.as_ref().map(tailscale_container))
        .collect(),
    );
    let dns_records = DnsRecords::default();
    let peer_clusters = PeerClusters::default();
    let dns_server = |records: &DnsRecords| {
        DnsServer::new(
            records.clone(),
            upstream_resolvers(&host_resolv),
            identity.cluster.name.clone(),
            peer_clusters.clone(),
        )
    };
    let dns = dns_server(&dns_records);
    let tailnet_dns = tailscale.as_ref().map(|_| dns_server(&dns_records));
    let wireguard_key = load_or_generate_key(&args.data_dir.join("keys")).await?;
    let mesh = MeshConfig {
        interface: WIREGUARD_INTERFACE.to_string(),
        listen_port: WIREGUARD_PORT,
        private_key_path: wireguard_key.private_key_path,
        local_subnet: identity.subnet.clone(),
        cluster_cidr: identity.cluster.cidr.clone(),
        bridge: BRIDGE_NAME.to_string(),
        peers: Vec::new(),
    };
    let board = StatusBoard::new();
    let (attachment, source) = Attachment::new(
        GrpcControlLink::new(credentials.clone()),
        identity.id,
        identity.control_seeds(),
        board.clone(),
        ingress,
        dns_records,
        Some((WireGuardMesh::host(), mesh)),
    );
    let tunnel = TunnelConnectors::new(
        runtime.clone(),
        config
            .cloudflare
            .as_ref()
            .map_or("", |cloudflare| cloudflare.tunnel.token.as_str()),
        config
            .cloudflare
            .as_ref()
            .map_or(0, |cloudflare| cloudflare.tunnel.replicas),
        attachment.attached(),
        config.ingress.ports.first().copied().unwrap_or(80),
    );
    let reconciler = Reconciler::new(
        runtime.clone(),
        Arc::new(source),
        board.clone(),
        ReconcilerSettings::default(),
    )
    .with_image_fetcher(Arc::new(MeshImageFetcher::new(
        runtime.clone(),
        NodeClients::new(credentials.clone()),
    )));
    let health = HealthMonitor::new(board.clone(), reconciler.plans());
    let reconciler_plans = reconciler.plans();
    let egress = EgressFirewall::new(
        HostCommandRunner,
        EgressPolicy {
            deny: config.egress.deny.clone(),
            allow: config.egress.allow.clone(),
            local_subnet: identity.subnet.clone(),
            cluster_cidr: identity.cluster.cidr.clone(),
        },
        board.clone(),
        reconciler.plans(),
    );

    let mut tasks = JoinSet::new();
    tasks.spawn(async move { reconciler.run().await.context("reconciler stopped") });
    tasks.spawn(async move {
        egress.run().await;
        Ok(())
    });
    tasks.spawn(async move {
        tunnel.run().await;
        Ok(())
    });
    tasks.spawn(async move {
        publish_replica_manifest(REPLICA_MANIFEST.into(), reconciler_plans).await;
        Ok(())
    });
    tasks.spawn(async move {
        serve_denied()
            .await
            .context("ingress deny responder stopped")
    });
    tasks.spawn(async move {
        health.run().await;
        Ok(())
    });
    tasks.spawn(async move {
        system.run().await;
        Ok(())
    });
    tasks.spawn(async move {
        dns.run(SocketAddr::new(gateway, DNS_PORT)).await;
        Ok(())
    });
    if let Some(tailnet_dns) = tailnet_dns {
        tasks.spawn(async move {
            tailnet_dns
                .run(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), DNS_PORT))
                .await;
            Ok(())
        });
        let cluster = identity.cluster.name.clone();
        tasks.spawn(async move {
            discover_peer_clusters(PathBuf::from(TAILSCALE_SOCKET_DIR), cluster, peer_clusters)
                .await;
            Ok(())
        });
    }
    tasks.spawn(async move {
        attachment.run().await;
        Ok(())
    });
    let grpc_listen = match args.grpc_listen {
        Some(listen) => listen,
        None => identity
            .advertise
            .parse()
            .context("--grpc-listen is required when the advertise address is not IP:port")?,
    };
    let consensus = match identity.role {
        NodeRole::Voter => {
            let settings = ControlPlaneSettings {
                data_dir: args.data_dir.clone(),
                identity: identity.clone(),
                credentials,
                config,
                grpc_listen,
                api_listen: args.listen,
                api_socket: args.api_socket.clone(),
                telemetry_socket: args.telemetry_socket.clone(),
                admin_source: args.admin_source.clone(),
            };
            Some(control_plane::spawn(&mut tasks, settings, runtime).await?)
        }
        NodeRole::Agent => {
            let node_grpc = tonic::transport::Server::builder()
                .tls_config(credentials.server_tls())?
                .add_service(NodeServer::new(NodeService::new(
                    args.telemetry_socket.clone(),
                    None,
                    runtime.clone(),
                    args.data_dir.clone(),
                )))
                .serve(grpc_listen);
            tasks.spawn(async move { node_grpc.await.context("node gRPC server stopped") });
            let local_api = local_router(board, args.telemetry_socket.clone());
            let socket_listener = bind_api_socket(&args.api_socket)?;
            tasks.spawn(async move {
                axum::serve(socket_listener, local_api)
                    .await
                    .context("local API socket stopped")
            });
            None
        }
    };
    tracing::info!(node = %identity.name, role = ?identity.role, subnet = %identity.subnet, "maestro node started");

    let outcome = tokio::select! {
        finished = tasks.join_next() => match finished {
            Some(Ok(result)) => result,
            Some(Err(error)) => Err(error.into()),
            None => Ok(()),
        },
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("shutting down");
            Ok(())
        }
    };
    tasks.abort_all();
    if let Some(consensus) = consensus {
        consensus.shutdown().await?;
    }
    outcome
}

pub async fn proxy_logs(
    axum::extract::State(socket): axum::extract::State<Arc<PathBuf>>,
    axum::extract::RawQuery(query): axum::extract::RawQuery,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    let path = format!("/logs?{}", query.unwrap_or_default());
    match unix_http::get(&socket, &path).await {
        Ok((status, body)) => (
            status,
            [(axum::http::header::CONTENT_TYPE, "application/json")],
            body,
        )
            .into_response(),
        Err(error) => (
            axum::http::StatusCode::BAD_GATEWAY,
            axum::Json(serde_json::json!({ "error": format!("telemetry unavailable: {error}") })),
        )
            .into_response(),
    }
}

pub fn create_token(data_dir: &Path, subject: &str, ttl_days: u64) -> anyhow::Result<()> {
    let auth = ApiAuth::new(&api_secret(data_dir)?);
    println!(
        "{}",
        auth.issue(subject, Duration::from_secs(ttl_days * 24 * 60 * 60))
    );
    Ok(())
}

pub fn api_secret(data_dir: &Path) -> anyhow::Result<Vec<u8>> {
    let path = data_dir.join("keys").join("api.key");
    match std::fs::read(&path) {
        Ok(secret) => Ok(secret),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            use std::io::Write;
            use std::os::unix::fs::OpenOptionsExt;
            let secret = rand::random::<[u8; 32]>().to_vec();
            std::fs::create_dir_all(data_dir.join("keys"))?;
            std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&path)?
                .write_all(&secret)?;
            Ok(secret)
        }
        Err(error) => Err(error.into()),
    }
}

pub fn encryption_key_path(data_dir: &Path) -> PathBuf {
    data_dir.join("keys").join("encryption.key")
}

pub fn bind_api_socket(path: &Path) -> anyhow::Result<tokio::net::UnixListener> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    match std::fs::remove_file(path) {
        Err(error) if error.kind() != std::io::ErrorKind::NotFound => Err(error)?,
        _ => {}
    }
    tokio::net::UnixListener::bind(path)
        .with_context(|| format!("failed to bind API socket {}", path.display()))
}

fn local_router(board: StatusBoard, telemetry_socket: PathBuf) -> axum::Router {
    axum::Router::new()
        .route(
            "/api/health",
            axum::routing::get(|| async { axum::Json(serde_json::json!({ "status": "ok" })) }),
        )
        .route(
            "/api/replicas",
            axum::routing::get(move || {
                let board = board.clone();
                async move { axum::Json(board.snapshot()) }
            }),
        )
        .route(
            "/api/disks",
            axum::routing::get(crate::node_service::local_disks),
        )
        .merge(
            axum::Router::new()
                .route("/api/logs", axum::routing::get(proxy_logs))
                .with_state(Arc::new(telemetry_socket)),
        )
}

pub fn load_config(source: Option<&str>) -> anyhow::Result<ClusterConfig> {
    match source {
        Some(source) => Ok(ClusterConfig::load_with(
            source,
            &crate::secret_sources::read_config_source,
        )?),
        None => Ok(ClusterConfig::default()),
    }
}

fn advertise_ip(advertise: &str) -> Option<IpAddr> {
    advertise
        .parse::<SocketAddr>()
        .map(|address| address.ip())
        .ok()
}

fn cni_dirs() -> Vec<PathBuf> {
    match std::env::var_os("CNI_PATH") {
        Some(paths) => std::env::split_paths(&paths).collect(),
        None => DEFAULT_CNI_DIRS.iter().map(PathBuf::from).collect(),
    }
}

fn subnet_gateway(subnet: &str) -> anyhow::Result<IpAddr> {
    let network = subnet
        .split_once('/')
        .map(|(network, _)| network)
        .context("node subnet must be a CIDR")?
        .parse::<std::net::Ipv4Addr>()?;
    let [first, second, third, _] = network.octets();
    Ok(IpAddr::from([first, second, third, 1]))
}

fn write_container_resolv_conf(
    data_dir: &Path,
    gateway: IpAddr,
    cluster: &str,
) -> anyhow::Result<PathBuf> {
    let path = data_dir.join("resolv.conf");
    std::fs::write(
        &path,
        format!("nameserver {gateway}\nsearch {cluster}.maestro.internal maestro.internal\n"),
    )?;
    Ok(path)
}

fn host_resolv_conf() -> PathBuf {
    let resolved = PathBuf::from("/run/systemd/resolve/resolv.conf");
    match resolved.exists() {
        true => resolved,
        false => PathBuf::from("/etc/resolv.conf"),
    }
}
