use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use clap::Args;
use maestro_agent::{
    ADMIN_CONTAINER_ID, BuildKitBuilder, BuildKitSettings, BuildRequest, DnsRecords, DnsServer,
    HealthMonitor, IngressWriter, LocalPlanSource, Reconciler, ReconcilerSettings, StatusBoard,
    SystemSupervisor, admin_container, apply_workload_slice_limits, detect_capacity,
    internal_names, system_reserve, traefik_container, upstream_resolvers,
};
use maestro_control::{
    ApiAuth, ApiState, Deployer, DeployerSettings, ImageBuilder, ObservedReplicas, RoutingInput,
    build_routes, require_token, router,
};
use maestro_core::{
    ClusterConfig, Deployment, NodeRecord, NodeRole, NodeState, ReplicaId, ReplicaPhase,
    ReplicaStatus, RouteTable,
};
use maestro_runtime::{CniNetwork, ContainerdRuntime, ContainerdSettings, Runtime};
use maestro_store::{Consensus, ConsensusSettings, Mutation, SecretKey};

use crate::identity::NodeIdentity;
use crate::log_driver;

const RAFT_ADDRESS: &str = "127.0.0.1:7400";
const LEADER_TIMEOUT: Duration = Duration::from_secs(30);
const DEPLOYER_INTERVAL: Duration = Duration::from_secs(1);
const ROUTING_RESYNC: Duration = Duration::from_secs(10);
const DNS_PORT: u16 = 53;
const BUILDKIT_ADDRESS: &str = "unix:///run/buildkit/buildkitd.sock";
const ADMIN_DOCKERFILE: &str = "Dockerfile.admin";
const ADMIN_BUILD_RETRY: Duration = Duration::from_secs(60);
const CONTAINERD_NAMESPACE: &str = "maestro";
const BRIDGE_NAME: &str = "maestro0";
const DEFAULT_CNI_DIRS: [&str; 3] = ["/usr/local/libexec/cni", "/opt/cni/bin", "/usr/lib/cni"];

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
}

struct BoardReplicas(StatusBoard);

struct SealedSpecBuilder {
    builder: BuildKitBuilder,
    key: Arc<SecretKey>,
}

impl ImageBuilder for SealedSpecBuilder {
    async fn build(&self, deployment: Deployment) -> Result<String, String> {
        let spec = self
            .key
            .open_spec(&deployment.spec)
            .map_err(|error| error.to_string())?;
        let build = spec
            .build
            .ok_or_else(|| "deployment has no build configuration".to_string())?;
        match (&build.env.source, &build.secrets.source) {
            (None, None) => {
                let request = BuildRequest {
                    deployment_id: deployment.id.to_string(),
                    service_id: deployment.service_id.to_string(),
                    repo: build.repo,
                    branch: build.branch,
                    context_dir: None,
                    dockerfile: build.dockerfile,
                    registry: build.registry,
                    image: None,
                    build_args: build.env.items,
                    secrets: build.secrets.items,
                };
                self.builder.build(&request).await
            }
            _ => Err("external secret sources are not supported yet".to_string()),
        }
    }
}

impl ObservedReplicas for BoardReplicas {
    fn replica(&self, replica_id: &ReplicaId) -> Option<ReplicaStatus> {
        self.0.get(replica_id)
    }

    fn replicas(&self) -> Vec<ReplicaStatus> {
        self.0.snapshot()
    }
}

pub async fn init(data_dir: &Path) -> anyhow::Result<()> {
    let identity = NodeIdentity::create(data_dir)?;
    SecretKey::load_or_create(&encryption_key_path(data_dir))?;
    api_secret(data_dir)?;
    let consensus = open_consensus(data_dir, &identity).await?;
    consensus.bootstrap_single_voter().await?;
    consensus.wait_until_leader(LEADER_TIMEOUT).await?;
    consensus.shutdown().await?;
    println!(
        "initialized cluster on node {} ({}) in {}",
        identity.name,
        identity.id,
        data_dir.display()
    );
    Ok(())
}

pub async fn run_agent(args: AgentArgs) -> anyhow::Result<()> {
    let config = match &args.config {
        Some(source) => ClusterConfig::load(source)?,
        None => ClusterConfig::default(),
    };
    let identity = NodeIdentity::load(&args.data_dir)?;
    let key = Arc::new(SecretKey::load_or_create(&encryption_key_path(
        &args.data_dir,
    ))?);
    let consensus = Arc::new(open_consensus(&args.data_dir, &identity).await?);
    consensus.wait_until_leader(LEADER_TIMEOUT).await?;

    let capacity = detect_capacity()?;
    let reserved = system_reserve(&capacity, &config.node.reserve);
    if let Err(error) = apply_workload_slice_limits(&capacity, &reserved).await {
        tracing::warn!(%error, "workload slice limits were not applied");
    }
    let subnet = first_node_subnet(&config.cluster.cidr)?;
    let gateway = subnet_gateway(&subnet)?;
    let host_resolv = std::fs::read_to_string(host_resolv_conf()).unwrap_or_default();
    let container_resolv =
        write_container_resolv_conf(&args.data_dir, gateway, &config.cluster.name)?;
    let dns_records = DnsRecords::default();
    let dns = DnsServer::new(dns_records.clone(), upstream_resolvers(&host_resolv));
    consensus
        .write(vec![Mutation::PutNode {
            node: NodeRecord {
                id: identity.id,
                name: identity.name.clone(),
                role: NodeRole::Voter,
                endpoint: RAFT_ADDRESS.to_string(),
                subnet: Some(subnet.clone()),
                runs_workloads: true,
                capacity,
                reserved,
                labels: config.node.labels.clone(),
                state: NodeState::Active,
            },
        }])
        .await?;

    let runtime_dir = args.data_dir.join("runtime");
    let log_uri = match log_driver::ensure_binary_link() {
        Some(binary) => Some(log_driver::uri(&binary, &args.data_dir.join("logs"))),
        None => {
            tracing::warn!("maestro-log-driver link is missing; container logs go to plain files");
            None
        }
    };
    let network = CniNetwork::bridge(BRIDGE_NAME, &subnet, cni_dirs(), &args.data_dir.join("cni"));
    let runtime = Arc::new(
        ContainerdRuntime::connect(
            ContainerdSettings {
                socket: args.containerd_socket.clone(),
                namespace: CONTAINERD_NAMESPACE.to_string(),
                snapshotter: "overlayfs".to_string(),
                state_dir: runtime_dir,
                resolv_conf: container_resolv,
                log_uri,
            },
            network,
        )
        .await?,
    );
    let ingress = IngressWriter::new(args.data_dir.join("traefik"), &config.ingress.ports);
    let system = SystemSupervisor::new(
        runtime.clone(),
        vec![traefik_container(
            ingress.directory(),
            &config.ingress.ports,
        )],
    );
    let board = StatusBoard::new();
    let source = Arc::new(LocalPlanSource::new(
        consensus.state_handle(),
        key.clone(),
        identity.id,
    ));
    let reconciler = Reconciler::new(
        runtime.clone(),
        source,
        board.clone(),
        ReconcilerSettings::default(),
    );
    let health = HealthMonitor::new(board.clone(), reconciler.plans());
    let observed = Arc::new(BoardReplicas(board.clone()));
    let buildkit = BuildKitSettings {
        buildctl: PathBuf::from("buildctl"),
        address: BUILDKIT_ADDRESS.to_string(),
        work_dir: args.data_dir.join("builds/work"),
        log_dir: args.data_dir.join("builds/logs"),
        git_token: None,
    };
    let admin = run_admin(
        runtime.clone(),
        BuildKitBuilder::new(buildkit.clone()),
        args.admin_source.clone(),
        args.api_socket.clone(),
    );
    let builder = Arc::new(SealedSpecBuilder {
        builder: BuildKitBuilder::new(buildkit),
        key: key.clone(),
    });
    let deployer = Deployer::new(
        consensus.clone(),
        observed.clone(),
        builder,
        DeployerSettings {
            defaults: config.defaults.resources.clone(),
            ..DeployerSettings::default()
        },
    );
    let api = router(Arc::new(ApiState {
        writer: consensus.clone(),
        observed,
        key,
    }));
    let auth = Arc::new(ApiAuth::new(&api_secret(&args.data_dir)?));
    let authenticated_api = api
        .clone()
        .layer(axum::middleware::from_fn_with_state(auth, require_token));
    let listener = tokio::net::TcpListener::bind(args.listen)
        .await
        .with_context(|| format!("failed to listen on {}", args.listen))?;
    let socket_listener = bind_api_socket(&args.api_socket)?;
    tracing::info!(node = %identity.name, listen = %args.listen, %subnet, "maestro agent started");

    let leader = consensus.clone();
    let routing_board = board.clone();
    tokio::select! {
        result = reconciler.run() => result?,
        _ = health.run() => {}
        _ = system.run() => {}
        _ = admin => {}
        _ = dns.run(SocketAddr::new(gateway, DNS_PORT)) => {}
        _ = run_routing(consensus.clone(), routing_board, ingress, dns_records, config.cluster.name.clone()) => {}
        _ = run_deployer(deployer, leader) => {}
        result = axum::serve(listener, authenticated_api) => result?,
        result = axum::serve(socket_listener, api) => result?,
        _ = tokio::signal::ctrl_c() => tracing::info!("shutting down"),
    }
    consensus.shutdown().await?;
    Ok(())
}

async fn run_deployer(
    deployer: Deployer<Consensus, BoardReplicas, SealedSpecBuilder>,
    consensus: Arc<Consensus>,
) {
    let mut ticker = tokio::time::interval(DEPLOYER_INTERVAL);
    loop {
        ticker.tick().await;
        if consensus.is_leader()
            && let Err(error) = deployer.tick().await
        {
            tracing::warn!(%error, "deployer tick failed");
        }
    }
}

async fn run_admin(
    runtime: Arc<ContainerdRuntime>,
    builder: BuildKitBuilder,
    source: Option<PathBuf>,
    api_socket: PathBuf,
) {
    match source {
        None => {
            tracing::info!("admin UI disabled; pass --admin-source to enable it");
            std::future::pending::<()>().await
        }
        Some(source) => {
            let version = env!("CARGO_PKG_VERSION");
            let image = format!("maestro.local/maestro-admin:{version}");
            while runtime.pull(&image).await.is_err() {
                tracing::info!(%image, "building the admin UI image");
                let request = BuildRequest {
                    deployment_id: format!("maestro-admin-{version}"),
                    service_id: ADMIN_CONTAINER_ID.to_string(),
                    repo: None,
                    branch: None,
                    context_dir: Some(source.clone()),
                    dockerfile: ADMIN_DOCKERFILE.to_string(),
                    registry: None,
                    image: Some(image.clone()),
                    build_args: Default::default(),
                    secrets: Default::default(),
                };
                if let Err(error) = builder.build(&request).await {
                    tracing::warn!(%error, "admin UI image build failed; retrying");
                    tokio::time::sleep(ADMIN_BUILD_RETRY).await;
                }
            }
            SystemSupervisor::new(runtime, vec![admin_container(&image, &api_socket)])
                .run()
                .await
        }
    }
}

async fn run_routing(
    consensus: Arc<Consensus>,
    board: StatusBoard,
    mut ingress: IngressWriter,
    dns_records: DnsRecords,
    cluster: String,
) {
    let mut board_changes = board.subscribe();
    let mut store_changes = consensus.state().subscribe();
    let mut previous = RouteTable::default();
    loop {
        match route_table(&consensus, &board, &previous) {
            Ok(table) => {
                if let Err(error) = ingress.write(&table).await {
                    tracing::warn!(%error, "failed to write ingress config");
                }
                previous = table;
            }
            Err(error) => tracing::warn!(%error, "failed to compute routes"),
        }
        match dns_names(&consensus, &board, &cluster) {
            Ok(names) => dns_records.replace(names),
            Err(error) => tracing::warn!(%error, "failed to compute DNS records"),
        }
        tokio::select! {
            _ = board_changes.changed() => {}
            _ = store_changes.recv() => {}
            _ = tokio::time::sleep(ROUTING_RESYNC) => {}
        }
    }
}

fn dns_names(
    consensus: &Consensus,
    board: &StatusBoard,
    cluster: &str,
) -> anyhow::Result<HashMap<String, Vec<IpAddr>>> {
    let state = consensus.state();
    let healthy = state
        .nodes()?
        .iter()
        .map(|node| state.assignments(node.id))
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .flatten()
        .filter_map(|assignment| {
            let status = board.get(&assignment.replica_id)?;
            let ip = status
                .ip
                .filter(|_| status.phase == ReplicaPhase::Healthy)?;
            Some((assignment, ip))
        })
        .collect::<Vec<_>>();
    let names = healthy
        .iter()
        .flat_map(|(assignment, ip)| {
            let replica_names = internal_names(cluster, assignment.replica_id.as_str());
            let service_names = internal_names(cluster, assignment.service_id.as_str());
            replica_names
                .into_iter()
                .chain(service_names)
                .map(move |name| (name, *ip))
        })
        .fold(
            HashMap::<String, Vec<IpAddr>>::new(),
            |mut names, (name, ip)| {
                names.entry(name).or_default().push(ip);
                names
            },
        );
    Ok(names)
}

fn route_table(
    consensus: &Consensus,
    board: &StatusBoard,
    previous: &RouteTable,
) -> anyhow::Result<RouteTable> {
    let state = consensus.state();
    let services = state.services()?;
    let deployments = services
        .iter()
        .map(|(service_id, _)| state.deployments(service_id))
        .collect::<Result<Vec<_>, _>>()?
        .concat();
    let assignments = state
        .nodes()?
        .iter()
        .map(|node| state.assignments(node.id))
        .collect::<Result<Vec<_>, _>>()?
        .concat();
    let observed = |replica_id: &ReplicaId| board.get(replica_id);
    Ok(build_routes(
        &RoutingInput {
            services: &services,
            deployments: &deployments,
            assignments: &assignments,
            observed: &observed,
        },
        previous,
    ))
}

async fn open_consensus(data_dir: &Path, identity: &NodeIdentity) -> anyhow::Result<Consensus> {
    Ok(Consensus::open(
        ConsensusSettings::new(identity.id, RAFT_ADDRESS, data_dir.join("raft")),
        None,
    )
    .await?)
}

fn bind_api_socket(path: &Path) -> anyhow::Result<tokio::net::UnixListener> {
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

fn first_node_subnet(cluster_cidr: &str) -> anyhow::Result<String> {
    let (network, _) = cluster_cidr
        .split_once('/')
        .context("cluster cidr must look like 10.200.0.0/16")?;
    let octets = network
        .split('.')
        .map(str::parse::<u8>)
        .collect::<Result<Vec<_>, _>>()
        .context("cluster cidr must be an IPv4 network")?;
    match octets.as_slice() {
        [first, second, _, _] => Ok(format!("{first}.{second}.1.0/24")),
        _ => anyhow::bail!("cluster cidr must be an IPv4 network"),
    }
}

fn cni_dirs() -> Vec<PathBuf> {
    match std::env::var_os("CNI_PATH") {
        Some(paths) => std::env::split_paths(&paths).collect(),
        None => DEFAULT_CNI_DIRS.iter().map(PathBuf::from).collect(),
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

fn api_secret(data_dir: &Path) -> anyhow::Result<Vec<u8>> {
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

fn encryption_key_path(data_dir: &Path) -> PathBuf {
    data_dir.join("keys").join("encryption.key")
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
