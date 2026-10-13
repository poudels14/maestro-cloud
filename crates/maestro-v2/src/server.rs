use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use clap::Args;
use maestro_agent::{
    HealthMonitor, IngressWriter, LocalPlanSource, Reconciler, ReconcilerSettings, StatusBoard,
    SystemSupervisor, apply_workload_slice_limits, detect_capacity, system_reserve,
    traefik_container,
};
use maestro_control::{
    ApiState, Deployer, DeployerSettings, ObservedReplicas, RoutingInput, build_routes, router,
};
use maestro_core::{
    ClusterConfig, NodeRecord, NodeRole, NodeState, ReplicaId, ReplicaStatus, RouteTable,
};
use maestro_runtime::{CniNetwork, ContainerdRuntime, ContainerdSettings};
use maestro_store::{Consensus, ConsensusSettings, Mutation};

use crate::identity::NodeIdentity;
use crate::log_driver;

const RAFT_ADDRESS: &str = "127.0.0.1:7400";
const LEADER_TIMEOUT: Duration = Duration::from_secs(30);
const DEPLOYER_INTERVAL: Duration = Duration::from_secs(1);
const ROUTING_RESYNC: Duration = Duration::from_secs(10);
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
}

struct BoardReplicas(StatusBoard);

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
    let consensus = Arc::new(open_consensus(&args.data_dir, &identity).await?);
    consensus.wait_until_leader(LEADER_TIMEOUT).await?;

    let capacity = detect_capacity()?;
    let reserved = system_reserve(&capacity, &config.node.reserve);
    if let Err(error) = apply_workload_slice_limits(&capacity, &reserved).await {
        tracing::warn!(%error, "workload slice limits were not applied");
    }
    let subnet = first_node_subnet(&config.cluster.cidr)?;
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
                resolv_conf: host_resolv_conf(),
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
    let source = Arc::new(LocalPlanSource::new(consensus.state_handle(), identity.id));
    let reconciler = Reconciler::new(
        runtime.clone(),
        source,
        board.clone(),
        ReconcilerSettings::default(),
    );
    let health = HealthMonitor::new(board.clone(), reconciler.plans());
    let observed = Arc::new(BoardReplicas(board.clone()));
    let deployer = Deployer::new(
        consensus.clone(),
        observed.clone(),
        DeployerSettings {
            defaults: config.defaults.resources.clone(),
            ..DeployerSettings::default()
        },
    );
    let api = router(Arc::new(ApiState {
        writer: consensus.clone(),
        observed,
    }));
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
        _ = run_routing(consensus.clone(), routing_board, ingress) => {}
        _ = run_deployer(deployer, leader) => {}
        result = axum::serve(listener, api.clone()) => result?,
        result = axum::serve(socket_listener, api) => result?,
        _ = tokio::signal::ctrl_c() => tracing::info!("shutting down"),
    }
    consensus.shutdown().await?;
    Ok(())
}

async fn run_deployer(deployer: Deployer<Consensus, BoardReplicas>, consensus: Arc<Consensus>) {
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

async fn run_routing(consensus: Arc<Consensus>, board: StatusBoard, mut ingress: IngressWriter) {
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
        tokio::select! {
            _ = board_changes.changed() => {}
            _ = store_changes.recv() => {}
            _ = tokio::time::sleep(ROUTING_RESYNC) => {}
        }
    }
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
    Ok(Consensus::open(ConsensusSettings {
        node_id: identity.id,
        address: RAFT_ADDRESS.to_string(),
        data_dir: data_dir.join("raft"),
    })
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

fn host_resolv_conf() -> PathBuf {
    let resolved = PathBuf::from("/run/systemd/resolve/resolv.conf");
    match resolved.exists() {
        true => resolved,
        false => PathBuf::from("/etc/resolv.conf"),
    }
}
