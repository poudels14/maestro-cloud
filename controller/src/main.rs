mod builder;
mod cli;
mod cluster;
mod config;
mod deployment;
mod engine;
mod error;
mod firewall;
mod health;
mod logs;
mod metrics;
mod probe;
mod runtime;
mod server;
mod signal;
mod slack;
mod supervisor;
mod utils;
mod validation;

use utils::crypto::{SecretString, derive_key};

use std::{
    net::Ipv4Addr,
    path::{Path, PathBuf},
    sync::Arc,
};

use clap::{Args, CommandFactory, Parser, Subcommand, error::ErrorKind};
use error::{Error, Result};
use signal::spawn_shutdown_signal_bus;

use crate::cluster::{
    ClusterMetrics, ClusterService, EtcdLeaderElector, EtcdNodeRegistry, LeaderElector,
    NodeRegistry, NodeRole,
    adapters::{
        ClusterDnsWriter, EtcdPortAllocator, EtcdTraefikSink, StoreDeploymentLookupBuilder,
        StoreServiceCatalog,
    },
    assignment_store::{AssignmentReconciler, AssignmentStore},
    engine_executor::{DeploymentLookup, EngineReplicaExecutorBuilder},
    etcd_assignment_store::EtcdAssignmentStore,
    leader_loop::{LeaderLoop, PlanObserver},
    node_id,
    scheduler::DefaultScheduler,
    service::node_info_from,
};
use crate::deployment::{
    ControllerConfig,
    controller::DeploymentController,
    types::{ClusterBootstrapBuilder, ClusterBootstrapMode, ClusterPeer},
};
use crate::supervisor::controller::JobSupervisor;

const DEFAULT_CONFIG_PATH: &str = "maestro.jsonc";
const DEFAULT_CLUSTER_CONFIG_PATH: &str = "maestro.cluster.jsonc";
const DEFAULT_SERVICE_CONFIG_PATH: &str = "maestro.service.jsonc";
const DEFAULT_API_PORT: u16 = 3001;

#[derive(Debug, Parser)]
#[command(name = "maestro", disable_help_subcommand = true)]
struct Cli {
    #[command(subcommand)]
    command: Option<CliCommand>,
}

#[derive(Debug, Subcommand)]
enum CliCommand {
    /// Manage local Maestro config files
    Config {
        #[command(subcommand)]
        command: ConfigCommand,
    },
    /// Manage Maestro daemons running on this host (controller, probe, log reader)
    Daemon {
        #[command(subcommand)]
        command: DaemonCommand,
    },
    /// Inspect the active cluster
    Cluster {
        #[command(subcommand)]
        command: ClusterCommand,
    },
    /// Manage Maestro API contexts
    Contexts {
        #[command(subcommand)]
        command: ContextsCommand,
    },
    /// Manage services in the active context
    Services {
        #[command(subcommand)]
        command: ServicesCommand,
    },
    /// Stream logs from the active context
    Logs(cli::logs::RemoteLogsArgs),
}

#[derive(Debug, Subcommand)]
enum ConfigCommand {
    /// Create a starter config file (interactive — choose cluster or services)
    Init,
    /// Validate a local config file (auto-detects cluster vs services)
    Validate {
        #[arg(help = "Path to the config file to validate")]
        path: PathBuf,
    },
}

#[derive(Debug, Subcommand)]
enum ServicesCommand {
    /// List services in the active context
    Ls,
    /// Deploy services from the config file (dry run by default)
    Rollout {
        #[arg(
            long = "config",
            help = "Path to maestro.cluster.jsonc (default: maestro.cluster.jsonc)"
        )]
        config: Option<PathBuf>,
        #[arg(
            long = "apply",
            help = "Apply the rollout (without this flag, only shows a diff)"
        )]
        apply: bool,
        #[arg(long = "force", help = "Force rollout even if deploy is frozen")]
        force: bool,
        #[arg(
            long = "service",
            help = "Only deploy the named service(s). Can be repeated. Default: all services in the config."
        )]
        services: Vec<String>,
        #[arg(
            short = 'y',
            long = "yes",
            help = "Skip the cluster-confirmation prompt"
        )]
        yes: bool,
    },
    /// Trigger a redeployment of a running service
    Redeploy {
        #[arg(help = "Service ID to redeploy")]
        service_id: String,
    },
    /// Cancel a queued or building deployment
    Cancel {
        #[arg(help = "Service ID")]
        service_id: String,
        #[arg(help = "Deployment ID to cancel")]
        deployment_id: String,
    },
    /// Deploy a service from a local context (tarball uploaded to the cluster)
    Up {
        #[arg(
            long = "config",
            help = "Path to maestro.service.jsonc (default: maestro.service.jsonc)"
        )]
        config: Option<PathBuf>,
        #[arg(
            long = "context",
            help = "Path to the build context directory (default: current directory)"
        )]
        context: Option<PathBuf>,
    },
}

#[derive(Debug, Subcommand)]
enum ClusterCommand {
    /// Show information about the active cluster
    Info,
    /// Show the controller's effective config (secrets are masked)
    Config,
    /// Restart the maestro controller (stops all containers and restarts the process)
    Restart {
        #[arg(
            short = 'y',
            long = "yes",
            help = "Skip the cluster-confirmation prompt"
        )]
        yes: bool,
    },
    /// Upgrade system components
    #[command(after_help = "Example: maestro cluster upgrade system")]
    Upgrade {
        #[command(subcommand)]
        target: UpgradeTarget,
        #[arg(
            short = 'y',
            long = "yes",
            help = "Skip the cluster-confirmation prompt"
        )]
        yes: bool,
    },
}

#[derive(Debug, Subcommand)]
enum ContextsCommand {
    /// Add or update a context
    Set(ContextSetArgs),
    /// Set the active context
    Use {
        #[arg(help = "Context name")]
        name: Option<String>,
    },
    /// List configured contexts
    Ls,
    /// Remove a context
    Remove {
        #[arg(help = "Context name")]
        name: String,
    },
    /// Generate and save a JWT auth token for the active context
    Login {
        #[arg(long = "days", help = "Token lifetime in days (default: 7)")]
        days: Option<u64>,
    },
}

#[derive(Debug, Args)]
struct ContextSetArgs {
    #[arg(help = "Context name")]
    name: Option<String>,
    #[arg(help = "Maestro API host for this context")]
    host: Option<String>,
}

#[derive(Debug, Subcommand)]
enum DaemonCommand {
    /// Start the cluster controller and all system services
    Start(StartArgs),
    /// Read logs from the local log store
    Logs(LogsArgs),
    /// Run the health probe server (used internally by the probe container)
    Probe(ProbeArgs),
}

#[derive(Debug, Args)]
struct StartArgs {
    #[arg(
        long = "config",
        help = "Config source: file path, file://path, or aws-secret://secret-name"
    )]
    config: Option<String>,
    #[arg(
        long = "cluster-name",
        help = "Human-readable cluster name (maestro adds a random -xxxx suffix for canonical DNS)"
    )]
    cluster_name: Option<String>,
    #[arg(long = "etcd-port", help = "Host port for etcd (random if not set)")]
    etcd_port: Option<u16>,
    #[arg(
        long = "admin-port",
        help = "Host port for maestro controller (not exposed if not set)"
    )]
    admin_port: Option<u16>,
    #[arg(
        long = "ingress-port",
        help = "Host port(s) for ingress (can be repeated)"
    )]
    ingress_port: Vec<u16>,
    #[arg(long = "data-dir", help = "Directory for etcd data, logs, and state")]
    data_dir: PathBuf,
    #[arg(
        long = "network",
        help = "Container network name (default: maestro-{clustername-xxxx})"
    )]
    network: Option<String>,
    #[arg(
        long = "subnet",
        help = "Container network subnet CIDR (e.g., 172.22.0.0/16)"
    )]
    subnet: Option<String>,
    #[arg(
        long = "egress-deny",
        value_name = "CIDR",
        help = "Deny container egress to an IP/CIDR (can be repeated)"
    )]
    egress_deny: Vec<String>,
    #[arg(
        long = "enable-tailscale",
        help = "Enable Tailscale subnet routing and DNS"
    )]
    enable_tailscale: bool,
    #[arg(
        long = "tailscale-auth-key",
        env = "TS_AUTHKEY",
        help = "Tailscale auth key"
    )]
    tailscale_authkey: Option<String>,
    #[arg(
        long = "encryption-key",
        env = "MAESTRO_ENCRYPTION_KEY",
        help = "Master key for encrypting secrets"
    )]
    encryption_key: Option<String>,
    #[arg(
        long = "jwt-secret-key",
        env = "MAESTRO_JWT_SECRET_KEY",
        help = "Secret for signing JWT auth tokens (enables rollout authentication)"
    )]
    jwt_secret_key: Option<String>,
    #[arg(
        long = "tag",
        help = "Tags for log sinks like Datadog (key:value, can be repeated)"
    )]
    tags: Vec<String>,
    #[arg(
        long = "datadog-api-key",
        env = "DATADOG_API_KEY",
        help = "Datadog API key for log forwarding"
    )]
    dd_api_key: Option<String>,
    #[arg(
        long = "datadog-site",
        help = "Datadog site (e.g. datadoghq.com, us3.datadoghq.com, datadoghq.eu)"
    )]
    dd_site: Option<String>,
    #[arg(
        long = "datadog-no-ingress-logs",
        help = "Exclude maestro-ingress (Traefik) logs from Datadog (included by default)"
    )]
    dd_no_ingress_logs: bool,
    #[arg(
        long = "datadog-no-tailscale-logs",
        help = "Exclude maestro-tailscale logs from Datadog (included by default)"
    )]
    dd_no_tailscale_logs: bool,
    #[arg(
        long = "cloudflare-tunnel-token",
        env = "CLOUDFLARE_TUNNEL_TOKEN",
        help = "Cloudflare Tunnel token; enables maestro-cloudflared"
    )]
    cloudflare_tunnel_token: Option<String>,
    #[arg(long = "system", help = "Host system type for upgrades (e.g., nixos)")]
    system: Option<config::SystemType>,
    #[arg(long = "runtime", help = "Container runtime: docker or nerdctl")]
    runtime: Option<config::RuntimeType>,
    #[arg(long = "force", help = "Force recreate network if it conflicts")]
    force: bool,
    #[arg(
        long = "disable-etcd-cert",
        help = "Disable mTLS for etcd (insecure, for development only)"
    )]
    disable_etcd_cert: bool,
    #[arg(long = "project-dir", help = "Path to the maestro project directory")]
    project_dir: PathBuf,
    #[arg(
        long = "node-role",
        help = "Role for this node: controller, worker, or both (default: both)"
    )]
    node_role: Option<NodeRole>,
    #[arg(
        long = "cluster-bootstrap",
        help = "Cluster bootstrap mode: single or new-cluster. Auto-inferred from local data dir + peer probe if omitted (join-existing is always auto-detected)."
    )]
    cluster_bootstrap: Option<String>,
    #[arg(
        long = "cluster-peer",
        help = "Peer host (or host:port to override --etcd-peer-port). Can be repeated."
    )]
    cluster_peers: Vec<String>,
    #[arg(
        long = "etcd-peer-port",
        help = "Peer port for inter-etcd communication (default 2380; can also be set via cluster.etcd-peer-port in the config file)"
    )]
    etcd_peer_port: Option<u16>,
    #[arg(
        long = "advertise-host",
        help = "Hostname/IP other nodes use to reach this node's etcd peer port. Auto-detected from local NICs if --cluster-peer entries are IPs."
    )]
    advertise_host: Option<String>,
    #[arg(
        long = "enable-cluster-scheduling",
        help = "Enable cluster-aware replica scheduling (workloads spread across nodes)"
    )]
    enable_cluster_scheduling: bool,
    #[arg(
        long = "shared-registry",
        help = "Container registry shared by all nodes (e.g. registry.internal:5000). Required for multi-node deployments."
    )]
    shared_registry: Option<String>,
}

#[derive(Debug, Args)]
struct LogsArgs {
    #[arg(
        long = "source",
        help = "Source name (e.g., service name). Shows all sources if omitted"
    )]
    source: Option<String>,
    #[arg(long = "data-dir", help = "Maestro data directory")]
    data_dir: PathBuf,
    #[arg(
        long = "cluster-name",
        help = "Cluster name (read from maestro.jsonc if omitted)"
    )]
    cluster_name: Option<String>,
    #[arg(
        long = "tail",
        default_value_t = 100,
        help = "Number of recent entries"
    )]
    tail: usize,
    #[arg(long = "follow", short = 'f', help = "Follow log output")]
    follow: bool,
}

#[derive(Debug, Args)]
struct ProbeArgs {
    #[arg(
        long = "etcd-endpoint",
        env = "ETCD_ENDPOINT",
        default_value = "http://127.0.0.1:6401",
        help = "etcd endpoint URL"
    )]
    etcd_endpoint: String,
    #[arg(
        long = "port",
        env = "PORT",
        default_value_t = DEFAULT_API_PORT,
        help = "Port to listen on"
    )]
    port: u16,
}

#[derive(Debug, Subcommand)]
enum UpgradeTarget {
    /// Upgrade the host operating system
    System,
    /// Rolling-upgrade the entire cluster (workers first, leader last)
    Cluster {
        #[arg(long = "version", help = "Target version (defaults to 'latest')")]
        version: Option<String>,
    },
}

#[tokio::main]
async fn main() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    match run().await {
        Ok(true) => restart_self(),
        Ok(false) => {}
        Err(err) => {
            eprintln!("{err}");
            std::process::exit(2);
        }
    }
}

async fn run() -> crate::error::Result<bool> {
    let cli = match Cli::try_parse() {
        Ok(cli) => cli,
        Err(err)
            if matches!(
                err.kind(),
                ErrorKind::DisplayHelp | ErrorKind::DisplayVersion
            ) =>
        {
            err.print().map_err(|print_err| {
                Error::internal(format!("failed to print help: {print_err}"))
            })?;
            return Ok(false);
        }
        Err(err) => return Err(Error::invalid_input(err.to_string())),
    };

    match cli.command {
        None => {
            print!("{}", help_text());
            Ok(false)
        }
        Some(CliCommand::Daemon {
            command:
                DaemonCommand::Start(StartArgs {
                    config,
                    cluster_name,
                    admin_port,
                    ingress_port,
                    etcd_port,
                    data_dir,
                    network,
                    subnet,
                    egress_deny,
                    enable_tailscale,
                    tailscale_authkey,
                    encryption_key,
                    jwt_secret_key,
                    tags,
                    dd_api_key,
                    dd_site,
                    dd_no_ingress_logs,
                    dd_no_tailscale_logs,
                    cloudflare_tunnel_token,
                    system,
                    runtime: runtime_flag,
                    force,
                    disable_etcd_cert,
                    project_dir,
                    node_role,
                    cluster_bootstrap,
                    cluster_peers,
                    etcd_peer_port,
                    advertise_host,
                    enable_cluster_scheduling,
                    shared_registry,
                }),
        }) => {
            let explicit_datadog_site = dd_site
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(ToString::to_string);

            if config.is_none() && encryption_key.is_none() {
                return Err(Error::invalid_input("--encryption-key is required"));
            }
            let mut cfg = match config {
                Some(source) => config::load_config(&source)
                    .await
                    .map_err(|err| Error::invalid_config(err.to_string()))?,
                None => config::StartConfig {
                    cluster: config::ClusterConfig {
                        name: cluster_name
                            .ok_or_else(|| Error::invalid_input("--cluster-name is required"))?,
                        peers: Vec::new(),
                        bootstrap: None,
                        etcd_peer_port: None,
                        advertise_host: None,
                        scheduling_enabled: false,
                        shared_registry: None,
                        node_role: None,
                    },
                    ingress: config::IngressConfig {
                        port: None,
                        ports: {
                            if ingress_port.is_empty() {
                                return Err(Error::invalid_input("--ingress-port is required"));
                            }
                            ingress_port.clone()
                        },
                    },
                    subnet: None,
                    egress: Default::default(),
                    encryption_key: String::new(),
                    jwt_secret_key: None,
                    tailscale: None,
                    tags: tags.clone(),
                    datadog: None,
                    system: None,
                    runtime: Default::default(),
                    depot: Default::default(),
                    cloudflare: None,
                    slack: None,
                    disable_etcd_cert: false,
                    allow_cli_deployment: false,
                },
            };

            if let Some(key) = encryption_key {
                cfg.encryption_key = key;
            }
            if let Some(secret) = jwt_secret_key {
                cfg.jwt_secret_key = Some(secret);
            }
            if let Some(authkey) = tailscale_authkey {
                cfg.tailscale = Some(config::TailscaleConfig { auth_key: authkey });
            }
            if let Some(api_key) = dd_api_key {
                let dd = cfg.datadog.get_or_insert(config::DatadogConfig {
                    api_key: String::new(),
                    site: None,
                    include_ingress_logs: true,
                    include_tailscale_logs: true,
                    include_metrics: false,
                });
                dd.api_key = api_key;
            }
            if let Some(token) = cloudflare_tunnel_token {
                cfg.cloudflare = Some(config::CloudflareConfig {
                    tunnel: config::CloudflareTunnelConfig {
                        token: SecretString::new(token),
                        replicas: None,
                    },
                });
            }
            if let Some(dd) = cfg.datadog.as_mut() {
                if let Some(site) = explicit_datadog_site.clone() {
                    dd.site = Some(site);
                }
                if dd_no_ingress_logs {
                    dd.include_ingress_logs = false;
                }
                if dd_no_tailscale_logs {
                    dd.include_tailscale_logs = false;
                }
            }
            if subnet.is_some() {
                cfg.subnet = subnet;
            }
            if !egress_deny.is_empty() {
                cfg.egress.deny.extend(egress_deny.clone());
            }
            if let Some(runtime) = runtime_flag {
                cfg.runtime = runtime;
            }
            if let Some(sys) = system {
                cfg.system = Some(sys);
            }
            if disable_etcd_cert {
                cfg.disable_etcd_cert = true;
            }

            let datadog_site = cfg
                .datadog
                .as_ref()
                .and_then(|dd| dd.site.clone())
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty());

            let enable_tailscale = enable_tailscale || cfg.tailscale.is_some();
            if enable_tailscale && cfg.tailscale.is_none() {
                return Err(Error::invalid_input(
                    "tailscale requires --tailscale-auth-key, TS_AUTHKEY env var, or tailscale.auth-key in config",
                ));
            }
            let subnet = cfg.subnet.clone().ok_or_else(|| {
                Error::invalid_input("--subnet is required (format: 172.22.0.0/16)")
            })?;
            validate_subnet_cidr(&subnet)?;
            cfg.subnet = Some(subnet);
            let egress_deny = firewall::normalize_denies(&cfg.egress.deny)?;

            let maestro_config = serde_json::to_string(&cfg.masked()).map_err(|err| {
                Error::internal(format!("failed to serialize masked config: {err}"))
            })?;

            let (signal_tx, signal_task) = spawn_shutdown_signal_bus()?;
            let cluster_alias = cfg.cluster.name.to_lowercase();
            let data_dir = data_dir.join(&cluster_alias);
            std::fs::create_dir_all(&data_dir).map_err(|err| {
                Error::internal(format!(
                    "failed to create data directory {}: {err}",
                    data_dir.display()
                ))
            })?;
            verify_encryption_key(&data_dir, &cfg.encryption_key)?;
            let _lock = acquire_lock(&data_dir)?;
            let cluster_suffix = load_or_create_cluster_suffix(&data_dir)?;
            let cluster_name = format!("{cluster_alias}-{cluster_suffix}");
            let etcd_port = etcd_port.unwrap_or_else(|| {
                let listener = std::net::TcpListener::bind("127.0.0.1:0")
                    .expect("failed to bind to random port for etcd");
                listener
                    .local_addr()
                    .expect("failed to get local addr")
                    .port()
            });
            let etcd_scheme = if disable_etcd_cert { "http" } else { "https" };
            let etcd_endpoint = format!("{etcd_scheme}://127.0.0.1:{}", etcd_port);
            let network = network.unwrap_or_else(|| format!("maestro-{cluster_alias}"));

            let project_dir = std::fs::canonicalize(&project_dir).unwrap_or_else(|_| {
                std::env::current_dir()
                    .expect("failed to get current dir")
                    .join(&project_dir)
            });
            let tailscale_authkey = if enable_tailscale {
                cfg.tailscale.map(|t| t.auth_key)
            } else {
                None
            };
            let runtime_type = cfg.runtime;
            let runtime = runtime::create_provider(runtime_type);
            let build_command_env = cfg
                .depot
                .and_then(|depot| depot.token)
                .map(|token| {
                    let mut env = std::collections::HashMap::new();
                    env.insert("DEPOT_TOKEN".to_string(), token);
                    env
                })
                .unwrap_or_default();

            let log_store = Arc::new(
                logs::LogStore::open(&data_dir.join("logs/logs.db"))
                    .map_err(|err| Error::internal(format!("failed to open log store: {err}")))?,
            );
            let (log_collector, log_sender) = logs::LogCollector::new(log_store.clone());
            let collector_handle = log_collector.spawn();

            let tailscale_status = if enable_tailscale {
                "pending peer conflict check"
            } else {
                "inactive (tailscale disabled)"
            };
            let logger = logs::Logger::new(Some(log_sender.clone()));
            logger.emit("info", &format!("etcd endpoint {etcd_endpoint}"));
            logger.emit("info", &format!("container network {network}"));
            logger.emit(
                "info",
                &format!("canonical domain: {cluster_name}.maestro.internal (active)"),
            );
            logger.emit(
                "info",
                &format!("alias domain: {cluster_alias}.maestro.internal ({tailscale_status})"),
            );
            logger.emit("info", &format!("container runtime: {runtime_type}"));

            let mut background_handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();

            let enable_ingress_access_logs = cfg
                .datadog
                .as_ref()
                .map(|dd| dd.include_ingress_logs)
                .unwrap_or(false);

            let mut metrics_datadog_tx: Option<flume::Sender<metrics::MetricBatch>> = None;
            if let Some(dd) = cfg.datadog {
                if let Some(site) = datadog_site {
                    let include_metrics = dd.include_metrics;
                    let metrics_api_key = dd.api_key.clone();
                    let dd_sink = logs::DatadogSink::new(
                        dd.api_key,
                        &site,
                        dd.include_ingress_logs,
                        dd.include_tailscale_logs,
                    );
                    let dd_worker = logs::SinkWorker::new(
                        log_store.clone(),
                        Box::new(dd_sink),
                        signal_tx.subscribe(),
                    );
                    background_handles.push(dd_worker.spawn());
                    logger.emit("info", &format!("datadog log sink enabled (site: {site})"));

                    if include_metrics {
                        let (metrics_tx, metrics_rx) = flume::bounded(1024);
                        let metrics_sink = metrics::datadog::DatadogMetricsSink::new(
                            &site,
                            SecretString::new(metrics_api_key),
                            metrics_rx,
                            cluster_name.clone(),
                            parse_tags(cfg.tags.clone()).unwrap_or_default(),
                            signal_tx.subscribe(),
                            logger.clone(),
                        );
                        background_handles.push(metrics_sink.spawn());
                        metrics_datadog_tx = Some(metrics_tx);
                        logger.emit(
                            "info",
                            &format!("datadog metrics sink enabled (site: {site})"),
                        );
                    } else {
                        logger.emit(
                            "info",
                            "datadog metrics disabled; set datadog.include-metrics=true to enable",
                        );
                    }
                } else {
                    logger.emit("warn", "datadog config present but sink is disabled; set datadog.site in config or pass --datadog-site");
                }
            }

            let node_id = node_id::load_or_create(&data_dir).map_err(|err| {
                Error::internal(format!("failed to load or create node-id: {err}"))
            })?;

            // CLI flags override config-file values; config provides defaults.
            // Fields here all live under `cluster.*` in the config (so the
            // operator can set them via --config aws-secret://...).
            let effective_peer_specs: Vec<String> = if cluster_peers.is_empty() {
                cfg.cluster.peers.clone()
            } else {
                cluster_peers.clone()
            };
            let effective_cluster_bootstrap = cluster_bootstrap
                .clone()
                .or_else(|| cfg.cluster.bootstrap.clone());
            let effective_etcd_peer_port = etcd_peer_port
                .or(cfg.cluster.etcd_peer_port)
                .unwrap_or(2380);
            let effective_advertise_host = advertise_host
                .clone()
                .or_else(|| cfg.cluster.advertise_host.clone());
            let effective_scheduling_enabled =
                enable_cluster_scheduling || cfg.cluster.scheduling_enabled;
            let effective_shared_registry = shared_registry
                .clone()
                .or_else(|| cfg.cluster.shared_registry.clone());
            let effective_node_role = match node_role {
                Some(role) => role,
                None => match cfg.cluster.node_role.as_deref() {
                    Some(raw) => raw.parse::<NodeRole>().map_err(|err| {
                        Error::invalid_input(format!("invalid cluster.node-role in config: {err}"))
                    })?,
                    None => NodeRole::default(),
                },
            };
            logger.emit(
                "info",
                &format!("node-id {node_id} role {effective_node_role}"),
            );

            let bootstrap_peers: Vec<ClusterPeer> = effective_peer_specs
                .iter()
                .map(|spec| {
                    ClusterPeer::parse(spec).ok_or_else(|| {
                        Error::invalid_input(format!(
                            "invalid --cluster-peer `{spec}` (expected host or host:port)"
                        ))
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            let resolved_advertise_host =
                resolve_advertise_host(&bootstrap_peers, effective_advertise_host.as_deref())?;
            if let Some(host) = resolved_advertise_host.as_deref() {
                logger.emit("info", &format!("advertise-host resolved to {host}"));
            }
            let bootstrap_mode = match effective_cluster_bootstrap.as_deref() {
                Some("single") => ClusterBootstrapMode::Single,
                Some("new-cluster") => ClusterBootstrapMode::NewCluster,
                Some(other) => {
                    return Err(Error::invalid_input(format!(
                        "invalid cluster bootstrap value `{other}` (expected single or new-cluster; join-existing is auto-inferred)"
                    )));
                }
                None => {
                    let action = determine_bootstrap_action(
                        &data_dir,
                        &bootstrap_peers,
                        resolved_advertise_host.as_deref(),
                        effective_etcd_peer_port,
                    )?;
                    logger.emit("info", &format!("bootstrap action: {action:?}"));
                    match action {
                        BootstrapAction::Restart => ClusterBootstrapMode::JoinExisting,
                        BootstrapAction::SingleNode => ClusterBootstrapMode::Single,
                        BootstrapAction::BecomePrimary => ClusterBootstrapMode::NewCluster,
                        BootstrapAction::JoinAsSecondary { primary_address } => {
                            logger.emit(
                                "info",
                                &format!(
                                    "waiting for bootstrap primary at {primary_address} (timeout: {}s)",
                                    WAIT_FOR_PRIMARY_TIMEOUT.as_secs()
                                ),
                            );
                            wait_for_primary(&primary_address).await?;
                            logger.emit(
                                "info",
                                &format!("bootstrap primary {primary_address} is up; joining"),
                            );
                            ClusterBootstrapMode::JoinExisting
                        }
                    }
                }
            };
            let multi_node = bootstrap_mode != ClusterBootstrapMode::Single;
            if multi_node && bootstrap_peers.is_empty() {
                return Err(Error::invalid_input(
                    "multi-node bootstrap requires at least one --cluster-peer",
                ));
            }
            if multi_node && cfg.disable_etcd_cert {
                eprintln!(
                    "[maestro]: WARNING: --disable-etcd-cert was set but multi-node clusters \
                     require etcd mTLS for safety; enabling certs anyway"
                );
                cfg.disable_etcd_cert = false;
            }
            // Shadow the CLI-derived flag so downstream etcd-scheme + TLS
            // logic uses the (possibly overridden) effective value.
            let disable_etcd_cert = cfg.disable_etcd_cert;
            if multi_node && effective_scheduling_enabled && effective_shared_registry.is_none() {
                return Err(Error::invalid_input(
                    "multi-node cluster scheduling requires shared-registry (CLI --shared-registry or cluster.shared-registry in config)",
                ));
            }
            let cluster_bootstrap_config = ClusterBootstrapBuilder::default()
                .mode(bootstrap_mode)
                .peers(bootstrap_peers)
                .etcd_peer_port(effective_etcd_peer_port)
                .scheduling_enabled(effective_scheduling_enabled)
                .advertise_host(resolved_advertise_host)
                .shared_registry(effective_shared_registry)
                .build()
                .expect("ClusterBootstrap builder defaults are complete");

            let mut deployment_config = ControllerConfig {
                cluster_alias,
                cluster_name,
                node_id,
                node_role: effective_node_role,
                data_dir,
                etcd_port,
                probe_port: None,
                admin_port,
                ingress_ports: {
                    let mut ports = cfg.ingress.resolved_ports();
                    if !ingress_port.is_empty() {
                        ports = ingress_port;
                    }
                    if ports.is_empty() {
                        return Err(Error::invalid_input(
                            "ingress port is required (set ingress.port or ingress.ports in config, or pass --ingress-port)",
                        ));
                    }
                    ports
                },
                project_dir,
                network,
                subnet: cfg.subnet,
                tailscale_authkey,
                encryption_key: SecretString::new(cfg.encryption_key),
                jwt_secret_key: cfg.jwt_secret_key,
                build_command_env,
                tags: parse_tags(cfg.tags)?,
                system_type: cfg.system,
                force,
                disable_etcd_cert: cfg.disable_etcd_cert,
                enable_ingress_access_logs,
                maestro_config,
                cloudflare_tunnel_replicas: cfg
                    .cloudflare
                    .as_ref()
                    .and_then(|cf| cf.tunnel.replicas)
                    .unwrap_or(2)
                    .max(1),
                cloudflare_tunnel_token: cfg.cloudflare.map(|cf| cf.tunnel.token),
                slack_webhook_url: cfg.slack.map(|sl| sl.webhook_url),
                cluster_bootstrap: cluster_bootstrap_config,
            };

            let probe_host_port = deployment_config.probe_port.unwrap_or_else(|| {
                let listener =
                    std::net::TcpListener::bind("0.0.0.0:0").expect("failed to bind probe port");
                listener
                    .local_addr()
                    .expect("failed to get local addr")
                    .port()
            });
            deployment_config.probe_port = Some(probe_host_port);

            let firewall_config = firewall::FirewallConfig {
                subnet: deployment_config.subnet.clone(),
                deny: egress_deny,
            };
            firewall::apply(&firewall_config).await.map_err(|err| {
                Error::external(format!("failed to apply egress firewall: {err}"))
            })?;
            if !firewall_config.deny.is_empty() {
                logger.emit(
                    "info",
                    &format!(
                        "egress firewall enabled ({} denied CIDRs)",
                        firewall_config.deny.len()
                    ),
                );
                background_handles.push(firewall::spawn_reconciler(
                    firewall_config,
                    logger.clone(),
                    signal_tx.subscribe(),
                ));
            }

            let mut supervisor = JobSupervisor::new();
            let mut startup_shutdown_rx = signal_tx.subscribe();
            let system_info = tokio::select! {
                info = deployment::start_system_jobs(
                    &deployment_config,
                    &runtime,
                    &log_sender,
                    &logger,
                    &mut supervisor,
                ) => info,
                _ = startup_shutdown_rx.recv() => {
                    eprintln!("[maestro]: shutdown requested during startup");
                    supervisor.shutdown_all(supervisor::ShutdownRequest::Force).await;
                    return Ok(false);
                }
            };

            let derived_key = derive_key(deployment_config.encryption_key.as_str());
            let etcd_tls = if disable_etcd_cert {
                None
            } else {
                let certs_dir = deployment_config.certs_dir();
                deployment::build_etcd_tls_from_files(
                    &certs_dir.join("ca.pem").to_string_lossy(),
                    &certs_dir.join("client.pem").to_string_lossy(),
                    &certs_dir.join("client-key.pem").to_string_lossy(),
                )
            };
            let store: Arc<dyn deployment::store::ClusterStore> = Arc::new(
                deployment::etcd::EtcdStateStore::new(
                    &etcd_endpoint,
                    derived_key,
                    etcd_tls.clone(),
                )
                .await?,
            );

            let cluster_client = {
                let connect_options =
                    etcd_tls.map(|tls| etcd_client::ConnectOptions::new().with_tls(tls));
                let raw = etcd_client::Client::connect([&etcd_endpoint], connect_options)
                    .await
                    .map_err(|err| {
                        Error::external(format!("failed to connect to etcd for cluster: {err}"))
                    })?;
                Arc::new(tokio::sync::Mutex::new(raw))
            };
            let hostname = std::env::var("HOSTNAME")
                .or_else(|_| std::env::var("MAESTRO_HOSTNAME"))
                .unwrap_or_else(|_| "maestro-node".to_string());
            let node_info = node_info_from(
                deployment_config.node_id.clone(),
                hostname,
                deployment_config.node_role,
                probe_host_port,
                None,
                env!("CARGO_PKG_VERSION").to_string(),
                utils::time::current_time_millis().unwrap_or(0),
            );
            let cluster_registry: Arc<dyn NodeRegistry> = Arc::new(EtcdNodeRegistry::new(
                cluster_client.clone(),
                deployment_config.node_id.clone(),
            ));
            let cluster_elector_inner = Arc::new(EtcdLeaderElector::new(
                cluster_client.clone(),
                deployment_config.node_id.clone(),
            ));
            cluster_elector_inner.spawn_observer().await;
            let cluster_elector: Arc<dyn LeaderElector> = cluster_elector_inner.clone();
            let cluster_service =
                ClusterService::new(node_info, cluster_registry.clone(), cluster_elector.clone());
            let cluster_lifecycle_handles = cluster_service.clone().spawn(signal_tx.subscribe());

            let cluster_metrics = ClusterMetrics::new();
            let metrics_publisher_handle = cluster_metrics.spawn_publisher(
                cluster_client.clone(),
                deployment_config.node_id.clone(),
                std::time::Duration::from_secs(15),
                signal_tx.subscribe(),
            );
            background_handles.push(metrics_publisher_handle);
            let disk_publisher_handle = cluster::disk_snapshot::spawn_publisher(
                cluster_client.clone(),
                deployment_config.node_id.clone(),
                std::time::Duration::from_secs(30),
                signal_tx.subscribe(),
            );
            background_handles.push(disk_publisher_handle);
            let mut leader_loop_handle: Option<tokio::task::JoinHandle<()>> = None;
            let mut reconciler_handle: Option<tokio::task::JoinHandle<()>> = None;
            if deployment_config.cluster_bootstrap.scheduling_enabled {
                let dns_writer = Arc::new(ClusterDnsWriter::new(
                    system_info.dns_manager.clone(),
                    format!("{}.maestro.internal", deployment_config.cluster_name),
                ));
                let leader_loop = LeaderLoop {
                    catalog: Arc::new(StoreServiceCatalog::new(store.clone())),
                    registry: cluster_registry.clone(),
                    assignments: Arc::new(EtcdAssignmentStore::new(cluster_client.clone())),
                    port_allocator: Arc::new(EtcdPortAllocator::new(cluster_client.clone())),
                    scheduler: Arc::new(DefaultScheduler::new()),
                    traefik_sink: Arc::new(EtcdTraefikSink::new(cluster_client.clone())),
                    elector: cluster_elector.clone(),
                    tick_interval: std::time::Duration::from_secs(5),
                    default_entry_point: "web".to_string(),
                    on_plan_applied: Some(dns_writer as Arc<dyn PlanObserver>),
                    metrics: Some(cluster_metrics.clone()),
                };
                leader_loop_handle = Some(tokio::spawn(async move {
                    leader_loop.run_while_leader().await;
                }));
            }

            let probe_log_endpoint = format!("http://127.0.0.1:{probe_host_port}/api/logs");
            let http_sink = logs::HttpSink::new("controller", &probe_log_endpoint);
            let sink_worker = logs::SinkWorker::new(
                log_store.clone(),
                Box::new(http_sink),
                signal_tx.subscribe(),
            );
            background_handles.push(sink_worker.spawn());
            let deployment_signal_rx = signal_tx.subscribe();
            let watcher_signal_rx = signal_tx.subscribe();

            let watcher = builder::BuildWatcher::new(
                store.clone(),
                deployment_config.data_dir.clone(),
                watcher_signal_rx,
                logger.clone(),
            );
            let watcher_handle = tokio::spawn(watcher.run());

            let metrics_signal_rx = signal_tx.subscribe();
            let metrics_endpoint = format!("http://127.0.0.1:{probe_host_port}/api/metrics");
            let metrics_collector = metrics::MetricsCollector::new(
                metrics_endpoint,
                deployment_config.cluster_name.clone(),
                runtime.cli_name().to_string(),
                metrics_signal_rx,
                logger,
                metrics_datadog_tx,
            );
            let metrics_handle = tokio::spawn(metrics_collector.run());

            let scheduling_enabled = deployment_config.cluster_bootstrap.scheduling_enabled;
            let lookup_params = ReconcilerLookupParams {
                node_id: deployment_config.node_id.clone(),
                cluster_name: deployment_config.cluster_name.clone(),
                tags: deployment_config.tags.clone(),
                runtime_cli: runtime.cli_name().to_string(),
            };

            let mut controller = DeploymentController::new(
                deployment_config,
                store.clone(),
                supervisor,
                deployment_signal_rx,
                Some(log_sender.clone()),
                runtime,
                Some(system_info.dns_manager),
                system_info.nameserver_ip,
            );

            if scheduling_enabled {
                let assignment_store_concrete =
                    Arc::new(EtcdAssignmentStore::new(cluster_client.clone()));
                let watch_rx = assignment_store_concrete
                    .watch_for_node(&lookup_params.node_id)
                    .await;
                let assignment_store: Arc<dyn AssignmentStore> = assignment_store_concrete;
                let engine = controller.engine();
                let lookup = Arc::new(
                    StoreDeploymentLookupBuilder::default()
                        .store(store.clone())
                        .engine(engine.clone())
                        .runtime_cli(lookup_params.runtime_cli)
                        .cluster_name(lookup_params.cluster_name)
                        .log_sender(Some(log_sender.clone()))
                        .config_tags(lookup_params.tags)
                        .build()
                        .expect("StoreDeploymentLookup required fields satisfied"),
                ) as Arc<dyn DeploymentLookup>;
                let executor = Arc::new(
                    EngineReplicaExecutorBuilder::default()
                        .engine(engine)
                        .lookup(lookup)
                        .store(Some(store.clone()))
                        .runtime(Some(controller.runtime()))
                        .build()
                        .expect("EngineReplicaExecutor required fields satisfied"),
                );
                let reconciler = AssignmentReconciler {
                    node_id: lookup_params.node_id,
                    store: assignment_store,
                    executor,
                    poll_interval: std::time::Duration::from_secs(5),
                };
                let mut reconciler_shutdown = signal_tx.subscribe();
                let mut watch_rx = watch_rx;
                reconciler_handle = Some(tokio::spawn(async move {
                    let mut last_seen = Vec::new();
                    let mut ticker = tokio::time::interval(reconciler.poll_interval);
                    loop {
                        tokio::select! {
                            _ = reconciler_shutdown.recv() => break,
                            _ = ticker.tick() => {
                                if let Err(err) = reconciler.reconcile_once(&mut last_seen).await {
                                    eprintln!("[maestro]: reconciler error: {err}");
                                }
                            }
                            Some(_) = watch_rx.recv() => {
                                if let Err(err) = reconciler.reconcile_once(&mut last_seen).await {
                                    eprintln!("[maestro]: reconciler (watch) error: {err}");
                                }
                            }
                        }
                    }
                }));
                eprintln!(
                    "[maestro]: reconciler spawned (engine-backed) — replica lifecycle owned by cluster scheduler"
                );
            }

            let deployment_shutdown_tx = signal_tx.clone();

            let result = async move {
                let result = controller.run().await.map_err(Into::into);
                let _ = deployment_shutdown_tx.send(signal::ShutdownEvent::Graceful);
                result.map(|exit_reason| (exit_reason, controller))
            }
            .await;

            watcher_handle.abort();
            metrics_handle.abort();
            cluster_lifecycle_handles.abort();
            if let Some(handle) = leader_loop_handle.take() {
                handle.abort();
            }
            if let Some(handle) = reconciler_handle.take() {
                handle.abort();
            }
            for handle in &background_handles {
                handle.abort();
            }

            signal_task.abort();
            match result {
                Ok((exit_reason, controller)) => {
                    let mut supervisor = controller.into_supervisor();
                    supervisor
                        .shutdown_all(supervisor::ShutdownRequest::Force)
                        .await;
                    drop(supervisor);
                    match tokio::time::timeout(std::time::Duration::from_secs(15), collector_handle)
                        .await
                    {
                        Ok(_join_result) => {}
                        Err(_) => {
                            eprintln!(
                                "[maestro]: timed out waiting for log collector shutdown after 15s; continuing"
                            );
                        }
                    }
                    Ok(exit_reason == deployment::controller::ControllerExitReason::Restart)
                }
                Err(err) => Err(err),
            }
        }
        Some(CliCommand::Contexts { command }) => match command {
            ContextsCommand::Set(ContextSetArgs { name, host }) => {
                cli::contexts::set_context(name.as_deref(), host.as_deref())
            }
            ContextsCommand::Use { name } => cli::contexts::use_context(name.as_deref()),
            ContextsCommand::Ls => cli::contexts::list_contexts(),
            ContextsCommand::Remove { name } => cli::contexts::remove_context(&name),
            ContextsCommand::Login { days } => cli::auth::run_auth(days),
        }
        .map(|()| false),
        Some(CliCommand::Services { command }) => {
            let host = cli::contexts::active_host()?;
            match command {
                ServicesCommand::Rollout {
                    config,
                    apply,
                    force,
                    services,
                    yes,
                } => {
                    let config_path =
                        config.unwrap_or_else(|| PathBuf::from(DEFAULT_CLUSTER_CONFIG_PATH));
                    cli::rollout::run_rollout(&config_path, &host, apply, force, &services, yes)
                        .await
                        .map(|()| false)
                }
                ServicesCommand::Ls => cli::services::run_services(&host).await.map(|()| false),
                ServicesCommand::Redeploy { service_id } => {
                    cli::redeploy::run_redeploy(&host, &service_id)
                        .await
                        .map(|()| false)
                }
                ServicesCommand::Cancel {
                    service_id,
                    deployment_id,
                } => cli::cancel::run_cancel(&host, &service_id, &deployment_id)
                    .await
                    .map(|()| false),
                ServicesCommand::Up { config, context } => {
                    let config_path =
                        config.unwrap_or_else(|| PathBuf::from(DEFAULT_SERVICE_CONFIG_PATH));
                    let context_dir = context.unwrap_or_else(|| PathBuf::from("."));
                    cli::up::run_up(&host, &config_path, &context_dir)
                        .await
                        .map(|()| false)
                }
            }
        }
        Some(CliCommand::Cluster { command }) => {
            let host = cli::contexts::active_host()?;
            match command {
                ClusterCommand::Info => cli::info::run_info(&host).await.map(|()| false),
                ClusterCommand::Config => cli::config::run_config(&host).await.map(|()| false),
                ClusterCommand::Restart { yes } => {
                    cli::restart::run_restart(&host, yes).await.map(|()| false)
                }
                ClusterCommand::Upgrade { target, yes } => match target {
                    UpgradeTarget::System => cli::upgrade::run_upgrade_system(&host, yes)
                        .await
                        .map(|()| false),
                    UpgradeTarget::Cluster { version } => {
                        cli::upgrade::run_upgrade_cluster(&host, yes, version)
                            .await
                            .map(|()| false)
                    }
                },
            }
        }
        Some(CliCommand::Logs(args)) => {
            let host = cli::contexts::active_host()?;
            cli::logs::run_logs(&host, args).await.map(|()| false)
        }
        Some(CliCommand::Daemon {
            command:
                DaemonCommand::Probe(ProbeArgs {
                    etcd_endpoint,
                    port,
                }),
        }) => probe::run(&etcd_endpoint, port)
            .await
            .map(|()| false)
            .map_err(|err| Error::internal(err.to_string())),
        Some(CliCommand::Daemon {
            command:
                DaemonCommand::Logs(LogsArgs {
                    source,
                    data_dir,
                    cluster_name,
                    tail,
                    follow,
                }),
        }) => {
            let cluster_name = if let Some(name) = cluster_name {
                name
            } else if let Ok(cfg) = config::load_config(DEFAULT_CONFIG_PATH).await {
                cfg.cluster.name.to_lowercase()
            } else {
                return Err(Error::invalid_input(
                    "provide --cluster-name or create maestro.jsonc with cluster.name",
                ));
            };
            let db_path = data_dir.join(&cluster_name).join("logs/logs.db");
            let store = logs::LogStore::open(&db_path)
                .map_err(|err| Error::internal(format!("failed to open log store: {err}")))?;

            let entries = if let Some(src) = &source {
                store.read_tail(src, tail).await
            } else {
                store.read_tail_all(tail).await
            }
            .map_err(|err| Error::internal(format!("failed to read logs: {err}")))?;

            let show_source = source.is_none();
            for entry in &entries {
                print_log_entry(entry, show_source);
            }

            if follow {
                let mut last_seq = entries.last().map(|e| e.seq).unwrap_or(0);
                loop {
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    let new_entries = if let Some(src) = &source {
                        store.read_after_for_source(src, last_seq, 500).await
                    } else {
                        store.read_after_all(last_seq, 500).await
                    }
                    .map_err(|err| Error::internal(format!("failed to read logs: {err}")))?;
                    for entry in &new_entries {
                        print_log_entry(entry, show_source);
                        last_seq = entry.seq;
                    }
                }
            }

            Ok(false)
        }
        Some(CliCommand::Config { command }) => match command {
            ConfigCommand::Init => cli::config::run_init().map(|()| false),
            ConfigCommand::Validate { path } => cli::config::run_validate(&path).map(|()| false),
        },
    }
}

fn help_text() -> String {
    let mut cmd = Cli::command();
    let mut out = Vec::new();
    cmd.write_long_help(&mut out)
        .expect("help rendering should succeed");

    match String::from_utf8(out) {
        Ok(text) => format!("{text}\n"),
        Err(err) => format!("{}\n", String::from_utf8_lossy(err.as_bytes())),
    }
}

fn load_or_create_cluster_suffix(data_dir: &Path) -> crate::error::Result<String> {
    let system_dir = data_dir.join("system");
    std::fs::create_dir_all(&system_dir).map_err(|err| {
        Error::internal(format!(
            "failed to create system directory {}: {err}",
            system_dir.display()
        ))
    })?;

    let suffix_path = system_dir.join("cluster-instance-id");
    if suffix_path.exists() {
        let raw = std::fs::read_to_string(&suffix_path).map_err(|err| {
            Error::internal(format!(
                "failed to read cluster instance id {}: {err}",
                suffix_path.display()
            ))
        })?;
        let suffix = raw.trim().to_lowercase();
        if is_valid_cluster_suffix(&suffix) {
            return Ok(suffix);
        }
        return Err(Error::invalid_config(format!(
            "invalid cluster instance id in {}: expected 4 lowercase letters or digits",
            suffix_path.display()
        )));
    }

    let suffix = utils::nanoid::unique_id(4).to_lowercase();
    if !is_valid_cluster_suffix(&suffix) {
        return Err(Error::internal(
            "generated invalid cluster instance id".to_string(),
        ));
    }

    let tmp_path = suffix_path.with_extension("tmp");
    std::fs::write(&tmp_path, format!("{suffix}\n")).map_err(|err| {
        Error::internal(format!(
            "failed to write cluster instance id temp file {}: {err}",
            tmp_path.display()
        ))
    })?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&tmp_path, std::fs::Permissions::from_mode(0o600));
    }
    std::fs::rename(&tmp_path, &suffix_path).map_err(|err| {
        Error::internal(format!(
            "failed to persist cluster instance id {}: {err}",
            suffix_path.display()
        ))
    })?;

    Ok(suffix)
}

fn is_valid_cluster_suffix(value: &str) -> bool {
    value.len() == 4
        && value
            .chars()
            .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit())
}

fn acquire_lock(data_dir: &Path) -> crate::error::Result<std::fs::File> {
    use std::fs::OpenOptions;
    use std::io::Write;

    let lock_path = data_dir.join(".lock");
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(&lock_path)
        .map_err(|err| Error::internal(format!("failed to open lock file: {err}")))?;

    #[cfg(unix)]
    {
        use std::io::Read;
        use std::os::unix::io::AsRawFd;
        let fd = file.as_raw_fd();
        if unsafe { libc::flock(fd, libc::LOCK_EX | libc::LOCK_NB) } != 0 {
            let mut existing_pid = String::new();
            let _ = (&file).read_to_string(&mut existing_pid);
            let pid_info = existing_pid.trim();
            let msg = if pid_info.is_empty() {
                "another controller is already using this data directory".to_string()
            } else {
                format!("another controller (pid {pid_info}) is already using this data directory")
            };
            return Err(Error::conflict(msg));
        }
    }

    file.set_len(0).ok();
    let mut f = &file;
    let _ = f.write_all(format!("{}", std::process::id()).as_bytes());

    Ok(file)
}

fn print_log_entry(entry: &logs::LogEntry, show_source: bool) {
    let ts = chrono::DateTime::from_timestamp_millis(entry.ts)
        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
        .unwrap_or_else(|| entry.ts.to_string());
    let attrs = entry
        .attrs
        .iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join(" ");
    let suffix = if attrs.is_empty() {
        String::new()
    } else {
        format!("  {attrs}")
    };
    if show_source {
        println!(
            "{ts}  {:<5}  [{}]  {}{suffix}",
            entry.level.to_uppercase(),
            entry.source,
            entry.text
        );
    } else {
        println!(
            "{ts}  {:<5}  {}{suffix}",
            entry.level.to_uppercase(),
            entry.text
        );
    }
}

fn validate_subnet_cidr(cidr: &str) -> crate::error::Result<()> {
    let err = || Error::invalid_input("invalid --subnet format; expected CIDR like 172.22.0.0/16");
    match cidr.split_once('/') {
        Some((ip, prefix)) => match (ip.parse::<Ipv4Addr>(), prefix.parse::<u8>()) {
            (Ok(_), Ok(p)) if p <= 32 => Ok(()),
            _ => Err(err()),
        },
        None => Err(err()),
    }
}

fn verify_encryption_key(data_dir: &Path, encryption_key: &str) -> crate::error::Result<()> {
    use sha2::{Digest, Sha256};

    let key_file = data_dir.join(".encryption-key-hash");
    let key_hash = format!(
        "{:x}",
        Sha256::digest(format!("maestro-key-verify:{encryption_key}"))
    );

    if key_file.exists() {
        let stored = std::fs::read_to_string(&key_file).map_err(|err| {
            Error::internal(format!("failed to read encryption key hash file: {err}"))
        })?;
        if stored.trim() != key_hash {
            return Err(Error::invalid_input(
                "encryption key does not match the one used to initialize this data directory",
            ));
        }
    } else {
        std::fs::write(&key_file, &key_hash).map_err(|err| {
            Error::internal(format!("failed to write encryption key hash file: {err}"))
        })?;
    }
    Ok(())
}

fn parse_tags(tags: Vec<String>) -> crate::error::Result<Vec<String>> {
    for tag in &tags {
        let parts: Vec<&str> = tag.splitn(2, ':').collect();
        if parts.len() != 2 || parts[0].is_empty() || parts[1].is_empty() {
            return Err(Error::invalid_input(format!(
                "invalid tag \"{tag}\": expected format key:value"
            )));
        }
    }
    Ok(tags)
}

/// Bootstrap coordination plan. The lowest-sorted peer is designated as the
/// bootstrap primary — its only job is to come up first so secondaries have
/// something to wait for. Once etcd is up the "primary" designation is
/// meaningless: etcd's Raft elects its own leader, maestro's [`LeaderElector`]
/// elects the scheduler leader, both independently of bootstrap order.
#[derive(Debug)]
enum BootstrapAction {
    /// Local etcd data dir already has cluster state. Etcd recovers from
    /// disk; the `--initial-cluster-state` flag is informational at that point.
    Restart,
    /// No peers configured — standalone node.
    SingleNode,
    /// We're the lowest-sorted peer. Start the cluster with the full
    /// declared member list; secondaries will join as they come up.
    BecomePrimary,
    /// We're not the primary. Wait for the primary's etcd peer port to be
    /// reachable, then start ourselves with `--initial-cluster-state=existing`.
    /// As long as we were in the primary's original `--initial-cluster`
    /// declaration, etcd accepts us without a separate `member add`.
    JoinAsSecondary { primary_address: String },
}

const WAIT_FOR_PRIMARY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(600);
const WAIT_FOR_PRIMARY_PROBE_INTERVAL: std::time::Duration = std::time::Duration::from_secs(3);
const WAIT_FOR_PRIMARY_PROBE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);

/// Resolve the advertise_host: explicit operator value wins; otherwise
/// enumerate local NIC IPs and match against peer-list entries. Returns
/// `None` only when there are no peers (single-node).
fn resolve_advertise_host(peers: &[ClusterPeer], explicit: Option<&str>) -> Result<Option<String>> {
    if peers.is_empty() {
        return Ok(None);
    }
    if let Some(host) = explicit {
        return Ok(Some(host.to_string()));
    }
    detect_advertise_host(peers).map(Some).ok_or_else(|| {
        Error::invalid_input(
            "could not auto-detect advertise_host. Set --advertise-host \
                 (or cluster.advertise-host in config), or list this node's IP \
                 in --cluster-peer so auto-detection can match against a local NIC.",
        )
    })
}

fn determine_bootstrap_action(
    data_dir: &Path,
    peers: &[ClusterPeer],
    advertise_host: Option<&str>,
    default_peer_port: u16,
) -> Result<BootstrapAction> {
    let etcd_data_dir = data_dir.join("system/etcd/data");
    let has_etcd_state = std::fs::read_dir(&etcd_data_dir)
        .map(|mut entries| entries.next().is_some())
        .unwrap_or(false);
    if has_etcd_state {
        return Ok(BootstrapAction::Restart);
    }
    if peers.is_empty() {
        return Ok(BootstrapAction::SingleNode);
    }
    let self_host = advertise_host.ok_or_else(|| {
        Error::invalid_input(
            "internal: advertise_host must be resolved before determine_bootstrap_action",
        )
    })?;
    let mut sorted = peers.to_vec();
    sorted.sort_by(|left, right| left.host.cmp(&right.host));
    let primary = &sorted[0];
    if self_host == primary.host {
        Ok(BootstrapAction::BecomePrimary)
    } else {
        let primary_address = format!(
            "{}:{}",
            primary.host,
            primary.effective_peer_port(default_peer_port)
        );
        Ok(BootstrapAction::JoinAsSecondary { primary_address })
    }
}

/// Enumerate local NIC IPs and find the one that appears in the peer list.
/// Returns the matching peer host string (so the comparison stays
/// string-based downstream — no normalization issues).
fn detect_advertise_host(peers: &[ClusterPeer]) -> Option<String> {
    use std::net::IpAddr;
    let local_ips: Vec<IpAddr> = if_addrs::get_if_addrs()
        .ok()?
        .into_iter()
        .map(|iface| iface.ip())
        .filter(|ip| !ip.is_loopback() && !ip.is_unspecified())
        .collect();
    for peer in peers {
        let peer_ip: IpAddr = match peer.host.parse() {
            Ok(ip) => ip,
            Err(_) => continue, // hostname — can't reliably match by IP enumeration
        };
        if local_ips.contains(&peer_ip) {
            return Some(peer.host.clone());
        }
    }
    None
}

async fn wait_for_primary(address: &str) -> Result<()> {
    use tokio::net::TcpStream;
    let deadline = tokio::time::Instant::now() + WAIT_FOR_PRIMARY_TIMEOUT;
    loop {
        if tokio::time::Instant::now() >= deadline {
            return Err(Error::external(format!(
                "primary {address} did not become reachable within {}s",
                WAIT_FOR_PRIMARY_TIMEOUT.as_secs()
            )));
        }
        let connect =
            tokio::time::timeout(WAIT_FOR_PRIMARY_PROBE_TIMEOUT, TcpStream::connect(address)).await;
        if matches!(connect, Ok(Ok(_))) {
            return Ok(());
        }
        tokio::time::sleep(WAIT_FOR_PRIMARY_PROBE_INTERVAL).await;
    }
}

struct ReconcilerLookupParams {
    node_id: String,
    cluster_name: String,
    tags: Vec<String>,
    runtime_cli: String,
}

fn restart_self() -> ! {
    use std::os::unix::process::CommandExt;
    let exe = std::env::current_exe().expect("failed to get current executable path");
    let args: Vec<String> = std::env::args().skip(1).collect();
    eprintln!("[maestro]: restarting process");
    let err = std::process::Command::new(&exe).args(&args).exec();
    eprintln!("[maestro]: exec failed: {err}");
    std::process::exit(1);
}

#[cfg(test)]
mod bootstrap_inference_tests {
    use super::*;
    use crate::utils::nanoid::unique_id;

    fn tmp_data_dir(label: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("maestro-bootstrap-{label}-{}", unique_id(8)));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn peer(host: &str) -> ClusterPeer {
        ClusterPeer {
            host: host.to_string(),
            peer_port: None,
        }
    }

    #[test]
    fn empty_data_dir_with_no_peers_is_single() {
        let dir = tmp_data_dir("single");
        assert!(matches!(
            determine_bootstrap_action(&dir, &[], None, 2380).unwrap(),
            BootstrapAction::SingleNode
        ));
    }

    #[test]
    fn populated_data_dir_is_restart() {
        let dir = tmp_data_dir("restart");
        let etcd_data = dir.join("system/etcd/data");
        std::fs::create_dir_all(etcd_data.join("member/wal")).unwrap();
        std::fs::write(etcd_data.join("member/wal/0.wal"), b"fake wal").unwrap();
        assert!(matches!(
            determine_bootstrap_action(&dir, &[peer("10.0.0.1")], None, 2380).unwrap(),
            BootstrapAction::Restart
        ));
    }

    #[test]
    fn lowest_sorted_peer_becomes_primary() {
        let dir = tmp_data_dir("primary");
        let peers = vec![peer("10.0.0.3"), peer("10.0.0.1"), peer("10.0.0.2")];
        let action = determine_bootstrap_action(&dir, &peers, Some("10.0.0.1"), 2380).unwrap();
        assert!(matches!(action, BootstrapAction::BecomePrimary));
    }

    #[test]
    fn non_primary_peer_joins_as_secondary() {
        let dir = tmp_data_dir("secondary");
        let peers = vec![peer("10.0.0.3"), peer("10.0.0.1"), peer("10.0.0.2")];
        let action = determine_bootstrap_action(&dir, &peers, Some("10.0.0.2"), 2380).unwrap();
        match action {
            BootstrapAction::JoinAsSecondary { primary_address } => {
                assert_eq!(primary_address, "10.0.0.1:2380");
            }
            other => panic!("expected JoinAsSecondary, got {other:?}"),
        }
    }

    #[test]
    fn secondary_respects_per_peer_port_override() {
        let dir = tmp_data_dir("port");
        let peers = vec![
            ClusterPeer {
                host: "10.0.0.1".to_string(),
                peer_port: Some(2390),
            },
            peer("10.0.0.2"),
        ];
        let action = determine_bootstrap_action(&dir, &peers, Some("10.0.0.2"), 2380).unwrap();
        match action {
            BootstrapAction::JoinAsSecondary { primary_address } => {
                assert_eq!(primary_address, "10.0.0.1:2390");
            }
            other => panic!("expected JoinAsSecondary, got {other:?}"),
        }
    }

    #[test]
    fn peers_without_advertise_host_and_no_matching_local_ip_is_rejected() {
        // Pick two unroutable IPs that won't match any NIC on this machine.
        let dir = tmp_data_dir("noadvertise");
        let peers = vec![peer("169.254.99.10"), peer("169.254.99.11")];
        let result = determine_bootstrap_action(&dir, &peers, None, 2380);
        assert!(
            result.is_err(),
            "should refuse when no advertise_host and no NIC matches the peer list"
        );
    }

    #[test]
    fn resolve_advertise_host_finds_matching_local_nic() {
        let local_ips: Vec<String> = if_addrs::get_if_addrs()
            .unwrap_or_default()
            .into_iter()
            .map(|iface| iface.ip())
            .filter(|ip| !ip.is_loopback() && !ip.is_unspecified())
            .map(|ip| ip.to_string())
            .collect();
        if local_ips.is_empty() {
            eprintln!("skipping: no non-loopback NIC on this machine");
            return;
        }
        let chosen = local_ips[0].clone();
        let peers = vec![peer("169.254.99.10"), peer(&chosen)];
        let resolved = resolve_advertise_host(&peers, None)
            .unwrap()
            .expect("should find a local NIC matching a peer");
        assert_eq!(resolved, chosen);
    }

    #[test]
    fn resolve_advertise_host_passes_through_explicit() {
        let peers = vec![peer("10.0.0.1"), peer("10.0.0.2")];
        let resolved = resolve_advertise_host(&peers, Some("10.0.0.2")).unwrap();
        assert_eq!(resolved.as_deref(), Some("10.0.0.2"));
    }

    #[test]
    fn resolve_advertise_host_returns_none_for_single_node() {
        let resolved = resolve_advertise_host(&[], None).unwrap();
        assert!(resolved.is_none());
    }
}
