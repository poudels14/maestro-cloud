mod builder;
mod cli;
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
use error::Error;
use signal::spawn_shutdown_signal_bus;

use crate::{
    deployment::{ControllerConfig, controller::DeploymentController},
    supervisor::controller::JobSupervisor,
};

const DEFAULT_CONFIG_PATH: &str = "maestro.jsonc";
const DEFAULT_CLUSTER_CONFIG_PATH: &str = "maestro.cluster.jsonc";
const DEFAULT_SERVICE_CONFIG_PATH: &str = "maestro.service.jsonc";
const DEFAULT_API_PORT: u16 = 3001;

#[derive(Debug, Parser)]
#[command(name = "maestro", version, disable_help_subcommand = true)]
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
    /// Read logs from the locally running probe API
    Logs(LogsArgs),
    /// Inspect, export, or purge controller sink dead letters
    DeadLetters(DeadLettersArgs),
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
        long = "egress-allow",
        value_name = "CIDR",
        help = "Allow container egress to an IP/CIDR even if covered by an egress-deny range (can be repeated)"
    )]
    egress_allow: Vec<String>,
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
        long = "tailscale-advertise-routes",
        value_name = "CIDR",
        value_delimiter = ',',
        help = "Subnet routes to advertise via Tailscale (comma-separated or repeated)"
    )]
    tailscale_advertise_routes: Vec<String>,
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
}

#[derive(Debug, Args)]
struct LogsArgs {
    #[arg(
        long = "source",
        help = "System source name or service/deployment/unit. Shows all sources if omitted"
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
struct DeadLettersArgs {
    #[arg(long = "data-dir", help = "Maestro data directory")]
    data_dir: PathBuf,
    #[arg(
        long = "cluster-name",
        help = "Cluster name (read from maestro.jsonc if omitted)"
    )]
    cluster_name: Option<String>,
    #[command(subcommand)]
    command: DeadLettersCommand,
}

#[derive(Debug, Subcommand)]
enum DeadLettersCommand {
    /// List dead-letter metadata and aggregate storage use
    List {
        #[arg(long, default_value = "datadog")]
        sink: String,
        #[arg(long, default_value_t = 100)]
        limit: usize,
    },
    /// Export full dead-letter records as JSON Lines
    Export {
        #[arg(long, default_value = "datadog")]
        sink: String,
        #[arg(long)]
        output: PathBuf,
    },
    /// Purge dead letters after they have been exported or investigated
    Purge {
        #[arg(long, default_value = "datadog")]
        sink: String,
        #[arg(
            long,
            conflicts_with = "through_seq",
            required_unless_present = "through_seq"
        )]
        all: bool,
        #[arg(long, conflicts_with = "all", value_name = "SEQ")]
        through_seq: Option<i64>,
    },
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
                    egress_allow,
                    enable_tailscale,
                    tailscale_authkey,
                    tailscale_advertise_routes,
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
                    log_backup: None,
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
                cfg.tailscale = Some(config::TailscaleConfig {
                    auth_key: authkey,
                    advertise_routes: Vec::new(),
                });
            }
            if !tailscale_advertise_routes.is_empty()
                && let Some(ts) = cfg.tailscale.as_mut()
            {
                ts.advertise_routes.extend(tailscale_advertise_routes);
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
            if !egress_allow.is_empty() {
                cfg.egress.allow.extend(egress_allow.clone());
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
            let egress_deny = firewall::normalize_cidrs(&cfg.egress.deny, "deny")?;
            let egress_allow = firewall::normalize_cidrs(&cfg.egress.allow, "allow")?;

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
            let (tailscale_authkey, tailscale_advertise_routes) = if enable_tailscale {
                match cfg.tailscale {
                    Some(ts) => (Some(ts.auth_key), ts.advertise_routes),
                    None => (None, Vec::new()),
                }
            } else {
                (None, Vec::new())
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
            log_store
                .register_sink("controller")
                .await
                .map_err(|err| Error::internal(format!("failed to register probe sink: {err}")))?;
            if cfg.datadog.is_some() && datadog_site.is_some() {
                log_store.register_sink("datadog").await.map_err(|err| {
                    Error::internal(format!("failed to register Datadog sink: {err}"))
                })?;
            } else {
                log_store.unregister_sink("datadog").await.map_err(|err| {
                    Error::internal(format!("failed to unregister Datadog sink: {err}"))
                })?;
            }
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
                    let log_sink = logs::DatadogSink::new(
                        dd.api_key.clone(),
                        &site,
                        dd.include_ingress_logs,
                        dd.include_tailscale_logs,
                        log_store.clone(),
                    );
                    background_handles.push(
                        logs::SinkWorker::new(
                            log_store.clone(),
                            Box::new(log_sink),
                            signal_tx.subscribe(),
                        )
                        .spawn(),
                    );
                    logger.emit("info", &format!("datadog log sink enabled (site: {site})"));

                    let include_metrics = dd.include_metrics;
                    let metrics_api_key = dd.api_key.clone();
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

            let mut deployment_config = ControllerConfig {
                cluster_alias,
                cluster_name,
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
                tailscale_advertise_routes,
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
                allow: egress_allow,
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
                deployment::etcd::EtcdStateStore::new(&etcd_endpoint, derived_key, etcd_tls)
                    .await?,
            );
            let probe_log_endpoint = format!("http://127.0.0.1:{probe_host_port}/api/logs");
            let node_id = load_or_create_node_id(&deployment_config.data_dir)?;
            let http_sink = logs::HttpSink::new("controller", node_id, &probe_log_endpoint);
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

            let mut controller = DeploymentController::new(
                deployment_config,
                store,
                supervisor,
                deployment_signal_rx,
                Some(log_sender.clone()),
                runtime,
                Some(system_info.dns_manager),
                system_info.nameserver_ip,
            );
            let deployment_shutdown_tx = signal_tx.clone();

            let result = async move {
                let result = controller.run().await.map_err(Into::into);
                let _ = deployment_shutdown_tx.send(signal::ShutdownEvent::Graceful);
                result.map(|exit_reason| (exit_reason, controller))
            }
            .await;

            watcher_handle.abort();
            metrics_handle.abort();
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
                ClusterCommand::Upgrade {
                    target: UpgradeTarget::System,
                    yes,
                } => cli::upgrade::run_upgrade_system(&host, yes)
                    .await
                    .map(|()| false),
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
            let cluster_name = resolve_local_cluster_name(cluster_name).await?;
            let port_path = data_dir.join(&cluster_name).join("system/probe/api-port");
            let port = std::fs::read_to_string(&port_path)
                .map_err(|err| {
                    Error::not_found(format!(
                        "probe API location is unavailable at {}: {err}; is the controller running?",
                        port_path.display()
                    ))
                })?
                .trim()
                .parse::<u16>()
                .map_err(|err| {
                    Error::internal(format!(
                        "invalid probe API port in {}: {err}",
                        port_path.display()
                    ))
                })?;
            cli::logs::run_daemon_logs(&format!("http://127.0.0.1:{port}"), source, tail, follow)
                .await
                .map(|()| false)
        }
        Some(CliCommand::Daemon {
            command:
                DaemonCommand::DeadLetters(DeadLettersArgs {
                    data_dir,
                    cluster_name,
                    command,
                }),
        }) => {
            let cluster_name = resolve_local_cluster_name(cluster_name).await?;
            let db_path = data_dir.join(cluster_name).join("logs/logs.db");
            match command {
                DeadLettersCommand::List { sink, limit } => {
                    cli::dead_letters::list(&db_path, &sink, limit).await?
                }
                DeadLettersCommand::Export { sink, output } => {
                    cli::dead_letters::export(&db_path, &sink, &output).await?
                }
                DeadLettersCommand::Purge {
                    sink,
                    all: _,
                    through_seq,
                } => cli::dead_letters::purge(&db_path, &sink, through_seq).await?,
            }
            Ok(false)
        }
        Some(CliCommand::Config { command }) => match command {
            ConfigCommand::Init => cli::config::run_init().map(|()| false),
            ConfigCommand::Validate { path } => cli::config::run_validate(&path).map(|()| false),
        },
    }
}

async fn resolve_local_cluster_name(cluster_name: Option<String>) -> crate::error::Result<String> {
    if let Some(name) = cluster_name {
        return Ok(name.to_lowercase());
    }
    if let Ok(cfg) = config::load_config(DEFAULT_CONFIG_PATH).await {
        return Ok(cfg.cluster.name.to_lowercase());
    }
    Err(Error::invalid_input(
        "provide --cluster-name or create maestro.jsonc with cluster.name",
    ))
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

fn load_or_create_node_id(data_dir: &Path) -> crate::error::Result<String> {
    use std::io::Write;

    let directory = data_dir.join("logs");
    let path = directory.join("node-id");
    let temp = directory.join("node-id.tmp");
    match std::fs::remove_file(&temp) {
        Ok(()) => {}
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => {
            return Err(Error::internal(format!(
                "failed to remove stale spool identity {}: {err}",
                temp.display()
            )));
        }
    }
    if let Ok(existing) = std::fs::read_to_string(&path) {
        let existing = existing.trim();
        if !existing.is_empty() {
            return Ok(existing.to_string());
        }
    }

    std::fs::create_dir_all(&directory).map_err(|err| {
        Error::internal(format!(
            "failed to create spool identity directory {}: {err}",
            directory.display()
        ))
    })?;
    let node_id = format!("controller-{}", utils::nanoid::unique_id(16));
    let mut file = std::fs::File::create(&temp)
        .map_err(|err| Error::internal(format!("failed to create spool identity: {err}")))?;
    file.write_all(node_id.as_bytes())
        .and_then(|()| file.sync_all())
        .map_err(|err| Error::internal(format!("failed to persist spool identity: {err}")))?;
    std::fs::rename(&temp, &path)
        .map_err(|err| Error::internal(format!("failed to install spool identity: {err}")))?;
    if let Ok(directory) = std::fs::File::open(&directory) {
        let _ = directory.sync_all();
    }
    Ok(node_id)
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
mod spool_identity_tests {
    use super::load_or_create_node_id;

    #[test]
    fn controller_spool_identity_is_stable_for_its_data_directory() {
        let root = std::env::temp_dir().join(format!(
            "maestro-node-id-test-{}-{}",
            std::process::id(),
            crate::utils::nanoid::unique_id(8)
        ));
        let first = load_or_create_node_id(&root).expect("create node id");
        let stale_temp = root.join("logs/node-id.tmp");
        std::fs::write(&stale_temp, "stale").expect("stale temp");
        let second = load_or_create_node_id(&root).expect("read node id");
        assert_eq!(first, second);
        assert!(first.starts_with("controller-"));
        assert!(!stale_temp.exists());
        std::fs::remove_dir_all(root).ok();
    }
}
