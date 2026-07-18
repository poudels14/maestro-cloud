mod builder;
mod cli;
mod cluster;
mod cluster_stats;
mod config;
mod deployment;
mod engine;
mod error;
mod exec;
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
const DEFAULT_SERVICES_CONFIG_PATH: &str = "maestro.services.jsonc";
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
    /// Open a shell or run a command in a live service replica
    Ssh(cli::ssh::SshArgs),
}

#[derive(Debug, Subcommand)]
enum ConfigCommand {
    /// Create a starter config file (interactive — choose cluster or services)
    Init,
    /// Validate a config source (auto-detects cluster vs services)
    Validate {
        #[arg(help = "Config source: file path, file://path, or aws-secret://secret-name")]
        source: String,
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
            help = "Path to maestro.services.jsonc (default: maestro.services.jsonc)"
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
            help = "Path to maestro.services.jsonc (default: maestro.services.jsonc)"
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
    /// Convert an existing legacy single-node data directory into a cluster seed
    Enable {
        #[arg(
            long = "config",
            default_value = DEFAULT_CONFIG_PATH,
            help = "Path to the cluster-enabled maestro.jsonc"
        )]
        config: String,
        #[arg(long = "data-dir", help = "Base Maestro data directory")]
        data_dir: PathBuf,
    },
    /// Initialize the shared cluster identity and CA on the configured master
    InitCa {
        #[arg(
            long = "config",
            default_value = DEFAULT_CONFIG_PATH,
            help = "Path to maestro.jsonc"
        )]
        config: String,
        #[arg(long = "data-dir", help = "Base Maestro data directory")]
        data_dir: PathBuf,
    },
    /// Issue a node-endpoint-bound certificate bundle from the initialized cluster CA
    IssueNode {
        #[arg(
            long = "config",
            default_value = DEFAULT_CONFIG_PATH,
            help = "Path to maestro.jsonc"
        )]
        config: String,
        #[arg(long = "data-dir", help = "Base Maestro data directory")]
        data_dir: PathBuf,
        #[arg(long = "host-ip", help = "Private control IP of the node")]
        host_ip: Ipv4Addr,
        #[arg(
            long = "api-port",
            help = "Node API port (defaults to a matching configured endpoint or 3000)"
        )]
        api_port: Option<u16>,
        #[arg(long = "node-id", help = "Prepared 12-character node identity")]
        node_id: String,
        #[arg(long, help = "Reserved Docker /24 for the node")]
        subnet: String,
        #[arg(
            long = "role",
            default_value = "worker",
            help = "master, hybrid, voter, or worker"
        )]
        role: cluster::NodeRole,
        #[arg(long = "output", help = "Output directory for the certificate bundle")]
        output: Option<PathBuf>,
    },
    /// Prepare a node identity or join it to an existing cluster
    Join {
        #[arg(help = "Private leader address (host or host:port)")]
        leader: Option<String>,
        #[arg(
            long,
            help = "Generate the node join key and print its approval identity"
        )]
        prepare: bool,
        #[arg(
            long = "config",
            default_value = DEFAULT_CONFIG_PATH,
            help = "Path to maestro.jsonc"
        )]
        config: String,
        #[arg(long = "data-dir", help = "Base Maestro data directory")]
        data_dir: PathBuf,
    },
    /// Approve a one-time voter admission on the current leader
    ApproveNode {
        #[arg(long = "role", default_value = "hybrid")]
        role: cluster::NodeRole,
        #[arg(long = "node-id")]
        node_id: String,
        #[arg(long = "host-ip")]
        host_ip: Ipv4Addr,
        #[arg(
            long = "api-port",
            help = "Node API port (defaults to a matching configured endpoint or 3000)"
        )]
        api_port: Option<u16>,
        #[arg(long)]
        subnet: String,
        #[arg(long = "public-key-sha256")]
        public_key_sha256: String,
    },
    /// Show information about the active cluster
    Info,
    /// List live cluster nodes and their data-plane status
    Nodes,
    /// Drain workloads from a cluster node
    Drain {
        #[arg(help = "Cluster node ID")]
        node_id: String,
    },
    /// Make a drained cluster node schedulable again
    Restore {
        #[arg(help = "Cluster node ID")]
        node_id: String,
    },
    /// Drain and permanently remove a node from cluster membership
    RemoveNode {
        #[arg(help = "Cluster node ID")]
        node_id: String,
    },
    /// Show the controller's effective config (secrets are masked)
    Config,
    /// Restart a selected cluster node, every node serially, or only the local controller
    Restart {
        #[arg(
            value_name = "NODE_ID",
            conflicts_with_all = ["all", "local"],
            help = "Cluster node to drain, restart, verify, and restore"
        )]
        node_id: Option<String>,
        #[arg(
            long,
            conflicts_with = "local",
            help = "Drain and restart every node serially with the leader last"
        )]
        all: bool,
        #[arg(
            long,
            conflicts_with = "all",
            help = "Immediately restart only the controller reached by the active context"
        )]
        local: bool,
        #[arg(
            short = 'y',
            long = "yes",
            help = "Skip the cluster-confirmation prompt"
        )]
        yes: bool,
    },
    /// Upgrade system components
    #[command(
        after_help = "Examples:\n  maestro cluster upgrade\n  maestro cluster upgrade system"
    )]
    Upgrade {
        #[command(subcommand)]
        target: Option<UpgradeTarget>,
        #[arg(
            short = 'y',
            long = "yes",
            help = "Skip the cluster-confirmation prompt"
        )]
        yes: bool,
    },
    /// Manually abort a stale upgrade run and remove its deployment freeze
    Unfreeze {
        #[arg(long = "upgrade-run", help = "Exact upgrade run ID")]
        upgrade_run: String,
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
        long = "role",
        help = "Local node role: master, hybrid, voter, or worker"
    )]
    role: Option<cluster::NodeRole>,
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

struct StartConfigOverrides {
    cluster_name: Option<String>,
    encryption_key: Option<String>,
    role: Option<cluster::NodeRole>,
    jwt_secret_key: Option<String>,
    tailscale_authkey: Option<String>,
    tailscale_advertise_routes: Vec<String>,
    datadog_api_key: Option<String>,
    datadog_site: Option<String>,
    datadog_no_ingress_logs: bool,
    datadog_no_tailscale_logs: bool,
    cloudflare_tunnel_token: Option<String>,
    subnet: Option<String>,
    egress_deny: Vec<String>,
    egress_allow: Vec<String>,
    tags: Vec<String>,
    runtime: Option<config::RuntimeType>,
    system: Option<config::SystemType>,
    disable_etcd_cert: bool,
}

fn apply_start_config_overrides(cfg: &mut config::StartConfig, overrides: StartConfigOverrides) {
    if let Some(name) = overrides.cluster_name {
        cfg.cluster.name = name;
    }
    if let Some(key) = overrides.encryption_key {
        cfg.encryption_key = key;
    }
    if let Some(role) = overrides.role {
        cfg.node.role = role;
        if let Some(name) = cfg.node.name.as_deref()
            && let Some(node) = cfg.cluster.nodes.get_mut(name)
        {
            node.role = role;
        }
    }
    if let Some(secret) = overrides.jwt_secret_key {
        cfg.jwt_secret_key = Some(secret);
    }
    if let Some(authkey) = overrides.tailscale_authkey {
        if let Some(tailscale) = cfg.tailscale.as_mut() {
            tailscale.auth_key = authkey;
        } else {
            cfg.tailscale = Some(config::TailscaleConfig {
                auth_key: authkey,
                advertise_routes: Vec::new(),
            });
        }
    }
    if !overrides.tailscale_advertise_routes.is_empty()
        && let Some(tailscale) = cfg.tailscale.as_mut()
    {
        tailscale.advertise_routes = overrides.tailscale_advertise_routes;
    }
    if let Some(api_key) = overrides.datadog_api_key {
        let datadog = cfg.datadog.get_or_insert(config::DatadogConfig {
            api_key: String::new(),
            site: None,
            include_ingress_logs: true,
            include_tailscale_logs: true,
            logs: config::DatadogLogsConfig::default(),
            include_metrics: false,
        });
        datadog.api_key = api_key;
    }
    if let Some(token) = overrides.cloudflare_tunnel_token {
        if let Some(cloudflare) = cfg.cloudflare.as_mut() {
            cloudflare.tunnel.token = SecretString::new(token);
        } else {
            cfg.cloudflare = Some(config::CloudflareConfig {
                tunnel: config::CloudflareTunnelConfig {
                    token: SecretString::new(token),
                    replicas: None,
                },
            });
        }
    }
    if let Some(datadog) = cfg.datadog.as_mut() {
        if let Some(site) = overrides.datadog_site {
            datadog.site = Some(site);
        }
        if overrides.datadog_no_ingress_logs {
            datadog.include_ingress_logs = false;
        }
        if overrides.datadog_no_tailscale_logs {
            datadog.include_tailscale_logs = false;
        }
    }
    if let Some(subnet) = overrides.subnet {
        cfg.subnet = Some(subnet.clone());
        if let Some(name) = cfg.node.name.as_deref()
            && let Some(node) = cfg.cluster.nodes.get_mut(name)
        {
            node.subnet = subnet;
        }
    }
    if !overrides.egress_deny.is_empty() {
        cfg.egress.deny = overrides.egress_deny;
    }
    if !overrides.egress_allow.is_empty() {
        cfg.egress.allow = overrides.egress_allow;
    }
    if !overrides.tags.is_empty() {
        cfg.tags = overrides.tags;
    }
    if let Some(runtime) = overrides.runtime {
        cfg.runtime = runtime;
    }
    if let Some(system) = overrides.system {
        cfg.system = Some(system);
    }
    if overrides.disable_etcd_cert {
        cfg.disable_etcd_cert = true;
    }
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
                    role,
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
                            .clone()
                            .ok_or_else(|| Error::invalid_input("--cluster-name is required"))?,
                        ..Default::default()
                    },
                    node: Default::default(),
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
                    github: None,
                    homepage: None,
                    log_backup: None,
                    disable_etcd_cert: false,
                    allow_cli_deployment: false,
                    allow_exec: false,
                },
            };

            apply_start_config_overrides(
                &mut cfg,
                StartConfigOverrides {
                    cluster_name,
                    encryption_key,
                    role,
                    jwt_secret_key,
                    tailscale_authkey,
                    tailscale_advertise_routes,
                    datadog_api_key: dd_api_key,
                    datadog_site: explicit_datadog_site,
                    datadog_no_ingress_logs: dd_no_ingress_logs,
                    datadog_no_tailscale_logs: dd_no_tailscale_logs,
                    cloudflare_tunnel_token,
                    subnet,
                    egress_deny,
                    egress_allow,
                    tags,
                    runtime: runtime_flag,
                    system,
                    disable_etcd_cert,
                },
            );

            let datadog_site = cfg
                .datadog
                .as_ref()
                .and_then(|dd| dd.site.clone())
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty());
            let runtime_type = cfg.runtime;
            let runtime = runtime::create_provider(runtime_type);

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
            let cluster_alias = cfg.cluster.name.to_lowercase();
            let cluster_data_dir = data_dir.join(&cluster_alias);
            let migration_in_progress = cluster::migration::is_in_progress(&cluster_data_dir);
            if cfg.cluster.nodes.is_empty()
                && (migration_in_progress || cluster_data_dir.join("system/cluster-id").exists())
            {
                return Err(Error::invalid_config(
                    "this data directory is cluster-enabled; cluster.nodes cannot be removed",
                ));
            }
            let automatic_legacy_migration = migration_in_progress
                || cluster::migration::is_legacy_candidate(
                    &cfg.cluster,
                    cfg.node.role,
                    &cluster_data_dir,
                );
            cluster::network::validate_cluster_config(
                &cfg.cluster,
                cfg.subnet.as_deref(),
                cfg.node.role,
            )
            .map_err(|err| Error::invalid_config(err.to_string()))?;
            if !cfg.cluster.nodes.is_empty()
                && cfg
                    .jwt_secret_key
                    .as_deref()
                    .is_none_or(|secret| secret.len() < 32)
            {
                return Err(Error::invalid_config(
                    "jwt-secret-key must contain at least 32 characters in cluster mode",
                ));
            }
            if !cfg.cluster.nodes.is_empty() && cfg.disable_etcd_cert {
                cfg.disable_etcd_cert = false;
                eprintln!("[maestro]: --disable-etcd-cert is ignored in multi-node mode");
            }
            let egress_deny = firewall::normalize_cidrs(&cfg.egress.deny, "deny")?;
            let egress_allow = firewall::normalize_cidrs(&cfg.egress.allow, "allow")?;

            let (signal_tx, signal_task) = spawn_shutdown_signal_bus()?;
            let data_dir = cluster_data_dir;
            std::fs::create_dir_all(&data_dir).map_err(|err| {
                Error::internal(format!(
                    "failed to create data directory {}: {err}",
                    data_dir.display()
                ))
            })?;
            verify_encryption_key(&data_dir, &cfg.encryption_key)?;
            let _lock = acquire_lock(&data_dir)?;
            if !cfg.cluster.nodes.is_empty() {
                let (_, selected) = cfg
                    .cluster
                    .selected_node()
                    .map_err(|error| Error::invalid_config(error.to_string()))?;
                let ports = cluster::network::load_or_allocate_cluster_ports(
                    &data_dir,
                    selected.endpoint.host_ip(),
                    cfg.cluster.api_port,
                )
                .map_err(|error| Error::invalid_config(error.to_string()))?;
                cfg.cluster
                    .set_local_ports(ports.gateway, ports.etcd_client, ports.etcd_peer);
                eprintln!(
                    "[maestro]: cluster ports: API {}, gateway {}, etcd client {}, etcd peer {}",
                    cfg.cluster.api_port,
                    cfg.cluster.gateway_port,
                    cfg.cluster.etcd_client_port,
                    cfg.cluster.etcd_peer_port
                );
            }
            if automatic_legacy_migration {
                let legacy_etcd =
                    cluster::migration::legacy_etcd_container_name(&cfg.cluster, &data_dir)
                        .map_err(|error| Error::invalid_config(error.to_string()))?;
                runtime
                    .remove_container(&legacy_etcd)
                    .await
                    .map_err(|error| {
                        Error::internal(format!(
                            "failed to stop legacy etcd container `{legacy_etcd}`: {error}"
                        ))
                    })?;
                let host_ip = cluster::network::resolve_cluster_host_ip(
                    &cfg.cluster,
                    &data_dir,
                    cfg.node.role,
                    cfg.subnet.as_deref(),
                )
                .map_err(|error| Error::invalid_config(error.to_string()))?
                .ok_or_else(|| Error::invalid_config("failed to resolve cluster host IP"))?;
                let migration =
                    cluster::migration::migrate(&cfg.cluster, cfg.node.role, &data_dir, host_ip)
                        .map_err(|error| {
                            Error::invalid_config(format!(
                                "automatic legacy cluster migration failed: {error}"
                            ))
                        })?;
                eprintln!(
                    "[maestro]: migrated legacy etcd member into cluster `{}`; CA SHA-256: {}; offline backup: {}; recovery manifest: {}",
                    migration.cluster_id,
                    migration.ca_sha256,
                    migration.etcd_backup.display(),
                    migration.etcd_backup_manifest.display()
                );
            }
            if !cfg.cluster.nodes.is_empty() && !automatic_legacy_migration {
                let host_ip = cluster::network::resolve_cluster_host_ip(
                    &cfg.cluster,
                    &data_dir,
                    cfg.node.role,
                    cfg.subnet.as_deref(),
                )
                .map_err(|error| Error::invalid_config(error.to_string()))?
                .ok_or_else(|| Error::invalid_config("failed to resolve cluster host IP"))?;
                let is_seed = cfg.node.role == cluster::NodeRole::Master;
                if is_seed {
                    let identity = cluster::provision::ensure_seed_identity(
                        &cfg.cluster,
                        cfg.node.role,
                        &data_dir,
                        host_ip,
                    )?;
                    if identity.created {
                        eprintln!(
                            "[maestro]: automatically initialized cluster `{}` ({})",
                            cfg.cluster.name, identity.cluster_id
                        );
                    }
                } else if !cluster::provision::identity_installed(&data_dir)? {
                    cluster::provision::auto_join(&cfg, &data_dir, host_ip, signal_tx.subscribe())
                        .await?;
                }
            }
            let maestro_config = serde_json::to_string(&cfg.masked()).map_err(|err| {
                Error::internal(format!("failed to serialize masked config: {err}"))
            })?;
            let github_config = cfg.github.clone();
            let homepage = cfg.homepage.clone();
            let ingestion_token = load_or_create_ingestion_token(&data_dir)?;
            let cluster_runtime = if cfg.cluster.nodes.is_empty() {
                None
            } else {
                let host_ip = cluster::network::resolve_cluster_host_ip(
                    &cfg.cluster,
                    &data_dir,
                    cfg.node.role,
                    cfg.subnet.as_deref(),
                )
                .map_err(|err| Error::invalid_config(err.to_string()))?
                .ok_or_else(|| Error::invalid_config("failed to resolve cluster host IP"))?;
                let cluster_id = cluster::identity::load_cluster_id(&data_dir)
                    .map_err(|err| Error::invalid_config(err.to_string()))?;
                let node_id = cluster::identity::load_or_create_node_id(&data_dir)
                    .map_err(|err| Error::invalid_config(err.to_string()))?;
                let certs_dir = data_dir.join("system/certs");
                let node_certs = utils::certs::read_etcd_certs(&certs_dir).map_err(|err| {
                    Error::invalid_config(format!(
                        "node certificate bundle is missing or invalid after automatic cluster provisioning: {err}"
                    ))
                })?;
                if cfg.node.role.is_voter() {
                    let ca = utils::certs::load_cluster_ca(&certs_dir.join("cluster-ca")).map_err(
                        |err| {
                            Error::invalid_config(format!(
                                "voter cluster CA material is missing or invalid: {err}"
                            ))
                        },
                    )?;
                    if ca.cert_pem != node_certs.ca_pem {
                        return Err(Error::invalid_config(
                            "voter CA private material does not match the provisioned node certificate bundle",
                        ));
                    }
                }
                let local_endpoint = cfg
                    .cluster
                    .local_endpoint(host_ip, cfg.node.role)
                    .map_err(|err| Error::invalid_config(err.to_string()))?;
                let voter_cache = cluster::join::load_voter_cache(&data_dir, &cluster_id)
                    .map_err(|err| Error::invalid_config(err.to_string()))?
                    .ok_or_else(|| {
                        Error::invalid_config("cluster voter cache is missing after provisioning")
                    })?;
                let initial_voters = voter_cache.initial_voter_endpoints;
                if initial_voters.is_empty() {
                    return Err(Error::invalid_config(
                        "cluster voter cache has no initial voter endpoints",
                    ));
                }
                let voter_endpoints = voter_cache.voter_endpoints;
                if voter_endpoints.is_empty() {
                    return Err(Error::invalid_config(
                        "cluster voter cache has no current voter endpoints",
                    ));
                }
                Some(cluster::ClusterRuntime {
                    cluster_id,
                    node_id,
                    instance_id: cluster::identity::new_instance_id(),
                    host_ip,
                    role: cfg.node.role,
                    initial_voters,
                    voter_endpoints,
                    subnet: cfg.subnet.clone().expect("validated cluster subnet"),
                    control_allow_cidrs: cfg.cluster.control_allow_cidrs.clone(),
                    api_port: local_endpoint.api_port,
                    gateway_port: local_endpoint.gateway_port,
                    etcd_client_port: local_endpoint.etcd_client_port,
                    etcd_peer_port: local_endpoint.etcd_peer_port,
                    labels: cfg.cluster.labels.clone(),
                })
            };
            let cluster_name = if let Some(cluster) = &cluster_runtime {
                format!("{cluster_alias}-{}", &cluster.cluster_id[..8])
            } else {
                let cluster_suffix = load_or_create_cluster_suffix(&data_dir)?;
                format!("{cluster_alias}-{cluster_suffix}")
            };
            let etcd_port = if let Some(cluster) = &cluster_runtime {
                if etcd_port.is_some_and(|port| port != cluster.etcd_client_port) {
                    return Err(Error::invalid_input(
                        "--etcd-port conflicts with the persisted automatic etcd client port",
                    ));
                }
                cluster.etcd_client_port
            } else {
                etcd_port.unwrap_or_else(|| {
                    let listener = std::net::TcpListener::bind("127.0.0.1:0")
                        .expect("failed to bind to random port for etcd");
                    listener
                        .local_addr()
                        .expect("failed to get local addr")
                        .port()
                })
            };
            let etcd_endpoints = if let Some(cluster) = &cluster_runtime {
                cluster::join::load_voter_cache(&data_dir, &cluster.cluster_id)
                    .map_err(|err| Error::invalid_config(err.to_string()))?
                    .map(|cache| cache.client_endpoints())
                    .unwrap_or_else(|| cluster.client_endpoints())
            } else {
                let etcd_scheme = if cfg.disable_etcd_cert {
                    "http"
                } else {
                    "https"
                };
                vec![format!("{etcd_scheme}://127.0.0.1:{etcd_port}")]
            };
            let etcd_endpoint = if cluster_runtime
                .as_ref()
                .is_some_and(|cluster| cluster.role == cluster::NodeRole::Worker)
            {
                etcd_endpoints[0].clone()
            } else if let Some(cluster) = &cluster_runtime {
                format!("https://{}:{etcd_port}", cluster.host_ip)
            } else {
                etcd_endpoints[0].clone()
            };
            let container_etcd_endpoints = deployment::container_etcd_endpoints(
                cluster_runtime.as_ref(),
                &etcd_endpoints,
                cfg.disable_etcd_cert,
            );
            let network = network.unwrap_or_else(|| {
                cluster_runtime
                    .as_ref()
                    .and_then(cluster::ClusterRuntime::resource_suffix)
                    .map(|suffix| format!("maestro-{cluster_alias}-{suffix}"))
                    .unwrap_or_else(|| format!("maestro-{cluster_alias}"))
            });

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
            let sink_runtime_stats = cluster_stats::SinkRuntimeRegistry::default();
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

            // Local ingress traffic analytics and blocklist suggestions depend on
            // structured access logs. Datadog delivery remains independently
            // controlled by datadog.include-ingress-logs.
            let enable_ingress_access_logs = true;

            let mut metrics_datadog_tx: Option<flume::Sender<metrics::MetricBatch>> = None;
            if let Some(dd) = cfg.datadog {
                if let Some(site) = datadog_site {
                    let log_sink = logs::DatadogSink::new(
                        dd.api_key.clone(),
                        &site,
                        dd.include_ingress_logs,
                        dd.include_tailscale_logs,
                        !dd.logs.include_healthcheck,
                        log_store.clone(),
                    );
                    background_handles.push(
                        logs::SinkWorker::new(
                            log_store.clone(),
                            Box::new(log_sink),
                            signal_tx.subscribe(),
                        )
                        .with_runtime_stats(sink_runtime_stats.clone())
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

            let recreate_legacy_network =
                cluster::migration::network_reconfiguration_required(&data_dir);
            let cloudflare_tunnel_replicas = cfg
                .cloudflare
                .as_ref()
                .and_then(|cf| cf.tunnel.replicas)
                .unwrap_or(2)
                .max(1);
            if cloudflare_tunnel_replicas > deployment::MAX_CLOUDFLARED_REPLICAS {
                return Err(Error::invalid_config(format!(
                    "cloudflare.tunnel.replicas cannot exceed {}",
                    deployment::MAX_CLOUDFLARED_REPLICAS
                )));
            }
            let mut deployment_config = ControllerConfig {
                cluster_alias,
                cluster_name,
                cluster: cluster_runtime,
                data_dir,
                etcd_port,
                etcd_endpoints,
                container_etcd_endpoints,
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
                ingestion_token,
                internal_control_token: SecretString::new(utils::nanoid::unique_id(48)),
                join_secret: cfg.cluster.join_secret.clone().map(SecretString::new),
                jwt_secret_key: cfg.jwt_secret_key,
                build_command_env,
                tags: parse_tags(cfg.tags)?,
                system_type: cfg.system,
                force: force || recreate_legacy_network,
                disable_etcd_cert: cfg.disable_etcd_cert,
                enable_ingress_access_logs,
                maestro_config,
                cloudflare_tunnel_replicas,
                cloudflare_tunnel_token: cfg.cloudflare.map(|cf| cf.tunnel.token),
                slack_webhook_url: cfg.slack.map(|sl| sl.webhook_url),
                allow_exec: cfg.allow_exec,
            };

            let probe_host_port = deployment_config
                .cluster
                .as_ref()
                .map(|cluster| cluster.api_port)
                .or(deployment_config.probe_port)
                .unwrap_or_else(|| {
                    let listener = std::net::TcpListener::bind("0.0.0.0:0")
                        .expect("failed to bind probe port");
                    listener
                        .local_addr()
                        .expect("failed to get local addr")
                        .port()
                });
            deployment_config.probe_port = Some(probe_host_port);

            if let Some(cluster) = deployment_config.cluster.as_ref()
                && cluster.role.is_voter()
            {
                let suffix = deployment_config.system_name();
                runtime
                    .remove_container(&format!("maestro-etcd-{suffix}"))
                    .await
                    .map_err(|error| {
                        Error::external(format!(
                            "failed to stop local etcd before recovery coordination: {error}"
                        ))
                    })?;
                runtime
                    .remove_container(&format!("maestro-probe-{suffix}"))
                    .await
                    .map_err(|error| {
                        Error::external(format!(
                            "failed to stop local cluster API before recovery coordination: {error}"
                        ))
                    })?;
                let certs = utils::certs::read_etcd_certs(&deployment_config.certs_dir()).map_err(
                    |error| {
                        Error::invalid_config(format!(
                            "failed to load certificates for automatic cluster recovery: {error}"
                        ))
                    },
                )?;
                let join_secret = deployment_config
                    .join_secret
                    .as_ref()
                    .ok_or_else(|| Error::invalid_config("cluster.join-secret is required"))?;
                match cluster::recovery::coordinate(
                    cluster,
                    &deployment_config.cluster_alias,
                    &deployment_config.data_dir,
                    join_secret.as_str(),
                    &certs,
                    Some(runtime.cli_name()),
                    signal_tx.subscribe(),
                )
                .await
                .map_err(|error| {
                    Error::external(format!("automatic cluster recovery failed: {error}"))
                })? {
                    cluster::recovery::RecoveryDecision::Normal => {}
                    cluster::recovery::RecoveryDecision::WaitForExisting => logger.emit(
                        "warn",
                        "local etcd member data is absent; waiting to rejoin the recovered cluster",
                    ),
                    cluster::recovery::RecoveryDecision::ForceNewCluster => {
                        logger.emit("warn", "rebuilding etcd quorum from this surviving voter");
                    }
                    cluster::recovery::RecoveryDecision::BootstrapClean => logger.emit(
                        "warn",
                        "all configured voters confirmed empty; initializing a clean cluster",
                    ),
                }
            }

            let dns_enabled = deployment_config.cluster.is_some()
                || deployment_config.tailscale_authkey.is_some();
            let dns_subnet = if dns_enabled {
                let subnet = deployment_config.subnet.as_deref().ok_or_else(|| {
                    Error::invalid_config("container subnet is required when DNS is enabled")
                })?;
                Some(cluster::network::Ipv4Cidr::parse(subnet).map_err(|error| {
                    Error::invalid_config(format!(
                        "invalid container subnet for DNS firewall access: {error}"
                    ))
                })?)
            } else {
                None
            };
            let dns_upstreams = if dns_enabled {
                deployment::dns::load_host_resolvers().map_err(|error| {
                    Error::external(format!("failed to load host DNS resolvers: {error}"))
                })?
            } else {
                Vec::new()
            };
            if let Some(subnet) = dns_subnet {
                deployment::dns::validate_host_resolvers(&dns_upstreams, subnet)
                    .map_err(|error| Error::invalid_config(error.to_string()))?;
            }
            let dns_source = dns_subnet
                .map(|subnet| {
                    subnet.system_address_from_end(1).ok_or_else(|| {
                        Error::invalid_config(format!(
                            "container subnet `{subnet}` has no address for the DNS container"
                        ))
                    })
                })
                .transpose()?;
            if !dns_upstreams.is_empty() {
                logger.emit(
                    "info",
                    &format!(
                        "DNS forwarding through host resolver(s): {}",
                        dns_upstreams
                            .iter()
                            .map(Ipv4Addr::to_string)
                            .collect::<Vec<_>>()
                            .join(", ")
                    ),
                );
            }

            let mut system_egress_allows = match (deployment_config.subnet.as_deref(), dns_source) {
                (Some(subnet), Some(source)) => {
                    firewall::dns_allows_for_resolvers(subnet, source, &dns_upstreams)
                }
                _ => Vec::new(),
            };
            system_egress_allows.extend(firewall::unrestricted_allows_for_system_sources(
                &deployment::system_container_egress_sources(&deployment_config),
            ));

            let firewall_config = firewall::FirewallConfig {
                table_name: deployment_config.cluster.as_ref().map_or_else(
                    || firewall::DEFAULT_TABLE_NAME.to_string(),
                    |cluster| format!("maestro_egress_{}", cluster.api_port),
                ),
                subnet: deployment_config.subnet.clone(),
                deny: egress_deny,
                allow: egress_allow,
                system_allows: system_egress_allows,
                service_allows: Vec::new(),
            };
            let firewall_manager = firewall::FirewallManager::new(firewall_config.clone());
            firewall_manager.apply().await.map_err(|err| {
                Error::external(format!("failed to apply egress firewall: {err}"))
            })?;
            let service_egress_firewall =
                (!firewall_config.deny.is_empty()).then(|| firewall_manager.clone());
            if !firewall_config.deny.is_empty() {
                logger.emit(
                    "info",
                    &format!(
                        "egress firewall enabled ({} denied CIDRs)",
                        firewall_config.deny.len()
                    ),
                );
                background_handles.push(firewall::spawn_reconciler(
                    firewall_manager,
                    logger.clone(),
                    signal_tx.subscribe(),
                ));
            }

            let mut supervisor = JobSupervisor::new();
            let mut startup_shutdown_rx = signal_tx.subscribe();
            let system_info = tokio::select! {
                info = deployment::start_system_jobs(
                    &deployment_config,
                    &dns_upstreams,
                    &runtime,
                    &log_sender,
                    &logger,
                    &mut supervisor,
                    signal_tx.subscribe(),
                ) => info,
                _ = startup_shutdown_rx.recv() => {
                    eprintln!("[maestro]: shutdown requested during startup");
                    supervisor.shutdown_all(supervisor::ShutdownRequest::Force).await;
                    return Ok(false);
                }
            };
            background_handles.extend(system_info.cluster_handles);
            let leader_elector = system_info.leader_elector.clone();

            let derived_key = derive_key(deployment_config.encryption_key.as_str());
            let etcd_tls = if deployment_config.disable_etcd_cert {
                None
            } else {
                let certs_dir = deployment_config.certs_dir();
                deployment::build_etcd_tls_from_files(
                    &certs_dir.join("ca.pem").to_string_lossy(),
                    &certs_dir.join("client.pem").to_string_lossy(),
                    &certs_dir.join("client-key.pem").to_string_lossy(),
                )
            };
            if let (Some(cluster), Some(tls)) =
                (deployment_config.cluster.clone(), etcd_tls.clone())
                && cluster.role.is_voter()
            {
                background_handles.push(cluster::recovery::spawn_local_member_monitor(
                    cluster,
                    tls,
                    signal_tx.clone(),
                    signal_tx.subscribe(),
                    logger.clone(),
                ));
            }
            if let (Some(cluster), Some(tls)) =
                (deployment_config.cluster.clone(), etcd_tls.clone())
            {
                background_handles.push(tokio::spawn(cluster::join::run_voter_cache_sync(
                    deployment_config.data_dir.clone(),
                    cluster,
                    deployment_config.etcd_endpoints.clone(),
                    tls,
                    signal_tx.subscribe(),
                    logger.clone(),
                )));
            }
            let mut cluster_registry: Option<std::sync::Arc<dyn cluster::registry::NodeRegistry>> =
                None;
            if let Some(cluster) = deployment_config.cluster.clone() {
                let registry = Arc::new(
                    cluster::registry::EtcdNodeRegistry::connect(
                        &deployment_config.etcd_endpoints,
                        etcd_tls.clone(),
                        cluster.node_id.clone(),
                    )
                    .await
                    .map_err(|err| {
                        Error::external(format!("failed to connect cluster registry: {err}"))
                    })?,
                );
                let data_plane_ready = false;
                let tailscale_container =
                    format!("maestro-tailscale-{}", deployment_config.system_name());
                let tailscale_ip = cluster::data_plane::inspect_tailscale_ip(
                    runtime.as_ref(),
                    &tailscale_container,
                )
                .await;
                let node_info = cluster::NodeInfo {
                    node_id: cluster.node_id.clone(),
                    instance_id: cluster.instance_id.clone(),
                    hostname: cluster::identity::local_hostname(),
                    role: cluster.role,
                    cluster_host_ip: cluster.host_ip,
                    cluster_api_port: cluster.api_port,
                    cluster_gateway_port: cluster.gateway_port,
                    subnet: deployment_config
                        .subnet
                        .clone()
                        .expect("cluster subnet was validated"),
                    tailscale_ip,
                    data_plane_ready,
                    data_plane_checked_at_ms: i64::try_from(
                        utils::time::current_time_millis().unwrap_or_default(),
                    )
                    .unwrap_or_default(),
                    data_plane_error: Some("node gateway validation is pending".to_string()),
                    version: env!("CARGO_PKG_VERSION").to_string(),
                    started_at_ms: i64::try_from(
                        utils::time::current_time_millis().unwrap_or_default(),
                    )
                    .unwrap_or_default(),
                    labels: cluster.labels.clone(),
                };
                background_handles.push(registry.clone().spawn(
                    node_info,
                    signal_tx.subscribe(),
                    logger.clone(),
                ));
                cluster_registry = Some(registry.clone());
                let ingress_gate = deployment_config
                    .cloudflare_tunnel_token
                    .as_ref()
                    .filter(|_| cluster.role.runs_workloads())
                    .map(|_| cluster::data_plane::IngressConnectorGate {
                        network: deployment_config.network.clone(),
                        containers: (1..=deployment_config.cloudflare_tunnel_replicas)
                            .map(|replica| cluster::data_plane::IngressConnector {
                                name: format!(
                                    "maestro-cloudflared-{}-{replica}",
                                    deployment_config.system_name()
                                ),
                                static_ip: deployment::cloudflared_ip_from_cidr(
                                    deployment_config
                                        .subnet
                                        .as_deref()
                                        .expect("cluster subnet was validated"),
                                    replica,
                                )
                                .expect("cloudflared replica address was validated"),
                            })
                            .collect(),
                    });
                background_handles.push(cluster::data_plane::spawn(
                    registry,
                    runtime.clone(),
                    cluster,
                    deployment_config.certs_dir(),
                    ingress_gate,
                    signal_tx.subscribe(),
                    logger.clone(),
                ));
            }
            let store: Arc<dyn deployment::store::ClusterStore> = Arc::new(
                deployment::etcd::EtcdStateStore::new_with_endpoints(
                    &deployment_config.etcd_endpoints,
                    derived_key,
                    etcd_tls.clone(),
                )
                .await?,
            );
            if deployment_config.cluster_mode()
                && cluster::migration::image_publication_required(&deployment_config.data_dir)
            {
                let elector = leader_elector.as_ref().ok_or_else(|| {
                    Error::external("legacy image migration requires a leader elector")
                })?;
                let token = elector
                    .wait_until_leading(std::time::Duration::from_secs(30))
                    .await
                    .map_err(|error| {
                        Error::external(format!(
                            "legacy image migration seed did not acquire leadership: {error}"
                        ))
                    })?;
                cluster::migration::publish_legacy_images(
                    &deployment_config.data_dir,
                    runtime.clone(),
                    store.clone(),
                    &token,
                )
                .await
                .map_err(|error| {
                    Error::external(format!(
                        "failed to make migrated service images cluster-ready; cluster startup cannot continue: {error}"
                    ))
                })?;
                logger.emit(
                    "info",
                    "migrated service images are available in their configured build registries",
                );
            }
            let cluster_assignment_store: Option<
                Arc<dyn cluster::assignment_store::AssignmentStore>,
            > = if deployment_config.cluster.is_some() {
                Some(Arc::new(
                    cluster::assignment_store::EtcdAssignmentStore::connect(
                        &deployment_config.etcd_endpoints,
                        etcd_tls.clone(),
                    )
                    .await
                    .map_err(|err| {
                        Error::external(format!(
                            "failed to connect cluster assignment store: {err}"
                        ))
                    })?,
                ))
            } else {
                None
            };
            let upgrade_orchestrator = match (
                deployment_config.cluster.as_ref(),
                leader_elector.clone(),
                cluster_registry.clone(),
                cluster_assignment_store.clone(),
            ) {
                (Some(cluster), Some(elector), Some(registry), Some(assignments)) => {
                    Some(Arc::new(
                        cluster::upgrade::ClusterUpgradeOrchestrator::new(
                            cluster.node_id.clone(),
                            cluster.host_ip.into(),
                            &deployment_config.certs_dir(),
                            elector,
                            registry,
                            assignments,
                            store.clone(),
                            deployment_config.jwt_secret_key.clone(),
                            logger.clone(),
                        )
                        .map_err(|error| {
                            Error::external(format!(
                                "failed to initialize cluster maintenance: {error}"
                            ))
                        })?,
                    ))
                }
                _ => None,
            };
            if let Some(upgrade) = &upgrade_orchestrator {
                background_handles.push(tokio::spawn(upgrade.clone().run(signal_tx.subscribe())));
            }
            let join = match (
                deployment_config.cluster.clone(),
                deployment_config.join_secret.as_ref(),
                etcd_tls.clone(),
            ) {
                (Some(cluster), Some(join_secret), Some(tls)) => {
                    Some(cluster::join::JoinCoordinator::new(
                        cluster,
                        deployment_config.cluster_alias.clone(),
                        deployment_config.data_dir.clone(),
                        join_secret.as_str().to_string(),
                        deployment_config.etcd_endpoints.clone(),
                        tls,
                    ))
                }
                _ => None,
            };
            let local_node_id = deployment_config
                .cluster
                .as_ref()
                .map(|cluster| cluster.node_id.clone());
            let runtime_suffix = deployment_config
                .cluster
                .as_ref()
                .and_then(cluster::ClusterRuntime::resource_suffix);
            let control = cluster::control::ControlServer::new(
                deployment_config
                    .data_dir
                    .join("system/control/control.sock"),
                deployment_config
                    .internal_control_token
                    .as_str()
                    .to_string(),
                leader_elector.clone(),
                cluster_registry.clone(),
                join,
                upgrade_orchestrator.clone(),
                store.clone(),
                runtime.clone(),
                deployment_config.allow_exec,
                deployment_config.data_dir.clone(),
                local_node_id,
                runtime_suffix,
                logger.clone(),
            );
            background_handles.push(tokio::spawn(control.run(signal_tx.subscribe())));
            if let Some(cluster) = &deployment_config.cluster {
                background_handles.push(tokio::spawn(cluster::telemetry::run_disk_reporter(
                    store.clone(),
                    cluster.node_id.clone(),
                    signal_tx.subscribe(),
                    logger.clone(),
                )));
            }
            if deployment_config.cluster_mode() {
                let cluster = deployment_config
                    .cluster
                    .as_ref()
                    .expect("cluster mode requires cluster runtime");
                let assignment_store = cluster_assignment_store
                    .clone()
                    .expect("cluster assignment store was initialized");
                let traffic_manager = Arc::new(
                    cluster::traefik::EtcdTrafficManager::connect(
                        &deployment_config.etcd_endpoints,
                        etcd_tls.clone(),
                    )
                    .await
                    .map_err(|err| {
                        Error::external(format!("failed to connect cluster routing store: {err}"))
                    })?,
                );
                if cluster.role.runs_workloads() {
                    background_handles.push(tokio::spawn(cluster::traefik::run_dns_sync(
                        traffic_manager.clone(),
                        system_info.dns_manager.clone(),
                        system_info
                            .ingress_ip
                            .clone()
                            .expect("cluster ingress requires a static network address"),
                        signal_tx.subscribe(),
                        logger.clone(),
                    )));
                }
                if cluster.role.runs_workloads() {
                    let mut executor = cluster::executor::EngineReplicaExecutor::new(
                        &deployment_config,
                        runtime.clone(),
                        store.clone(),
                        assignment_store.clone(),
                        Some(log_sender.clone()),
                    )?;
                    if let Some(firewall) = service_egress_firewall.clone() {
                        executor.set_egress_firewall(firewall);
                    }
                    let reconciler = cluster::reconciler::AssignmentReconciler::new(
                        cluster.node_id.clone(),
                        assignment_store.clone(),
                        executor,
                        logger.clone(),
                    );
                    background_handles.push(tokio::spawn(reconciler.run(signal_tx.subscribe())));
                }
                if cluster.role.is_voter()
                    && let (Some(elector), Some(registry)) =
                        (leader_elector.clone(), cluster_registry.clone())
                {
                    let slack = slack::SlackNotifier::new(
                        deployment_config.slack_webhook_url.clone(),
                        Some(store.clone()),
                        deployment_config.cluster_name.clone(),
                        logger.clone(),
                    );
                    let leader_loop = cluster::leader_loop::LeaderLoop::new(
                        cluster.cluster_id.clone(),
                        deployment_config.cluster_name.clone(),
                        elector,
                        assignment_store,
                        registry,
                        store.clone(),
                        traffic_manager,
                        logger.clone(),
                        slack,
                    );
                    background_handles.push(tokio::spawn(leader_loop.run(signal_tx.subscribe())));
                }
            }
            let probe_log_endpoint = format!("http://127.0.0.1:{probe_host_port}/api/logs");
            let node_id = load_or_create_node_id(&deployment_config.data_dir)?;
            let ingestion_token = deployment_config.ingestion_token.as_str().to_string();
            let http_sink = logs::HttpSink::new("controller", node_id, &probe_log_endpoint)
                .with_ingestion_token(&ingestion_token);
            let sink_worker = logs::SinkWorker::new(
                log_store.clone(),
                Box::new(http_sink),
                signal_tx.subscribe(),
            )
            .with_runtime_stats(sink_runtime_stats.clone());
            background_handles.push(sink_worker.spawn());

            let metrics_endpoint = format!("http://127.0.0.1:{probe_host_port}/api/metrics");
            let stats_reporter = cluster_stats::ClusterStatsReporter::new(
                metrics_endpoint.clone(),
                log_store.clone(),
                sink_runtime_stats,
                signal_tx.subscribe(),
                Some(ingestion_token.clone()),
            );
            background_handles.push(tokio::spawn(stats_reporter.run()));
            let deployment_signal_rx = signal_tx.subscribe();
            let cluster_mode = deployment_config.cluster_mode();
            let maintenance_only = deployment_config
                .cluster
                .as_ref()
                .is_some_and(|cluster| !cluster.role.is_voter());
            let watcher_store = store.clone();
            let watcher_data_dir = deployment_config.data_dir.clone();
            let watcher_logger = logger.clone();
            let preview_api = github_config
                .as_ref()
                .map(|github| builder::GithubClient::new(github.token.as_str()))
                .transpose()
                .map_err(|error| {
                    Error::invalid_config(format!("failed to initialize GitHub client: {error}"))
                })?
                .map(|client| Arc::new(client) as Arc<dyn builder::PullRequestApi>);
            let preview_watcher_config =
                github_config
                    .as_ref()
                    .map(|github| builder::PrWatcherConfig {
                        preview_domain: github.preview_domain.clone(),
                        poll_interval: std::time::Duration::from_secs(github.poll_interval_secs),
                        max_concurrent_previews: github.max_concurrent_previews,
                        homepage: homepage.clone(),
                    });
            let watcher_handle = if cluster_mode {
                None
            } else {
                Some(tokio::spawn(
                    builder::BuildWatcher::new(
                        watcher_store.clone(),
                        watcher_data_dir.clone(),
                        signal_tx.subscribe(),
                        watcher_logger.clone(),
                    )
                    .run(),
                ))
            };
            let preview_watcher_handle = if cluster_mode {
                None
            } else if let (Some(api), Some(config)) =
                (preview_api.clone(), preview_watcher_config.clone())
            {
                Some(tokio::spawn(
                    builder::PrWatcher::new(
                        watcher_store.clone(),
                        api,
                        config,
                        signal_tx.subscribe(),
                        watcher_logger.clone(),
                    )
                    .run(),
                ))
            } else {
                None
            };

            let metrics_signal_rx = signal_tx.subscribe();
            let metrics_collector = metrics::MetricsCollector::new(
                metrics_endpoint,
                deployment_config.system_name(),
                runtime.cli_name().to_string(),
                metrics_signal_rx,
                logger,
                metrics_datadog_tx,
                Some(ingestion_token),
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
            if !cluster_mode && let Some(firewall) = service_egress_firewall {
                controller.set_egress_firewall(firewall);
            }
            if cluster_mode && let Some(elector) = &leader_elector {
                controller
                    .observe_leadership(cluster::elector::LeaderElector::watch(elector.as_ref()));
            }
            let deployment_shutdown_tx = signal_tx.clone();
            let deployment_wait_shutdown_rx = signal_tx.subscribe();

            let result = async move {
                let mut wait_shutdown = deployment_wait_shutdown_rx;
                if maintenance_only {
                    let exit_reason = controller
                        .run_node_maintenance()
                        .await
                        .map_err(Error::from)?;
                    let _ = deployment_shutdown_tx.send(signal::ShutdownEvent::Graceful);
                    return Ok((exit_reason, controller));
                }
                loop {
                    if let Some(elector) = &leader_elector
                        && cluster_mode
                    {
                        let mut leadership =
                            cluster::elector::LeaderElector::watch(elector.as_ref());
                        loop {
                            if matches!(
                                leadership.borrow().clone(),
                                cluster::types::LeadershipState::Leading(_)
                            ) {
                                break;
                            }
                            tokio::select! {
                                changed = leadership.changed() => {
                                    if changed.is_err() {
                                        return Err(Error::external("cluster leader elector stopped"));
                                    }
                                }
                                _ = wait_shutdown.recv() => {
                                    return Ok((deployment::controller::ControllerExitReason::Shutdown, controller));
                                }
                            }
                        }
                    }
                    let scoped_watcher = cluster_mode.then(|| {
                        tokio::spawn(
                            builder::BuildWatcher::new(
                                watcher_store.clone(),
                                watcher_data_dir.clone(),
                                deployment_shutdown_tx.subscribe(),
                                watcher_logger.clone(),
                            )
                            .run(),
                        )
                    });
                    let scoped_preview_watcher = if cluster_mode {
                        if let (Some(api), Some(config)) =
                            (preview_api.clone(), preview_watcher_config.clone())
                        {
                            Some(tokio::spawn(
                                builder::PrWatcher::new(
                                    watcher_store.clone(),
                                    api,
                                    config,
                                    deployment_shutdown_tx.subscribe(),
                                    watcher_logger.clone(),
                                )
                                .run(),
                            ))
                        } else {
                            None
                        }
                    } else {
                        None
                    };
                    let controller_result = controller.run().await;
                    if let Some(handle) = scoped_watcher {
                        handle.abort();
                    }
                    if let Some(handle) = scoped_preview_watcher {
                        handle.abort();
                    }
                    let exit_reason = controller_result.map_err(Error::from)?;
                    if exit_reason == deployment::controller::ControllerExitReason::Demoted {
                        continue;
                    }
                    let _ = deployment_shutdown_tx.send(signal::ShutdownEvent::Graceful);
                    return Ok((exit_reason, controller));
                }
            }
            .await;

            if let Some(handle) = watcher_handle {
                handle.abort();
            }
            if let Some(handle) = preview_watcher_handle {
                handle.abort();
            }
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
                        config.unwrap_or_else(|| PathBuf::from(DEFAULT_SERVICES_CONFIG_PATH));
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
                        config.unwrap_or_else(|| PathBuf::from(DEFAULT_SERVICES_CONFIG_PATH));
                    let context_dir = context.unwrap_or_else(|| PathBuf::from("."));
                    cli::up::run_up(&host, &config_path, &context_dir)
                        .await
                        .map(|()| false)
                }
            }
        }
        Some(CliCommand::Cluster { command }) => match command {
            ClusterCommand::Enable { config, data_dir } => {
                enable_legacy_cluster(&config, &data_dir)
                    .await
                    .map(|()| false)
            }
            ClusterCommand::InitCa { config, data_dir } => {
                init_cluster_ca(&config, &data_dir).await.map(|()| false)
            }
            ClusterCommand::IssueNode {
                config,
                data_dir,
                host_ip,
                api_port,
                node_id,
                subnet,
                role,
                output,
            } => issue_cluster_node(
                &config,
                &data_dir,
                host_ip,
                api_port,
                &node_id,
                &subnet,
                role,
                output.as_deref(),
            )
            .await
            .map(|()| false),
            ClusterCommand::Join {
                leader,
                prepare,
                config,
                data_dir,
            } => {
                if prepare {
                    if leader.is_some() {
                        return Err(Error::invalid_input(
                            "leader address is not accepted with --prepare",
                        ));
                    }
                    cli::cluster_lifecycle::prepare_join(&config, &data_dir)
                        .await
                        .map(|()| false)
                } else {
                    let leader = leader.ok_or_else(|| {
                        Error::invalid_input("leader address is required unless --prepare is used")
                    })?;
                    cli::cluster_lifecycle::join_cluster(&leader, &config, &data_dir)
                        .await
                        .map(|()| false)
                }
            }
            ClusterCommand::ApproveNode {
                role,
                node_id,
                host_ip,
                api_port,
                subnet,
                public_key_sha256,
            } => {
                let host = cli::contexts::active_host()?;
                cli::cluster_lifecycle::approve_node(
                    &host,
                    node_id,
                    role,
                    host_ip,
                    api_port,
                    subnet,
                    public_key_sha256,
                )
                .await
                .map(|()| false)
            }
            ClusterCommand::Info => {
                let host = cli::contexts::active_host()?;
                cli::info::run_info(&host).await.map(|()| false)
            }
            ClusterCommand::Nodes => {
                let host = cli::contexts::active_host()?;
                cli::nodes::run_nodes(&host).await.map(|()| false)
            }
            ClusterCommand::Drain { node_id } => {
                let host = cli::contexts::active_host()?;
                cli::nodes::set_drain_state(&host, &node_id, true)
                    .await
                    .map(|()| false)
            }
            ClusterCommand::Restore { node_id } => {
                let host = cli::contexts::active_host()?;
                cli::nodes::set_drain_state(&host, &node_id, false)
                    .await
                    .map(|()| false)
            }
            ClusterCommand::RemoveNode { node_id } => {
                let host = cli::contexts::active_host()?;
                cli::cluster_lifecycle::remove_node(&host, &node_id)
                    .await
                    .map(|()| false)
            }
            ClusterCommand::Config => {
                let host = cli::contexts::active_host()?;
                cli::config::run_config(&host).await.map(|()| false)
            }
            ClusterCommand::Restart {
                node_id,
                all,
                local,
                yes,
            } => {
                let host = cli::contexts::active_host()?;
                if local {
                    cli::restart::run_restart(&host, yes).await
                } else {
                    cli::restart::run_coordinated_restart(&host, node_id.as_deref(), all, yes).await
                }
                .map(|()| false)
            }
            ClusterCommand::Upgrade { target: _, yes } => {
                let host = cli::contexts::active_host()?;
                cli::upgrade::run_upgrade(&host, yes).await.map(|()| false)
            }
            ClusterCommand::Unfreeze { upgrade_run } => {
                let host = cli::contexts::active_host()?;
                cli::upgrade::run_cluster_unfreeze(&host, &upgrade_run)
                    .await
                    .map(|()| false)
            }
        },
        Some(CliCommand::Logs(args)) => {
            let host = cli::contexts::active_host()?;
            cli::logs::run_logs(&host, args).await.map(|()| false)
        }
        Some(CliCommand::Ssh(args)) => {
            let host = cli::contexts::active_host()?;
            let exit_code = cli::ssh::run_ssh(&host, args).await?;
            if exit_code != 0 {
                std::process::exit(exit_code);
            }
            Ok(false)
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
            ConfigCommand::Validate { source } => {
                cli::config::run_validate(&source).await.map(|()| false)
            }
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

fn load_or_create_ingestion_token(
    data_dir: &Path,
) -> crate::error::Result<utils::crypto::SecretString> {
    let directory = data_dir.join("system/probe");
    std::fs::create_dir_all(&directory).map_err(|err| {
        Error::internal(format!(
            "failed to create probe secret directory {}: {err}",
            directory.display()
        ))
    })?;
    let path = directory.join("ingestion-token");
    if let Ok(value) = std::fs::read_to_string(&path) {
        let value = value.trim();
        if value.len() >= 32 {
            return Ok(utils::crypto::SecretString::new(value.to_string()));
        }
        return Err(Error::invalid_config(format!(
            "invalid ingestion token in {}",
            path.display()
        )));
    }
    let value = utils::nanoid::unique_id(48);
    let temporary = directory.join(format!("ingestion-token.tmp-{}", std::process::id()));
    std::fs::write(&temporary, format!("{value}\n")).map_err(|err| {
        Error::internal(format!(
            "failed to write ingestion token {}: {err}",
            temporary.display()
        ))
    })?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&temporary, std::fs::Permissions::from_mode(0o600))
            .map_err(|err| Error::internal(format!("failed to protect ingestion token: {err}")))?;
    }
    std::fs::rename(&temporary, &path).map_err(|err| {
        Error::internal(format!(
            "failed to persist ingestion token {}: {err}",
            path.display()
        ))
    })?;
    Ok(utils::crypto::SecretString::new(value))
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

async fn enable_legacy_cluster(
    config_source: &str,
    base_data_dir: &Path,
) -> crate::error::Result<()> {
    let config = config::load_config(config_source)
        .await
        .map_err(|err| Error::invalid_config(err.to_string()))?;
    cluster::network::validate_cluster_config(
        &config.cluster,
        config.subnet.as_deref(),
        config.node.role,
    )
    .map_err(|err| Error::invalid_config(err.to_string()))?;
    if config.cluster.nodes.is_empty() || config.node.role != cluster::NodeRole::Master {
        return Err(Error::invalid_config(
            "cluster enable requires the configured master node",
        ));
    }
    if config
        .cluster
        .join_secret
        .as_deref()
        .is_none_or(|secret| secret.len() < 32)
    {
        return Err(Error::invalid_config(
            "cluster.join-secret must contain at least 32 characters",
        ));
    }
    if config
        .jwt_secret_key
        .as_deref()
        .is_none_or(|secret| secret.len() < 32)
    {
        return Err(Error::invalid_config(
            "jwt-secret-key must contain at least 32 characters",
        ));
    }
    let data_dir = base_data_dir.join(config.cluster.name.to_lowercase());
    std::fs::create_dir_all(&data_dir)?;
    let _lock = acquire_lock(&data_dir)?;
    let legacy_etcd = cluster::migration::legacy_etcd_container_name(&config.cluster, &data_dir)
        .map_err(|error| Error::invalid_config(error.to_string()))?;
    runtime::create_provider(config.runtime)
        .remove_container(&legacy_etcd)
        .await
        .map_err(|error| {
            Error::internal(format!(
                "failed to stop legacy etcd container `{legacy_etcd}`: {error}"
            ))
        })?;
    let host_ip = cluster::network::resolve_cluster_host_ip(
        &config.cluster,
        &data_dir,
        config.node.role,
        config.subnet.as_deref(),
    )
    .map_err(|err| Error::invalid_config(err.to_string()))?
    .ok_or_else(|| Error::invalid_config("failed to resolve cluster host IP"))?;
    let migration =
        cluster::migration::migrate(&config.cluster, config.node.role, &data_dir, host_ip)
            .map_err(|error| Error::invalid_config(error.to_string()))?;
    println!("[maestro]: legacy single-node cluster migration prepared");
    println!("cluster id: {}", migration.cluster_id);
    println!("CA SHA-256: {}", migration.ca_sha256);
    println!("offline etcd backup: {}", migration.etcd_backup.display());
    println!(
        "recovery manifest: {}",
        migration.etcd_backup_manifest.display()
    );
    println!(
        "legacy certificate backup: {}",
        data_dir.join("system/certs.legacy-backup").display()
    );
    Ok(())
}

async fn init_cluster_ca(config_source: &str, base_data_dir: &Path) -> crate::error::Result<()> {
    let config = config::load_config(config_source)
        .await
        .map_err(|err| Error::invalid_config(err.to_string()))?;
    cluster::network::validate_cluster_config(
        &config.cluster,
        config.subnet.as_deref(),
        config.node.role,
    )
    .map_err(|err| Error::invalid_config(err.to_string()))?;
    if config.cluster.nodes.is_empty() {
        return Err(Error::invalid_config(
            "cluster.nodes is required for multi-node CA initialization",
        ));
    }
    if config.node.role != cluster::NodeRole::Master {
        return Err(Error::invalid_config(
            "cluster init-ca must run on the configured master node",
        ));
    }
    let data_dir = base_data_dir.join(config.cluster.name.to_lowercase());
    std::fs::create_dir_all(&data_dir)?;
    let host_ip = cluster::network::resolve_cluster_host_ip(
        &config.cluster,
        &data_dir,
        config.node.role,
        config.subnet.as_deref(),
    )
    .map_err(|err| Error::invalid_config(err.to_string()))?
    .ok_or_else(|| Error::invalid_config("cluster host IP was not resolved"))?;
    let local_endpoint = config
        .cluster
        .local_endpoint(host_ip, config.node.role)
        .map_err(|err| Error::invalid_config(err.to_string()))?;
    let (master_name, master) = config
        .cluster
        .master_node()
        .map_err(|err| Error::invalid_config(err.to_string()))?;
    if master.endpoint.host_ip() != local_endpoint.host_ip {
        return Err(Error::invalid_config(format!(
            "cluster init-ca must run on master node `{master_name}` ({}), resolved this node as {}",
            master.endpoint,
            local_endpoint.api_address()
        )));
    }
    let ca_dir = data_dir.join("system/certs/cluster-ca");
    if ca_dir.exists() || data_dir.join("system/cluster-id").exists() {
        return Err(Error::conflict(format!(
            "cluster identity or CA already exists under {}",
            data_dir.display()
        )));
    }
    let ca = utils::certs::generate_cluster_ca()
        .map_err(|err| Error::internal(format!("failed to generate cluster CA: {err}")))?;
    let cluster_id = cluster::identity::create_cluster_id(&data_dir)
        .map_err(|err| Error::internal(err.to_string()))?;
    utils::certs::write_cluster_ca(&ca_dir, &ca)
        .map_err(|err| Error::internal(format!("failed to persist cluster CA: {err}")))?;
    let provision_dir = data_dir.join("system/cluster-provision");
    for voter in config
        .cluster
        .nodes
        .values()
        .filter(|node| node.role.is_voter())
    {
        let host_ip = voter.endpoint.host_ip();
        let api_port = voter.endpoint.explicit_api_port().unwrap_or(3000);
        let certs = utils::certs::generate_cluster_node_certs_for_endpoint(
            &ca, host_ip, api_port, voter.role,
        )
        .map_err(|err| {
            Error::internal(format!(
                "failed to issue certificates for {}: {err}",
                voter.endpoint
            ))
        })?;
        let voter_dir = provision_dir.join(cluster::identity::endpoint_identity_suffix(
            host_ip, api_port,
        ));
        utils::certs::write_etcd_certs(&voter_dir, &certs).map_err(|err| {
            Error::internal(format!(
                "failed to persist certificates for {}: {err}",
                voter.endpoint
            ))
        })?;
        if host_ip == local_endpoint.host_ip && api_port == local_endpoint.api_port {
            utils::certs::write_etcd_certs(&data_dir.join("system/certs"), &certs).map_err(
                |err| Error::internal(format!("failed to install seed certificates: {err}")),
            )?;
        }
    }
    cluster::bootstrap::arm_seed(&data_dir, &cluster_id, host_ip)
        .map_err(|err| Error::internal(format!("failed to arm bootstrap seed: {err}")))?;
    let fingerprint = certificate_fingerprint(&ca.cert_pem)?;
    println!("cluster id: {cluster_id}");
    println!("bootstrap node: {}", local_endpoint.api_address());
    println!("CA SHA-256: {fingerprint}");
    println!("cluster material: {}", data_dir.join("system").display());
    println!("host certificate bundles: {}", provision_dir.display());
    Ok(())
}

async fn issue_cluster_node(
    config_source: &str,
    base_data_dir: &Path,
    host_ip: Ipv4Addr,
    api_port: Option<u16>,
    node_id: &str,
    subnet: &str,
    role: cluster::NodeRole,
    output: Option<&Path>,
) -> crate::error::Result<()> {
    let config = config::load_config(config_source)
        .await
        .map_err(|err| Error::invalid_config(err.to_string()))?;
    cluster::network::validate_cluster_config(
        &config.cluster,
        config.subnet.as_deref(),
        config.node.role,
    )
    .map_err(|err| Error::invalid_config(err.to_string()))?;
    if !host_ip.is_private() || host_ip.is_loopback() || host_ip.is_unspecified() {
        return Err(Error::invalid_input(
            "--host-ip must be a private, non-loopback IPv4 address",
        ));
    }
    if !cluster::network::control_ip_allowed(&config.cluster, host_ip)
        .map_err(|err| Error::invalid_config(err.to_string()))?
    {
        return Err(Error::invalid_input(
            "--host-ip is absent from cluster.control-allow-cidrs",
        ));
    }
    let subnet = cluster::network::Ipv4Cidr::parse(subnet)
        .map_err(|err| Error::invalid_input(err.to_string()))?;
    if subnet.prefix() != 24 || !subnet.is_private() {
        return Err(Error::invalid_input("--subnet must be a private IPv4 /24"));
    }
    if subnet.contains(host_ip)
        || config.cluster.control_allow_cidrs.iter().any(|cidr| {
            cluster::network::Ipv4Cidr::parse(cidr).is_ok_and(|control| subnet.overlaps(control))
        })
    {
        return Err(Error::invalid_input(
            "--subnet must not overlap the node host IP or cluster control network",
        ));
    }
    let data_dir = base_data_dir.join(config.cluster.name.to_lowercase());
    let ca = utils::certs::load_cluster_ca(&data_dir.join("system/certs/cluster-ca")).map_err(
        |err| Error::invalid_config(format!("failed to load initialized cluster CA: {err}")),
    )?;
    let resolved_api_port = api_port.unwrap_or_else(|| {
        config
            .cluster
            .nodes
            .values()
            .find(|node| node.endpoint.host_ip() == host_ip)
            .and_then(|node| node.endpoint.explicit_api_port())
            .unwrap_or(3000)
    });
    if role == cluster::NodeRole::Worker
        && config.cluster.nodes.values().any(|node| {
            node.role.is_voter()
                && node.endpoint.host_ip() == host_ip
                && node.endpoint.explicit_api_port().unwrap_or(3000) == resolved_api_port
        })
    {
        return Err(Error::invalid_input(
            "a worker endpoint cannot also be configured as a voter in cluster.nodes",
        ));
    }
    let certs = utils::certs::generate_cluster_node_certs_for_endpoint(
        &ca,
        host_ip,
        resolved_api_port,
        role,
    )
    .map_err(|err| Error::internal(format!("failed to issue node certificates: {err}")))?;
    let issuer_certs =
        utils::certs::read_etcd_certs(&data_dir.join("system/certs")).map_err(|err| {
            Error::invalid_config(format!("failed to load issuer node certificate: {err}"))
        })?;
    let cluster_id = cluster::identity::load_cluster_id(&data_dir)
        .map_err(|err| Error::invalid_config(err.to_string()))?;
    let voter_endpoints = cluster::join::load_voter_cache(&data_dir, &cluster_id)
        .map_err(|err| Error::invalid_config(err.to_string()))?
        .ok_or_else(|| Error::invalid_config("cluster voter cache is missing"))?
        .client_endpoints();
    cluster::auth::provision_node_users(
        &voter_endpoints,
        deployment::build_etcd_tls_options(Some(&issuer_certs))
            .expect("cluster certificate produces TLS options"),
        host_ip,
        resolved_api_port,
        node_id,
        &subnet.to_string(),
        role,
    )
    .await
    .map_err(|err| Error::external(format!("failed to provision etcd RBAC identity: {err}")))?;
    let output =
        output.map(Path::to_path_buf).unwrap_or_else(|| {
            data_dir.join("system/cluster-provision").join(
                cluster::identity::endpoint_identity_suffix(host_ip, resolved_api_port),
            )
        });
    utils::certs::write_etcd_certs(&output, &certs).map_err(|err| {
        Error::internal(format!("failed to write node certificate bundle: {err}"))
    })?;
    println!("issued {role} certificate bundle for {host_ip}");
    println!("bundle: {}", output.display());
    if role.is_voter() {
        println!(
            "copy the voter CA directory separately from {}",
            data_dir.join("system/certs/cluster-ca").display()
        );
    }
    Ok(())
}

fn certificate_fingerprint(certificate_pem: &str) -> crate::error::Result<String> {
    utils::certs::certificate_fingerprint(certificate_pem)
        .map_err(|error| Error::invalid_config(error.to_string()))
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
mod start_config_override_tests {
    use super::{StartConfigOverrides, apply_start_config_overrides};

    #[test]
    fn explicit_cli_values_override_config_without_erasing_siblings() {
        let mut config: crate::config::StartConfig = json5::from_str(
            r#"{
                cluster: { name: "config-name" },
                ingress: { port: 8080 },
                subnet: "172.22.1.0/24",
                egress: {
                    deny: ["10.0.0.0/8"],
                    allow: ["10.1.0.0/16"]
                },
                "encryption-key": "config-key",
                "jwt-secret-key": "config-jwt",
                tags: ["source:config"],
                tailscale: {
                    "auth-key": "config-auth",
                    "advertise-routes": ["172.22.1.0/24"]
                },
                datadog: {
                    "api-key": "config-dd",
                    site: "datadoghq.com",
                    "include-ingress-logs": true,
                    "include-tailscale-logs": true,
                    "include-metrics": true
                },
                cloudflare: {
                    tunnel: { token: "config-cf", replicas: 4 }
                }
            }"#,
        )
        .expect("base config");

        apply_start_config_overrides(
            &mut config,
            StartConfigOverrides {
                cluster_name: Some("cli-name".to_string()),
                encryption_key: Some("cli-key".to_string()),
                role: Some(crate::cluster::NodeRole::Worker),
                jwt_secret_key: Some("cli-jwt".to_string()),
                tailscale_authkey: Some("cli-auth".to_string()),
                tailscale_advertise_routes: vec!["172.22.2.0/24".to_string()],
                datadog_api_key: Some("cli-dd".to_string()),
                datadog_site: Some("datadoghq.eu".to_string()),
                datadog_no_ingress_logs: true,
                datadog_no_tailscale_logs: true,
                cloudflare_tunnel_token: Some("cli-cf".to_string()),
                subnet: Some("172.22.2.0/24".to_string()),
                egress_deny: vec!["192.168.0.0/16".to_string()],
                egress_allow: vec!["192.168.1.0/24".to_string()],
                tags: vec!["source:cli".to_string()],
                runtime: Some(crate::config::RuntimeType::Nerdctl),
                system: Some(crate::config::SystemType::Nixos),
                disable_etcd_cert: true,
            },
        );

        assert_eq!(config.cluster.name, "cli-name");
        assert_eq!(config.encryption_key, "cli-key");
        assert_eq!(config.node.role, crate::cluster::NodeRole::Worker);
        assert_eq!(config.jwt_secret_key.as_deref(), Some("cli-jwt"));
        assert_eq!(config.subnet.as_deref(), Some("172.22.2.0/24"));
        assert_eq!(config.tags, vec!["source:cli"]);
        assert_eq!(config.egress.deny, vec!["192.168.0.0/16"]);
        assert_eq!(config.egress.allow, vec!["192.168.1.0/24"]);
        assert_eq!(config.runtime, crate::config::RuntimeType::Nerdctl);
        assert_eq!(config.system, Some(crate::config::SystemType::Nixos));
        assert!(config.disable_etcd_cert);

        let tailscale = config.tailscale.expect("tailscale");
        assert_eq!(tailscale.auth_key, "cli-auth");
        assert_eq!(tailscale.advertise_routes, vec!["172.22.2.0/24"]);

        let datadog = config.datadog.expect("datadog");
        assert_eq!(datadog.api_key, "cli-dd");
        assert_eq!(datadog.site.as_deref(), Some("datadoghq.eu"));
        assert!(!datadog.include_ingress_logs);
        assert!(!datadog.include_tailscale_logs);
        assert!(datadog.include_metrics);

        let tunnel = config.cloudflare.expect("cloudflare").tunnel;
        assert_eq!(tunnel.token.as_str(), "cli-cf");
        assert_eq!(tunnel.replicas, Some(4));
    }
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

#[cfg(test)]
mod cluster_upgrade_cli_tests {
    use clap::Parser;

    use super::{Cli, CliCommand, ClusterCommand};

    #[test]
    fn coordinated_upgrade_does_not_require_a_version_flag() {
        let cli = Cli::try_parse_from(["maestro", "cluster", "upgrade"])
            .expect("parse coordinated cluster upgrade");
        assert!(matches!(
            cli.command,
            Some(CliCommand::Cluster {
                command: ClusterCommand::Upgrade {
                    target: None,
                    yes: false,
                }
            })
        ));
    }

    #[test]
    fn system_upgrade_alias_uses_the_same_command_without_a_version_override() {
        let cli = Cli::try_parse_from(["maestro", "cluster", "upgrade", "system"])
            .expect("parse system upgrade alias");
        assert!(matches!(
            cli.command,
            Some(CliCommand::Cluster {
                command: ClusterCommand::Upgrade {
                    target: Some(super::UpgradeTarget::System),
                    yes: false,
                }
            })
        ));
        assert!(
            Cli::try_parse_from(["maestro", "cluster", "upgrade", "--version", "0.3.4"]).is_err()
        );
    }

    #[test]
    fn cluster_restart_accepts_a_node_or_all_but_not_both() {
        let selected = Cli::try_parse_from(["maestro", "cluster", "restart", "node-a"])
            .expect("parse selected-node restart");
        assert!(matches!(
            selected.command,
            Some(CliCommand::Cluster {
                command: ClusterCommand::Restart {
                    node_id: Some(node_id),
                    all: false,
                    local: false,
                    yes: false,
                }
            }) if node_id == "node-a"
        ));

        let all = Cli::try_parse_from(["maestro", "cluster", "restart", "--all", "--yes"])
            .expect("parse all-node restart");
        assert!(matches!(
            all.command,
            Some(CliCommand::Cluster {
                command: ClusterCommand::Restart {
                    node_id: None,
                    all: true,
                    local: false,
                    yes: true,
                }
            })
        ));
        assert!(Cli::try_parse_from(["maestro", "cluster", "restart", "node-a", "--all"]).is_err());
    }

    #[test]
    fn explicit_local_restart_preserves_the_immediate_path() {
        let cli = Cli::try_parse_from(["maestro", "cluster", "restart", "--local"])
            .expect("parse local restart");
        assert!(matches!(
            cli.command,
            Some(CliCommand::Cluster {
                command: ClusterCommand::Restart {
                    node_id: None,
                    all: false,
                    local: true,
                    yes: false,
                }
            })
        ));
    }
}
