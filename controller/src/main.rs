mod builder;
mod cli;
mod config;
mod deployment;
mod error;
mod logs;
mod metrics;
mod probe;
mod runtime;
mod server;
mod signal;
mod supervisor;
mod utils;
mod validation;

use utils::crypto::{SecretString, derive_key};

use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use clap::{CommandFactory, Parser, Subcommand};
use error::Error;
use signal::spawn_shutdown_signal_bus;

use crate::{
    deployment::{DeploymentConfig, controller::DeploymentController},
    supervisor::controller::JobSupervisor,
};

const DEFAULT_CLUSTER_CONFIG_PATH: &str = "maestro.cluster.jsonc";
const DEFAULT_API_PORT: u16 = 3001;
const DEFAULT_ROLLOUT_HOST: &str = "http://127.0.0.1:3001";

#[derive(Debug, Parser)]
#[command(name = "maestro", disable_help_subcommand = true)]
struct Cli {
    #[command(subcommand)]
    command: Option<CliCommand>,
}

#[derive(Debug, Subcommand)]
enum CliCommand {
    /// Start the cluster controller and all system services
    Start {
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
            long = "jwt-secret",
            env = "MAESTRO_JWT_SECRET",
            help = "Secret for signing JWT auth tokens (enables rollout authentication)"
        )]
        jwt_secret: Option<String>,
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
            long = "datadog-include-system-logs",
            help = "Include maestro system logs in Datadog (excluded by default)"
        )]
        dd_include_system_logs: bool,
        #[arg(long = "system", help = "Host system type for upgrades (e.g., nixos)")]
        system: Option<config::SystemType>,
        #[arg(long = "runtime", help = "Container runtime: docker or nerdctl")]
        runtime: Option<config::RuntimeType>,
        #[arg(long = "project-dir", help = "Path to the maestro project directory")]
        project_dir: PathBuf,
    },
    /// Deploy services from the config file (dry run by default)
    Rollout {
        #[arg(long = "host", default_value = DEFAULT_ROLLOUT_HOST, help = "Maestro API host")]
        host: String,
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
            long = "jwt-secret",
            env = "MAESTRO_JWT_SECRET",
            help = "Secret for signing JWT auth tokens"
        )]
        jwt_secret: Option<String>,
    },
    /// Trigger a redeployment of a running service
    Redeploy {
        #[arg(help = "Service ID to redeploy")]
        service_id: String,
        #[arg(long = "host", default_value = DEFAULT_ROLLOUT_HOST, help = "Maestro API host")]
        host: String,
    },
    /// Cancel a queued or building deployment
    Cancel {
        #[arg(help = "Service ID")]
        service_id: String,
        #[arg(help = "Deployment ID to cancel")]
        deployment_id: String,
        #[arg(long = "host", default_value = DEFAULT_ROLLOUT_HOST, help = "Maestro API host")]
        host: String,
    },
    /// Run the health probe server (used internally by the probe container)
    Probe {
        #[arg(
            long = "etcd-endpoint",
            env = "ETCD_ENDPOINT",
            default_value = "http://127.0.0.1:6401",
            help = "etcd endpoint URL"
        )]
        etcd_endpoint: String,
        #[arg(long = "port", env = "PORT", default_value_t = DEFAULT_API_PORT, help = "Port to listen on")]
        port: u16,
    },
    /// Read logs from the local log store
    Logs {
        #[arg(
            long = "source",
            help = "Source name (e.g., service name). Shows all sources if omitted"
        )]
        source: Option<String>,
        #[arg(long = "data-dir", help = "Maestro data directory")]
        data_dir: PathBuf,
        #[arg(
            long = "tail",
            default_value_t = 100,
            help = "Number of recent entries"
        )]
        tail: usize,
        #[arg(long = "follow", short = 'f', help = "Follow log output")]
        follow: bool,
    },
    /// Upgrade system components
    Upgrade {
        #[command(subcommand)]
        target: UpgradeTarget,
    },
    /// Create a default maestro.cluster.jsonc config file
    Init,
}

#[derive(Debug, Subcommand)]
enum UpgradeTarget {
    /// Upgrade the host operating system
    System {
        #[arg(long = "host", default_value = DEFAULT_ROLLOUT_HOST, help = "Maestro API host")]
        host: String,
    },
}

#[tokio::main]
async fn main() {
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
    let cli = Cli::try_parse().map_err(|err| Error::invalid_input(err.to_string()))?;

    match cli.command {
        None => {
            print!("{}", help_text());
            Ok(false)
        }
        Some(CliCommand::Start {
            config,
            cluster_name,
            admin_port,
            ingress_port,
            etcd_port,
            data_dir,
            network,
            subnet,
            enable_tailscale,
            tailscale_authkey,
            encryption_key,
            jwt_secret,
            tags,
            dd_api_key,
            dd_site,
            dd_include_system_logs,
            system,
            runtime: runtime_flag,
            project_dir,
        }) => {
            let explicit_datadog_site = dd_site
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(ToString::to_string);

            let mut cfg = match config {
                Some(source) => {
                    let mut cfg = config::load_config(&source)
                        .await
                        .map_err(|err| Error::invalid_config(err.to_string()))?;
                    if let Some(key) = encryption_key {
                        cfg.encryption_key = key;
                    }
                    if let Some(secret) = jwt_secret {
                        cfg.jwt_secret = Some(secret);
                    }
                    if let Some(authkey) = tailscale_authkey {
                        cfg.tailscale = Some(config::TailscaleConfig { auth_key: authkey });
                    }
                    if let Some(api_key) = dd_api_key {
                        let dd = cfg.datadog.get_or_insert(config::DatadogConfig {
                            api_key: String::new(),
                            site: None,
                            include_system_logs: false,
                        });
                        dd.api_key = api_key;
                    }
                    if subnet.is_some() {
                        cfg.subnet = subnet;
                    }
                    cfg
                }
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
                    subnet,
                    encryption_key: encryption_key
                        .ok_or_else(|| Error::invalid_input("--encryption-key is required"))?,
                    jwt_secret,
                    tailscale: tailscale_authkey
                        .map(|key| config::TailscaleConfig { auth_key: key }),
                    tags,
                    datadog: dd_api_key.map(|api_key| config::DatadogConfig {
                        api_key,
                        site: explicit_datadog_site.clone(),
                        include_system_logs: dd_include_system_logs,
                    }),
                    system: None,
                    runtime: Default::default(),
                },
            };

            if let (Some(site), Some(dd)) = (explicit_datadog_site.clone(), cfg.datadog.as_mut()) {
                dd.site = Some(site);
            }

            let enable_tailscale = enable_tailscale || cfg.tailscale.is_some();
            if enable_tailscale && cfg.tailscale.is_none() {
                return Err(Error::invalid_input(
                    "tailscale requires --tailscale-auth-key, TS_AUTHKEY env var, or tailscale.auth-key in config",
                ));
            }

            let (signal_tx, signal_task) = spawn_shutdown_signal_bus()?;
            std::fs::create_dir_all(&data_dir).map_err(|err| {
                Error::internal(format!(
                    "failed to create data directory {}: {err}",
                    data_dir.display()
                ))
            })?;
            verify_encryption_key(&data_dir, &cfg.encryption_key)?;
            let _lock = acquire_lock(&data_dir)?;
            let cluster_alias = cfg.cluster.name.to_lowercase();
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
            let etcd_endpoint = format!("http://127.0.0.1:{}", etcd_port);
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
            let runtime_type = runtime_flag.unwrap_or(cfg.runtime);
            if runtime_type == config::RuntimeType::Nerdctl && cfg.subnet.is_none() {
                return Err(Error::invalid_input(
                    "nerdctl runtime requires --subnet to be set for deterministic CNI network addressing",
                ));
            }
            let runtime = runtime::create_provider(runtime_type);

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
            let logger = logs::SystemLogger::new(Some(log_sender.clone()));
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

            if let Some(dd) = cfg.datadog {
                if let Some(site) = explicit_datadog_site {
                    let dd_sink = logs::DatadogSink::new(dd.api_key, &site, dd.include_system_logs);
                    let dd_worker = logs::SinkWorker::new(log_store.clone(), Box::new(dd_sink));
                    let _dd_handle = dd_worker.spawn();
                    logger.emit("info", &format!("datadog log sink enabled (site: {site})"));
                } else {
                    logger.emit("warn", "datadog config present but sink is disabled; pass --datadog-site to enable");
                }
            }

            let mut deployment_config = DeploymentConfig {
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
                encryption_key: SecretString::new(cfg.encryption_key),
                jwt_secret: cfg.jwt_secret,
                tags: parse_tags(cfg.tags)?,
                system_type: system.or(cfg.system),
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

            let mut supervisor = JobSupervisor::new();
            let system_info = deployment::start_system_jobs(
                &deployment_config,
                &runtime,
                &log_sender,
                &logger,
                &mut supervisor,
            )
            .await;

            let derived_key = derive_key(deployment_config.encryption_key.as_str());
            let store: Arc<dyn deployment::store::ClusterStore> =
                Arc::new(deployment::etcd::EtcdStateStore::new(&etcd_endpoint, derived_key).await?);
            let probe_log_endpoint = format!("http://127.0.0.1:{probe_host_port}/api/logs");
            let http_sink = logs::HttpSink::new("controller", &probe_log_endpoint);
            let sink_worker = logs::SinkWorker::new(log_store.clone(), Box::new(http_sink));
            let _sink_handle = sink_worker.spawn();
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
        Some(CliCommand::Rollout {
            host,
            config,
            apply,
            force,
            jwt_secret,
        }) => {
            let config_path = config.unwrap_or_else(|| PathBuf::from(DEFAULT_CLUSTER_CONFIG_PATH));
            cli::rollout::run_rollout(&config_path, &host, apply, force, jwt_secret.as_deref())
                .await
                .map(|()| false)
        }
        Some(CliCommand::Redeploy { service_id, host }) => {
            cli::redeploy::run_redeploy(&host, &service_id)
                .await
                .map(|()| false)
        }
        Some(CliCommand::Cancel {
            service_id,
            deployment_id,
            host,
        }) => cli::cancel::run_cancel(&host, &service_id, &deployment_id)
            .await
            .map(|()| false),
        Some(CliCommand::Probe {
            etcd_endpoint,
            port,
        }) => probe::run(&etcd_endpoint, port)
            .await
            .map(|()| false)
            .map_err(|err| Error::internal(err.to_string())),
        Some(CliCommand::Logs {
            source,
            data_dir,
            tail,
            follow,
        }) => {
            let db_path = data_dir.join("logs/logs.db");
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
        Some(CliCommand::Upgrade {
            target: UpgradeTarget::System { host },
        }) => cli::upgrade::run_upgrade_system(&host)
            .await
            .map(|()| false),
        Some(CliCommand::Init) => cli::init_config(
            Path::new("maestro.jsonc"),
            Path::new(DEFAULT_CLUSTER_CONFIG_PATH),
        )
        .map(|()| false),
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
    if show_source {
        println!(
            "{ts}  {:<5}  [{}]  {}",
            entry.level.to_uppercase(),
            entry.source,
            entry.text
        );
    } else {
        println!("{ts}  {:<5}  {}", entry.level.to_uppercase(), entry.text);
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
