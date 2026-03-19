mod builder;
mod cli;
mod deployment;
mod error;
mod logs;
mod metrics;
mod probe;
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

const DEFAULT_CONFIG_PATH: &str = "maestro.jsonc";
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
        #[arg(long = "cluster-name", help = "Unique name for this cluster")]
        cluster_name: String,
        #[arg(long = "etcd-port", help = "Host port for etcd (random if not set)")]
        etcd_port: Option<u16>,
        #[arg(
            long = "admin-port",
            help = "Host port for maestro controller (not exposed if not set)"
        )]
        admin_port: Option<u16>,
        #[arg(long = "port", help = "Host port for ingress")]
        web_port: u16,
        #[arg(long = "data-dir", help = "Directory for etcd data, logs, and state")]
        data_dir: PathBuf,
        #[arg(
            long = "network",
            help = "Docker network name (default: maestro-{cluster-name})"
        )]
        network: Option<String>,
        #[arg(
            long = "subnet",
            help = "Docker network subnet CIDR (e.g., 172.22.0.0/16)"
        )]
        subnet: Option<String>,
        #[arg(
            long = "enable-tailscale",
            help = "Enable Tailscale subnet routing and DNS"
        )]
        enable_tailscale: bool,
        #[arg(
            long = "tailscale-authkey",
            env = "TS_AUTHKEY",
            help = "Tailscale auth key"
        )]
        tailscale_authkey: Option<String>,
        #[arg(
            long = "secret-key",
            env = "MAESTRO_SECRET_KEY",
            help = "Master key for encrypting secrets (required)"
        )]
        secret_key: String,
        #[arg(
            long = "tag",
            help = "Tags for log sinks like Datadog (key:value, can be repeated)"
        )]
        tags: Vec<String>,
        #[arg(
            long = "dd-api-key",
            env = "DATADOG_API_KEY",
            help = "Datadog API key for log forwarding"
        )]
        dd_api_key: Option<String>,
        #[arg(
            long = "dd-site",
            help = "Datadog site (e.g. datadoghq.com, us3.datadoghq.com, datadoghq.eu)"
        )]
        dd_site: Option<String>,
        #[arg(
            long = "dd-include-system-logs",
            help = "Include maestro system logs in Datadog (excluded by default)"
        )]
        dd_include_system_logs: bool,
    },
    /// Deploy services from the config file (dry run by default)
    Rollout {
        #[arg(long = "host", default_value = DEFAULT_ROLLOUT_HOST, help = "Maestro API host")]
        host: String,
        #[arg(
            long = "config",
            help = "Path to maestro.jsonc (default: maestro.jsonc)"
        )]
        config: Option<PathBuf>,
        #[arg(
            long = "apply",
            help = "Apply the rollout (without this flag, only shows a diff)"
        )]
        apply: bool,
        #[arg(long = "force", help = "Force rollout even if deploy is frozen")]
        force: bool,
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
        #[arg(help = "Source name (e.g., service name). Shows all sources if omitted")]
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
    /// Create a default maestro.jsonc config file
    Init,
}

#[tokio::main]
async fn main() {
    if let Err(err) = run().await {
        eprintln!("{err}");
        std::process::exit(2);
    }
}

async fn run() -> crate::error::Result<()> {
    let cli = Cli::try_parse().map_err(|err| Error::invalid_input(err.to_string()))?;

    match cli.command {
        None => {
            print!("{}", help_text());
            Ok(())
        }
        Some(CliCommand::Start {
            cluster_name,
            admin_port,
            web_port,
            etcd_port,
            data_dir,
            network,
            subnet,
            enable_tailscale,
            tailscale_authkey,
            secret_key,
            tags,
            dd_api_key,
            dd_site,
            dd_include_system_logs,
        }) => {
            if enable_tailscale && tailscale_authkey.is_none() {
                return Err(Error::invalid_input(
                    "--enable-tailscale requires --tailscale-authkey or TS_AUTHKEY env var",
                ));
            }
            let tailscale_authkey = if enable_tailscale {
                tailscale_authkey
            } else {
                None
            };
            let cluster_name = cluster_name.to_lowercase();
            let (signal_tx, signal_task) = spawn_shutdown_signal_bus()?;
            std::fs::create_dir_all(&data_dir).map_err(|err| {
                Error::internal(format!(
                    "failed to create data directory {}: {err}",
                    data_dir.display()
                ))
            })?;
            verify_secret_key(&data_dir, &secret_key)?;
            let _lock = acquire_lock(&data_dir)?;
            let etcd_port = etcd_port.unwrap_or_else(|| {
                let listener = std::net::TcpListener::bind("127.0.0.1:0")
                    .expect("failed to bind to random port for etcd");
                listener
                    .local_addr()
                    .expect("failed to get local addr")
                    .port()
            });
            let etcd_endpoint = format!("http://127.0.0.1:{}", etcd_port);
            println!("[maestro]: etcd endpoint {etcd_endpoint}");
            let network = network.unwrap_or_else(|| format!("maestro-{cluster_name}"));
            println!("[maestro]: docker network {network}");

            let project_dir = std::env::current_dir()
                .expect("failed to get current dir")
                .parent()
                .expect("failed to get parent dir")
                .to_path_buf();
            let mut deployment_config = DeploymentConfig {
                cluster_name,
                data_dir,
                etcd_port,
                probe_port: admin_port,
                web_port,
                project_dir,
                network,
                subnet,
                tailscale_authkey,
                secret_key: SecretString::new(secret_key),
                tags: parse_tags(tags)?,
            };
            let log_store = Arc::new(
                logs::LogStore::open(&deployment_config.data_dir.join("logs/logs.db"))
                    .map_err(|err| Error::internal(format!("failed to open log store: {err}")))?,
            );

            if let Some(dd_site) = dd_site {
                let dd_api_key = dd_api_key.ok_or_else(|| {
                    Error::invalid_input("missing --dd-api-key or DATADOG_API_KEY env var")
                })?;
                let dd_sink = logs::DatadogSink::new(dd_api_key, &dd_site, dd_include_system_logs);
                let dd_worker = logs::SinkWorker::new(log_store.clone(), Box::new(dd_sink));
                let _dd_handle = dd_worker.spawn();
                eprintln!("[maestro]: datadog log sink enabled (site: {dd_site})");
            }

            let (log_collector, log_sender) = logs::LogCollector::new(log_store.clone());
            let collector_handle = log_collector.spawn();

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
            deployment::start_system_jobs(&deployment_config, &log_sender, &mut supervisor).await;

            let derived_key = derive_key(deployment_config.secret_key.as_str());
            let store: Arc<dyn deployment::store::ClusterStore> =
                Arc::new(deployment::etcd::EtcdStateStore::new(&etcd_endpoint, derived_key).await?);
            let probe_log_endpoint = format!("http://127.0.0.1:{probe_host_port}/api/logs");
            let http_sink = logs::HttpSink::new("probe", &probe_log_endpoint);
            let sink_worker = logs::SinkWorker::new(log_store.clone(), Box::new(http_sink));
            let _sink_handle = sink_worker.spawn();
            let deployment_signal_rx = signal_tx.subscribe();
            let watcher_signal_rx = signal_tx.subscribe();

            let watcher = builder::BuildWatcher::new(
                store.clone(),
                deployment_config.data_dir.clone(),
                watcher_signal_rx,
            );
            let watcher_handle = tokio::spawn(watcher.run());

            let metrics_signal_rx = signal_tx.subscribe();
            let metrics_endpoint = format!("http://127.0.0.1:{probe_host_port}/api/metrics");
            let metrics_collector = metrics::MetricsCollector::new(
                metrics_endpoint,
                deployment_config.cluster_name.clone(),
                metrics_signal_rx,
            );
            let metrics_handle = tokio::spawn(metrics_collector.run());

            let mut controller = DeploymentController::new(
                deployment_config,
                store,
                supervisor,
                deployment_signal_rx,
                Some(log_sender),
            );
            let deployment_shutdown_tx = signal_tx.clone();

            let result = async move {
                let result = controller.run().await.map_err(Into::into);
                let _ = deployment_shutdown_tx.send(signal::ShutdownEvent::Graceful);
                result.map(|()| controller)
            }
            .await;

            watcher_handle.abort();
            metrics_handle.abort();

            signal_task.abort();
            match result {
                Ok(controller) => {
                    let mut supervisor = controller.into_supervisor();
                    supervisor
                        .shutdown_all(supervisor::ShutdownRequest::Force)
                        .await;
                    drop(supervisor);
                    let _ = collector_handle.await;
                    Ok(())
                }
                Err(err) => Err(err),
            }
        }
        Some(CliCommand::Rollout {
            host,
            config,
            apply,
            force,
        }) => {
            let config_path = config.unwrap_or_else(|| PathBuf::from(DEFAULT_CONFIG_PATH));
            cli::run_rollout(&config_path, &host, apply, force).await
        }
        Some(CliCommand::Redeploy { service_id, host }) => {
            cli::run_redeploy(&host, &service_id).await
        }
        Some(CliCommand::Cancel {
            service_id,
            deployment_id,
            host,
        }) => cli::run_cancel(&host, &service_id, &deployment_id).await,
        Some(CliCommand::Probe {
            etcd_endpoint,
            port,
        }) => probe::run(&etcd_endpoint, port)
            .await
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

            Ok(())
        }
        Some(CliCommand::Init) => cli::init_config(Path::new(DEFAULT_CONFIG_PATH)),
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

fn verify_secret_key(data_dir: &Path, secret_key: &str) -> crate::error::Result<()> {
    use sha2::{Digest, Sha256};

    let key_file = data_dir.join(".secret-key-hash");
    let key_hash = format!(
        "{:x}",
        Sha256::digest(format!("maestro-key-verify:{secret_key}"))
    );

    if key_file.exists() {
        let stored = std::fs::read_to_string(&key_file).map_err(|err| {
            Error::internal(format!("failed to read secret key hash file: {err}"))
        })?;
        if stored.trim() != key_hash {
            return Err(Error::invalid_input(
                "secret key does not match the one used to initialize this data directory",
            ));
        }
    } else {
        std::fs::write(&key_file, &key_hash).map_err(|err| {
            Error::internal(format!("failed to write secret key hash file: {err}"))
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
