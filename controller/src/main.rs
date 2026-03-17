mod cli;
mod deployment;
mod error;
mod probe;
mod server;
mod signal;
mod supervisor;
mod utils;

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
const DEFAULT_API_PORT: u16 = 6400;
const DEFAULT_ROLLOUT_HOST: &str = "http://127.0.0.1:6400";

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
    },
    /// Deploy services from the config file
    Rollout {
        #[arg(long = "host", default_value = DEFAULT_ROLLOUT_HOST, help = "Maestro API host")]
        host: String,
        #[arg(
            long = "config",
            help = "Path to maestro.jsonc (default: maestro.jsonc)"
        )]
        config: Option<PathBuf>,
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
            default_value = "http://127.0.0.1:6479",
            help = "etcd endpoint URL"
        )]
        etcd_endpoint: String,
        #[arg(long = "port", env = "PORT", default_value_t = DEFAULT_API_PORT, help = "Port to listen on")]
        port: u16,
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
            let deployment_config = DeploymentConfig {
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
            };
            let mut supervisor = JobSupervisor::new();
            deployment::start_system_jobs(&deployment_config, &mut supervisor).await;

            let derived_key = derive_key(deployment_config.secret_key.as_str());
            let store: Arc<dyn deployment::store::ClusterStore> =
                Arc::new(deployment::etcd::EtcdStateStore::new(&etcd_endpoint, derived_key).await?);
            let deployment_signal_rx = signal_tx.subscribe();

            let mut controller = DeploymentController::new(
                deployment_config,
                store,
                supervisor,
                deployment_signal_rx,
            );
            let deployment_shutdown_tx = signal_tx.clone();

            let result = async move {
                let result = controller.run().await.map_err(Into::into);
                let _ = deployment_shutdown_tx.send(signal::ShutdownEvent::Graceful);
                result.map(|()| controller)
            }
            .await;

            signal_task.abort();
            match result {
                Ok(controller) => {
                    let mut supervisor = controller.into_supervisor();
                    supervisor
                        .shutdown_all(supervisor::ShutdownRequest::Force)
                        .await;
                    Ok(())
                }
                Err(err) => Err(err),
            }
        }
        Some(CliCommand::Rollout { host, config }) => {
            let config_path = config.unwrap_or_else(|| PathBuf::from(DEFAULT_CONFIG_PATH));
            cli::run_rollout(&config_path, &host).await
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
