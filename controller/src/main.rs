mod cli;
mod deployment;
mod error;
mod probe;
mod server;
mod signal;
mod supervisor;
mod utils;

use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use clap::{CommandFactory, Parser, Subcommand};
use error::Error;
use signal::spawn_shutdown_signal_bus;

use crate::{
    deployment::{DeploymentConfig, controller::DeploymentController, etcd::EtcdStateStore},
    supervisor::controller::JobSupervisor,
};

const DEFAULT_CONFIG_PATH: &str = "maestro.jsonc";
const DEFAULT_BIND_PORT: u16 = 6400;
const DEFAULT_ROLLOUT_HOST: &str = "http://127.0.0.1:6400";

#[derive(Debug, Parser)]
#[command(name = "maestro", disable_help_subcommand = true)]
struct Cli {
    #[command(subcommand)]
    command: Option<CliCommand>,
}

#[derive(Debug, Subcommand)]
enum CliCommand {
    Start {
        #[arg(long = "expose_etcd")]
        expose_etcd: Option<u16>,
        #[arg(long = "port", default_value_t = DEFAULT_BIND_PORT)]
        port: u16,
        #[arg(long = "data-dir")]
        data_dir: PathBuf,
        #[arg(long = "network", default_value = "maestro")]
        network: String,
    },
    Rollout {
        #[arg(long = "host", default_value = DEFAULT_ROLLOUT_HOST)]
        host: String,
    },
    Cancel {
        service_id: String,
        deployment_id: String,
        #[arg(long = "host", default_value = DEFAULT_ROLLOUT_HOST)]
        host: String,
    },
    Probe {
        #[arg(
            long = "etcd-endpoint",
            env = "ETCD_ENDPOINT",
            default_value = "http://127.0.0.1:6479"
        )]
        etcd_endpoint: String,
    },
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
            port,
            expose_etcd,
            data_dir,
            network,
        }) => {
            let (signal_tx, signal_task) = spawn_shutdown_signal_bus()?;
            std::fs::create_dir_all(&data_dir).map_err(|err| {
                Error::internal(format!(
                    "failed to create data directory {}: {err}",
                    data_dir.display()
                ))
            })?;
            let bind_addr = format!("127.0.0.1:{port}");
            let etcd_port = expose_etcd.unwrap_or(6479);
            let etcd_endpoint = format!("http://127.0.0.1:{}", etcd_port);
            println!("[maestro]: etcd endpoint {etcd_endpoint}");

            let project_dir = std::env::current_dir()
                .expect("failed to get current dir")
                .parent()
                .expect("failed to get parent dir")
                .to_path_buf();
            let deployment_config = DeploymentConfig {
                data_dir,
                etcd_port,
                project_dir,
                network,
            };
            let mut supervisor = JobSupervisor::new();
            deployment::start_system_jobs(&deployment_config, &mut supervisor).await;

            let store: Arc<dyn deployment::store::ClusterStore> =
                Arc::new(EtcdStateStore::new(&etcd_endpoint).await?);
            let server =
                server::Server::new(store.clone(), deployment_config.deployment_logs_dir());
            let server_signal_rx = signal_tx.subscribe();
            let deployment_signal_rx = signal_tx.subscribe();

            let mut controller = DeploymentController::new(
                deployment_config,
                store,
                supervisor,
                deployment_signal_rx,
            );
            let server_shutdown_tx = signal_tx.clone();
            let deployment_shutdown_tx = signal_tx.clone();

            let server_future = async move {
                let result = server.serve(&bind_addr, server_signal_rx).await;
                let _ = server_shutdown_tx.send(signal::ShutdownEvent::Graceful);
                result
            };
            let deployment_future = async move {
                let result = controller.run().await.map_err(Into::into);
                let _ = deployment_shutdown_tx.send(signal::ShutdownEvent::Graceful);
                result.map(|()| controller)
            };

            let result = tokio::try_join!(server_future, deployment_future);
            signal_task.abort();
            match result {
                Ok((_, controller)) => {
                    let mut supervisor = controller.into_supervisor();
                    supervisor
                        .shutdown_all(supervisor::ShutdownRequest::Force)
                        .await;
                    Ok(())
                }
                Err(err) => Err(err),
            }
        }
        Some(CliCommand::Rollout { host }) => {
            cli::run_rollout(Path::new(DEFAULT_CONFIG_PATH), &host).await
        }
        Some(CliCommand::Cancel {
            service_id,
            deployment_id,
            host,
        }) => cli::run_cancel(&host, &service_id, &deployment_id).await,
        Some(CliCommand::Probe { etcd_endpoint }) => probe::run(&etcd_endpoint)
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
