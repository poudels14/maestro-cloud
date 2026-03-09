mod cli;
mod deployment;
mod error;
mod server;
mod signal;
mod supervisor;
mod utils;

use std::{path::Path, sync::Arc};

use clap::{CommandFactory, Parser, Subcommand};
use error::Error;
use signal::spawn_shutdown_signal_bus;

use crate::{
    deployment::controller::DeploymentController,
    supervisor::{SupervisedJobConfig, SupervisedJobStatus, controller::JobSupervisor},
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
        Some(CliCommand::Start { port, expose_etcd }) => {
            let bind_addr = format!("127.0.0.1:{port}");
            let etcd_port = expose_etcd.unwrap_or(6479);
            let etcd_endpoint = format!("http://127.0.0.1:{}", etcd_port);

            let mut supervisor = JobSupervisor::new();

            let job_id = supervisor.start_job(SupervisedJobConfig {
                id: "maestro-etcd".to_string(),
                command: format!(
                    "docker run --name maestro-etcd -p {}:2379 --rm quay.io/coreos/etcd:v3.6.8 etcd {}",
                    etcd_port,
                    "--listen-client-urls=http://0.0.0.0:2379 --advertise-client-urls=http://127.0.0.1:6479"
                ),
                name: "maestro-etcd".to_string(),
                max_restarts: 100,
                restart_delay_ms: 100,
                shutdown_grace_period_ms: 10_000,
            });
            if let Some(job_id) = job_id {
                loop {
                    let status = supervisor.job_status(&job_id).await;
                    match status {
                        Some(s) => {
                            if s.finished() || s == SupervisedJobStatus::Running {
                                break;
                            }
                        }
                        None => break,
                    }
                }
            }

            let store: Arc<dyn deployment::store::ClusterStore> =
                Arc::new(deployment::etcd::EtcdStateStore::new(&etcd_endpoint).await?);
            let server = server::Server::new(store.clone());
            let (signal_tx, signal_task) = spawn_shutdown_signal_bus()?;
            let server_signal_rx = signal_tx.subscribe();
            let deployment_signal_rx = signal_tx.subscribe();
            println!("[maestro]: etcd endpoint {etcd_endpoint}");

            let mut controller = DeploymentController::new(store, supervisor, deployment_signal_rx);
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
                result
            };

            let result = tokio::try_join!(server_future, deployment_future).map(|_| ());
            signal_task.abort();
            result
        }
        Some(CliCommand::Rollout { host }) => {
            cli::run_rollout(Path::new(DEFAULT_CONFIG_PATH), &host).await
        }
        Some(CliCommand::Cancel {
            service_id,
            deployment_id,
            host,
        }) => cli::run_cancel(&host, &service_id, &deployment_id).await,
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
