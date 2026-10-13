mod client;
mod dev;
mod identity;
mod log_driver;
mod server;

use std::path::PathBuf;

use clap::{Parser, Subcommand};
use maestro_telemetry::TelemetrySettings;
use tracing_subscriber::EnvFilter;

const DEFAULT_API: &str = "http://127.0.0.1:3000";

#[derive(Parser)]
#[command(name = "maestro-v2", about = "Maestro v2 deployment system")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    #[command(subcommand)]
    Server(ServerCommand),
    #[command(subcommand)]
    Dev(dev::DevCommand),
    Agent(server::AgentArgs),
    Telemetry {
        #[arg(long, default_value = "/var/lib/maestro/logs")]
        log_dir: PathBuf,
        #[arg(long, default_value = "/var/lib/maestro/telemetry")]
        data_dir: PathBuf,
        #[arg(long, default_value = "/run/maestro/telemetry.sock")]
        socket: PathBuf,
    },
    Rollout {
        #[arg(short, long, default_value = "maestro.services.jsonc")]
        file: PathBuf,
        #[arg(long)]
        apply: bool,
        #[arg(long, env = "MAESTRO_API", default_value = DEFAULT_API)]
        api: String,
    },
    Services {
        #[arg(long, env = "MAESTRO_API", default_value = DEFAULT_API)]
        api: String,
    },
    Deploy {
        service: String,
        #[arg(long, env = "MAESTRO_API", default_value = DEFAULT_API)]
        api: String,
    },
    Replicas {
        #[arg(long, env = "MAESTRO_API", default_value = DEFAULT_API)]
        api: String,
    },
    Nodes {
        #[arg(long, env = "MAESTRO_API", default_value = DEFAULT_API)]
        api: String,
    },
}

#[derive(Subcommand)]
enum ServerCommand {
    Init {
        #[arg(long, default_value = "/var/lib/maestro")]
        data_dir: PathBuf,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    match log_driver::invoked_as_log_driver() {
        true => log_driver::run().await,
        false => run_cli().await,
    }
}

async fn run_cli() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();
    match Cli::parse().command {
        Command::Server(ServerCommand::Init { data_dir }) => server::init(&data_dir).await,
        Command::Dev(command) => dev::run(command).await,
        Command::Agent(args) => server::run_agent(args).await,
        Command::Telemetry {
            log_dir,
            data_dir,
            socket,
        } => {
            let settings = TelemetrySettings {
                log_dir,
                data_dir,
                socket,
                ..TelemetrySettings::default()
            };
            Ok(maestro_telemetry::run(settings).await?)
        }
        Command::Rollout { file, apply, api } => client::rollout(&api, &file, apply).await,
        Command::Services { api } => client::services(&api).await,
        Command::Deploy { service, api } => client::deploy(&api, &service).await,
        Command::Replicas { api } => client::replicas(&api).await,
        Command::Nodes { api } => client::nodes(&api).await,
    }
}
