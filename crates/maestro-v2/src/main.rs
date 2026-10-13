mod agent_resolver;
mod client;
mod contexts;
mod dev;
mod identity;
mod log_driver;
mod server;
mod unix_http;

use std::path::PathBuf;

use clap::{Parser, Subcommand};
use maestro_telemetry::TelemetrySettings;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(name = "maestro-v2", about = "Maestro v2 deployment system")]
struct Cli {
    #[arg(long, global = true, env = "MAESTRO_API")]
    api: Option<String>,
    #[arg(long, global = true, env = "MAESTRO_TOKEN", hide_env_values = true)]
    token: Option<String>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    #[command(subcommand)]
    Server(ServerCommand),
    #[command(subcommand)]
    Dev(dev::DevCommand),
    #[command(subcommand)]
    Context(contexts::ContextCommand),
    #[command(subcommand)]
    Token(TokenCommand),
    Agent(server::AgentArgs),
    Telemetry {
        #[arg(long, default_value = "/var/lib/maestro/logs")]
        log_dir: PathBuf,
        #[arg(long, default_value = "/var/lib/maestro/telemetry")]
        data_dir: PathBuf,
        #[arg(long, default_value = "/run/maestro/telemetry.sock")]
        socket: PathBuf,
        #[arg(long, default_value = "/run/maestro/api.sock")]
        api_socket: PathBuf,
    },
    Rollout {
        #[arg(short, long, default_value = "maestro.services.jsonc")]
        file: PathBuf,
        #[arg(long)]
        apply: bool,
    },
    Services,
    Deploy {
        service: String,
    },
    Replicas,
    Nodes,
    Logs {
        #[arg(long)]
        service: Option<String>,
        #[arg(short, long)]
        query: Option<String>,
        #[arg(short = 'n', long, default_value_t = 100)]
        limit: usize,
    },
}

#[derive(Subcommand)]
enum ServerCommand {
    Init {
        #[arg(long, default_value = "/var/lib/maestro")]
        data_dir: PathBuf,
    },
}

#[derive(Subcommand)]
enum TokenCommand {
    Create {
        #[arg(long, default_value = "/var/lib/maestro")]
        data_dir: PathBuf,
        #[arg(long, default_value = "cli")]
        subject: String,
        #[arg(long, default_value_t = 30)]
        ttl_days: u64,
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
    let cli = Cli::parse();
    match cli.command {
        Command::Server(ServerCommand::Init { data_dir }) => server::init(&data_dir).await,
        Command::Dev(command) => dev::run(command).await,
        Command::Context(command) => contexts::run(command),
        Command::Token(TokenCommand::Create {
            data_dir,
            subject,
            ttl_days,
        }) => server::create_token(&data_dir, &subject, ttl_days),
        Command::Agent(args) => server::run_agent(args).await,
        Command::Telemetry {
            log_dir,
            data_dir,
            socket,
            api_socket,
        } => {
            let settings = TelemetrySettings {
                log_dir,
                data_dir,
                socket,
                resolver: agent_resolver::AgentReplicaResolver::spawn(api_socket),
                ..TelemetrySettings::default()
            };
            Ok(maestro_telemetry::run(settings).await?)
        }
        Command::Rollout { file, apply } => {
            client::rollout(&contexts::resolve(cli.api, cli.token)?, &file, apply).await
        }
        Command::Services => client::services(&contexts::resolve(cli.api, cli.token)?).await,
        Command::Deploy { service } => {
            client::deploy(&contexts::resolve(cli.api, cli.token)?, &service).await
        }
        Command::Replicas => client::replicas(&contexts::resolve(cli.api, cli.token)?).await,
        Command::Nodes => client::nodes(&contexts::resolve(cli.api, cli.token)?).await,
        Command::Logs {
            service,
            query,
            limit,
        } => {
            let endpoint = contexts::resolve(cli.api, cli.token)?;
            client::logs(&endpoint, service.as_deref(), query.as_deref(), limit).await
        }
    }
}
