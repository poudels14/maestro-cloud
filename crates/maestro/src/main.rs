mod agent_resolver;
mod client;
mod cluster_service;
mod contexts;
mod control_link;
mod control_plane;
mod dev;
mod exec;
mod identity;
mod join;
mod log_driver;
mod node_service;
mod observability;
mod secret_sources;
mod server;
mod slack;
mod unix_http;
mod uploads;

use std::path::PathBuf;

use clap::{Parser, Subcommand};
use maestro_telemetry::TelemetrySettings;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(name = "maestro", about = "Maestro deployment system")]
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
    Join(join::JoinArgs),
    Telemetry {
        #[arg(long, default_value = "/var/lib/maestro/logs")]
        log_dir: PathBuf,
        #[arg(long, default_value = "/var/lib/maestro/telemetry")]
        data_dir: PathBuf,
        #[arg(long, default_value = "/run/maestro/telemetry.sock")]
        socket: PathBuf,
        #[arg(long, default_value = "/run/maestro/replicas.json")]
        replicas: PathBuf,
        #[arg(long, default_value = "/var/lib/maestro")]
        node_dir: PathBuf,
        #[arg(long)]
        config: Option<String>,
    },
    Rollout {
        #[arg(short, long, default_value = "maestro.services.jsonc")]
        file: PathBuf,
        #[arg(long)]
        apply: bool,
        #[arg(long)]
        force: bool,
    },
    Services {
        #[command(subcommand)]
        command: Option<ServicesCommand>,
    },
    #[command(subcommand)]
    Service(client::ServiceCommand),
    Deploy {
        service: String,
        #[arg(long)]
        force: bool,
    },
    Replicas,
    Nodes,
    Exec {
        service: String,
        #[arg(short, long)]
        replica: Option<u32>,
        #[arg(long)]
        deployment: Option<String>,
        #[arg(long)]
        no_tty: bool,
        #[arg(last = true)]
        command: Vec<String>,
    },
    #[command(subcommand)]
    Node(client::NodeCommand),
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
enum ServicesCommand {
    Up {
        #[arg(long, default_value = "maestro.services.jsonc")]
        config: PathBuf,
        #[arg(long, default_value = ".")]
        context: PathBuf,
    },
}

#[derive(Subcommand)]
enum ServerCommand {
    Init(server::InitArgs),
    JoinToken {
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
        Command::Server(ServerCommand::Init(args)) => server::init(args).await,
        Command::Server(ServerCommand::JoinToken { data_dir }) => join::create_token(&data_dir),
        Command::Join(args) => join::join(args).await,
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
            replicas,
            node_dir,
            config,
        } => {
            let config = server::load_config(config.as_deref())?;
            let identity = identity::NodeIdentity::load(&node_dir)?;
            let settings = TelemetrySettings {
                log_dir,
                data_dir,
                socket,
                resolver: agent_resolver::AgentReplicaResolver::spawn(replicas),
                datadog: datadog_config(&config, &identity),
                backup: config
                    .log_backup
                    .as_ref()
                    .map(|backup| maestro_telemetry::BackupConfig {
                        bucket: backup.bucket.clone(),
                        kms_key_id: backup.kms_key_id.clone(),
                        region: backup.region.clone(),
                        prefix: backup
                            .prefix
                            .clone()
                            .unwrap_or_else(|| identity.cluster.name.clone()),
                        node: identity.name.clone(),
                        retention_days: backup.retention_days,
                    }),
                ..TelemetrySettings::default()
            };
            Ok(maestro_telemetry::run(settings).await?)
        }
        Command::Rollout { file, apply, force } => {
            client::rollout(&contexts::resolve(cli.api, cli.token)?, &file, apply, force).await
        }
        Command::Services { command: None } => {
            client::services(&contexts::resolve(cli.api, cli.token)?).await
        }
        Command::Services {
            command: Some(ServicesCommand::Up { config, context }),
        } => client::services_up(&contexts::resolve(cli.api, cli.token)?, &config, &context).await,
        Command::Service(command) => {
            client::service(&contexts::resolve(cli.api, cli.token)?, command).await
        }
        Command::Deploy { service, force } => {
            client::deploy(&contexts::resolve(cli.api, cli.token)?, &service, force).await
        }
        Command::Replicas => client::replicas(&contexts::resolve(cli.api, cli.token)?).await,
        Command::Nodes => client::nodes(&contexts::resolve(cli.api, cli.token)?).await,
        Command::Exec {
            service,
            replica,
            deployment,
            no_tty,
            command,
        } => {
            let endpoint = contexts::resolve(cli.api, cli.token)?;
            let code = exec::run_cli(
                &endpoint,
                &service,
                replica,
                deployment.as_deref(),
                !no_tty,
                command,
            )
            .await?;
            std::process::exit(code)
        }
        Command::Node(command) => {
            client::node(&contexts::resolve(cli.api, cli.token)?, command).await
        }
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

fn datadog_config(
    config: &maestro_core::ClusterConfig,
    identity: &identity::NodeIdentity,
) -> Option<maestro_telemetry::DatadogConfig> {
    let settings = config.datadog.as_ref()?;
    match settings
        .site
        .as_deref()
        .map(str::trim)
        .filter(|site| !site.is_empty())
    {
        None => {
            tracing::warn!("datadog is configured without a site; not shipping logs or metrics");
            None
        }
        Some(site) => {
            let (logs_url, metrics_url) = maestro_telemetry::DatadogConfig::site_urls(site);
            Some(maestro_telemetry::DatadogConfig {
                api_key: settings.api_key.clone(),
                logs_url,
                metrics_url,
                tags: config.tags.clone(),
                cluster: identity.cluster.name.clone(),
                node: identity.name.clone(),
                include_ingress_logs: settings.include_ingress_logs,
                include_tailscale_logs: settings.include_tailscale_logs,
                include_healthcheck: settings.logs.include_healthcheck,
                include_metrics: settings.include_metrics,
            })
        }
    }
}
