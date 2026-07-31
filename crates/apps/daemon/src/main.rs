use std::path::PathBuf;
use std::time::Duration;

use clap::{Parser, Subcommand};
use daemon::{
    ClusterConfigFallbacks, DEFAULT_DNS_RESOLVER_PORT, DeadLetterAdminCommand,
    DeadLetterAdminOutput, DnsResolverLaunchConfig, LocalLogOptions, administer_dead_letters,
    launch_daemon, load_launch_config, load_launch_config_with_fallbacks, run_dns_resolver,
    stream_local_logs,
};
use logs::{LogSequence, LogSinkId};
use node_agent::{TailscaleDnsPluginSettings, TailscaleDnsRoute};
use tokio::io::AsyncWriteExt;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::util::SubscriberInitExt;

#[derive(Debug, Parser)]
#[command(
    name = "daemon",
    version = kernel_api::MAESTRO_VERSION,
    about = "Maestro control-plane daemon"
)]
struct Cli {
    #[command(subcommand)]
    command: DaemonCommand,
}

#[derive(Debug, Subcommand)]
enum DaemonCommand {
    /// Starts the declared node roles from current cluster config and local bootstrap state.
    Start {
        /// Cluster configuration source fetched on every daemon start.
        #[arg(long)]
        config: String,
        /// Fallback workload subnet for a one-node config that omits cluster.nodes.*.subnet.
        #[arg(long)]
        subnet: Option<cluster::Ipv4Cidr>,
        /// Owner-only node bootstrap document.
        launch: PathBuf,
    },
    /// Runs the internal authoritative resolver role on a delegated runtime network.
    #[command(hide = true)]
    Dns {
        #[arg(long)]
        cluster_id: kernel_api::ClusterId,
        #[arg(long)]
        node_id: kernel_api::NodeId,
        #[arg(long = "endpoint", required = true)]
        endpoints: Vec<String>,
        #[arg(long)]
        certificate_authority: PathBuf,
        #[arg(long)]
        client_certificate: PathBuf,
        #[arg(long)]
        client_private_key: PathBuf,
        #[arg(long)]
        store_encryption_secret: PathBuf,
        #[arg(long, default_value_t = DEFAULT_DNS_RESOLVER_PORT)]
        port: u16,
        #[arg(long, default_value_t = 30)]
        resync_seconds: u64,
        #[arg(long = "cross-cluster-dns")]
        cross_cluster_dns: Vec<String>,
    },
    /// Reads logs from the running local node over its authenticated node API.
    Logs {
        /// Owner-only node bootstrap document used to authenticate the local query.
        launch: PathBuf,
        /// Current cluster configuration source.
        #[arg(long)]
        config: String,
        /// System component or service/deployment/workload source.
        #[arg(long)]
        source: Option<String>,
        /// Number of recent matching records.
        #[arg(long, default_value_t = 100)]
        tail: usize,
        /// Continue polling for new matching records.
        #[arg(short = 'f', long)]
        follow: bool,
    },
    /// Inspects, exports, or purges node-local sink dead letters.
    DeadLetters {
        launch: PathBuf,
        #[command(subcommand)]
        command: DeadLetterCommand,
    },
}

#[derive(Debug, Subcommand)]
enum DeadLetterCommand {
    /// Lists bounded metadata and aggregate retained bytes.
    List {
        #[arg(long, default_value = "datadog")]
        sink: String,
        #[arg(long, default_value_t = 100)]
        limit: usize,
    },
    /// Exports every retained record as ordered JSON Lines.
    Export {
        #[arg(long, default_value = "datadog")]
        sink: String,
        #[arg(long)]
        output: PathBuf,
    },
    /// Purges records only after explicit all-or-through selection.
    Purge {
        #[arg(long, default_value = "datadog")]
        sink: String,
        #[arg(
            long = "all",
            conflicts_with = "through_seq",
            required_unless_present = "through_seq"
        )]
        all: bool,
        #[arg(long, conflicts_with = "all", value_name = "SEQUENCE")]
        through_seq: Option<u64>,
    },
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let cli = match Cli::try_parse() {
        Ok(cli) => cli,
        Err(error) => error.exit(),
    };
    initialize_tracing();
    if let Err(error) = run(cli).await {
        tracing::error!(error = %error, "maestro daemon failed");
        std::process::exit(1);
    }
}

fn initialize_tracing() {
    let filter = EnvFilter::builder()
        .with_default_directive(tracing::Level::INFO.into())
        .from_env_lossy();
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .json()
        .flatten_event(true)
        .with_ansi(false)
        .with_writer(std::io::stderr)
        .finish()
        .init();
}

async fn run(cli: Cli) -> Result<(), Box<dyn std::error::Error>> {
    match cli.command {
        DaemonCommand::Start {
            config,
            subnet,
            launch,
        } => start(config, subnet, launch).await,
        DaemonCommand::Dns {
            cluster_id,
            node_id,
            endpoints,
            certificate_authority,
            client_certificate,
            client_private_key,
            store_encryption_secret,
            port,
            resync_seconds,
            cross_cluster_dns,
        } => {
            let dns_plugin_settings = parse_cross_cluster_dns(&cluster_id, &cross_cluster_dns)?;
            dns(DnsResolverLaunchConfig {
                cluster_id,
                node_id,
                endpoints,
                certificate_authority,
                client_certificate,
                client_private_key,
                store_encryption_secret,
                port,
                resync_interval: Duration::from_secs(resync_seconds),
                dns_plugin_settings,
            })
            .await
        }
        DaemonCommand::Logs {
            launch,
            config,
            source,
            tail,
            follow,
        } => {
            let config = load_launch_config(&launch, &config).await?;
            let mut output = std::io::stdout().lock();
            stream_local_logs(
                &config,
                &LocalLogOptions {
                    source,
                    tail,
                    follow,
                },
                &mut output,
            )
            .await?;
            Ok(())
        }
        DaemonCommand::DeadLetters { launch, command } => {
            let command = admin_command(command)?;
            let output = administer_dead_letters(&launch, command).await?;
            write_admin_output(output).await
        }
    }
}

fn parse_cross_cluster_dns(
    local_cluster_id: &kernel_api::ClusterId,
    values: &[String],
) -> Result<Option<TailscaleDnsPluginSettings>, Box<dyn std::error::Error>> {
    if values.is_empty() {
        return Ok(None);
    }
    let routes = values
        .iter()
        .map(
            |value| -> Result<TailscaleDnsRoute, Box<dyn std::error::Error>> {
                let (cluster_id, nameservers) = value.split_once('=').ok_or_else(|| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        format!("cross-cluster DNS route `{value}` must use CLUSTER=IP[,IP]"),
                    )
                })?;
                let cluster_id = kernel_api::ClusterId::new(cluster_id)?;
                let nameservers = nameservers
                    .split(',')
                    .map(str::parse)
                    .collect::<Result<Vec<std::net::Ipv4Addr>, _>>()?;
                Ok(TailscaleDnsRoute::new(cluster_id, nameservers))
            },
        )
        .collect::<Result<Vec<_>, Box<dyn std::error::Error>>>()?;
    TailscaleDnsPluginSettings::new(local_cluster_id.clone(), routes, Duration::from_secs(5))
        .map(Some)
        .map_err(Into::into)
}

async fn dns(config: DnsResolverLaunchConfig) -> Result<(), Box<dyn std::error::Error>> {
    let (shutdown, receiver) = tokio::sync::watch::channel(false);
    let mut running = Box::pin(run_dns_resolver(config, receiver));
    tokio::select! {
        signal = shutdown_signal() => {
            signal?;
            let _ = shutdown.send(true);
            running.await?;
            Ok(())
        }
        result = &mut running => {
            result?;
            Ok(())
        }
    }
}

async fn start(
    config_source: String,
    subnet: Option<cluster::Ipv4Cidr>,
    path: PathBuf,
) -> Result<(), Box<dyn std::error::Error>> {
    let fallbacks = ClusterConfigFallbacks::default().with_single_node_subnet(subnet);
    let config = load_launch_config_with_fallbacks(&path, &config_source, &fallbacks).await?;
    let cluster_id = config.cluster.cluster_id.clone();
    let node_id = config.node_id.clone();
    tracing::info!(%cluster_id, %node_id, "starting maestro daemon");
    let mut running = launch_daemon(config).await?;
    tracing::info!(%cluster_id, %node_id, "maestro daemon started");
    tokio::select! {
        signal = shutdown_signal() => {
            signal?;
            tracing::info!(%cluster_id, %node_id, "shutting down maestro daemon");
            running.shutdown().await?;
            tracing::info!(%cluster_id, %node_id, "maestro daemon stopped");
            Ok(())
        }
        failure = running.wait_for_failure() => {
            if let Err(error) = running.shutdown().await {
                tracing::error!(
                    %cluster_id,
                    %node_id,
                    error = %error,
                    "maestro daemon cleanup after role failure failed"
                );
            }
            Err(failure.into())
        }
    }
}

fn admin_command(
    command: DeadLetterCommand,
) -> Result<DeadLetterAdminCommand, logs::LogSinkIdError> {
    Ok(match command {
        DeadLetterCommand::List { sink, limit } => DeadLetterAdminCommand::List {
            sink_id: LogSinkId::new(sink)?,
            limit,
        },
        DeadLetterCommand::Export { sink, output } => DeadLetterAdminCommand::Export {
            sink_id: LogSinkId::new(sink)?,
            output,
        },
        DeadLetterCommand::Purge {
            sink,
            all: _,
            through_seq,
        } => DeadLetterAdminCommand::Purge {
            sink_id: LogSinkId::new(sink)?,
            through: through_seq.map(LogSequence),
        },
    })
}

async fn write_admin_output(
    output: DeadLetterAdminOutput,
) -> Result<(), Box<dyn std::error::Error>> {
    match output {
        DeadLetterAdminOutput::Listed(document) => {
            let mut stdout = tokio::io::stdout();
            stdout.write_all(document.as_bytes()).await?;
            stdout.write_all(b"\n").await?;
            stdout.flush().await?;
        }
        DeadLetterAdminOutput::Exported {
            sink_id,
            count,
            output,
        } => {
            let message = format!(
                "[maestro]: exported {count} {} dead letters to {}\n",
                sink_id.as_str(),
                output.display()
            );
            let mut stderr = tokio::io::stderr();
            stderr.write_all(message.as_bytes()).await?;
            stderr.flush().await?;
        }
        DeadLetterAdminOutput::Purged { sink_id, count } => {
            let message = format!(
                "[maestro]: purged {count} {} dead letters\n",
                sink_id.as_str()
            );
            let mut stderr = tokio::io::stderr();
            stderr.write_all(message.as_bytes()).await?;
            stderr.flush().await?;
        }
    }
    Ok(())
}

async fn shutdown_signal() -> std::io::Result<()> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result,
            signal = terminate.recv() => signal.map_or_else(
                || Err(std::io::Error::other("SIGTERM listener closed")),
                |_| Ok(()),
            ),
        }
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await
    }
}
