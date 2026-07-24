use std::path::PathBuf;
use std::time::Duration;

use clap::{Parser, Subcommand};
use daemon::{
    DEFAULT_DNS_RESOLVER_PORT, DeadLetterAdminCommand, DeadLetterAdminOutput,
    DnsResolverLaunchConfig, LocalLogOptions, administer_dead_letters, launch_daemon,
    load_launch_config, run_dns_resolver, stream_local_logs,
};
use logs::{LogSequence, LogSinkId};
use tokio::io::AsyncWriteExt;

#[derive(Debug, Parser)]
#[command(name = "daemon", version, about = "Maestro control-plane daemon")]
struct Cli {
    #[command(subcommand)]
    command: DaemonCommand,
}

#[derive(Debug, Subcommand)]
enum DaemonCommand {
    /// Starts the declared node roles from a protected launch document.
    Start { config: PathBuf },
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
    },
    /// Reads logs from the running local node over its authenticated node API.
    Logs {
        /// Protected launch document used to authenticate the local query.
        config: PathBuf,
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
        config: PathBuf,
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
    if let Err(error) = run(cli).await {
        eprintln!("maestro daemon failed: {error}");
        std::process::exit(1);
    }
}

async fn run(cli: Cli) -> Result<(), Box<dyn std::error::Error>> {
    match cli.command {
        DaemonCommand::Start { config } => start(config).await,
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
        } => {
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
            })
            .await
        }
        DaemonCommand::Logs {
            config,
            source,
            tail,
            follow,
        } => {
            let config = load_launch_config(&config)?;
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
        DaemonCommand::DeadLetters { config, command } => {
            let command = admin_command(command)?;
            let output = administer_dead_letters(&config, command).await?;
            write_admin_output(output).await
        }
    }
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

async fn start(path: PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    let config = tokio::task::spawn_blocking(move || load_launch_config(&path)).await??;
    let mut running = launch_daemon(config).await?;
    tokio::select! {
        signal = shutdown_signal() => {
            signal?;
            running.shutdown().await?;
            Ok(())
        }
        failure = running.wait_for_failure() => {
            if let Err(error) = running.shutdown().await {
                eprintln!("maestro daemon cleanup after role failure failed: {error}");
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
