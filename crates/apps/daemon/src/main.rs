use std::ffi::OsString;
use std::path::PathBuf;

use clap::{Parser, Subcommand};
use daemon::{
    DeadLetterAdminCommand, DeadLetterAdminOutput, administer_dead_letters, launch_daemon,
    load_launch_config,
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
    let arguments = normalize_arguments(std::env::args_os().collect());
    let cli = match Cli::try_parse_from(arguments) {
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
        DaemonCommand::DeadLetters { config, command } => {
            let command = admin_command(command)?;
            let output = administer_dead_letters(&config, command).await?;
            write_admin_output(output).await
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

fn normalize_arguments(mut arguments: Vec<OsString>) -> Vec<OsString> {
    let first = arguments.get(1).and_then(|argument| argument.to_str());
    let is_native_command = first.is_some_and(|argument| {
        matches!(
            argument,
            "start" | "dead-letters" | "help" | "-h" | "--help" | "-V" | "--version"
        ) || argument.starts_with('-')
    });
    if first.is_some() && !is_native_command {
        arguments.insert(1, OsString::from("start"));
    }
    arguments
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
