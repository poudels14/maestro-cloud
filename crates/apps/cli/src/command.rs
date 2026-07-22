use std::io::{BufRead, Write};
use std::path::PathBuf;

use clap::{Parser, Subcommand};

use crate::CliError;
use crate::cluster_command::ClusterCommand;
use crate::config::{self, ConfigKind};
use crate::contexts::ContextStore;
use crate::log_command::LogCommand;
use crate::login::{DEFAULT_LOGIN_DAYS, login};
use crate::services_command::ServiceCommand;

/// Rewritten Maestro operator command-line client.
#[derive(Debug, Parser)]
#[command(name = "maestro-next", version, about)]
pub struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Inspect and operate the active Maestro cluster.
    Cluster {
        #[command(subcommand)]
        command: ClusterCommand,
    },
    /// Create and validate Maestro configuration files.
    Config {
        #[command(subcommand)]
        command: ConfigCommand,
    },
    /// Manage API origins and operator credentials.
    Contexts {
        #[command(subcommand)]
        command: ContextCommand,
    },
    /// Stream normalized logs from the active Maestro API context.
    Logs {
        #[command(flatten)]
        command: LogCommand,
    },
    /// Inspect and operate deployable services.
    Services {
        #[command(subcommand)]
        command: ServiceCommand,
    },
}

#[derive(Debug, Subcommand)]
enum ConfigCommand {
    /// Create a cluster or services config without overwriting existing files.
    Init {
        /// Config kind; prompted when omitted.
        #[arg(value_enum)]
        kind: Option<ConfigKind>,
        /// Destination path instead of the conventional filename.
        #[arg(long, value_name = "PATH")]
        output: Option<PathBuf>,
    },
    /// Validate a local, file://, or aws-secret:// config source.
    Validate {
        /// Config source to validate.
        source: String,
    },
}

#[derive(Debug, Subcommand)]
enum ContextCommand {
    /// Add or update a named API origin.
    Set {
        /// Context name; prompted when omitted.
        name: Option<String>,
        /// Maestro API origin; prompted when omitted.
        host: Option<String>,
        /// PEM certificate authority used to verify the API server.
        #[arg(long, value_name = "PATH")]
        ca_certificate: Option<PathBuf>,
    },
    /// Select the active context.
    Use {
        /// Context name; prompted when omitted.
        name: Option<String>,
    },
    /// List configured contexts without exposing credentials.
    Ls,
    /// Remove a named context.
    Remove {
        /// Context name.
        name: String,
    },
    /// Sign and save an operator token for the active context.
    Login {
        /// Token lifetime in days.
        #[arg(long, default_value_t = DEFAULT_LOGIN_DAYS)]
        days: u64,
    },
}

/// Executes one parsed CLI invocation against its configured context store.
pub async fn run(
    cli: Cli,
    input: &mut dyn BufRead,
    output: &mut dyn Write,
) -> Result<(), CliError> {
    match cli.command {
        Command::Cluster { command } => crate::cluster_command::run(command, output).await,
        Command::Config { command } => match command {
            ConfigCommand::Init { kind, output: path } => {
                let kind = match kind {
                    Some(kind) => kind,
                    None => ConfigKind::parse(&required(
                        None,
                        "Config kind (cluster/services)",
                        input,
                        output,
                    )?)?,
                };
                config::init(kind, path.as_deref(), output).map(|_| ())
            }
            ConfigCommand::Validate { source } => {
                config::validate(
                    &source,
                    output,
                    &crate::config_source::SystemConfigSourceReader,
                )
                .await
            }
        },
        Command::Contexts { command } => {
            let contexts = ContextStore::from_environment()?;
            match command {
                ContextCommand::Set {
                    name,
                    host,
                    ca_certificate,
                } => {
                    let name = required(name, "Context name", input, output)?;
                    let host = required(host, "Maestro API host", input, output)?;
                    let ca_certificate = ca_certificate
                        .map(|path| {
                            std::fs::read_to_string(&path).map_err(|source| {
                                CliError::io(
                                    format!("failed to read CA certificate {}", path.display()),
                                    source,
                                )
                            })
                        })
                        .transpose()?;
                    let normalized = contexts.set(&name, &host, ca_certificate)?;
                    writeln!(output, "[maestro]: set context `{name}` -> {normalized}")
                        .map_err(|source| CliError::io("failed to write command output", source))
                }
                ContextCommand::Use { name } => {
                    let name = required(name, "Context name", input, output)?;
                    contexts.use_context(&name)?;
                    writeln!(output, "[maestro]: active context set to `{name}`")
                        .map_err(|source| CliError::io("failed to write command output", source))
                }
                ContextCommand::Ls => list_contexts(&contexts, output),
                ContextCommand::Remove { name } => {
                    contexts.remove(&name)?;
                    writeln!(output, "[maestro]: removed context `{name}`")
                        .map_err(|source| CliError::io("failed to write command output", source))
                }
                ContextCommand::Login { days } => {
                    login(&contexts, days)?;
                    writeln!(
                        output,
                        "[maestro]: operator token saved to the active context (expires in {days} days)"
                    )
                    .map_err(|source| CliError::io("failed to write command output", source))
                }
            }
        }
        Command::Logs { command } => crate::log_command::run(command, output).await,
        Command::Services { command } => crate::services_command::run(command, input, output).await,
    }
}

fn required(
    value: Option<String>,
    label: &str,
    input: &mut dyn BufRead,
    output: &mut dyn Write,
) -> Result<String, CliError> {
    let value = match value {
        Some(value) => value,
        None => {
            write!(output, "{label}: ")
                .map_err(|source| CliError::io("failed to write prompt", source))?;
            output
                .flush()
                .map_err(|source| CliError::io("failed to flush prompt", source))?;
            let mut value = String::new();
            input
                .read_line(&mut value)
                .map_err(|source| CliError::io("failed to read prompt", source))?;
            value
        }
    };
    let value = value.trim();
    if value.is_empty() {
        return Err(CliError::invalid_input(format!("{label} is required")));
    }
    Ok(value.to_string())
}

fn list_contexts(contexts: &ContextStore, output: &mut dyn Write) -> Result<(), CliError> {
    let listings = contexts.list()?;
    if listings.is_empty() {
        writeln!(output, "[maestro]: no contexts configured")
            .map_err(|source| CliError::io("failed to write command output", source))?;
    }
    for context in listings {
        let marker = if context.active { '*' } else { ' ' };
        writeln!(output, "{marker} {:<24} {}", context.name, context.host)
            .map_err(|source| CliError::io("failed to write command output", source))?;
    }
    Ok(())
}
