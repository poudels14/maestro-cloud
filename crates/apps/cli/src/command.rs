use std::io::{BufRead, Write};

use clap::{Parser, Subcommand};

use crate::CliError;
use crate::contexts::ContextStore;
use crate::login::{DEFAULT_LOGIN_DAYS, login};

/// Rewritten Maestro operator command-line client.
#[derive(Debug, Parser)]
#[command(name = "maestro-next", version, about)]
pub struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Manage API origins and operator credentials.
    Contexts {
        #[command(subcommand)]
        command: ContextCommand,
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
pub fn run(cli: Cli, input: &mut dyn BufRead, output: &mut dyn Write) -> Result<(), CliError> {
    let contexts = ContextStore::from_environment()?;
    match cli.command {
        Command::Contexts { command } => match command {
            ContextCommand::Set { name, host } => {
                let name = required(name, "Context name", input, output)?;
                let host = required(host, "Maestro API host", input, output)?;
                let normalized = contexts.set(&name, &host)?;
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
        },
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
