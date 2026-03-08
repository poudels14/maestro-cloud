mod cli;
mod error;
mod server;
#[allow(dead_code)]
mod service;
mod supervisor;

use std::path::Path;

use clap::{CommandFactory, Parser, Subcommand};
use error::Error;

const DEFAULT_CONFIG_PATH: &str = "maestro.jsonc";
const DEFAULT_BIND_PORT: u16 = 6400;
const DEFAULT_ETCD_ENDPOINT: &str = "http://127.0.0.1:2379";
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
        #[arg(long = "etcd", default_value = DEFAULT_ETCD_ENDPOINT)]
        etcd: String,
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
        Some(CliCommand::Start { etcd, port }) => {
            let bind_addr = format!("127.0.0.1:{port}");
            server::run_server(&bind_addr, &etcd).await
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

#[cfg(test)]
#[path = "tests/main.rs"]
mod tests;
