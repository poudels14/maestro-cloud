mod cli;
mod server;
#[allow(dead_code)]
mod service;

use std::path::Path;

use clap::{CommandFactory, Parser, Subcommand};

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
    Up {
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

async fn run() -> Result<(), String> {
    let cli = Cli::try_parse().map_err(|err| err.to_string())?;

    match cli.command {
        None => {
            print!("{}", help_text());
            Ok(())
        }
        Some(CliCommand::Up { etcd, port }) => {
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
        Some(CliCommand::Init) => cli::run_init(Path::new(DEFAULT_CONFIG_PATH)),
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
mod tests {
    use super::*;

    fn parse_for_test(args: &[&str]) -> Result<Cli, String> {
        let mut argv = vec!["maestro"];
        argv.extend(args.iter().copied());
        Cli::try_parse_from(argv).map_err(|err| err.to_string())
    }

    #[test]
    fn no_args_parse_successfully() {
        let cli = parse_for_test(&[]).expect("should parse");
        assert!(cli.command.is_none());
    }

    #[test]
    fn up_accepts_defaults() {
        let cli = parse_for_test(&["up"]).expect("should parse");
        match cli.command {
            Some(CliCommand::Up { etcd, port }) => {
                assert_eq!(etcd, DEFAULT_ETCD_ENDPOINT);
                assert_eq!(port, DEFAULT_BIND_PORT);
            }
            _ => panic!("expected up command"),
        }
    }

    #[test]
    fn up_accepts_custom_etcd_endpoint_and_port() {
        let cli = parse_for_test(&["up", "--etcd", "https://some-host:2379/", "--port", "6500"])
            .expect("should parse");
        match cli.command {
            Some(CliCommand::Up { etcd, port }) => {
                assert_eq!(etcd, "https://some-host:2379/");
                assert_eq!(port, 6500);
            }
            _ => panic!("expected up command"),
        }
    }

    #[test]
    fn rollout_accepts_no_arguments() {
        let cli = parse_for_test(&["rollout"]).expect("should parse");
        match cli.command {
            Some(CliCommand::Rollout { host }) => assert_eq!(host, DEFAULT_ROLLOUT_HOST),
            _ => panic!("expected rollout command"),
        }
    }

    #[test]
    fn rollout_accepts_custom_host() {
        let cli = parse_for_test(&["rollout", "--host", "https://api.example.com:7777"])
            .expect("should parse");
        match cli.command {
            Some(CliCommand::Rollout { host }) => assert_eq!(host, "https://api.example.com:7777"),
            _ => panic!("expected rollout command"),
        }
    }

    #[test]
    fn cancel_accepts_required_args() {
        let cli =
            parse_for_test(&["cancel", "svc-1", "deployment-svc-1-0001"]).expect("should parse");
        match cli.command {
            Some(CliCommand::Cancel {
                service_id,
                deployment_id,
                host,
            }) => {
                assert_eq!(service_id, "svc-1");
                assert_eq!(deployment_id, "deployment-svc-1-0001");
                assert_eq!(host, DEFAULT_ROLLOUT_HOST);
            }
            _ => panic!("expected cancel command"),
        }
    }

    #[test]
    fn up_rejects_config_flag() {
        let err = parse_for_test(&["up", "--config", "maestro.jsonc"]).expect_err("should fail");
        assert!(err.contains("--config"));
    }
}
