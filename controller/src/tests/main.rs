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
fn start_accepts_defaults() {
    let cli = parse_for_test(&["start"]).expect("should parse");
    match cli.command {
        Some(CliCommand::Start { etcd, port }) => {
            assert_eq!(etcd, DEFAULT_ETCD_ENDPOINT);
            assert_eq!(port, DEFAULT_BIND_PORT);
        }
        _ => panic!("expected start command"),
    }
}

#[test]
fn start_accepts_custom_etcd_endpoint_and_port() {
    let cli = parse_for_test(&[
        "start",
        "--etcd",
        "https://some-host:2379/",
        "--port",
        "6500",
    ])
    .expect("should parse");
    match cli.command {
        Some(CliCommand::Start { etcd, port }) => {
            assert_eq!(etcd, "https://some-host:2379/");
            assert_eq!(port, 6500);
        }
        _ => panic!("expected start command"),
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
    let cli = parse_for_test(&["cancel", "svc-1", "deployment-svc-1-0001"]).expect("should parse");
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
fn start_rejects_config_flag() {
    let err = parse_for_test(&["start", "--config", "maestro.jsonc"]).expect_err("should fail");
    assert!(err.contains("--config"));
}
