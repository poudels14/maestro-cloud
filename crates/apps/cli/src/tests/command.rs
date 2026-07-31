use clap::{CommandFactory, Parser};

use crate::{Cli, run};

#[test]
fn packaged_command_name_is_maestro() {
    let command = Cli::command();
    assert_eq!(command.get_name(), "maestro");
}

#[test]
fn context_command_surface_matches_the_rewrite_contract() {
    assert!(Cli::try_parse_from(["maestro", "config", "init", "cluster"]).is_ok());
    assert!(
        Cli::try_parse_from([
            "maestro",
            "config",
            "init",
            "services",
            "--output",
            "services.jsonc",
        ])
        .is_ok()
    );
    assert!(
        Cli::try_parse_from([
            "maestro",
            "cluster",
            "sync-preview-config",
            "--config",
            "aws-secret://maestro/production/config.json",
        ])
        .is_ok()
    );
    assert!(
        Cli::try_parse_from([
            "maestro",
            "cluster",
            "rotate-jwt-key",
            "--config",
            "maestro.jsonc",
            "--launch",
            "/run/maestro/launch.json",
        ])
        .is_ok()
    );
    assert!(
        Cli::try_parse_from([
            "maestro",
            "cluster",
            "prepare-cutover",
            "--config",
            "maestro.jsonc",
            "--authority-data-dir",
            "/var/lib/maestro-cutover",
            "--data-dir",
            "/var/lib/maestro",
            "--etcd-binary",
            "/run/current-system/sw/bin/etcd",
            "--store-secret-file",
            "/run/maestro/store-secret",
            "--output-dir",
            "/run/maestro/cutover-launches",
        ])
        .is_ok()
    );
    assert!(Cli::try_parse_from(["maestro", "cluster", "upgrade", "system", "--yes"]).is_err());
    assert!(
        Cli::try_parse_from([
            "maestro",
            "cluster",
            "bootstrap",
            "--config",
            "maestro.jsonc",
            "--data-dir",
            "/var/lib/maestro",
            "--etcd-binary",
            "/run/current-system/sw/bin/etcd",
            "--output",
            "/var/lib/maestro/launch.json",
        ])
        .is_ok()
    );
    assert!(
        Cli::try_parse_from([
            "maestro",
            "cluster",
            "join",
            "https://10.20.0.11:3000",
            "--config",
            "maestro.jsonc",
            "--data-dir",
            "/var/lib/maestro",
            "--etcd-binary",
            "/run/current-system/sw/bin/etcd",
            "--output",
            "/var/lib/maestro/launch.json",
        ])
        .is_ok()
    );
    assert!(
        Cli::try_parse_from([
            "maestro",
            "cluster",
            "join",
            "https://10.20.0.11:3000",
            "--prepare",
            "--data-dir",
            "/var/lib/maestro",
        ])
        .is_err()
    );
    assert!(
        Cli::try_parse_from([
            "maestro",
            "cluster",
            "join",
            "--data-dir",
            "/var/lib/maestro",
        ])
        .is_err()
    );
    assert!(Cli::try_parse_from(["maestro", "config", "validate", "maestro.jsonc"]).is_ok());
    assert!(Cli::try_parse_from(["maestro", "contexts", "set", "dev", "localhost:3000",]).is_ok());
    assert!(Cli::try_parse_from(["maestro", "contexts", "use", "dev"]).is_ok());
    assert!(Cli::try_parse_from(["maestro", "contexts", "ls"]).is_ok());
    assert!(Cli::try_parse_from(["maestro", "contexts", "remove", "dev"]).is_ok());
    assert!(Cli::try_parse_from(["maestro", "contexts", "login", "--days", "30"]).is_ok());
    assert!(
        Cli::try_parse_from([
            "maestro",
            "auth",
            "token",
            "--config",
            "maestro.jsonc",
            "--expires-in",
            "1h",
            "--access-level",
            "read-only",
        ])
        .is_ok()
    );
    assert!(
        Cli::try_parse_from([
            "maestro",
            "auth",
            "token",
            "--jwt-secret-key",
            "operator-test-secret-with-at-least-32-characters",
            "--expires-in",
            "forever",
            "--access-level",
            "operator",
        ])
        .is_err()
    );
    assert!(Cli::try_parse_from(["maestro", "cluster", "info"]).is_ok());
    assert!(Cli::try_parse_from(["maestro", "cluster", "nodes"]).is_ok());
    assert!(Cli::try_parse_from(["maestro", "cluster", "config"]).is_ok());
    assert!(
        Cli::try_parse_from([
            "maestro",
            "cluster",
            "rotate-tailscale-key",
            "--auth-key-source",
            "aws-secret://maestro/production/tailscale-auth-key",
            "--idempotency-key",
            "tailscale-key-1",
        ])
        .is_ok()
    );
    assert!(
        Cli::try_parse_from([
            "maestro",
            "cluster",
            "init-ca",
            "--config",
            "maestro.jsonc",
            "--data-dir",
            "/var/lib/maestro",
        ])
        .is_ok()
    );
    assert!(
        Cli::try_parse_from([
            "maestro",
            "cluster",
            "issue-node",
            "--config",
            "maestro.jsonc",
            "--data-dir",
            "/var/lib/maestro",
            "--node-id",
            "node-a",
            "--output",
            "/run/maestro/node-a.json",
        ])
        .is_ok()
    );
    for command in ["drain", "restore"] {
        assert!(
            Cli::try_parse_from([
                "maestro",
                "cluster",
                command,
                "node-a",
                "--idempotency-key",
                "node-command-1",
            ])
            .is_ok()
        );
    }
    assert!(
        Cli::try_parse_from([
            "maestro",
            "cluster",
            "remove-node",
            "node-a",
            "--idempotency-key",
            "remove-node-1",
        ])
        .is_ok()
    );
    assert!(Cli::try_parse_from(["maestro", "cluster", "upgrades"]).is_ok());
    assert!(
        Cli::try_parse_from([
            "maestro",
            "cluster",
            "restart",
            "node-a",
            "--restart-run-id",
            "restart-node-a",
            "--idempotency-key",
            "restart-node-a-1",
            "--yes",
        ])
        .is_ok()
    );
    assert!(Cli::try_parse_from(["maestro", "cluster", "restart", "--all", "-y"]).is_ok());
    assert!(Cli::try_parse_from(["maestro", "cluster", "restart"]).is_ok());
    assert!(Cli::try_parse_from(["maestro", "cluster", "restart", "--local"]).is_ok());
    assert!(Cli::try_parse_from(["maestro", "cluster", "restart", "node-a", "--all"]).is_err());
    assert!(Cli::try_parse_from(["maestro", "cluster", "restart", "node-a", "--local"]).is_err());
    assert!(Cli::try_parse_from(["maestro", "cluster", "restart", "--all", "--local"]).is_err());
    assert!(
        Cli::try_parse_from([
            "maestro",
            "exec",
            "api",
            "--deployment",
            "api-v1",
            "--replica",
            "2",
            "--node",
            "node-a",
            "--",
            "/bin/sh",
            "-lc",
            "echo ready",
        ])
        .is_ok()
    );
    assert!(
        Cli::try_parse_from([
            "maestro",
            "cluster",
            "upgrade",
            "--target-version",
            "2.0.0",
            "--batch",
            "all",
            "--node",
            "node-a",
            "--upgrade-run-id",
            "upgrade-1",
            "--yes",
        ])
        .is_ok()
    );
    assert!(Cli::try_parse_from(["maestro", "cluster", "upgrade", "--batch=all", "-y",]).is_ok());
    assert!(
        Cli::try_parse_from([
            "maestro",
            "cluster",
            "unfreeze",
            "--upgrade-run",
            "upgrade-1",
        ])
        .is_ok()
    );
    assert!(Cli::try_parse_from(["maestro", "services", "ls"]).is_ok());
    assert!(Cli::try_parse_from(["maestro", "services", "deployments", "api"]).is_ok());
    assert!(
        Cli::try_parse_from([
            "maestro",
            "services",
            "rollout",
            "--config",
            "services.jsonc",
            "--service",
            "api",
            "--apply",
            "-y",
        ])
        .is_ok()
    );
    assert!(
        Cli::try_parse_from([
            "maestro",
            "services",
            "redeploy",
            "api",
            "--idempotency-key",
            "redeploy-1",
        ])
        .is_ok()
    );
    assert!(Cli::try_parse_from(["maestro", "services", "cancel", "api", "deployment-1",]).is_ok());
    for command in ["restart", "remove"] {
        assert!(
            Cli::try_parse_from(["maestro", "services", command, "api", "deployment-1",]).is_ok()
        );
    }
    for command in ["freeze", "unfreeze", "delete"] {
        assert!(Cli::try_parse_from(["maestro", "services", command, "api",]).is_ok());
    }
    assert!(Cli::try_parse_from(["maestro", "services", "replicas", "set", "api", "3",]).is_ok());
    assert!(Cli::try_parse_from(["maestro", "services", "replicas", "clear", "api",]).is_ok());
    assert!(
        Cli::try_parse_from([
            "maestro",
            "services",
            "up",
            "api",
            "--config",
            "services.jsonc",
            "--context",
            "./api",
            "--idempotency-key",
            "up-1",
        ])
        .is_ok()
    );
    assert!(
        Cli::try_parse_from([
            "maestro",
            "services",
            "up",
            "--config",
            "services.jsonc",
            "--context",
            "./api",
        ])
        .is_ok()
    );
}

#[tokio::test]
async fn config_init_prompts_without_loading_an_api_context()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let destination = directory.path().join("services.jsonc");
    let cli = Cli::try_parse_from([
        "maestro",
        "config",
        "init",
        "--output",
        destination.to_str().ok_or("non-UTF-8 destination")?,
    ])?;
    let mut input = std::io::Cursor::new(b"services\n".to_vec());
    let mut output = Vec::new();
    run(cli, &mut input, &mut output).await?;
    assert!(destination.exists());
    let output = String::from_utf8(output)?;
    assert!(output.contains("Config kind (cluster/services):"));
    assert!(output.contains("[maestro]: created"));
    Ok(())
}

#[tokio::test]
async fn auth_token_uses_the_cluster_config_key_or_an_explicit_key()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let config_path = directory.path().join("maestro.jsonc");
    crate::config::init(
        crate::config::ConfigKind::Cluster,
        Some(&config_path),
        &mut Vec::new(),
    )?;
    for arguments in [
        vec![
            "maestro",
            "auth",
            "token",
            "--config",
            config_path.to_str().ok_or("non-UTF-8 config path")?,
            "--expires-in",
            "15m",
            "--access-level",
            "read-only",
        ],
        vec![
            "maestro",
            "auth",
            "token",
            "--jwt-secret-key",
            "operator-test-secret-with-at-least-32-characters",
            "--expires-in",
            "15m",
            "--access-level",
            "operator",
        ],
    ] {
        let cli = Cli::try_parse_from(arguments)?;
        let mut output = Vec::new();
        run(cli, &mut std::io::Cursor::new(Vec::new()), &mut output).await?;
        let token = String::from_utf8(output)?;
        assert_eq!(token.trim().split('.').count(), 3);
    }
    Ok(())
}

#[tokio::test]
async fn restart_confirmation_can_abort_before_loading_an_api_context()
-> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::try_parse_from(["maestro", "cluster", "restart", "node-a"])?;
    let mut input = std::io::Cursor::new(b"no\n".to_vec());
    let mut output = Vec::new();

    run(cli, &mut input, &mut output).await?;

    let output = String::from_utf8(output)?;
    assert!(output.contains("Restart cluster node `node-a`? [y/N]:"));
    assert!(output.ends_with("[maestro]: aborted\n"));
    Ok(())
}

#[tokio::test]
async fn upgrade_confirmation_warns_and_aborts_before_loading_an_api_context()
-> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::try_parse_from([
        "maestro",
        "cluster",
        "upgrade",
        "--batch=all",
        "--node",
        "node-b",
        "--node",
        "node-a",
    ])?;
    let mut input = std::io::Cursor::new(b"no\n".to_vec());
    let mut output = Vec::new();

    run(cli, &mut input, &mut output).await?;

    let output = String::from_utf8(output)?;
    let expected = format!(
        "Upgrade cluster nodes `node-b`, `node-a` to Maestro {} or newer in one batch; \
         services and the control plane will be unavailable? [y/N]:",
        kernel_api::MAESTRO_VERSION
    );
    assert!(output.contains(&expected));
    assert!(output.ends_with("[maestro]: aborted\n"));
    Ok(())
}
