use clap::Parser;

use crate::{Cli, run};

#[test]
fn context_command_surface_matches_the_rewrite_contract() {
    assert!(Cli::try_parse_from(["maestro-next", "config", "init", "cluster"]).is_ok());
    assert!(
        Cli::try_parse_from([
            "maestro-next",
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
            "maestro-next",
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
            "maestro-next",
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
            "maestro-next",
            "cluster",
            "prepare-join",
            "--config",
            "maestro.jsonc",
            "--data-dir",
            "/var/lib/maestro",
        ])
        .is_ok()
    );
    assert!(
        Cli::try_parse_from([
            "maestro-next",
            "cluster",
            "approve-node",
            "node-a",
            "1111111111111111111111111111111111111111111111111111111111111111",
        ])
        .is_ok()
    );
    assert!(Cli::try_parse_from(["maestro-next", "config", "validate", "maestro.jsonc"]).is_ok());
    assert!(
        Cli::try_parse_from(["maestro-next", "contexts", "set", "dev", "localhost:3000",]).is_ok()
    );
    assert!(Cli::try_parse_from(["maestro-next", "contexts", "use", "dev"]).is_ok());
    assert!(Cli::try_parse_from(["maestro-next", "contexts", "ls"]).is_ok());
    assert!(Cli::try_parse_from(["maestro-next", "contexts", "remove", "dev"]).is_ok());
    assert!(Cli::try_parse_from(["maestro-next", "contexts", "login", "--days", "30"]).is_ok());
    assert!(Cli::try_parse_from(["maestro-next", "cluster", "info"]).is_ok());
    assert!(Cli::try_parse_from(["maestro-next", "cluster", "nodes"]).is_ok());
    assert!(Cli::try_parse_from(["maestro-next", "cluster", "config"]).is_ok());
    assert!(
        Cli::try_parse_from([
            "maestro-next",
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
            "maestro-next",
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
            "maestro-next",
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
                "maestro-next",
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
            "maestro-next",
            "cluster",
            "remove-node",
            "node-a",
            "--idempotency-key",
            "remove-node-1",
        ])
        .is_ok()
    );
    assert!(Cli::try_parse_from(["maestro-next", "cluster", "upgrades"]).is_ok());
    assert!(
        Cli::try_parse_from([
            "maestro-next",
            "cluster",
            "restart",
            "node-a",
            "--restart-run-id",
            "restart-node-a",
            "--idempotency-key",
            "restart-node-a-1",
        ])
        .is_ok()
    );
    assert!(Cli::try_parse_from(["maestro-next", "cluster", "restart", "--all"]).is_ok());
    assert!(Cli::try_parse_from(["maestro-next", "cluster", "restart"]).is_err());
    assert!(
        Cli::try_parse_from(["maestro-next", "cluster", "restart", "node-a", "--all"]).is_err()
    );
    assert!(
        Cli::try_parse_from([
            "maestro-next",
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
            "maestro-next",
            "cluster",
            "upgrade",
            "system",
            "--target-version",
            "2.0.0",
            "--batch",
            "all",
            "--node",
            "node-a",
            "--upgrade-run-id",
            "upgrade-1",
        ])
        .is_ok()
    );
    assert!(
        Cli::try_parse_from([
            "maestro-next",
            "cluster",
            "unfreeze",
            "--upgrade-run",
            "upgrade-1",
        ])
        .is_ok()
    );
    assert!(Cli::try_parse_from(["maestro-next", "services", "ls"]).is_ok());
    assert!(Cli::try_parse_from(["maestro-next", "services", "deployments", "api"]).is_ok());
    assert!(
        Cli::try_parse_from([
            "maestro-next",
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
            "maestro-next",
            "services",
            "redeploy",
            "api",
            "--idempotency-key",
            "redeploy-1",
        ])
        .is_ok()
    );
    assert!(
        Cli::try_parse_from(["maestro-next", "services", "cancel", "api", "deployment-1",]).is_ok()
    );
    for command in ["restart", "remove"] {
        assert!(
            Cli::try_parse_from(["maestro-next", "services", command, "api", "deployment-1",])
                .is_ok()
        );
    }
    for command in ["freeze", "unfreeze", "delete"] {
        assert!(Cli::try_parse_from(["maestro-next", "services", command, "api",]).is_ok());
    }
    assert!(
        Cli::try_parse_from(["maestro-next", "services", "replicas", "set", "api", "3",]).is_ok()
    );
    assert!(Cli::try_parse_from(["maestro-next", "services", "replicas", "clear", "api",]).is_ok());
    assert!(
        Cli::try_parse_from([
            "maestro-next",
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
}

#[tokio::test]
async fn config_init_prompts_without_loading_an_api_context()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let destination = directory.path().join("services.jsonc");
    let cli = Cli::try_parse_from([
        "maestro-next",
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
