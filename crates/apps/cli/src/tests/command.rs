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
    assert!(Cli::try_parse_from(["maestro-next", "config", "validate", "maestro.jsonc"]).is_ok());
    assert!(
        Cli::try_parse_from(["maestro-next", "contexts", "set", "dev", "localhost:3000",]).is_ok()
    );
    assert!(Cli::try_parse_from(["maestro-next", "contexts", "use", "dev"]).is_ok());
    assert!(Cli::try_parse_from(["maestro-next", "contexts", "ls"]).is_ok());
    assert!(Cli::try_parse_from(["maestro-next", "contexts", "remove", "dev"]).is_ok());
    assert!(Cli::try_parse_from(["maestro-next", "contexts", "login", "--days", "30"]).is_ok());
    assert!(Cli::try_parse_from(["maestro-next", "services", "ls"]).is_ok());
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
