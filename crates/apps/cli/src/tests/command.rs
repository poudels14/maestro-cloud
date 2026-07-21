use clap::Parser;

use crate::Cli;

#[test]
fn context_command_surface_matches_the_rewrite_contract() {
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
}
