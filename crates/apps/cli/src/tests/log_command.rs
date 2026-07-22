use clap::Parser;

use crate::Cli;

#[test]
fn log_command_surface_matches_the_harvested_cli() {
    assert!(Cli::try_parse_from(["maestro-next", "logs", "--no-follow"]).is_ok());
    assert!(
        Cli::try_parse_from([
            "maestro-next",
            "logs",
            "--service",
            "api",
            "--deployment",
            "api-v1",
            "--query",
            "level:error",
            "--tail",
            "250",
            "--output",
            "json",
            "--no-follow",
        ])
        .is_ok()
    );
    assert!(
        Cli::try_parse_from([
            "maestro-next",
            "logs",
            "--system",
            "daemon",
            "--service",
            "api",
        ])
        .is_err()
    );
    assert!(Cli::try_parse_from(["maestro-next", "logs", "--deployment", "api-v1",]).is_err());
}
