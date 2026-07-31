use std::path::Path;

use clap::Parser;

use crate::Cli;
use crate::daemon_command::daemon_sibling;

#[test]
fn daemon_arguments_are_forwarded_without_reinterpretation() {
    assert!(
        Cli::try_parse_from([
            "maestro",
            "daemon",
            "start",
            "--data-dir",
            "/data/maestro/test",
            "--future-daemon-flag",
        ])
        .is_ok()
    );
    assert!(
        Cli::try_parse_from([
            "maestro",
            "daemon",
            "dead-letters",
            "/run/maestro/launch.json",
            "purge",
            "--all",
        ])
        .is_ok()
    );
}

#[test]
fn packaged_daemon_is_resolved_beside_maestro() {
    assert_eq!(
        daemon_sibling(Path::new("/opt/maestro/bin/maestro")),
        Path::new("/opt/maestro/bin/maestro-daemon")
    );
}
