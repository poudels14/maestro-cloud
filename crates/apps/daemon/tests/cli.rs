use std::process::Command;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[test]
fn daemon_help_uses_clap_success_semantics() -> TestResult {
    let output = Command::new(env!("CARGO_BIN_EXE_daemon"))
        .arg("--help")
        .output()?;
    assert!(output.status.success());
    assert!(output.stderr.is_empty());
    assert!(String::from_utf8(output.stdout)?.contains("Maestro control-plane daemon"));
    Ok(())
}

#[test]
fn daemon_reports_its_package_version() -> TestResult {
    let output = Command::new(env!("CARGO_BIN_EXE_daemon"))
        .arg("--version")
        .output()?;

    assert!(output.status.success());
    assert!(output.stderr.is_empty());
    assert_eq!(
        String::from_utf8(output.stdout)?,
        format!("daemon {}\n", env!("CARGO_PKG_VERSION"))
    );
    Ok(())
}

#[test]
fn explicit_and_legacy_start_forms_select_the_same_config() -> TestResult {
    let directory = tempfile::tempdir()?;
    let config = directory.path().join("missing-launch.json");
    let explicit = daemon_command().args(["start"]).arg(&config).output()?;
    let legacy = daemon_command().arg(&config).output()?;

    assert_eq!(explicit.status.code(), Some(1));
    assert_eq!(legacy.status.code(), Some(1));
    assert!(explicit.stdout.is_empty());
    assert_eq!(explicit.stdout, legacy.stdout);
    assert_eq!(explicit.stderr, legacy.stderr);
    Ok(())
}

#[test]
fn dead_letter_commands_require_explicit_purge_scope() -> TestResult {
    let directory = tempfile::tempdir()?;
    let config = directory.path().join("missing-launch.json");
    let rejected = daemon_command()
        .arg("dead-letters")
        .arg(&config)
        .arg("purge")
        .output()?;
    assert_eq!(rejected.status.code(), Some(2));

    let all = daemon_command()
        .arg("dead-letters")
        .arg(&config)
        .args(["purge", "--all"])
        .output()?;
    let through = daemon_command()
        .arg("dead-letters")
        .arg(&config)
        .args(["purge", "--through-seq", "42"])
        .output()?;
    assert_eq!(all.status.code(), Some(1));
    assert_eq!(through.status.code(), Some(1));
    Ok(())
}

fn daemon_command() -> Command {
    Command::new(env!("CARGO_BIN_EXE_daemon"))
}
