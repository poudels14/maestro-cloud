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
fn start_requires_its_explicit_subcommand() -> TestResult {
    let directory = tempfile::tempdir()?;
    let config = directory.path().join("missing-launch.json");
    let explicit = daemon_command().args(["start"]).arg(&config).output()?;
    let positional = daemon_command().arg(&config).output()?;

    assert_eq!(explicit.status.code(), Some(1));
    assert!(explicit.stdout.is_empty());
    let event = structured_error(explicit.stderr)?;
    assert_eq!(
        event.get("level").and_then(serde_json::Value::as_str),
        Some("ERROR")
    );
    assert_eq!(
        event.get("message").and_then(serde_json::Value::as_str),
        Some("maestro daemon failed")
    );
    assert!(
        event
            .get("error")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|error| error.contains("daemon launch document"))
    );
    assert_eq!(positional.status.code(), Some(2));
    assert!(positional.stdout.is_empty());
    assert!(String::from_utf8(positional.stderr)?.contains("unrecognized subcommand"));
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

#[test]
fn local_logs_command_retains_source_tail_and_follow_options() -> TestResult {
    let directory = tempfile::tempdir()?;
    let config = directory.path().join("missing-launch.json");
    let output = daemon_command()
        .arg("logs")
        .arg(&config)
        .args([
            "--source",
            "api/deployment/workload",
            "--tail",
            "25",
            "--follow",
        ])
        .output()?;

    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    let error = String::from_utf8(output.stderr)?;
    assert!(error.contains("daemon launch document"));
    assert!(!error.contains("unexpected argument"));
    Ok(())
}

fn daemon_command() -> Command {
    Command::new(env!("CARGO_BIN_EXE_daemon"))
}

fn structured_error(stderr: Vec<u8>) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    let stderr = String::from_utf8(stderr)?;
    let mut lines = stderr.lines();
    let line = lines
        .next()
        .ok_or_else(|| std::io::Error::other("daemon emitted no structured error"))?;
    if lines.next().is_some() {
        return Err(std::io::Error::other("daemon emitted more than one error event").into());
    }
    serde_json::from_str(line).map_err(Into::into)
}
