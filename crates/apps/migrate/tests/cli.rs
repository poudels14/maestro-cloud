use std::process::Command;

use migrate::LegacySnapshot;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[test]
fn command_surface_separates_capture_plan_apply_and_verify() -> TestResult {
    let help = migration_command().arg("--help").output()?;
    assert!(help.status.success());
    let stdout = String::from_utf8(help.stdout)?;
    assert!(stdout.contains("capture"));
    assert!(stdout.contains("plan"));
    assert!(stdout.contains("apply"));
    assert!(stdout.contains("verify"));
    assert!(stdout.contains("store-plan"));
    assert!(stdout.contains("store-restore"));
    assert!(stdout.contains("store-verify"));
    assert!(stdout.contains("telemetry-plan"));
    assert!(stdout.contains("telemetry-apply"));
    assert!(stdout.contains("telemetry-verify"));

    for command in [
        "capture",
        "apply",
        "verify",
        "store-plan",
        "store-restore",
        "store-verify",
        "telemetry-plan",
        "telemetry-apply",
        "telemetry-verify",
    ] {
        let output = migration_command().arg(command).output()?;
        assert_eq!(output.status.code(), Some(2));
    }
    Ok(())
}

#[cfg(unix)]
#[test]
fn plan_rejects_a_public_master_secret_file() -> TestResult {
    use std::os::unix::fs::PermissionsExt;

    let directory = tempfile::tempdir()?;
    let snapshot = directory.path().join("snapshot.json");
    std::fs::write(
        &snapshot,
        LegacySnapshot::new(Vec::new())?.encode_artifact()?,
    )?;
    std::fs::set_permissions(&snapshot, std::fs::Permissions::from_mode(0o600))?;
    let secret = directory.path().join("master-secret");
    std::fs::write(&secret, "a".repeat(32))?;
    std::fs::set_permissions(&secret, std::fs::Permissions::from_mode(0o644))?;

    let output = migration_command()
        .args([
            "plan",
            "--snapshot",
            snapshot.to_str().ok_or("snapshot path is not UTF-8")?,
            "--master-secret-file",
            secret.to_str().ok_or("secret path is not UTF-8")?,
        ])
        .output()?;
    assert_eq!(output.status.code(), Some(1));
    let event = structured_error(output.stderr)?;
    assert_eq!(
        event.get("level").and_then(serde_json::Value::as_str),
        Some("ERROR")
    );
    assert_eq!(
        event.get("message").and_then(serde_json::Value::as_str),
        Some("maestro migration failed")
    );
    assert!(
        event
            .get("error")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|error| error.contains("owner-only permissions"))
    );
    Ok(())
}

fn migration_command() -> Command {
    Command::new(env!("CARGO_BIN_EXE_maestro-migrate"))
}

fn structured_error(stderr: Vec<u8>) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    let stderr = String::from_utf8(stderr)?;
    let mut lines = stderr.lines();
    let line = lines
        .next()
        .ok_or_else(|| std::io::Error::other("migration tool emitted no structured error"))?;
    if lines.next().is_some() {
        return Err(
            std::io::Error::other("migration tool emitted more than one error event").into(),
        );
    }
    serde_json::from_str(line).map_err(Into::into)
}
