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

    for command in ["capture", "apply", "verify"] {
        let output = migration_command().arg(command).output()?;
        assert_eq!(output.status.code(), Some(2));
    }
    Ok(())
}

#[test]
fn migration_tool_reports_its_package_version() -> TestResult {
    let output = migration_command().arg("--version").output()?;

    assert!(output.status.success());
    assert!(output.stderr.is_empty());
    assert_eq!(
        String::from_utf8(output.stdout)?,
        format!("maestro-migrate {}\n", env!("CARGO_PKG_VERSION"))
    );
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
    assert!(String::from_utf8(output.stderr)?.contains("owner-only permissions"));
    Ok(())
}

fn migration_command() -> Command {
    Command::new(env!("CARGO_BIN_EXE_maestro-migrate"))
}
