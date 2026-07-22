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
