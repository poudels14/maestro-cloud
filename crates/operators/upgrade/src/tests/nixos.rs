use std::collections::VecDeque;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};

use async_trait::async_trait;
use semver::Version;

use crate::nixos::{
    NixosCommand, NixosCommandError, NixosCommandOutput, NixosCommandRunner, parse_package_version,
    validate_source_version,
};
use crate::{
    NixosUpgradeStager, NixosUpgradeStagerSettings, NixosUpgradeStagingError,
    ProcessNixosUpgradeStager,
};

#[tokio::test]
async fn process_stager_validates_source_before_building_the_boot_generation()
-> Result<(), Box<dyn std::error::Error>> {
    let source = tempfile::tempdir()?;
    write_manifest(source.path(), "2.1.0").await?;
    let runner = Arc::new(RecordingRunner::new([
        Ok(output(Vec::new())),
        Ok(output(source.path().to_string_lossy().as_bytes().to_vec())),
        Ok(output(Vec::new())),
    ]));
    let stager = ProcessNixosUpgradeStager::with_runner(settings()?, runner.clone());

    let staged = stager.stage(&Version::new(2, 0, 0)).await?;

    assert_eq!(staged.path(), source.path());
    assert_eq!(staged.version(), &Version::new(2, 1, 0));
    assert_eq!(
        runner.commands(),
        vec![
            command(
                "/nix/bin/nix",
                ["flake", "update", "--flake", "/etc/maestro"]
            ),
            command(
                "/nix/bin/nix",
                [
                    "eval",
                    "--raw",
                    "/etc/maestro#nixosConfigurations.default.config.services.maestro.source",
                ],
            ),
            command(
                "/nix/bin/nixos-rebuild",
                ["boot", "--flake", "/etc/maestro#default"],
            ),
        ]
    );
    Ok(())
}

#[tokio::test]
async fn process_stager_rejects_a_stale_source_before_rebuilding()
-> Result<(), Box<dyn std::error::Error>> {
    let source = tempfile::tempdir()?;
    write_manifest(source.path(), "1.0.0").await?;
    let runner = Arc::new(RecordingRunner::new([
        Ok(output(Vec::new())),
        Ok(output(source.path().to_string_lossy().as_bytes().to_vec())),
    ]));
    let stager = ProcessNixosUpgradeStager::with_runner(settings()?, runner.clone());

    let error = stager
        .stage(&Version::new(1, 0, 1))
        .await
        .expect_err("stale source must be rejected");

    assert!(matches!(error, NixosUpgradeStagingError::Rejected { .. }));
    assert!(error.to_string().contains("not newer than running version"));
    assert_eq!(runner.commands().len(), 2);
    Ok(())
}

#[tokio::test]
async fn process_stager_rejects_a_source_below_the_requested_minimum()
-> Result<(), Box<dyn std::error::Error>> {
    let source = tempfile::tempdir()?;
    write_manifest(source.path(), "1.1.0").await?;
    let runner = Arc::new(RecordingRunner::new([
        Ok(output(Vec::new())),
        Ok(output(source.path().to_string_lossy().as_bytes().to_vec())),
    ]));
    let stager = ProcessNixosUpgradeStager::with_runner(settings()?, runner.clone());

    let error = stager
        .stage(&Version::new(1, 2, 0))
        .await
        .expect_err("source below the minimum must be rejected");

    assert!(matches!(error, NixosUpgradeStagingError::Rejected { .. }));
    assert!(error.to_string().contains("requested minimum 1.2.0"));
    assert_eq!(runner.commands().len(), 2);
    Ok(())
}

#[tokio::test]
async fn process_stager_classifies_command_failure_as_unavailable()
-> Result<(), Box<dyn std::error::Error>> {
    let runner = Arc::new(RecordingRunner::new([Err(NixosCommandError::Exit {
        status: "exit status: 1".to_string(),
        stderr: "network unavailable".to_string(),
    })]));
    let stager = ProcessNixosUpgradeStager::with_runner(settings()?, runner);

    let error = stager
        .stage(&Version::new(2, 0, 0))
        .await
        .expect_err("flake update failure must be retryable");

    assert!(matches!(
        error,
        NixosUpgradeStagingError::Unavailable { .. }
    ));
    assert!(error.to_string().contains("update NixOS flake"));
    Ok(())
}

#[test]
fn stager_settings_reject_ambiguous_paths_and_configuration_names() {
    let running = Version::new(1, 0, 0);
    assert!(
        NixosUpgradeStagerSettings::new(
            "etc/maestro",
            "default",
            "crates/apps/cli/Cargo.toml",
            running.clone(),
        )
        .is_err()
    );
    assert!(
        NixosUpgradeStagerSettings::new(
            "/etc/maestro",
            "default#other",
            "crates/apps/cli/Cargo.toml",
            running.clone(),
        )
        .is_err()
    );
    assert!(
        NixosUpgradeStagerSettings::new("/etc/maestro", "default", "../Cargo.toml", running,)
            .is_err()
    );
}

#[test]
fn checked_in_manifests_support_the_0_6_1_upgrade_bridge() -> Result<(), Box<dyn std::error::Error>>
{
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../..");
    let legacy_manifest = std::fs::read_to_string(workspace.join("crates/apps/daemon/Cargo.toml"))?;
    let current_manifest = std::fs::read_to_string(workspace.join("crates/apps/cli/Cargo.toml"))?;
    let running = Version::new(0, 6, 1);
    let minimum = parse_package_version(&current_manifest)?;

    validate_source_version(
        &parse_package_version(&legacy_manifest)?,
        &running,
        &minimum,
    )?;
    validate_source_version(&minimum, &running, &minimum)?;
    Ok(())
}

fn settings() -> Result<NixosUpgradeStagerSettings, NixosUpgradeStagingError> {
    NixosUpgradeStagerSettings::new(
        "/etc/maestro",
        "default",
        "crates/apps/cli/Cargo.toml",
        Version::new(1, 0, 0),
    )?
    .with_binaries("/nix/bin/nix", "/nix/bin/nixos-rebuild")
}

async fn write_manifest(root: &Path, version: &str) -> Result<(), std::io::Error> {
    let directory = root.join("crates/apps/cli");
    tokio::fs::create_dir_all(&directory).await?;
    tokio::fs::write(
        directory.join("Cargo.toml"),
        format!(
            "[workspace]\nmembers = []\n\n[package]\nname = \"maestro-cli\"\nversion = \"{version}\"\n\n[dependencies]\n"
        ),
    )
    .await
}

fn output(stdout: Vec<u8>) -> NixosCommandOutput {
    NixosCommandOutput { stdout }
}

fn command<const COUNT: usize>(executable: &str, arguments: [&str; COUNT]) -> NixosCommand {
    NixosCommand {
        executable: PathBuf::from(executable),
        arguments: arguments.into_iter().map(OsString::from).collect(),
    }
}

struct RecordingRunner {
    commands: Mutex<Vec<NixosCommand>>,
    results: Mutex<VecDeque<Result<NixosCommandOutput, NixosCommandError>>>,
}

impl RecordingRunner {
    fn new(
        results: impl IntoIterator<Item = Result<NixosCommandOutput, NixosCommandError>>,
    ) -> Self {
        Self {
            commands: Mutex::new(Vec::new()),
            results: Mutex::new(results.into_iter().collect()),
        }
    }

    fn commands(&self) -> Vec<NixosCommand> {
        lock(&self.commands).clone()
    }
}

#[async_trait]
impl NixosCommandRunner for RecordingRunner {
    async fn run(&self, command: NixosCommand) -> Result<NixosCommandOutput, NixosCommandError> {
        lock(&self.commands).push(command);
        lock(&self.results)
            .pop_front()
            .ok_or_else(|| NixosCommandError::Output {
                message: "fake runner exhausted".to_string(),
            })?
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}
