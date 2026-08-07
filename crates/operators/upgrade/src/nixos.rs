use std::ffi::OsString;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;

use async_trait::async_trait;
use semver::Version;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::process::Command;

const MAX_COMMAND_OUTPUT_BYTES: usize = 64 * 1_024;
const SYSTEMD_RUN_BINARY: &str = "systemd-run";
const NIX_CPU_WEIGHT: &str = "CPUWeight=10";
const NIX_IO_WEIGHT: &str = "IOWeight=10";
const NIX_MEMORY_HIGH: &str = "MemoryHigh=50%";
const NIX_MEMORY_MAX: &str = "MemoryMax=70%";
const NIX_NICE_LEVEL: &str = "10";

/// Validated source selected by a staged NixOS boot generation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NixosUpgradeSource {
    version: Version,
}

impl NixosUpgradeSource {
    pub(crate) fn new(version: Version) -> Self {
        Self { version }
    }

    /// Returns the Maestro version declared by the evaluated source.
    pub fn version(&self) -> &Version {
        &self.version
    }
}

/// Node-local boundary that prepares, but does not activate, a NixOS upgrade.
///
/// Successful calls guarantee that a boot generation was built from a source
/// newer than the running daemon and no older than the requested version.
/// Replays converge on the currently selected flake state without activating
/// it. Cancellation kills an active child process.
#[async_trait]
pub trait NixosUpgradeStager: Send + Sync {
    /// Updates the configured flake, validates its Maestro source, and builds it for boot.
    async fn stage(
        &self,
        minimum_version: &Version,
    ) -> Result<NixosUpgradeSource, NixosUpgradeStagingError>;
}

/// Static paths and version policy for the production NixOS staging adapter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NixosUpgradeStagerSettings {
    flake: PathBuf,
    configuration: String,
    running_version: Version,
    nix_binary: PathBuf,
    nixos_rebuild_binary: PathBuf,
}

impl NixosUpgradeStagerSettings {
    /// Validates one flake selection used to build and verify upgrades.
    pub fn new(
        flake: impl Into<PathBuf>,
        configuration: impl Into<String>,
        running_version: Version,
    ) -> Result<Self, NixosUpgradeStagingError> {
        let settings = Self {
            flake: flake.into(),
            configuration: configuration.into(),
            running_version,
            nix_binary: PathBuf::from("nix"),
            nixos_rebuild_binary: PathBuf::from("nixos-rebuild"),
        };
        settings.validate()?;
        Ok(settings)
    }

    /// Selects the standard Maestro NixOS flake and canonical release version.
    pub fn production(running_version: Version) -> Result<Self, NixosUpgradeStagingError> {
        Self::new("/etc/maestro", "default", running_version)
    }

    /// Replaces process paths for hermetic packaging without changing arguments.
    pub fn with_binaries(
        mut self,
        nix_binary: impl Into<PathBuf>,
        nixos_rebuild_binary: impl Into<PathBuf>,
    ) -> Result<Self, NixosUpgradeStagingError> {
        self.nix_binary = nix_binary.into();
        self.nixos_rebuild_binary = nixos_rebuild_binary.into();
        self.validate()?;
        Ok(self)
    }

    fn validate(&self) -> Result<(), NixosUpgradeStagingError> {
        if !self.flake.is_absolute() {
            return Err(rejected("NixOS upgrade flake path must be absolute"));
        }
        if self.flake.to_str().is_none() {
            return Err(rejected("NixOS upgrade flake path must be valid UTF-8"));
        }
        if self.configuration.is_empty()
            || !self.configuration.chars().all(|character| {
                character.is_ascii_alphanumeric() || matches!(character, '-' | '_')
            })
        {
            return Err(rejected(
                "NixOS configuration must contain only ASCII letters, digits, '-' or '_'",
            ));
        }
        if self.nix_binary.as_os_str().is_empty()
            || self.nixos_rebuild_binary.as_os_str().is_empty()
        {
            return Err(rejected("NixOS command paths must not be empty"));
        }
        Ok(())
    }

    fn version_attribute(&self) -> OsString {
        format!(
            "{}#nixosConfigurations.{}.config.services.maestro.package.version",
            self.flake.display(),
            self.configuration
        )
        .into()
    }

    fn configuration_selector(&self) -> OsString {
        format!("{}#{}", self.flake.display(), self.configuration).into()
    }
}

/// Production NixOS stager using bounded, kill-on-cancel child processes.
pub struct ProcessNixosUpgradeStager {
    settings: NixosUpgradeStagerSettings,
    runner: Arc<dyn NixosCommandRunner>,
}

impl ProcessNixosUpgradeStager {
    /// Builds a process adapter from validated static settings.
    pub fn new(settings: NixosUpgradeStagerSettings) -> Self {
        Self::with_runner(settings, Arc::new(ProcessNixosCommandRunner::isolated()))
    }

    pub(crate) fn with_runner(
        settings: NixosUpgradeStagerSettings,
        runner: Arc<dyn NixosCommandRunner>,
    ) -> Self {
        Self { settings, runner }
    }
}

#[async_trait]
impl NixosUpgradeStager for ProcessNixosUpgradeStager {
    async fn stage(
        &self,
        minimum_version: &Version,
    ) -> Result<NixosUpgradeSource, NixosUpgradeStagingError> {
        self.runner
            .run(NixosCommand::new(
                self.settings.nix_binary.clone(),
                [
                    OsString::from("flake"),
                    OsString::from("update"),
                    OsString::from("--flake"),
                    self.settings.flake.clone().into_os_string(),
                ],
            ))
            .await
            .map_err(|error| unavailable("update NixOS flake", error))?;
        let evaluated_version = self
            .runner
            .run(NixosCommand::new(
                self.settings.nix_binary.clone(),
                [
                    OsString::from("eval"),
                    OsString::from("--raw"),
                    self.settings.version_attribute(),
                ],
            ))
            .await
            .map_err(|error| unavailable("evaluate updated Maestro version", error))?;
        let source_version = parse_maestro_version(&evaluated_version.stdout)?;
        validate_source_version(
            &source_version,
            &self.settings.running_version,
            minimum_version,
        )?;
        self.runner
            .run(NixosCommand::new(
                self.settings.nixos_rebuild_binary.clone(),
                [
                    OsString::from("boot"),
                    OsString::from("--flake"),
                    self.settings.configuration_selector(),
                ],
            ))
            .await
            .map_err(|error| unavailable("build NixOS boot generation", error))?;
        Ok(NixosUpgradeSource::new(source_version))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NixosCommand {
    pub(crate) executable: PathBuf,
    pub(crate) arguments: Vec<OsString>,
}

impl NixosCommand {
    pub(crate) fn new(executable: PathBuf, arguments: impl IntoIterator<Item = OsString>) -> Self {
        Self {
            executable,
            arguments: arguments.into_iter().collect(),
        }
    }
}

pub(crate) struct NixosCommandOutput {
    pub(crate) stdout: Vec<u8>,
}

#[async_trait]
pub(crate) trait NixosCommandRunner: Send + Sync {
    async fn run(&self, command: NixosCommand) -> Result<NixosCommandOutput, NixosCommandError>;
}

pub(crate) struct ProcessNixosCommandRunner {
    isolate_resources: bool,
}

impl ProcessNixosCommandRunner {
    pub(crate) const fn direct() -> Self {
        Self {
            isolate_resources: false,
        }
    }

    pub(crate) const fn isolated() -> Self {
        Self {
            isolate_resources: true,
        }
    }

    pub(crate) fn process_invocation(&self, invocation: NixosCommand) -> NixosCommand {
        if !self.isolate_resources {
            return invocation;
        }

        // A scope keeps the upgrade process as our child, so cancellation still
        // kills it, while systemd accounts and schedules the complete Nix process
        // tree independently from Maestro and its embedded etcd process.
        let mut arguments = vec![
            OsString::from("--quiet"),
            OsString::from("--scope"),
            OsString::from("--collect"),
            OsString::from("--nice"),
            OsString::from(NIX_NICE_LEVEL),
            OsString::from("--property"),
            OsString::from(NIX_CPU_WEIGHT),
            OsString::from("--property"),
            OsString::from(NIX_IO_WEIGHT),
            OsString::from("--property"),
            OsString::from(NIX_MEMORY_HIGH),
            OsString::from("--property"),
            OsString::from(NIX_MEMORY_MAX),
            invocation.executable.clone().into_os_string(),
        ];
        arguments.extend(invocation.arguments);
        NixosCommand::new(PathBuf::from(SYSTEMD_RUN_BINARY), arguments)
    }
}

#[async_trait]
impl NixosCommandRunner for ProcessNixosCommandRunner {
    async fn run(&self, invocation: NixosCommand) -> Result<NixosCommandOutput, NixosCommandError> {
        let invocation = self.process_invocation(invocation);
        let mut command = Command::new(&invocation.executable);
        command
            .args(&invocation.arguments)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        let mut child = command.spawn().map_err(|error| NixosCommandError::Spawn {
            executable: invocation.executable.clone(),
            message: error.to_string(),
        })?;
        let stdout = child
            .stdout
            .take()
            .ok_or(NixosCommandError::MissingPipe { stream: "stdout" })?;
        let stderr = child
            .stderr
            .take()
            .ok_or(NixosCommandError::MissingPipe { stream: "stderr" })?;
        let output = tokio::try_join!(
            child.wait(),
            read_bounded(stdout, MAX_COMMAND_OUTPUT_BYTES),
            read_bounded(stderr, MAX_COMMAND_OUTPUT_BYTES),
        );
        let (status, stdout, stderr) = match output {
            Ok(output) => output,
            Err(error) => {
                let _ = child.kill().await;
                let _ = child.wait().await;
                return Err(NixosCommandError::Output {
                    message: error.to_string(),
                });
            }
        };
        if !status.success() {
            return Err(NixosCommandError::Exit {
                status: status.to_string(),
                stderr: safe_output(&stderr),
            });
        }
        Ok(NixosCommandOutput { stdout })
    }
}

async fn read_bounded(
    mut reader: impl AsyncRead + Unpin,
    maximum: usize,
) -> Result<Vec<u8>, std::io::Error> {
    let mut output = Vec::new();
    let mut buffer = [0_u8; 8 * 1_024];
    loop {
        let count = reader.read(&mut buffer).await?;
        if count == 0 {
            return Ok(output);
        }
        if output.len().saturating_add(count) > maximum {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("command output exceeded {maximum} bytes"),
            ));
        }
        let bytes = buffer.get(..count).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "command returned an invalid read length",
            )
        })?;
        output.extend_from_slice(bytes);
    }
}

fn parse_maestro_version(output: &[u8]) -> Result<Version, NixosUpgradeStagingError> {
    let value = std::str::from_utf8(output)
        .map_err(|_| rejected("evaluated Maestro version is not valid UTF-8"))?
        .trim();
    Version::parse(value)
        .map_err(|error| rejected(format!("invalid Maestro version `{value}`: {error}")))
}

pub(crate) fn validate_source_version(
    source: &Version,
    running: &Version,
    minimum: &Version,
) -> Result<(), NixosUpgradeStagingError> {
    if source <= running {
        return Err(rejected(format!(
            "updated Maestro source {source} is not newer than running version {running}"
        )));
    }
    if source < minimum {
        return Err(rejected(format!(
            "updated Maestro source {source} is older than requested minimum {minimum}"
        )));
    }
    Ok(())
}

fn safe_output(output: &[u8]) -> String {
    let message = String::from_utf8_lossy(output)
        .chars()
        .map(|character| {
            if character.is_control() {
                ' '
            } else {
                character
            }
        })
        .collect::<String>();
    if message.trim().is_empty() {
        "command exited unsuccessfully".to_string()
    } else {
        message.trim().to_string()
    }
}

fn unavailable(
    action: impl std::fmt::Display,
    error: impl std::fmt::Display,
) -> NixosUpgradeStagingError {
    NixosUpgradeStagingError::Unavailable {
        message: format!("failed to {action}: {error}"),
    }
}

fn rejected(message: impl Into<String>) -> NixosUpgradeStagingError {
    NixosUpgradeStagingError::Rejected {
        message: message.into(),
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum NixosCommandError {
    #[error("could not execute `{}`: {message}", executable.display())]
    Spawn {
        executable: PathBuf,
        message: String,
    },
    #[error("child process did not expose its {stream} pipe")]
    MissingPipe { stream: &'static str },
    #[error("child process output failed: {message}")]
    Output { message: String },
    #[error("child process exited with {status}: {stderr}")]
    Exit { status: String, stderr: String },
}

/// Matchable source policy or process failure while preparing a NixOS boot generation.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum NixosUpgradeStagingError {
    /// Process or filesystem availability prevented a conclusive staging result.
    #[error("NixOS upgrade staging is unavailable: {message}")]
    Unavailable { message: String },
    /// The selected source or static policy cannot satisfy the requested upgrade.
    #[error("NixOS upgrade staging rejected the source: {message}")]
    Rejected { message: String },
}
