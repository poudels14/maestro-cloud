use std::path::{Path, PathBuf};
use std::sync::Arc;

use semver::Version;
use serde::{Deserialize, Serialize};
use upgrade::{
    NixosUpgradeStager, NixosUpgradeStagerSettings, NodeRebooter, ProcessNixosUpgradeStager,
    ProcessNodeRebooter,
};

/// Explicit NixOS host-upgrade policy from the protected launch document.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NixosUpgradeLaunchConfig {
    /// Absolute flake directory whose lock file tracks the desired system.
    pub flake: PathBuf,
    /// NixOS configuration selected from the flake.
    #[serde(default = "default_configuration")]
    pub configuration: String,
    /// Rewritten daemon manifest below `services.maestro.source`.
    #[serde(default = "default_manifest_relative_path")]
    pub manifest_relative_path: PathBuf,
    /// Optional hermetic path to the Nix executable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nix_binary: Option<PathBuf>,
    /// Optional hermetic path to the NixOS rebuild executable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nixos_rebuild_binary: Option<PathBuf>,
    /// Optional hermetic path to systemctl.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub systemctl_binary: Option<PathBuf>,
}

impl NixosUpgradeLaunchConfig {
    /// Selects a flake with the standard configuration, manifest, and process paths.
    pub fn new(flake: impl Into<PathBuf>) -> Self {
        Self {
            flake: flake.into(),
            configuration: default_configuration(),
            manifest_relative_path: default_manifest_relative_path(),
            nix_binary: None,
            nixos_rebuild_binary: None,
            systemctl_binary: None,
        }
    }

    /// Validates paths and source-version settings without touching the host.
    pub fn validate(&self) -> Result<(), NixosUpgradeLaunchError> {
        self.configure().map(|_| ())
    }

    pub(crate) fn configure(&self) -> Result<ConfiguredNixosUpgrade, NixosUpgradeLaunchError> {
        validate_optional_binary(self.nix_binary.as_deref(), "nix")?;
        validate_optional_binary(self.nixos_rebuild_binary.as_deref(), "nixos-rebuild")?;
        validate_optional_binary(self.systemctl_binary.as_deref(), "systemctl")?;
        if self.nix_binary.is_some() != self.nixos_rebuild_binary.is_some() {
            return Err(NixosUpgradeLaunchError::IncompleteNixBinaries);
        }
        let running_version = Version::parse(env!("CARGO_PKG_VERSION")).map_err(|error| {
            NixosUpgradeLaunchError::InvalidRunningVersion {
                message: error.to_string(),
            }
        })?;
        let mut settings = NixosUpgradeStagerSettings::new(
            self.flake.clone(),
            self.configuration.clone(),
            self.manifest_relative_path.clone(),
            running_version.clone(),
        )?;
        if let (Some(nix), Some(rebuild)) = (&self.nix_binary, &self.nixos_rebuild_binary) {
            settings = settings.with_binaries(nix.clone(), rebuild.clone())?;
        }
        let rebooter = match &self.systemctl_binary {
            Some(binary) => ProcessNodeRebooter::with_binary(binary.clone()),
            None => ProcessNodeRebooter::new(),
        };
        Ok(ConfiguredNixosUpgrade {
            stager: Arc::new(ProcessNixosUpgradeStager::new(settings)),
            rebooter: Arc::new(rebooter),
            running_version,
        })
    }
}

#[cfg_attr(any(target_os = "macos", feature = "macos-platform"), allow(dead_code))]
pub(crate) struct ConfiguredNixosUpgrade {
    pub(crate) stager: Arc<dyn NixosUpgradeStager>,
    pub(crate) rebooter: Arc<dyn NodeRebooter>,
    pub(crate) running_version: Version,
}

fn validate_optional_binary(
    path: Option<&Path>,
    name: &'static str,
) -> Result<(), NixosUpgradeLaunchError> {
    if path.is_some_and(|path| !path.is_absolute()) {
        Err(NixosUpgradeLaunchError::RelativeBinary { name })
    } else {
        Ok(())
    }
}

fn default_configuration() -> String {
    "default".to_string()
}

fn default_manifest_relative_path() -> PathBuf {
    PathBuf::from("crates/apps/daemon/Cargo.toml")
}

/// Invalid NixOS host-upgrade launch policy.
#[derive(Debug, thiserror::Error)]
pub enum NixosUpgradeLaunchError {
    /// Flake, configuration, manifest, or staging process policy was invalid.
    #[error(transparent)]
    Staging(#[from] upgrade::NixosUpgradeStagingError),
    /// An explicit binary path must not depend on the launch environment's PATH.
    #[error("explicit {name} binary path must be absolute")]
    RelativeBinary { name: &'static str },
    /// Nix and nixos-rebuild overrides must be selected as one hermetic pair.
    #[error("nix and nixos-rebuild binary overrides must both be configured")]
    IncompleteNixBinaries,
    /// The daemon package itself declared a non-semantic version.
    #[error("daemon package version is invalid: {message}")]
    InvalidRunningVersion { message: String },
}
