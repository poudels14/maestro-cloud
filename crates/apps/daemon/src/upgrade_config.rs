use std::path::Path;
use std::sync::Arc;

use cluster::NixosUpgradeLaunchConfig;
use semver::Version;
use upgrade::{
    NixosUpgradeStager, NixosUpgradeStagerSettings, NodeRebooter, ProcessNixosUpgradeStager,
    ProcessNodeRebooter,
};

/// Validates paths and source-version settings without touching the host.
pub(crate) fn validate_nixos_upgrade(
    config: &NixosUpgradeLaunchConfig,
) -> Result<(), NixosUpgradeLaunchError> {
    configure_nixos_upgrade(config).map(|_| ())
}

pub(crate) fn configure_nixos_upgrade(
    config: &NixosUpgradeLaunchConfig,
) -> Result<ConfiguredNixosUpgrade, NixosUpgradeLaunchError> {
    validate_optional_binary(config.nix_binary.as_deref(), "nix")?;
    validate_optional_binary(config.nixos_rebuild_binary.as_deref(), "nixos-rebuild")?;
    validate_optional_binary(config.systemctl_binary.as_deref(), "systemctl")?;
    if config.nix_binary.is_some() != config.nixos_rebuild_binary.is_some() {
        return Err(NixosUpgradeLaunchError::IncompleteNixBinaries);
    }
    let running_version = Version::parse(env!("CARGO_PKG_VERSION")).map_err(|error| {
        NixosUpgradeLaunchError::InvalidRunningVersion {
            message: error.to_string(),
        }
    })?;
    let mut settings = NixosUpgradeStagerSettings::new(
        config.flake.clone(),
        config.configuration.clone(),
        config.manifest_relative_path.clone(),
        running_version.clone(),
    )?;
    if let (Some(nix), Some(rebuild)) = (&config.nix_binary, &config.nixos_rebuild_binary) {
        settings = settings.with_binaries(nix.clone(), rebuild.clone())?;
    }
    let rebooter = match &config.systemctl_binary {
        Some(binary) => ProcessNodeRebooter::with_binary(binary.clone()),
        None => ProcessNodeRebooter::new(),
    };
    Ok(ConfiguredNixosUpgrade {
        stager: Arc::new(ProcessNixosUpgradeStager::new(settings)),
        rebooter: Arc::new(rebooter),
        running_version,
    })
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
