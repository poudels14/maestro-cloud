use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use build::{DepotBuildBackend, DepotBuildSettings, ProcessDepotBuildBackend};
use cluster::DepotLaunchConfig;
use runtime::ArtifactStore;

pub(crate) fn validate_depot(config: &DepotLaunchConfig) -> Result<(), DepotLaunchError> {
    if config.token.expose().trim().is_empty() || config.token.expose().contains('\0') {
        return Err(DepotLaunchError::Invalid(
            "token cannot be empty or contain a null byte".to_owned(),
        ));
    }
    if config.executable.as_os_str().is_empty()
        || config
            .executable
            .as_os_str()
            .as_encoded_bytes()
            .contains(&0)
    {
        return Err(DepotLaunchError::Invalid(
            "executable cannot be empty or contain a null byte".to_owned(),
        ));
    }
    if config.timeout_secs == 0 {
        return Err(DepotLaunchError::Invalid(
            "timeout must be greater than zero".to_owned(),
        ));
    }
    Ok(())
}

pub(crate) fn configure_depot(
    config: &DepotLaunchConfig,
    state_root: PathBuf,
    artifacts: Arc<dyn ArtifactStore>,
) -> Result<Arc<dyn DepotBuildBackend>, DepotLaunchError> {
    validate_depot(config)?;
    let mut settings = DepotBuildSettings::new(config.token.clone(), state_root);
    settings.executable = config.executable.clone();
    settings.build_timeout = Duration::from_secs(config.timeout_secs);
    Ok(Arc::new(ProcessDepotBuildBackend::new(
        settings, artifacts,
    )?))
}

/// Invalid Depot process configuration.
#[derive(Debug, thiserror::Error)]
pub enum DepotLaunchError {
    /// Static launch configuration is invalid.
    #[error("invalid Depot configuration: {0}")]
    Invalid(String),
    /// The protected build adapter rejected its settings.
    #[error(transparent)]
    Build(#[from] runtime::ArtifactStoreError),
}
