mod cancel;
mod rollout;

use std::{io::Write, path::Path};

use crate::error::{Error, Result};

const DEFAULT_CONFIG_TEMPLATE: &str = include_str!("../default.jsonc");

pub async fn run_rollout(config_path: &Path, host: &str) -> Result<()> {
    rollout::run_rollout(config_path, host).await
}

pub async fn run_cancel(host: &str, service_id: &str, deployment_id: &str) -> Result<()> {
    cancel::run_cancel(host, service_id, deployment_id).await
}

pub fn init_config(config_path: &Path) -> Result<()> {
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(config_path)
        .map_err(|err| {
            if err.kind() == std::io::ErrorKind::AlreadyExists {
                Error::conflict(format!("{} already exists", config_path.display()))
            } else {
                Error::internal(format!("failed to create {}: {err}", config_path.display()))
            }
        })?;

    file.write_all(DEFAULT_CONFIG_TEMPLATE.as_bytes())
        .map_err(|err| {
            Error::internal(format!("failed to write {}: {err}", config_path.display()))
        })?;

    println!("created {}", config_path.display());
    Ok(())
}

#[cfg(test)]
#[path = "../tests/cli/mod.rs"]
mod tests;
