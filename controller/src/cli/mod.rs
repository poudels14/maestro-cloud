mod cancel;
mod redeploy;
mod rollout;
mod upgrade;

use std::{io::Write, path::Path};

use crate::error::{Error, Result};

const DEFAULT_CLUSTER_TEMPLATE: &str = include_str!("../templates/cluster.jsonc");
const DEFAULT_START_TEMPLATE: &str = include_str!("../templates/maestro.jsonc");

pub async fn run_rollout(
    config_path: &Path,
    host: &str,
    apply: bool,
    force: bool,
    jwt_secret: Option<&str>,
) -> Result<()> {
    rollout::run_rollout(config_path, host, apply, force, jwt_secret).await
}

pub async fn run_cancel(host: &str, service_id: &str, deployment_id: &str) -> Result<()> {
    cancel::run_cancel(host, service_id, deployment_id).await
}

pub async fn run_redeploy(host: &str, service_id: &str) -> Result<()> {
    redeploy::run_redeploy(host, service_id).await
}

pub async fn run_upgrade_system(host: &str) -> Result<()> {
    upgrade::run_upgrade_system(host).await
}

pub fn init_config(start_config_path: &Path, cluster_config_path: &Path) -> Result<()> {
    write_template(start_config_path, DEFAULT_START_TEMPLATE)?;
    write_template(cluster_config_path, DEFAULT_CLUSTER_TEMPLATE)?;
    Ok(())
}

fn write_template(path: &Path, content: &str) -> Result<()> {
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|err| {
            if err.kind() == std::io::ErrorKind::AlreadyExists {
                Error::conflict(format!("{} already exists", path.display()))
            } else {
                Error::internal(format!("failed to create {}: {err}", path.display()))
            }
        })?;

    file.write_all(content.as_bytes())
        .map_err(|err| Error::internal(format!("failed to write {}: {err}", path.display())))?;

    println!("created {}", path.display());
    Ok(())
}

#[cfg(test)]
#[path = "../tests/cli/mod.rs"]
mod tests;
