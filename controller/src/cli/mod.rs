pub mod cancel;
pub mod redeploy;
pub mod rollout;
pub mod upgrade;

use std::{io::Write, path::Path};

use crate::error::{Error, Result};

const DEFAULT_CLUSTER_TEMPLATE: &str = include_str!("../templates/cluster.jsonc");
const DEFAULT_START_TEMPLATE: &str = include_str!("../templates/maestro.jsonc");

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
