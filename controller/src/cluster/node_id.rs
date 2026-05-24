use std::path::Path;

use anyhow::{Result, anyhow};

use super::types::NodeId;
use crate::utils;

const NODE_ID_FILENAME: &str = "node-id";
const NODE_ORIGIN_FILENAME: &str = "node-origin";
const NODE_ID_LENGTH: usize = 12;

pub fn load_or_create(data_dir: &Path) -> Result<NodeId> {
    let system_dir = data_dir.join("system");
    std::fs::create_dir_all(&system_dir).map_err(|err| {
        anyhow!(
            "failed to create system directory {}: {err}",
            system_dir.display()
        )
    })?;
    let node_id_path = system_dir.join(NODE_ID_FILENAME);
    let origin_path = system_dir.join(NODE_ORIGIN_FILENAME);
    let current_hostname = local_hostname();

    if node_id_path.exists() {
        let raw = std::fs::read_to_string(&node_id_path)
            .map_err(|err| anyhow!("failed to read node-id {}: {err}", node_id_path.display()))?;
        let trimmed = raw.trim().to_string();
        if !is_valid(&trimmed) {
            return Err(anyhow!(
                "invalid node-id in {}: expected at least 4 alphanumeric characters",
                node_id_path.display()
            ));
        }
        if origin_path.exists() {
            let recorded_hostname = std::fs::read_to_string(&origin_path)
                .map_err(|err| anyhow!("failed to read node-origin: {err}"))?
                .trim()
                .to_string();
            if recorded_hostname != current_hostname {
                return Err(anyhow!(
                    "node-id {trimmed} was created on host `{recorded_hostname}` but this host is `{current_hostname}`. \
                     This usually means the data directory was copied from another machine. \
                     Refusing to start with a duplicate node identity — delete {} and {} to mint a fresh one.",
                    node_id_path.display(),
                    origin_path.display()
                ));
            }
        } else {
            atomic_write(&origin_path, &current_hostname)?;
        }
        return Ok(trimmed);
    }

    let generated = utils::nanoid::unique_id(NODE_ID_LENGTH).to_lowercase();
    atomic_write(&node_id_path, &generated)?;
    atomic_write(&origin_path, &current_hostname)?;
    Ok(generated)
}

fn atomic_write(path: &Path, contents: &str) -> Result<()> {
    let tmp_path = path.with_extension("tmp");
    std::fs::write(&tmp_path, format!("{contents}\n"))
        .map_err(|err| anyhow!("failed to write {}: {err}", tmp_path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&tmp_path, std::fs::Permissions::from_mode(0o600));
    }
    std::fs::rename(&tmp_path, path)
        .map_err(|err| anyhow!("failed to persist {}: {err}", path.display()))?;
    Ok(())
}

fn local_hostname() -> String {
    std::env::var("HOSTNAME")
        .or_else(|_| std::env::var("MAESTRO_HOSTNAME"))
        .ok()
        .filter(|hostname| !hostname.trim().is_empty())
        .or_else(|| {
            std::fs::read_to_string("/etc/hostname")
                .ok()
                .map(|hostname| hostname.trim().to_string())
                .filter(|hostname| !hostname.is_empty())
        })
        .unwrap_or_else(|| "unknown-host".to_string())
}

fn is_valid(value: &str) -> bool {
    value.len() >= 4
        && value
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '-' || ch == '_')
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    fn unique_tmp_dir(label: &str) -> std::path::PathBuf {
        let dir = env::temp_dir().join(format!(
            "maestro-node-id-{label}-{}",
            utils::nanoid::unique_id(8)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn fresh_data_dir_generates_and_persists_node_id() {
        let dir = unique_tmp_dir("fresh");
        let first = load_or_create(&dir).unwrap();
        let second = load_or_create(&dir).unwrap();
        assert_eq!(first, second, "node-id should be stable across calls");
    }

    #[test]
    fn data_dir_with_mismatched_hostname_is_refused() {
        let dir = unique_tmp_dir("collision");
        let system_dir = dir.join("system");
        std::fs::create_dir_all(&system_dir).unwrap();
        std::fs::write(system_dir.join(NODE_ID_FILENAME), "preexisting-id\n").unwrap();
        std::fs::write(system_dir.join(NODE_ORIGIN_FILENAME), "some-other-host\n").unwrap();

        let result = load_or_create(&dir);
        assert!(
            result.is_err(),
            "expected collision detection to refuse start, got {result:?}"
        );
        let message = result.unwrap_err().to_string();
        assert!(
            message.contains("data directory was copied"),
            "error should explain the cause: {message}"
        );
    }

    #[test]
    fn legacy_data_dir_without_origin_file_backfills_quietly() {
        let dir = unique_tmp_dir("legacy");
        let system_dir = dir.join("system");
        std::fs::create_dir_all(&system_dir).unwrap();
        std::fs::write(system_dir.join(NODE_ID_FILENAME), "legacy-id\n").unwrap();
        let id = load_or_create(&dir).unwrap();
        assert_eq!(id, "legacy-id");
        let origin_path = system_dir.join(NODE_ORIGIN_FILENAME);
        assert!(origin_path.exists(), "origin file should be backfilled");
    }
}
