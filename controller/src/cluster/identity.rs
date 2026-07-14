use std::io::Write;
use std::path::Path;

use anyhow::{Context, Result, bail};
use sha2::{Digest, Sha256};

use crate::utils;

pub fn load_cluster_id(data_dir: &Path) -> Result<String> {
    let path = data_dir.join("system/cluster-id");
    let cluster_id = std::fs::read_to_string(&path)
        .with_context(|| format!("missing cluster identity {}; run `maestro cluster init-ca` on cluster.nodes[0] and copy the cluster material to this voter", path.display()))?;
    let cluster_id = cluster_id.trim().to_string();
    if !is_lower_hex(&cluster_id, 32) {
        bail!("invalid cluster id in {}", path.display());
    }
    Ok(cluster_id)
}

pub fn create_cluster_id(data_dir: &Path) -> Result<String> {
    let cluster_id = new_cluster_id();
    persist_cluster_id(data_dir, &cluster_id)?;
    Ok(cluster_id)
}

pub fn new_cluster_id() -> String {
    random_hex(32)
}

pub fn persist_cluster_id(data_dir: &Path, cluster_id: &str) -> Result<()> {
    if !is_lower_hex(cluster_id, 32) {
        bail!("invalid cluster id");
    }
    let system_dir = data_dir.join("system");
    let path = system_dir.join("cluster-id");
    if path.exists() {
        let existing = load_cluster_id(data_dir)?;
        if existing != cluster_id {
            bail!("cluster identity already exists at {}", path.display());
        }
        return Ok(());
    }
    persist_new(&system_dir, "cluster-id", cluster_id.as_bytes())
}

pub fn load_or_create_node_id(data_dir: &Path) -> Result<String> {
    let system_dir = data_dir.join("system");
    let id_path = system_dir.join("node-id");
    let origin_path = system_dir.join("node-origin");
    let hostname = local_hostname();
    if let Ok(existing) = std::fs::read_to_string(&id_path) {
        let node_id = existing.trim().to_string();
        if !is_lower_alphanumeric(&node_id, 12) {
            bail!("invalid cluster node id in {}", id_path.display());
        }
        if let Ok(origin) = std::fs::read_to_string(&origin_path) {
            let origin = origin.trim();
            if origin != hostname {
                bail!(
                    "cluster data directory belongs to host `{origin}`, not `{hostname}`; delete system/node-id only when intentionally cloning a node"
                );
            }
        } else {
            persist_new(&system_dir, "node-origin", hostname.as_bytes())?;
        }
        Ok(node_id)
    } else {
        let node_id = random_node_id();
        persist_replace(&system_dir, "node-origin", hostname.as_bytes())?;
        persist_new(&system_dir, "node-id", node_id.as_bytes())?;
        Ok(node_id)
    }
}

pub fn new_instance_id() -> String {
    random_hex(32)
}

fn persist_new(directory: &Path, name: &str, value: &[u8]) -> Result<()> {
    std::fs::create_dir_all(directory)?;
    let path = directory.join(name);
    let temporary = directory.join(format!("{name}.tmp"));
    if path.exists() {
        bail!("refusing to replace existing {}", path.display());
    }
    let mut file = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temporary)
        .with_context(|| format!("failed to create {}", temporary.display()))?;
    file.write_all(value)?;
    file.sync_all()?;
    std::fs::rename(&temporary, &path)?;
    std::fs::File::open(directory)?.sync_all()?;
    Ok(())
}

fn persist_replace(directory: &Path, name: &str, value: &[u8]) -> Result<()> {
    std::fs::create_dir_all(directory)?;
    let path = directory.join(name);
    let temporary = directory.join(format!("{name}.tmp"));
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temporary)
        .with_context(|| format!("failed to create {}", temporary.display()))?;
    file.write_all(value)?;
    file.sync_all()?;
    std::fs::rename(&temporary, &path)?;
    std::fs::File::open(directory)?.sync_all()?;
    Ok(())
}

pub fn local_hostname() -> String {
    std::fs::read_to_string("/etc/hostname")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .or_else(|| std::env::var("HOSTNAME").ok())
        .unwrap_or_else(|| "unknown-host".to_string())
}

fn random_hex(length: usize) -> String {
    let entropy = format!(
        "{}:{}:{}:{}",
        utils::nanoid::unique_id(32),
        std::process::id(),
        crate::utils::time::current_time_millis().unwrap_or_default(),
        utils::nanoid::unique_id(32)
    );
    let digest = Sha256::digest(entropy.as_bytes());
    digest
        .iter()
        .flat_map(|byte| format!("{byte:02x}").chars().collect::<Vec<_>>())
        .take(length)
        .collect()
}

fn random_node_id() -> String {
    const ALPHABET: [char; 36] = [
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h',
        'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
    ];
    nanoid::nanoid!(12, &ALPHABET)
}

fn is_lower_hex(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .chars()
            .all(|character| character.is_ascii_digit() || ('a'..='f').contains(&character))
}

fn is_lower_alphanumeric(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .chars()
            .all(|character| character.is_ascii_lowercase() || character.is_ascii_digit())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_are_stable_and_separate() {
        let root = std::env::temp_dir().join(format!(
            "maestro-cluster-identity-{}-{}",
            std::process::id(),
            crate::utils::time::current_time_millis().unwrap_or_default()
        ));
        let first = load_or_create_node_id(&root).expect("create node id");
        let second = load_or_create_node_id(&root).expect("load node id");
        assert_eq!(first, second);
        assert!(is_lower_alphanumeric(&first, 12));
        std::fs::remove_file(root.join("system/node-id")).expect("remove copied identity");
        let reminted = load_or_create_node_id(&root).expect("re-mint node id");
        assert_ne!(first, reminted);
        let _ = std::fs::remove_dir_all(root);
    }
}
