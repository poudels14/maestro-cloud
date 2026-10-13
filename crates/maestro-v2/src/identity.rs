use std::path::Path;

use anyhow::Context;
use maestro_core::NodeId;
use serde_json::json;

const IDENTITY_FILE: &str = "node.json";

pub struct NodeIdentity {
    pub id: NodeId,
    pub name: String,
}

impl NodeIdentity {
    pub fn create(data_dir: &Path) -> anyhow::Result<Self> {
        let path = data_dir.join(IDENTITY_FILE);
        match path.exists() {
            true => Self::load(data_dir),
            false => {
                let identity = Self {
                    id: NodeId(rand::random::<u64>() >> 1),
                    name: hostname(),
                };
                std::fs::create_dir_all(data_dir)?;
                let encoded = json!({ "id": identity.id.0, "name": identity.name });
                std::fs::write(&path, serde_json::to_vec_pretty(&encoded)?)?;
                Ok(identity)
            }
        }
    }

    pub fn load(data_dir: &Path) -> anyhow::Result<Self> {
        let path = data_dir.join(IDENTITY_FILE);
        let bytes = std::fs::read(&path).with_context(|| {
            format!(
                "{} not found; run `maestro-v2 server init --data-dir {}` first",
                path.display(),
                data_dir.display()
            )
        })?;
        let value: serde_json::Value = serde_json::from_slice(&bytes)?;
        let id = value["id"]
            .as_u64()
            .context("node.json is missing a numeric id")?;
        let name = value["name"].as_str().unwrap_or("node").to_string();
        Ok(Self {
            id: NodeId(id),
            name,
        })
    }
}

fn hostname() -> String {
    std::fs::read_to_string("/etc/hostname")
        .ok()
        .map(|name| name.trim().to_string())
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| "node".to_string())
}
