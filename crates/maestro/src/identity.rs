use std::path::Path;

use anyhow::Context;
use maestro_core::{ClusterSettings, NodeId, NodeRole};
use serde::{Deserialize, Serialize};

const IDENTITY_FILE: &str = "node.json";
const DEFAULT_SUBNET: &str = "10.200.1.0/24";
const DEFAULT_ADVERTISE: &str = "127.0.0.1:7400";

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeIdentity {
    pub id: NodeId,
    pub name: String,
    #[serde(default = "default_role")]
    pub role: NodeRole,
    #[serde(default = "default_subnet")]
    pub subnet: String,
    #[serde(default = "default_advertise")]
    pub advertise: String,
    #[serde(default)]
    pub seeds: Vec<String>,
    #[serde(default)]
    pub cluster: ClusterSettings,
}

impl NodeIdentity {
    pub fn bootstrap(
        data_dir: &Path,
        advertise: &str,
        cluster: ClusterSettings,
    ) -> anyhow::Result<Self> {
        match data_dir.join(IDENTITY_FILE).exists() {
            true => Self::load(data_dir),
            false => {
                let subnet = first_node_subnet(&cluster.cidr)?;
                let identity = Self {
                    id: random_node_id(),
                    name: hostname(),
                    role: NodeRole::Voter,
                    subnet,
                    advertise: advertise.to_string(),
                    seeds: vec![advertise.to_string()],
                    cluster,
                };
                identity.save(data_dir)?;
                Ok(identity)
            }
        }
    }

    pub fn load(data_dir: &Path) -> anyhow::Result<Self> {
        let path = data_dir.join(IDENTITY_FILE);
        let bytes = std::fs::read(&path).with_context(|| {
            format!(
                "{} not found; run `maestro server init` or `maestro join` first",
                path.display()
            )
        })?;
        serde_json::from_slice(&bytes).with_context(|| format!("{} is invalid", path.display()))
    }

    pub fn save(&self, data_dir: &Path) -> anyhow::Result<()> {
        std::fs::create_dir_all(data_dir)?;
        std::fs::write(
            data_dir.join(IDENTITY_FILE),
            serde_json::to_vec_pretty(self)?,
        )?;
        Ok(())
    }

    pub fn control_seeds(&self) -> Vec<String> {
        match self.seeds.is_empty() {
            true => vec![self.advertise.clone()],
            false => self.seeds.clone(),
        }
    }
}

pub fn random_node_id() -> NodeId {
    NodeId(rand::random::<u64>() >> 11)
}

pub fn hostname() -> String {
    std::fs::read_to_string("/etc/hostname")
        .ok()
        .map(|name| name.trim().to_string())
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| "node".to_string())
}

fn first_node_subnet(cluster_cidr: &str) -> anyhow::Result<String> {
    let (network, _) = cluster_cidr
        .split_once('/')
        .context("cluster cidr must look like 10.200.0.0/16")?;
    let octets = network
        .split('.')
        .map(str::parse::<u8>)
        .collect::<Result<Vec<_>, _>>()
        .context("cluster cidr must be an IPv4 network")?;
    match octets.as_slice() {
        [first, second, _, _] => Ok(format!("{first}.{second}.1.0/24")),
        _ => anyhow::bail!("cluster cidr must be an IPv4 network"),
    }
}

fn default_role() -> NodeRole {
    NodeRole::Voter
}

fn default_subnet() -> String {
    DEFAULT_SUBNET.to_string()
}

fn default_advertise() -> String {
    DEFAULT_ADVERTISE.to_string()
}
