//! Daemon-published per-node disk usage. Replaces the `/:/host/root:ro` bind
//! mount that previously gave the probe container full read access to the
//! host filesystem.
//!
//! The daemon (which runs on the host directly and has all the visibility
//! `sysinfo` needs) periodically gathers disk info and writes a JSON snapshot
//! to etcd at `system/disks/{node-id}` with a leased TTL. The probe's
//! `/api/disks` endpoint reads from etcd.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, anyhow};
use etcd_client::{Client as EtcdClient, PutOptions};
use tokio::sync::{Mutex, broadcast};

use crate::signal::ShutdownEvent;

pub const DISK_PREFIX: &str = "system/disks/";
pub const DISK_LEASE_TTL_SECS: i64 = 90;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DiskInfo {
    pub name: String,
    pub mount_point: String,
    pub total_bytes: u64,
    pub available_bytes: u64,
    pub file_system: String,
}

pub fn collect_local() -> Vec<DiskInfo> {
    let disks = sysinfo::Disks::new_with_refreshed_list();
    disks
        .iter()
        .filter_map(|disk| {
            let mount = disk.mount_point().to_string_lossy().to_string();
            if !is_relevant_mount(&mount) {
                return None;
            }
            Some(DiskInfo {
                name: disk.name().to_string_lossy().to_string(),
                mount_point: mount,
                total_bytes: disk.total_space(),
                available_bytes: disk.available_space(),
                file_system: String::from_utf8_lossy(disk.file_system().as_encoded_bytes())
                    .to_string(),
            })
        })
        .collect()
}

fn is_relevant_mount(mount: &str) -> bool {
    !mount.starts_with("/dev")
        && !mount.starts_with("/proc")
        && !mount.starts_with("/sys")
        && !mount.starts_with("/snap")
        && !mount.starts_with("/var/lib/docker")
        && !mount.starts_with("/run")
}

pub fn spawn_publisher(
    client: Arc<Mutex<EtcdClient>>,
    node_id: String,
    interval: Duration,
    mut shutdown_rx: broadcast::Receiver<ShutdownEvent>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let key = format!("{DISK_PREFIX}{node_id}");
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        ticker.tick().await;
        loop {
            tokio::select! {
                _ = shutdown_rx.recv() => return,
                _ = ticker.tick() => {
                    let snapshot = collect_local();
                    if let Err(err) = publish(&client, &key, &snapshot).await {
                        eprintln!("disk snapshot publish failed: {err}");
                    }
                }
            }
        }
    })
}

async fn publish(client: &Arc<Mutex<EtcdClient>>, key: &str, disks: &[DiskInfo]) -> Result<()> {
    let body = serde_json::to_vec(disks)
        .map_err(|err| anyhow!("failed to serialize disk snapshot: {err}"))?;
    let mut etcd = client.lock().await;
    let lease = etcd
        .lease_grant(DISK_LEASE_TTL_SECS, None)
        .await
        .map_err(|err| anyhow!("failed to grant disk lease: {err}"))?;
    etcd.put(key, body, Some(PutOptions::new().with_lease(lease.id())))
        .await
        .map_err(|err| anyhow!("failed to put disk snapshot: {err}"))?;
    Ok(())
}

pub async fn read_all(
    client: &Arc<Mutex<EtcdClient>>,
) -> Result<Vec<crate::server::types::DiskInfo>> {
    let mut etcd = client.lock().await;
    let response = etcd
        .get(
            DISK_PREFIX,
            Some(etcd_client::GetOptions::new().with_prefix()),
        )
        .await
        .map_err(|err| anyhow!("failed to list disk snapshots: {err}"))?;
    let mut all = Vec::new();
    for kv in response.kvs() {
        let key = match kv.key_str() {
            Ok(key) => key,
            Err(_) => continue,
        };
        let node_id = key.trim_start_matches(DISK_PREFIX);
        let disks: Vec<DiskInfo> = match serde_json::from_slice(kv.value()) {
            Ok(disks) => disks,
            Err(err) => {
                eprintln!("skipping malformed disk snapshot {node_id}: {err}");
                continue;
            }
        };
        for disk in disks {
            all.push(crate::server::types::DiskInfo {
                name: disk.name,
                mount_point: disk.mount_point,
                total_bytes: disk.total_bytes,
                available_bytes: disk.available_bytes,
                file_system: disk.file_system,
                node_id: Some(node_id.to_string()),
            });
        }
    }
    Ok(all)
}
