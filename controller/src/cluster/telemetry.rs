use std::{collections::HashSet, sync::Arc, time::Duration};

use tokio::sync::broadcast;

use crate::{
    cluster::NodeDiskInfo, deployment::store::ClusterStore, logs::Logger, signal::ShutdownEvent,
};

pub fn collect_host_disks() -> Vec<NodeDiskInfo> {
    let disks = sysinfo::Disks::new_with_refreshed_list();
    let mut seen = HashSet::new();
    let mut values = disks
        .iter()
        .filter_map(|disk| {
            let mount_point = disk.mount_point().to_string_lossy().to_string();
            let name = disk.name().to_string_lossy().to_string();
            if mount_point.starts_with("/dev/")
                || mount_point.starts_with("/proc/")
                || mount_point.starts_with("/run/")
                || mount_point.starts_with("/sys/")
                || !seen.insert((name.clone(), mount_point.clone()))
            {
                return None;
            }
            Some(NodeDiskInfo {
                name,
                mount_point,
                total_bytes: disk.total_space(),
                available_bytes: disk.available_space(),
                file_system: String::from_utf8_lossy(disk.file_system().as_encoded_bytes())
                    .to_string(),
            })
        })
        .collect::<Vec<_>>();
    values.sort_by(|left, right| left.mount_point.cmp(&right.mount_point));
    values
}

pub async fn run_disk_reporter(
    store: Arc<dyn ClusterStore>,
    node_id: String,
    mut shutdown: broadcast::Receiver<ShutdownEvent>,
    logger: Logger,
) {
    let mut interval = tokio::time::interval(Duration::from_secs(30));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = shutdown.recv() => return,
            _ = interval.tick() => {
                if let Err(error) = store.publish_node_disks(&node_id, &collect_host_disks()).await {
                    logger.emit("warn", &format!("failed to publish node disk snapshot: {error}"));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn host_disk_snapshot_has_unique_mounts() {
        let values = collect_host_disks();
        let unique = values
            .iter()
            .map(|disk| disk.mount_point.as_str())
            .collect::<HashSet<_>>();
        assert_eq!(unique.len(), values.len());
    }
}
