use std::collections::HashSet;
use std::path::Path;

use serde::{Deserialize, Serialize};

const MOUNTS_FILE: &str = "/proc/mounts";
const DISK_FILE_SYSTEMS: [&str; 12] = [
    "ext2", "ext3", "ext4", "xfs", "btrfs", "zfs", "f2fs", "vfat", "exfat", "ntfs", "apfs", "hfs",
];

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Disk {
    pub name: String,
    pub mount_point: String,
    pub total_bytes: u64,
    pub available_bytes: u64,
    pub file_system: String,
}

pub fn disks() -> Vec<Disk> {
    let mounts = std::fs::read_to_string(MOUNTS_FILE)
        .map(|contents| parse_mounts(&contents))
        .unwrap_or_else(|_| vec![("/".to_string(), "/".to_string(), "unknown".to_string())]);
    let mut seen_devices = HashSet::new();
    mounts
        .into_iter()
        .filter(|(device, _, _)| seen_devices.insert(device.clone()))
        .filter_map(|(device, mount_point, file_system)| {
            let stats = rustix::fs::statvfs(Path::new(&mount_point)).ok()?;
            Some(Disk {
                name: device,
                mount_point,
                total_bytes: stats.f_blocks * stats.f_frsize,
                available_bytes: stats.f_bavail * stats.f_frsize,
                file_system,
            })
        })
        .collect()
}

fn parse_mounts(contents: &str) -> Vec<(String, String, String)> {
    contents
        .lines()
        .filter_map(|line| {
            let mut fields = line.split_whitespace();
            let device = fields.next()?;
            let mount_point = fields.next()?.replace("\\040", " ");
            let file_system = fields.next()?;
            DISK_FILE_SYSTEMS
                .contains(&file_system)
                .then(|| (device.to_string(), mount_point, file_system.to_string()))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keeps_only_disk_backed_mounts() {
        let mounts = parse_mounts(
            "/dev/vda1 / ext4 rw,relatime 0 0\n\
             proc /proc proc rw 0 0\n\
             overlay /run/containerd/x overlay rw 0 0\n\
             /dev/vdb1 /mnt/my\\040data xfs rw 0 0\n",
        );
        assert_eq!(
            mounts,
            [
                ("/dev/vda1".to_string(), "/".to_string(), "ext4".to_string()),
                (
                    "/dev/vdb1".to_string(),
                    "/mnt/my data".to_string(),
                    "xfs".to_string()
                ),
            ]
        );
    }
}
