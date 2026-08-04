use std::collections::BTreeSet;
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use procfs_core::FromRead;
use procfs_core::MountEntry as ProcMountEntry;

const MAX_MOUNTS_FILE_BYTES: u64 = 1024 * 1024;

/// Capacity and identity for one storage-backed host mount.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostDiskStats {
    /// Kernel mount source, such as `/dev/nvme0n1p1`.
    pub name: String,
    /// Host-visible mount point.
    pub mount_point: String,
    /// Filesystem capacity in bytes.
    pub total_bytes: u64,
    /// Bytes available to an unprivileged process.
    pub available_bytes: u64,
    /// Kernel filesystem type.
    pub file_system: String,
}

/// One mount that disappeared or could not report capacity during a snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostDiskFailure {
    /// Mount point whose capacity could not be read.
    pub mount_point: String,
    /// Safe operating-system or range diagnostic.
    pub message: String,
}

/// Finite disk inventory with failures isolated to individual mounts.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HostDiskReport {
    /// Storage-backed mounts sorted by mount point.
    pub disks: Vec<HostDiskStats>,
    /// Failed mounts sorted by mount point.
    pub failures: Vec<HostDiskFailure>,
}

/// Reads one complete host disk inventory without retaining state.
#[async_trait]
pub trait HostDiskReader: Send + Sync + 'static {
    /// Reads current mount identity and filesystem capacity.
    async fn read(&self) -> Result<HostDiskReport, HostDiskError>;
}

/// Bounded Linux mount-table and `statvfs` disk reader.
#[derive(Debug, Clone)]
pub struct LinuxHostDiskReader {
    mounts_file: PathBuf,
}

impl LinuxHostDiskReader {
    /// Constructs the production reader for the current host mount namespace.
    pub fn production() -> Self {
        Self {
            mounts_file: PathBuf::from("/proc/self/mounts"),
        }
    }

    /// Constructs a reader for an explicitly mounted host mount table.
    pub fn from_mounts_file(mounts_file: PathBuf) -> Self {
        Self { mounts_file }
    }
}

impl Default for LinuxHostDiskReader {
    fn default() -> Self {
        Self::production()
    }
}

#[async_trait]
impl HostDiskReader for LinuxHostDiskReader {
    async fn read(&self) -> Result<HostDiskReport, HostDiskError> {
        let mounts_file = self.mounts_file.clone();
        tokio::task::spawn_blocking(move || read_host_disks(&mounts_file))
            .await
            .map_err(|error| HostDiskError::Task {
                message: error.to_string(),
            })?
    }
}

fn read_host_disks(mounts_file: &Path) -> Result<HostDiskReport, HostDiskError> {
    let contents = read_bounded_file(mounts_file)?;
    let mounts = parse_mounts(&contents)?;
    let mut report = HostDiskReport::default();
    let mut seen_mounts = BTreeSet::new();
    for mount in mounts {
        if !relevant_mount(&mount) || !seen_mounts.insert(mount.mount_point.clone()) {
            continue;
        }
        match statvfs_space(Path::new(&mount.mount_point)) {
            Ok(Some((total_bytes, available_bytes))) => report.disks.push(HostDiskStats {
                name: mount.name,
                mount_point: mount.mount_point,
                total_bytes,
                available_bytes,
                file_system: mount.file_system,
            }),
            Ok(None) => report.failures.push(HostDiskFailure {
                mount_point: mount.mount_point,
                message: "filesystem reported zero capacity".to_owned(),
            }),
            Err(error) => report.failures.push(HostDiskFailure {
                mount_point: mount.mount_point,
                message: error.to_string(),
            }),
        }
    }
    report
        .disks
        .sort_by(|left, right| left.mount_point.cmp(&right.mount_point));
    report
        .failures
        .sort_by(|left, right| left.mount_point.cmp(&right.mount_point));
    Ok(report)
}

fn read_bounded_file(path: &Path) -> Result<String, HostDiskError> {
    let metadata =
        std::fs::symlink_metadata(path).map_err(|source| io_error("inspect", path, source))?;
    if !metadata.file_type().is_file() || metadata.file_type().is_symlink() {
        return Err(HostDiskError::UnsafePath {
            path: path.to_path_buf(),
        });
    }
    let file = File::open(path).map_err(|source| io_error("open", path, source))?;
    let mut contents = String::new();
    file.take(MAX_MOUNTS_FILE_BYTES.saturating_add(1))
        .read_to_string(&mut contents)
        .map_err(|source| io_error("read", path, source))?;
    if contents.len() as u64 > MAX_MOUNTS_FILE_BYTES {
        Err(HostDiskError::FileTooLarge {
            path: path.to_path_buf(),
        })
    } else {
        Ok(contents)
    }
}

#[derive(Debug, PartialEq, Eq)]
struct MountEntry {
    name: String,
    mount_point: String,
    file_system: String,
}

fn parse_mounts(contents: &str) -> Result<Vec<MountEntry>, HostDiskError> {
    let lines = contents
        .lines()
        .filter(|line| !line.trim().is_empty())
        .collect::<Vec<_>>();
    if let Some(line) = lines
        .iter()
        .find(|line| line.split_ascii_whitespace().count() != 6)
    {
        return Err(invalid(line));
    }
    // procfs-core decodes the kernel's other octal escapes but currently leaves
    // spaces encoded. A NUL is safe as a temporary sentinel because mount names
    // cannot contain one.
    let normalized = lines.join("\n").replace(r"\040", "\0");
    Vec::<ProcMountEntry>::from_read(normalized.as_bytes())
        .map_err(|error| invalid(&error.to_string()))?
        .into_iter()
        .map(|mount| {
            let mount_point = mount.fs_file.replace('\0', " ");
            if mount.fs_freq > 1 || mount.fs_passno > 2 || !Path::new(&mount_point).is_absolute() {
                return Err(invalid(&mount_point));
            }
            Ok(MountEntry {
                name: mount.fs_spec.replace('\0', " "),
                mount_point,
                file_system: mount.fs_vfstype.replace('\0', " "),
            })
        })
        .collect()
}

fn relevant_mount(mount: &MountEntry) -> bool {
    !matches!(
        mount.file_system.as_str(),
        "rootfs"
            | "sysfs"
            | "proc"
            | "devtmpfs"
            | "cgroup"
            | "cgroup2"
            | "pstore"
            | "squashfs"
            | "rpc_pipefs"
            | "iso9660"
            | "devpts"
            | "hugetlbfs"
            | "mqueue"
            | "tmpfs"
            | "cifs"
            | "nfs"
            | "nfs4"
            | "autofs"
    ) && !mount.mount_point.starts_with("/sys")
        && !mount.mount_point.starts_with("/proc")
        && (!mount.mount_point.starts_with("/run") || mount.mount_point.starts_with("/run/media"))
        && !mount.name.starts_with("sunrpc")
}

fn statvfs_space(path: &Path) -> std::io::Result<Option<(u64, u64)>> {
    let stats = nix::sys::statvfs::statvfs(path).map_err(std::io::Error::from)?;
    let block_size = u64::from(stats.fragment_size()).max(1);
    let total_bytes = u64::from(stats.blocks())
        .checked_mul(block_size)
        .ok_or_else(|| std::io::Error::other("filesystem capacity exceeds u64"))?;
    if total_bytes == 0 {
        return Ok(None);
    }
    let available_bytes = u64::from(stats.blocks_available())
        .checked_mul(block_size)
        .ok_or_else(|| std::io::Error::other("available filesystem capacity exceeds u64"))?;
    Ok(Some((total_bytes, available_bytes)))
}

fn invalid(line: &str) -> HostDiskError {
    HostDiskError::InvalidMount {
        line: line.to_owned(),
    }
}

fn io_error(operation: &'static str, path: &Path, source: std::io::Error) -> HostDiskError {
    HostDiskError::Io {
        operation,
        path: path.to_path_buf(),
        source,
    }
}

/// Failure to safely read or parse the Linux mount inventory.
#[derive(Debug, thiserror::Error)]
pub enum HostDiskError {
    /// The mount table was replaced with an unsafe file type.
    #[error("host mounts path `{}` is not a safe regular file", path.display())]
    UnsafePath {
        /// Rejected path.
        path: PathBuf,
    },
    /// The mount table exceeded the defensive read bound.
    #[error("host mounts file `{}` exceeds the read limit", path.display())]
    FileTooLarge {
        /// Oversized path.
        path: PathBuf,
    },
    /// A mount-table row did not match the documented grammar.
    #[error("host mounts file contains invalid row `{line}`")]
    InvalidMount {
        /// Invalid row.
        line: String,
    },
    /// Blocking filesystem work could not complete.
    #[error("host disk filesystem task failed: {message}")]
    Task {
        /// Task failure detail.
        message: String,
    },
    /// A mount-table filesystem operation failed.
    #[error("failed to {operation} host mounts path `{}`: {source}", path.display())]
    Io {
        /// Operation that failed.
        operation: &'static str,
        /// Mount-table path.
        path: PathBuf,
        /// Operating-system error.
        #[source]
        source: std::io::Error,
    },
}
