use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, Write};
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use kernel_api::WorkloadId;
use nix::fcntl::{Flock, FlockArg};
use serde::{Deserialize, Serialize};

use crate::{RuntimeError, WorkloadIdMapping, WorkloadUserNamespace};

pub(crate) const USER_NAMESPACE_HOST_BASE: u32 = 1_048_576;
pub(crate) const USER_NAMESPACE_RANGE_SIZE: u32 = 65_536;
pub(crate) const USER_NAMESPACE_SLOT_COUNT: u32 = 16_384;

const ALLOCATION_VERSION: u8 = 1;
const MAX_RECORD_SIZE: u64 = 4_096;
const LOCK_FILE: &str = ".lock";

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct AllocationRecord {
    version: u8,
    slot: u32,
}

#[derive(Debug, Default)]
struct AllocatorState {
    allocations: BTreeMap<WorkloadId, u32>,
    used_slots: BTreeSet<u32>,
}

/// Durable allocator for disjoint per-workload UID and GID ranges.
pub(crate) struct ContainerdUserNamespaceAllocator {
    root: PathBuf,
    state: Mutex<AllocatorState>,
}

impl ContainerdUserNamespaceAllocator {
    pub(crate) fn new(state_root: &Path) -> Result<Self, RuntimeError> {
        let root = state_root.join("user-namespaces");
        fs::create_dir_all(&root).map_err(|error| unavailable("create", &root, error))?;
        let metadata =
            fs::symlink_metadata(&root).map_err(|error| unavailable("inspect", &root, error))?;
        if !metadata.file_type().is_dir() || metadata.file_type().is_symlink() {
            return Err(RuntimeError::Rejected {
                message: format!(
                    "containerd user-namespace state `{}` is not a real directory",
                    root.display()
                ),
            });
        }
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700))
            .map_err(|error| unavailable("protect", &root, error))?;
        let _file_lock = lock_state(&root)?;
        let state = load_state(&root)?;
        Ok(Self {
            root,
            state: Mutex::new(state),
        })
    }

    pub(crate) fn allocate(
        &self,
        workload_id: &WorkloadId,
    ) -> Result<WorkloadUserNamespace, RuntimeError> {
        let mut state = self.state.lock().map_err(|_| RuntimeError::Unavailable {
            message: "containerd user-namespace allocator lock was poisoned".to_owned(),
        })?;
        let _file_lock = lock_state(&self.root)?;
        *state = load_state(&self.root)?;
        if let Some(slot) = state.allocations.get(workload_id).copied() {
            return namespace_for_slot(slot);
        }
        let slot = (0..USER_NAMESPACE_SLOT_COUNT)
            .find(|slot| !state.used_slots.contains(slot))
            .ok_or_else(|| RuntimeError::Rejected {
                message: format!(
                    "all {USER_NAMESPACE_SLOT_COUNT} containerd user-namespace ranges are allocated"
                ),
            })?;
        persist_record(
            &self.root,
            workload_id,
            AllocationRecord {
                version: ALLOCATION_VERSION,
                slot,
            },
        )?;
        state.allocations.insert(workload_id.clone(), slot);
        state.used_slots.insert(slot);
        namespace_for_slot(slot)
    }

    pub(crate) fn cleanup(
        &self,
        active_workload_ids: &BTreeSet<WorkloadId>,
    ) -> Result<usize, RuntimeError> {
        let mut state = self.state.lock().map_err(|_| RuntimeError::Unavailable {
            message: "containerd user-namespace allocator lock was poisoned".to_owned(),
        })?;
        let _file_lock = lock_state(&self.root)?;
        *state = load_state(&self.root)?;
        let stale = state
            .allocations
            .keys()
            .filter(|workload_id| !active_workload_ids.contains(*workload_id))
            .cloned()
            .collect::<Vec<_>>();
        for workload_id in &stale {
            let path = record_path(&self.root, workload_id);
            let metadata = fs::symlink_metadata(&path)
                .map_err(|error| unavailable("inspect", &path, error))?;
            if !metadata.file_type().is_file() || metadata.file_type().is_symlink() {
                return Err(RuntimeError::Rejected {
                    message: format!(
                        "containerd user-namespace record `{}` is not a regular file",
                        path.display()
                    ),
                });
            }
            fs::remove_file(&path).map_err(|error| unavailable("remove", &path, error))?;
            if let Some(slot) = state.allocations.remove(workload_id) {
                state.used_slots.remove(&slot);
            }
        }
        if !stale.is_empty() {
            sync_directory(&self.root)?;
        }
        Ok(stale.len())
    }
}

fn load_state(root: &Path) -> Result<AllocatorState, RuntimeError> {
    let mut state = AllocatorState::default();
    let entries = fs::read_dir(root).map_err(|error| unavailable("list", root, error))?;
    for entry in entries {
        let entry = entry.map_err(|error| unavailable("read", root, error))?;
        let path = entry.path();
        let file_name = entry.file_name();
        let file_name = file_name.to_str().ok_or_else(|| RuntimeError::Rejected {
            message: format!(
                "containerd user-namespace state contains a non-UTF-8 entry in `{}`",
                root.display()
            ),
        })?;
        if file_name == LOCK_FILE || file_name.starts_with(".allocation-") {
            continue;
        }
        let workload_id = file_name
            .strip_suffix(".json")
            .ok_or_else(|| RuntimeError::Rejected {
                message: format!(
                    "unexpected containerd user-namespace state entry `{}`",
                    path.display()
                ),
            })
            .and_then(|value| {
                WorkloadId::new(value).map_err(|error| RuntimeError::Rejected {
                    message: format!(
                        "invalid workload identity in user-namespace record `{}`: {error}",
                        path.display()
                    ),
                })
            })?;
        let metadata =
            fs::symlink_metadata(&path).map_err(|error| unavailable("inspect", &path, error))?;
        if !metadata.file_type().is_file()
            || metadata.file_type().is_symlink()
            || metadata.len() > MAX_RECORD_SIZE
        {
            return Err(RuntimeError::Rejected {
                message: format!(
                    "containerd user-namespace record `{}` is unsafe or oversized",
                    path.display()
                ),
            });
        }
        let record: AllocationRecord = serde_json::from_reader(BufReader::new(
            File::open(&path).map_err(|error| unavailable("open", &path, error))?,
        ))
        .map_err(|error| RuntimeError::Rejected {
            message: format!(
                "invalid containerd user-namespace record `{}`: {error}",
                path.display()
            ),
        })?;
        validate_record(&path, record)?;
        if !state.used_slots.insert(record.slot) {
            return Err(RuntimeError::Rejected {
                message: format!(
                    "containerd user-namespace slot {} is allocated more than once",
                    record.slot
                ),
            });
        }
        state.allocations.insert(workload_id, record.slot);
    }
    Ok(state)
}

fn validate_record(path: &Path, record: AllocationRecord) -> Result<(), RuntimeError> {
    if record.version != ALLOCATION_VERSION {
        return Err(RuntimeError::Rejected {
            message: format!(
                "unsupported containerd user-namespace record version {} in `{}`",
                record.version,
                path.display()
            ),
        });
    }
    if record.slot >= USER_NAMESPACE_SLOT_COUNT {
        return Err(RuntimeError::Rejected {
            message: format!(
                "containerd user-namespace slot {} in `{}` exceeds the configured range",
                record.slot,
                path.display()
            ),
        });
    }
    namespace_for_slot(record.slot).map(|_| ())
}

fn persist_record(
    root: &Path,
    workload_id: &WorkloadId,
    record: AllocationRecord,
) -> Result<(), RuntimeError> {
    let mut temporary = tempfile::Builder::new()
        .prefix(".allocation-")
        .tempfile_in(root)
        .map_err(|error| unavailable("create temporary record in", root, error))?;
    temporary
        .as_file()
        .set_permissions(fs::Permissions::from_mode(0o600))
        .map_err(|error| unavailable("protect temporary record in", root, error))?;
    serde_json::to_writer(temporary.as_file_mut(), &record).map_err(|error| {
        RuntimeError::Unavailable {
            message: format!(
                "failed to encode containerd user-namespace record for `{workload_id}`: {error}"
            ),
        }
    })?;
    temporary
        .as_file_mut()
        .flush()
        .map_err(|error| unavailable("flush temporary record in", root, error))?;
    temporary
        .as_file()
        .sync_all()
        .map_err(|error| unavailable("sync temporary record in", root, error))?;
    let path = record_path(root, workload_id);
    temporary
        .persist_noclobber(&path)
        .map_err(|error| unavailable("persist", &path, error.error))?;
    sync_directory(root)
}

fn namespace_for_slot(slot: u32) -> Result<WorkloadUserNamespace, RuntimeError> {
    let offset =
        slot.checked_mul(USER_NAMESPACE_RANGE_SIZE)
            .ok_or_else(|| RuntimeError::Rejected {
                message: "containerd user-namespace slot offset overflowed u32".to_owned(),
            })?;
    let host_id = USER_NAMESPACE_HOST_BASE
        .checked_add(offset)
        .ok_or_else(|| RuntimeError::Rejected {
            message: "containerd user-namespace host range overflowed u32".to_owned(),
        })?;
    let mapping = WorkloadIdMapping {
        container_id: 0,
        host_id,
        size: USER_NAMESPACE_RANGE_SIZE,
    };
    Ok(WorkloadUserNamespace {
        uid: mapping,
        gid: mapping,
    })
}

fn record_path(root: &Path, workload_id: &WorkloadId) -> PathBuf {
    root.join(format!("{workload_id}.json"))
}

fn sync_directory(path: &Path) -> Result<(), RuntimeError> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| unavailable("sync", path, error))
}

fn lock_state(root: &Path) -> Result<Flock<File>, RuntimeError> {
    let path = root.join(LOCK_FILE);
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .mode(0o600)
        .custom_flags(nix::libc::O_CLOEXEC | nix::libc::O_NOFOLLOW)
        .open(&path)
        .map_err(|error| unavailable("open lock for", &path, error))?;
    file.set_permissions(fs::Permissions::from_mode(0o600))
        .map_err(|error| unavailable("protect lock for", &path, error))?;
    Flock::lock(file, FlockArg::LockExclusive).map_err(|(_file, error)| RuntimeError::Unavailable {
        message: format!(
            "failed to lock containerd user-namespace state `{}`: {error}",
            root.display()
        ),
    })
}

fn unavailable(operation: &str, path: &Path, error: std::io::Error) -> RuntimeError {
    RuntimeError::Unavailable {
        message: format!(
            "failed to {operation} containerd user-namespace state `{}`: {error}",
            path.display()
        ),
    }
}
