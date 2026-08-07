use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use kernel_api::{ClusterId, NodeId, UpgradeRunId};
use serde::{Deserialize, Serialize};

use crate::{NodeUpgradeCommand, PlannedStoreRecovery};

const BOOT_ID_PATH: &str = "/proc/sys/kernel/random/boot_id";
const MARKER_FILE: &str = "system/planned-store-recovery.json";

/// Recovery authorization activated only after the host crosses a reboot boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActivatedStoreRecovery {
    pub run_id: UpgradeRunId,
    pub plan: PlannedStoreRecovery,
    pub issued_at_unix_ms: i64,
}

/// Durable run-scoped marker shared by the upgrade agent and early daemon startup.
#[derive(Debug)]
pub struct FileStoreRecoveryMarker {
    path: PathBuf,
    boot_id_path: PathBuf,
}

impl FileStoreRecoveryMarker {
    pub fn new(data_directory: &Path) -> Self {
        Self {
            path: data_directory.join(MARKER_FILE),
            boot_id_path: PathBuf::from(BOOT_ID_PATH),
        }
    }

    #[cfg(test)]
    pub(crate) fn with_boot_id_path(data_directory: &Path, boot_id_path: PathBuf) -> Self {
        Self {
            path: data_directory.join(MARKER_FILE),
            boot_id_path,
        }
    }

    pub fn prepare(
        &self,
        cluster_id: &ClusterId,
        command: &NodeUpgradeCommand,
    ) -> Result<(), StoreRecoveryMarkerError> {
        let Some(plan) = &command.store_recovery else {
            return Ok(());
        };
        if !plan.expected_members.contains(&command.node_id) {
            return Ok(());
        }
        let desired = Marker {
            cluster_id: cluster_id.clone(),
            node_id: command.node_id.clone(),
            run_id: command.run_id.clone(),
            plan: plan.clone(),
            issued_at_unix_ms: unix_time_millis()?,
            phase: MarkerPhase::Prepared,
        };
        if let Some(existing) = self.read()? {
            if existing.same_run(&desired) {
                return Ok(());
            }
            return Err(StoreRecoveryMarkerError::Collision {
                run_id: existing.run_id,
            });
        }
        self.write(&desired)
    }

    pub fn release(
        &self,
        cluster_id: &ClusterId,
        command: &NodeUpgradeCommand,
    ) -> Result<(), StoreRecoveryMarkerError> {
        let Some(plan) = &command.store_recovery else {
            return Ok(());
        };
        if !plan.expected_members.contains(&command.node_id) {
            return Ok(());
        }
        let mut marker = self.read()?.ok_or(StoreRecoveryMarkerError::NotPrepared)?;
        if marker.cluster_id != *cluster_id
            || marker.node_id != command.node_id
            || marker.run_id != command.run_id
            || marker.plan != *plan
        {
            return Err(StoreRecoveryMarkerError::Mismatch);
        }
        marker.phase = MarkerPhase::Released {
            boot_id: self.boot_id()?,
        };
        self.write(&marker)
    }

    pub fn activated(
        &self,
        cluster_id: &ClusterId,
        node_id: &NodeId,
    ) -> Result<Option<ActivatedStoreRecovery>, StoreRecoveryMarkerError> {
        let Some(marker) = self.read()? else {
            return Ok(None);
        };
        if marker.cluster_id != *cluster_id
            || marker.node_id != *node_id
            || !marker.plan.expected_members.contains(node_id)
        {
            return Err(StoreRecoveryMarkerError::Mismatch);
        }
        let MarkerPhase::Released { boot_id } = &marker.phase else {
            return Ok(None);
        };
        if *boot_id == self.boot_id()? {
            return Ok(None);
        }
        Ok(Some(ActivatedStoreRecovery {
            run_id: marker.run_id,
            plan: marker.plan,
            issued_at_unix_ms: marker.issued_at_unix_ms,
        }))
    }

    pub fn clear(&self, run_id: &UpgradeRunId) -> Result<(), StoreRecoveryMarkerError> {
        let Some(marker) = self.read()? else {
            return Ok(());
        };
        if marker.run_id != *run_id {
            return Ok(());
        }
        match fs::remove_file(&self.path) {
            Ok(()) => sync_parent(&self.path),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(io_error(&self.path, error)),
        }
    }

    fn boot_id(&self) -> Result<String, StoreRecoveryMarkerError> {
        let value = fs::read_to_string(&self.boot_id_path)
            .map_err(|error| io_error(&self.boot_id_path, error))?;
        let value = value.trim();
        if value.is_empty() {
            return Err(StoreRecoveryMarkerError::InvalidBootId);
        }
        Ok(value.to_owned())
    }

    fn read(&self) -> Result<Option<Marker>, StoreRecoveryMarkerError> {
        match fs::read(&self.path) {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .map(Some)
                .map_err(StoreRecoveryMarkerError::Decode),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(io_error(&self.path, error)),
        }
    }

    fn write(&self, marker: &Marker) -> Result<(), StoreRecoveryMarkerError> {
        let parent = self
            .path
            .parent()
            .ok_or(StoreRecoveryMarkerError::MissingParent)?;
        fs::create_dir_all(parent).map_err(|error| io_error(parent, error))?;
        let temporary = self.path.with_extension("tmp");
        let mut options = OpenOptions::new();
        options.create(true).truncate(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options
            .open(&temporary)
            .map_err(|error| io_error(&temporary, error))?;
        file.write_all(&serde_json::to_vec_pretty(marker)?)
            .and_then(|()| file.sync_all())
            .map_err(|error| io_error(&temporary, error))?;
        fs::rename(&temporary, &self.path).map_err(|error| io_error(&self.path, error))?;
        sync_parent(&self.path)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Marker {
    cluster_id: ClusterId,
    node_id: NodeId,
    run_id: UpgradeRunId,
    plan: PlannedStoreRecovery,
    issued_at_unix_ms: i64,
    phase: MarkerPhase,
}

impl Marker {
    fn same_run(&self, other: &Self) -> bool {
        self.cluster_id == other.cluster_id
            && self.node_id == other.node_id
            && self.run_id == other.run_id
            && self.plan == other.plan
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "kind")]
enum MarkerPhase {
    Prepared,
    Released { boot_id: String },
}

#[derive(Debug, thiserror::Error)]
pub enum StoreRecoveryMarkerError {
    #[error("planned store recovery marker has no parent directory")]
    MissingParent,
    #[error("planned store recovery marker has an invalid boot identity")]
    InvalidBootId,
    #[error("planned store recovery marker is malformed: {0}")]
    Decode(#[from] serde_json::Error),
    #[error("planned store recovery marker belongs to upgrade run `{run_id}`")]
    Collision { run_id: UpgradeRunId },
    #[error("planned store recovery marker was not prepared")]
    NotPrepared,
    #[error("planned store recovery marker does not match this cluster, node, or run")]
    Mismatch,
    #[error("planned store recovery marker I/O failed for `{path}`: {message}")]
    Io { path: PathBuf, message: String },
}

fn unix_time_millis() -> Result<i64, StoreRecoveryMarkerError> {
    let duration = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|error| StoreRecoveryMarkerError::Io {
            path: PathBuf::from("system clock"),
            message: error.to_string(),
        })?;
    i64::try_from(duration.as_millis()).map_err(|error| StoreRecoveryMarkerError::Io {
        path: PathBuf::from("system clock"),
        message: error.to_string(),
    })
}

fn sync_parent(path: &Path) -> Result<(), StoreRecoveryMarkerError> {
    let parent = path
        .parent()
        .ok_or(StoreRecoveryMarkerError::MissingParent)?;
    fs::File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| io_error(parent, error))
}

fn io_error(path: &Path, error: std::io::Error) -> StoreRecoveryMarkerError {
    StoreRecoveryMarkerError::Io {
        path: path.to_path_buf(),
        message: error.to_string(),
    }
}
