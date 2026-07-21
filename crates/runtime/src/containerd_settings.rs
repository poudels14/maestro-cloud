use std::path::PathBuf;
use std::time::Duration;

use crate::RuntimeError;

/// Node-local configuration for a native containerd runtime connection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContainerdRuntimeSettings {
    /// Containerd gRPC Unix socket.
    pub socket: PathBuf,
    /// Containerd namespace dedicated to this Maestro installation.
    pub namespace: String,
    /// Snapshotter containing pulled and unpacked image layers.
    pub snapshotter: String,
    /// OCI runtime plugin used for created containers.
    pub runtime_name: String,
    /// Owner-only directory for task IO and exec FIFOs.
    pub state_root: PathBuf,
    /// BuildKit client executable or command name.
    pub buildctl: PathBuf,
    /// BuildKit daemon address passed to the client without shell interpretation.
    pub buildkit_address: String,
    /// Maximum wall-clock duration of one BuildKit build.
    pub build_timeout: Duration,
    /// Maximum expanded bytes accepted from an uploaded build archive.
    pub max_build_context_bytes: u64,
    /// Maximum entries accepted from an uploaded build archive.
    pub max_build_context_entries: usize,
    /// Maximum OCI archive bytes accepted back from BuildKit.
    pub max_build_output_bytes: u64,
    /// File-follow polling interval when no new log bytes are available.
    pub log_poll_interval: Duration,
    /// Deadline for a forced task shutdown to become observable.
    pub kill_timeout: Duration,
}

impl ContainerdRuntimeSettings {
    pub(crate) fn validate(&self) -> Result<(), RuntimeError> {
        if !self.socket.is_absolute() || !self.state_root.is_absolute() {
            return Err(RuntimeError::InvalidSpec {
                message: "containerd socket and state root must be absolute paths".to_owned(),
            });
        }
        if self.state_root.to_str().is_none() || self.buildctl.as_os_str().is_empty() {
            return Err(RuntimeError::InvalidSpec {
                message: "containerd state root must be valid UTF-8 and buildctl cannot be empty"
                    .to_owned(),
            });
        }
        if self.namespace.is_empty() || self.snapshotter.is_empty() || self.runtime_name.is_empty()
        {
            return Err(RuntimeError::InvalidSpec {
                message: "containerd namespace, snapshotter, and runtime name cannot be empty"
                    .to_owned(),
            });
        }
        if !self
            .namespace
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        {
            return Err(RuntimeError::InvalidSpec {
                message:
                    "containerd namespace may contain only ASCII letters, digits, '-', '_', and '.'"
                        .to_owned(),
            });
        }
        if self.buildkit_address.is_empty() || self.buildkit_address.chars().any(char::is_control) {
            return Err(RuntimeError::InvalidSpec {
                message: "BuildKit address cannot be empty or contain control characters"
                    .to_owned(),
            });
        }
        if self.log_poll_interval.is_zero()
            || self.kill_timeout.is_zero()
            || self.build_timeout.is_zero()
        {
            return Err(RuntimeError::InvalidSpec {
                message: "containerd polling, kill, and build deadlines must be positive"
                    .to_owned(),
            });
        }
        if self.max_build_context_bytes == 0
            || self.max_build_context_entries == 0
            || self.max_build_output_bytes == 0
        {
            return Err(RuntimeError::InvalidSpec {
                message: "containerd build limits must be positive".to_owned(),
            });
        }
        Ok(())
    }
}

impl Default for ContainerdRuntimeSettings {
    fn default() -> Self {
        Self {
            socket: PathBuf::from("/run/containerd/containerd.sock"),
            namespace: "maestro".to_owned(),
            snapshotter: "overlayfs".to_owned(),
            runtime_name: "io.containerd.runc.v2".to_owned(),
            state_root: PathBuf::from("/var/lib/maestro/runtime/containerd"),
            buildctl: PathBuf::from("buildctl"),
            buildkit_address: "unix:///run/buildkit/buildkitd.sock".to_owned(),
            build_timeout: Duration::from_secs(30 * 60),
            max_build_context_bytes: 4 * 1_024 * 1_024 * 1_024,
            max_build_context_entries: 100_000,
            max_build_output_bytes: 20 * 1_024 * 1_024 * 1_024,
            log_poll_interval: Duration::from_millis(100),
            kill_timeout: Duration::from_secs(5),
        }
    }
}
