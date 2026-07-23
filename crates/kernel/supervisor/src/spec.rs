use std::collections::BTreeMap;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::SupervisorError;

/// Executable and argument vector passed without shell reinterpretation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessCommand {
    /// Absolute executable path.
    pub executable: PathBuf,
    /// Argument vector passed verbatim after `argv[0]`.
    pub arguments: Vec<String>,
}

/// Whether a process inherits the daemon's environment before explicit values are applied.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnvironmentInheritance {
    /// Preserve the daemon environment, then override configured names.
    Inherit,
    /// Clear the daemon environment and pass only configured names.
    Clear,
}

/// Explicit process environment and inheritance semantics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessEnvironment {
    /// Inherit or clear behavior.
    pub inheritance: EnvironmentInheritance,
    /// Environment variables applied after inheritance behavior.
    pub variables: BTreeMap<String, String>,
}

/// Host user and group applied before executing the child process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProcessUser {
    /// Host user identity.
    pub user_id: u32,
    /// Host group identity.
    pub group_id: u32,
}

/// Append-only runtime log destinations for a detached process.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessLogFiles {
    /// Absolute primary-process stdout path.
    pub stdout: PathBuf,
    /// Absolute primary-process stderr path.
    pub stderr: PathBuf,
}

/// Complete immutable child-process spawn specification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessSpec {
    /// Executable and arguments.
    pub command: ProcessCommand,
    /// Explicit environment.
    pub environment: ProcessEnvironment,
    /// Absolute working directory, or the daemon working directory when absent.
    pub working_directory: Option<PathBuf>,
    /// Append-only stdout and stderr files.
    pub logs: ProcessLogFiles,
    /// Optional host user and group.
    pub user: Option<ProcessUser>,
}

impl ProcessSpec {
    /// Validates paths and environment names before any filesystem or process mutation.
    pub fn validate(&self) -> Result<(), SupervisorError> {
        if !self.command.executable.is_absolute() {
            return Err(SupervisorError::InvalidSpec {
                message: "process executable must be an absolute path".to_owned(),
            });
        }
        if !self.logs.stdout.is_absolute() || !self.logs.stderr.is_absolute() {
            return Err(SupervisorError::InvalidSpec {
                message: "process log paths must be absolute".to_owned(),
            });
        }
        if self
            .working_directory
            .as_ref()
            .is_some_and(|directory| !directory.is_absolute())
        {
            return Err(SupervisorError::InvalidSpec {
                message: "process working directory must be absolute".to_owned(),
            });
        }
        if self
            .environment
            .variables
            .keys()
            .any(|name| name.is_empty() || name.contains('=') || name.contains('\0'))
        {
            return Err(SupervisorError::InvalidSpec {
                message: "process environment contains an invalid variable name".to_owned(),
            });
        }
        Ok(())
    }
}

/// Persistable operating-system identity resistant to PID reuse.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProcessHandle {
    pid: u32,
    start_time_ticks: u64,
}

impl ProcessHandle {
    /// Restores a handle persisted by the process runtime.
    pub fn from_parts(pid: u32, start_time_ticks: u64) -> Result<Self, SupervisorError> {
        if pid == 0 || start_time_ticks == 0 {
            Err(SupervisorError::InvalidSpec {
                message: "process handle requires non-zero PID and start time".to_owned(),
            })
        } else {
            Ok(Self {
                pid,
                start_time_ticks,
            })
        }
    }

    /// Returns the process-group leader PID.
    pub fn pid(self) -> u32 {
        self.pid
    }

    /// Returns Linux procfs start time measured in kernel clock ticks since boot.
    pub fn start_time_ticks(self) -> u64 {
        self.start_time_ticks
    }
}

/// Primary-process exit evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProcessExit {
    /// Conventional exit code when the process exited normally.
    pub code: Option<i32>,
    /// Unix signal number when a signal terminated the process.
    pub signal: Option<i32>,
}

/// Current detached-process state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessStatus {
    /// Procfs reports the exact persisted identity as live.
    Running,
    /// An owned child was reaped, or procfs reports a zombie/dead process.
    Exited(ProcessExit),
    /// No process currently exists at the persisted PID.
    Gone,
}

/// Explicit process-group signal supported by the kernel supervisor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessSignal {
    /// Request cooperative termination with SIGTERM.
    Terminate,
    /// Force termination with SIGKILL.
    Kill,
}

impl ProcessSignal {
    /// Returns the stable Unix signal name.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Terminate => "SIGTERM",
            Self::Kill => "SIGKILL",
        }
    }
}
