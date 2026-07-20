use std::fs::{self, File, OpenOptions};
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::os::unix::process::{CommandExt, ExitStatusExt};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};

use nix::sys::signal::{Signal, killpg};
use nix::unistd::Pid;

use crate::{
    EnvironmentInheritance, ProcessExit, ProcessHandle, ProcessSignal, ProcessSpec, ProcessStatus,
    SupervisorError,
};

pub(crate) fn spawn_process(spec: &ProcessSpec) -> Result<(ProcessHandle, Child), SupervisorError> {
    spec.validate()?;
    let stdout = open_log(&spec.logs.stdout)?;
    let stderr = open_log(&spec.logs.stderr)?;
    let mut command = Command::new(&spec.command.executable);
    command
        .args(&spec.command.arguments)
        .stdin(Stdio::null())
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr))
        .process_group(0);
    if spec.environment.inheritance == EnvironmentInheritance::Clear {
        command.env_clear();
    }
    command.envs(&spec.environment.variables);
    if let Some(directory) = &spec.working_directory {
        command.current_dir(directory);
    }
    if let Some(user) = spec.user {
        command.gid(user.group_id).uid(user.user_id);
    }
    let child = command.spawn().map_err(|error| SupervisorError::Io {
        operation: "spawn",
        path: spec.command.executable.clone(),
        message: error.to_string(),
    })?;
    let pid = child.id();
    let observation = inspect_pid(pid)?;
    let start_time_ticks = match observation {
        ProcObservation::Running { start_time_ticks }
        | ProcObservation::Exited { start_time_ticks } => start_time_ticks,
        ProcObservation::Gone => {
            return Err(SupervisorError::Io {
                operation: "inspect newly spawned",
                path: proc_stat_path(pid),
                message: "new child disappeared before exposing a procfs identity".to_owned(),
            });
        }
    };
    Ok((ProcessHandle::from_parts(pid, start_time_ticks)?, child))
}

pub(crate) fn inspect_process(handle: ProcessHandle) -> Result<ProcessStatus, SupervisorError> {
    match inspect_pid(handle.pid())? {
        ProcObservation::Running { start_time_ticks } => {
            validate_start_time(handle, start_time_ticks)?;
            Ok(ProcessStatus::Running)
        }
        ProcObservation::Exited { start_time_ticks } => {
            validate_start_time(handle, start_time_ticks)?;
            Ok(ProcessStatus::Exited(ProcessExit {
                code: None,
                signal: None,
            }))
        }
        ProcObservation::Gone => Ok(ProcessStatus::Gone),
    }
}

pub(crate) fn signal_process(
    handle: ProcessHandle,
    signal: ProcessSignal,
) -> Result<(), SupervisorError> {
    if inspect_process(handle)? != ProcessStatus::Running {
        return Ok(());
    }
    let pid = i32::try_from(handle.pid()).map_err(|_| SupervisorError::Signal {
        pid: handle.pid(),
        signal: signal.as_str(),
        message: "PID exceeds the platform process identifier range".to_owned(),
    })?;
    let unix_signal = match signal {
        ProcessSignal::Terminate => Signal::SIGTERM,
        ProcessSignal::Kill => Signal::SIGKILL,
    };
    killpg(Pid::from_raw(pid), unix_signal).map_err(|error| SupervisorError::Signal {
        pid: handle.pid(),
        signal: signal.as_str(),
        message: error.to_string(),
    })
}

pub(crate) fn process_exit(status: ExitStatus) -> ProcessExit {
    ProcessExit {
        code: status.code(),
        signal: status.signal(),
    }
}

fn open_log(path: &Path) -> Result<File, SupervisorError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|error| SupervisorError::Io {
            operation: "create log directory",
            path: parent.to_path_buf(),
            message: error.to_string(),
        })?;
    }
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .mode(0o600)
        .open(path)
        .map_err(|error| SupervisorError::Io {
            operation: "open log",
            path: path.to_path_buf(),
            message: error.to_string(),
        })?;
    file.set_permissions(fs::Permissions::from_mode(0o600))
        .map_err(|error| SupervisorError::Io {
            operation: "protect log",
            path: path.to_path_buf(),
            message: error.to_string(),
        })?;
    Ok(file)
}

fn inspect_pid(pid: u32) -> Result<ProcObservation, SupervisorError> {
    let path = proc_stat_path(pid);
    let contents = match fs::read_to_string(&path) {
        Ok(contents) => contents,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(ProcObservation::Gone);
        }
        Err(error) => {
            return Err(SupervisorError::Io {
                operation: "read proc status",
                path,
                message: error.to_string(),
            });
        }
    };
    parse_proc_stat(&contents).map_err(|message| SupervisorError::Io {
        operation: "parse proc status",
        path,
        message,
    })
}

fn parse_proc_stat(contents: &str) -> Result<ProcObservation, String> {
    let command_end = contents
        .rfind(") ")
        .ok_or_else(|| "proc stat command boundary is missing".to_owned())?;
    let fields = contents
        .get(command_end.saturating_add(2)..)
        .ok_or_else(|| "proc stat fields are missing".to_owned())?;
    let mut fields = fields.split_whitespace();
    let state = fields
        .next()
        .and_then(|value| value.chars().next())
        .ok_or_else(|| "proc stat state is missing".to_owned())?;
    let start_time_ticks = fields
        .nth(18)
        .ok_or_else(|| "proc stat start time is missing".to_owned())?
        .parse::<u64>()
        .map_err(|error| format!("proc stat start time is invalid: {error}"))?;
    if matches!(state, 'Z' | 'X' | 'x') {
        Ok(ProcObservation::Exited { start_time_ticks })
    } else {
        Ok(ProcObservation::Running { start_time_ticks })
    }
}

fn validate_start_time(
    handle: ProcessHandle,
    actual_start_time_ticks: u64,
) -> Result<(), SupervisorError> {
    if handle.start_time_ticks() == actual_start_time_ticks {
        Ok(())
    } else {
        Err(SupervisorError::IdentityMismatch {
            pid: handle.pid(),
            expected_start_time_ticks: handle.start_time_ticks(),
            actual_start_time_ticks,
        })
    }
}

fn proc_stat_path(pid: u32) -> PathBuf {
    PathBuf::from(format!("/proc/{pid}/stat"))
}

enum ProcObservation {
    Running { start_time_ticks: u64 },
    Exited { start_time_ticks: u64 },
    Gone,
}
