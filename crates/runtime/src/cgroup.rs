use std::path::{Path, PathBuf};

use crate::RuntimeError;

pub(crate) fn read_cgroup_path(process_id: u32) -> Result<PathBuf, RuntimeError> {
    let proc_path = PathBuf::from(format!("/proc/{process_id}/cgroup"));
    let contents =
        std::fs::read_to_string(&proc_path).map_err(|error| RuntimeError::Unavailable {
            message: format!("failed to read `{}`: {error}", proc_path.display()),
        })?;
    let relative = contents
        .lines()
        .find_map(|line| line.strip_prefix("0::"))
        .ok_or_else(|| RuntimeError::Rejected {
            message: format!("`{}` does not report a cgroup-v2 path", proc_path.display()),
        })?;
    Ok(Path::new("/sys/fs/cgroup").join(relative.trim_start_matches('/')))
}
