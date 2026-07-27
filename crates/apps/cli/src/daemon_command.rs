use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::process::Command;

use crate::CliError;

pub(crate) async fn run(mut arguments: Vec<OsString>) -> Result<(), CliError> {
    if arguments.is_empty() {
        arguments.push(OsString::from("--help"));
    }
    let executable = daemon_executable()?;
    let status = tokio::task::spawn_blocking(move || {
        Command::new(&executable)
            .args(arguments)
            .status()
            .map_err(|source| {
                CliError::io(format!("failed to start {}", executable.display()), source)
            })
    })
    .await
    .map_err(|error| CliError::exec(format!("daemon command task failed: {error}")))??;
    if status.success() {
        Ok(())
    } else {
        Err(CliError::ExecExit {
            code: status.code().unwrap_or(1),
        })
    }
}

fn daemon_executable() -> Result<PathBuf, CliError> {
    let current = std::env::current_exe()
        .map_err(|source| CliError::io("failed to locate the Maestro executable", source))?;
    let sibling = daemon_sibling(&current);
    if sibling.is_file() {
        Ok(sibling)
    } else {
        Ok(PathBuf::from(format!(
            "maestro-daemon{}",
            std::env::consts::EXE_SUFFIX
        )))
    }
}

pub(crate) fn daemon_sibling(current: &Path) -> PathBuf {
    current.with_file_name(format!("maestro-daemon{}", std::env::consts::EXE_SUFFIX))
}
