use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

use crate::{ExecMode, RuntimeError};

pub(crate) struct ExecPaths {
    pub(crate) directory: PathBuf,
    pub(crate) stdin: PathBuf,
    pub(crate) stdout: PathBuf,
    pub(crate) stderr: Option<PathBuf>,
}

pub(crate) async fn prepare_exec_paths(
    state_root: &Path,
    exec_id: &str,
    mode: ExecMode,
) -> Result<ExecPaths, RuntimeError> {
    let directory = state_root.join("exec").join(exec_id);
    let paths = ExecPaths {
        stdin: directory.join("stdin"),
        stdout: directory.join("stdout"),
        stderr: matches!(mode, ExecMode::Pipes).then(|| directory.join("stderr")),
        directory,
    };
    let blocking_paths = ExecPaths {
        directory: paths.directory.clone(),
        stdin: paths.stdin.clone(),
        stdout: paths.stdout.clone(),
        stderr: paths.stderr.clone(),
    };
    tokio::task::spawn_blocking(move || {
        fs::create_dir_all(&blocking_paths.directory)
            .map_err(|error| exec_file_error("create", &blocking_paths.directory, error))?;
        fs::set_permissions(&blocking_paths.directory, fs::Permissions::from_mode(0o700))
            .map_err(|error| exec_file_error("protect", &blocking_paths.directory, error))?;
        create_fifo(&blocking_paths.stdin)?;
        create_fifo(&blocking_paths.stdout)?;
        if let Some(stderr) = &blocking_paths.stderr {
            create_fifo(stderr)?;
        }
        Ok::<(), RuntimeError>(())
    })
    .await
    .map_err(|error| RuntimeError::Unavailable {
        message: format!("containerd exec FIFO task failed: {error}"),
    })??;
    Ok(paths)
}

pub(crate) async fn open_anchors(paths: &ExecPaths) -> Result<Vec<fs::File>, RuntimeError> {
    let stdin = paths.stdin.clone();
    let stdout = paths.stdout.clone();
    let stderr = paths.stderr.clone();
    tokio::task::spawn_blocking(move || {
        [Some(stdin), Some(stdout), stderr]
            .into_iter()
            .flatten()
            .map(|path| {
                fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(&path)
                    .map_err(|error| exec_file_error("anchor", &path, error))
            })
            .collect()
    })
    .await
    .map_err(|error| RuntimeError::Unavailable {
        message: format!("containerd exec FIFO anchor task failed: {error}"),
    })?
}

pub(crate) async fn open_async_io(
    paths: &ExecPaths,
) -> Result<(tokio::fs::File, tokio::fs::File, Option<tokio::fs::File>), RuntimeError> {
    let stdin = tokio::fs::OpenOptions::new()
        .write(true)
        .open(&paths.stdin)
        .await
        .map_err(exec_io_error)?;
    let stdout = tokio::fs::OpenOptions::new()
        .read(true)
        .open(&paths.stdout)
        .await
        .map_err(exec_io_error)?;
    let stderr = match &paths.stderr {
        Some(path) => Some(
            tokio::fs::OpenOptions::new()
                .read(true)
                .open(path)
                .await
                .map_err(exec_io_error)?,
        ),
        None => None,
    };
    Ok((stdin, stdout, stderr))
}

pub(crate) async fn remove_exec_directory(directory: &Path) {
    let _result = tokio::fs::remove_dir_all(directory).await;
}

fn create_fifo(path: &Path) -> Result<(), RuntimeError> {
    nix::unistd::mkfifo(path, nix::sys::stat::Mode::from_bits_truncate(0o600))
        .map_err(|error| exec_file_error("create FIFO", path, std::io::Error::from(error)))
}

pub(crate) fn exec_io_error(error: std::io::Error) -> RuntimeError {
    RuntimeError::Stream {
        message: format!("containerd exec IO failed: {error}"),
    }
}

fn exec_file_error(operation: &str, path: &Path, error: std::io::Error) -> RuntimeError {
    RuntimeError::Unavailable {
        message: format!(
            "failed to {operation} containerd exec path `{}`: {error}",
            path.display()
        ),
    }
}
