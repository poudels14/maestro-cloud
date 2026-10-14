use std::os::fd::FromRawFd;
use std::path::PathBuf;

use anyhow::Context;
use maestro_telemetry::{LogDriverSettings, pump};

pub const BINARY_NAME: &str = "maestro-log-driver";

const STDOUT_FD: i32 = 3;
const STDERR_FD: i32 = 4;
const READY_FD: i32 = 5;

pub fn invoked_as_log_driver() -> bool {
    std::env::args_os()
        .next()
        .map(PathBuf::from)
        .and_then(|path| path.file_name().map(|name| name == BINARY_NAME))
        .unwrap_or(false)
}

pub async fn run() -> anyhow::Result<()> {
    let container_id = std::env::var("CONTAINER_ID").context("CONTAINER_ID is not set")?;
    let arguments = std::env::args().skip(1).collect::<Vec<_>>();
    let log_dir = arguments
        .windows(2)
        .find(|pair| pair[0] == "--log-dir")
        .map(|pair| PathBuf::from(&pair[1]))
        .context("--log-dir is required")?;
    let (stdout, stderr, ready) = unsafe {
        (
            std::fs::File::from_raw_fd(STDOUT_FD),
            std::fs::File::from_raw_fd(STDERR_FD),
            std::fs::File::from_raw_fd(READY_FD),
        )
    };
    drop(ready);
    pump(
        &container_id,
        tokio::fs::File::from_std(stdout),
        tokio::fs::File::from_std(stderr),
        LogDriverSettings {
            log_dir,
            ..LogDriverSettings::default()
        },
    )
    .await?;
    Ok(())
}

pub fn ensure_binary_link() -> Option<PathBuf> {
    let executable = std::env::current_exe().ok()?;
    let link = executable.parent()?.join(BINARY_NAME);
    match link.exists() {
        true => Some(link),
        false => std::os::unix::fs::symlink(&executable, &link)
            .ok()
            .map(|_| link),
    }
}

pub fn uri(binary: &std::path::Path, log_dir: &std::path::Path) -> String {
    format!(
        "binary://{}?--log-dir={}",
        binary.display(),
        log_dir.display()
    )
}
