use std::path::{Path, PathBuf};
use std::process::Stdio;

use async_trait::async_trait;
use kernel_api::NodeFirewallSpec;
use tokio::io::AsyncWriteExt;
use tokio::process::Command;

use crate::{FirewallBackend, FirewallBackendError};

/// Linux nftables adapter checking and applying complete scripts through stdin.
#[derive(Debug, Clone)]
pub struct NftablesFirewallBackend {
    binary: PathBuf,
}

impl NftablesFirewallBackend {
    /// Uses the `nft` binary resolved through the process environment.
    pub fn new() -> Self {
        Self {
            binary: PathBuf::from("nft"),
        }
    }

    /// Uses an explicit binary path for hermetic packaging and adapter tests.
    pub fn with_binary(binary: impl Into<PathBuf>) -> Self {
        Self {
            binary: binary.into(),
        }
    }

    async fn execute(&self, arguments: &[&str], script: &str) -> Result<(), FirewallBackendError> {
        let mut command = Command::new(&self.binary);
        command
            .args(arguments)
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        let mut child = command
            .spawn()
            .map_err(|error| process_error(&self.binary, error))?;
        let mut stdin = child.stdin.take().ok_or_else(|| {
            FirewallBackendError::new("nft process did not expose its piped stdin")
        })?;
        stdin.write_all(script.as_bytes()).await.map_err(|error| {
            FirewallBackendError::new(format!("failed to write nft input: {error}"))
        })?;
        drop(stdin);
        let output = child
            .wait_with_output()
            .await
            .map_err(|error| process_error(&self.binary, error))?;
        if output.status.success() {
            Ok(())
        } else {
            Err(FirewallBackendError::new(format!(
                "nft exited with {}: {}",
                output.status,
                String::from_utf8_lossy(&output.stderr).trim()
            )))
        }
    }
}

impl Default for NftablesFirewallBackend {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl FirewallBackend for NftablesFirewallBackend {
    async fn apply(&self, desired: &NodeFirewallSpec) -> Result<(), FirewallBackendError> {
        self.execute(&["--check", "-f", "-"], &desired.script)
            .await?;
        self.execute(&["-f", "-"], &desired.script).await
    }
}

fn process_error(binary: &Path, error: std::io::Error) -> FirewallBackendError {
    FirewallBackendError::new(format!(
        "failed to execute nft binary `{}`: {error}",
        binary.display()
    ))
}
