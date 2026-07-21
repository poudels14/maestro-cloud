use std::ffi::OsString;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;

use crate::nixos::{NixosCommand, NixosCommandRunner, ProcessNixosCommandRunner};

/// Node-local boundary that requests a host reboot after boot staging succeeds.
#[async_trait]
pub trait NodeRebooter: Send + Sync {
    /// Requests a reboot and returns only after the host accepted the request.
    ///
    /// Cancellation may leave the reboot accepted. The durable command remains
    /// released, and a new daemon identity prevents a duplicate reboot request.
    async fn reboot(&self) -> Result<(), NodeRebootError>;
}

/// Matchable host reboot request failure.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("node reboot request failed: {message}")]
pub struct NodeRebootError {
    message: String,
}

impl NodeRebootError {
    /// Creates a sanitized reboot failure without host command output contracts.
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

/// Production reboot adapter using a bounded, kill-on-cancel system command.
pub struct ProcessNodeRebooter {
    binary: PathBuf,
    runner: Arc<dyn NixosCommandRunner>,
}

impl ProcessNodeRebooter {
    /// Uses `systemctl reboot` resolved through the process environment.
    pub fn new() -> Self {
        Self::with_runner(
            PathBuf::from("systemctl"),
            Arc::new(ProcessNixosCommandRunner),
        )
    }

    /// Uses an explicit systemctl path for hermetic host packaging.
    pub fn with_binary(binary: impl Into<PathBuf>) -> Self {
        Self::with_runner(binary.into(), Arc::new(ProcessNixosCommandRunner))
    }

    pub(crate) fn with_runner(binary: PathBuf, runner: Arc<dyn NixosCommandRunner>) -> Self {
        Self { binary, runner }
    }
}

impl Default for ProcessNodeRebooter {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl NodeRebooter for ProcessNodeRebooter {
    async fn reboot(&self) -> Result<(), NodeRebootError> {
        self.runner
            .run(NixosCommand::new(
                self.binary.clone(),
                [OsString::from("reboot")],
            ))
            .await
            .map(|_| ())
            .map_err(|error| NodeRebootError::new(error.to_string()))
    }
}
