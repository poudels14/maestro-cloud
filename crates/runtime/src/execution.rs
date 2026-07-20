use std::collections::BTreeMap;

use async_trait::async_trait;
use kernel_api::CommandSpec;
use serde::{Deserialize, Serialize};

use crate::RuntimeError;

/// Opaque backend cursor for resuming workload log delivery.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct LogCursor(String);

impl LogCursor {
    /// Wraps a backend cursor without interpreting its format.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Returns the opaque backend cursor.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Whether a log request ends at the current backend boundary or follows new records.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum LogMode {
    /// Read the available finite snapshot and then return end-of-stream.
    Snapshot,
    /// Continue waiting for records until the consumer cancels or the backend disconnects.
    Follow,
}

/// Workload log selection and resume position.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LogRequest {
    /// Last record already durably consumed, when resuming a stream.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub after: Option<LogCursor>,
    /// Finite snapshot or live-follow behavior.
    pub mode: LogMode,
}

/// Runtime stream that produced one log frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum LogSource {
    /// Standard output from the primary workload process.
    Stdout,
    /// Standard error from the primary workload process.
    Stderr,
}

/// One ordered chunk from a workload's runtime log facility.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogFrame {
    /// Cursor that becomes the next durable resume point after this frame is committed.
    pub cursor: LogCursor,
    /// Output stream associated with these bytes.
    pub source: LogSource,
    /// Unmodified backend bytes; parsing and line assembly belong to observability.
    pub payload: Vec<u8>,
}

/// Pull-based workload log stream.
#[async_trait]
pub trait LogStream: Send {
    /// Waits for the next frame. `None` means a finite snapshot ended cleanly; errors require the
    /// caller to re-open the stream from its last committed cursor.
    async fn next(&mut self) -> Result<Option<LogFrame>, RuntimeError>;
}

/// Terminal allocation requested for a workload exec command.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum ExecMode {
    /// Preserve separate stdout and stderr streams without allocating a terminal.
    Pipes,
    /// Allocate an interactive terminal at the specified initial dimensions.
    Terminal {
        /// Initial terminal width in character cells.
        columns: u16,
        /// Initial terminal height in character cells.
        rows: u16,
    },
}

/// One command executed inside an existing workload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecRequest {
    /// Executable and argument vector passed without shell reinterpretation.
    pub command: CommandSpec,
    /// Additional non-secret environment scoped to this command.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub environment: BTreeMap<String, String>,
    /// Pipe or terminal transport semantics.
    pub mode: ExecMode,
}

/// Input sent to a running exec session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecInput {
    /// Raw standard-input bytes.
    Stdin(Vec<u8>),
    /// Updated terminal dimensions; valid only for terminal sessions.
    Resize {
        /// New terminal width in character cells.
        columns: u16,
        /// New terminal height in character cells.
        rows: u16,
    },
    /// Close standard input while continuing to read command output.
    CloseStdin,
}

/// Output produced by a running exec session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecOutput {
    /// Raw standard-output or terminal bytes.
    Stdout(Vec<u8>),
    /// Raw standard-error bytes; terminal sessions merge these into stdout.
    Stderr(Vec<u8>),
    /// Terminal command result; no more output follows this item.
    Exited {
        /// Process exit code when the runtime reported one.
        code: Option<i32>,
    },
}

/// Bidirectional exec session owned by one caller.
#[async_trait]
pub trait ExecSession: Send {
    /// Applies one input operation with backend backpressure. Canceling before completion may leave
    /// the input unsent, so callers must not assume delivery without `Ok(())`.
    async fn send(&mut self, input: ExecInput) -> Result<(), RuntimeError>;

    /// Waits for one output item. `None` means the backend closed without a terminal exit frame.
    async fn next(&mut self) -> Result<Option<ExecOutput>, RuntimeError>;
}
