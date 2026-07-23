use std::pin::Pin;

use async_trait::async_trait;
use docker::Docker;
use docker::container::LogOutput;
use docker::errors::Error as DockerError;
use docker::models::EventMessage;
use futures_util::{Stream, StreamExt};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::docker_support::{runtime_event, stream_error};
use crate::{
    ExecInput, ExecMode, ExecOutput, ExecSession, LogCursor, LogFrame, LogSource, LogStream,
    RuntimeError, RuntimeEvent, RuntimeEventStream,
};

type DockerEvents = Pin<Box<dyn Stream<Item = Result<EventMessage, DockerError>> + Send>>;
type DockerOutput = Pin<Box<dyn Stream<Item = Result<LogOutput, DockerError>> + Send>>;
type DockerInput = Pin<Box<dyn AsyncWrite + Send>>;

pub(crate) struct DockerEventStream {
    events: DockerEvents,
}

impl DockerEventStream {
    pub(crate) fn new(events: DockerEvents) -> Self {
        Self { events }
    }
}

#[async_trait]
impl RuntimeEventStream for DockerEventStream {
    async fn next(&mut self) -> Result<Option<RuntimeEvent>, RuntimeError> {
        while let Some(message) = self.events.next().await {
            let message = message.map_err(stream_error)?;
            if let Some(event) = runtime_event(message)? {
                return Ok(Some(event));
            }
        }
        Ok(None)
    }
}

pub(crate) struct DockerLogStream {
    output: DockerOutput,
}

impl DockerLogStream {
    pub(crate) fn new(output: DockerOutput) -> Self {
        Self { output }
    }
}

#[async_trait]
impl LogStream for DockerLogStream {
    async fn next(&mut self) -> Result<Option<LogFrame>, RuntimeError> {
        match self.output.next().await {
            Some(Ok(output)) => decode_log(output).map(Some),
            Some(Err(error)) => Err(stream_error(error)),
            None => Ok(None),
        }
    }
}

pub(crate) struct DockerExecSession {
    client: Docker,
    exec_id: String,
    mode: ExecMode,
    input: DockerInput,
    output: DockerOutput,
    input_closed: bool,
    exit_emitted: bool,
}

impl DockerExecSession {
    pub(crate) fn new(
        client: Docker,
        exec_id: String,
        mode: ExecMode,
        input: DockerInput,
        output: DockerOutput,
    ) -> Self {
        Self {
            client,
            exec_id,
            mode,
            input,
            output,
            input_closed: false,
            exit_emitted: false,
        }
    }
}

#[async_trait]
impl ExecSession for DockerExecSession {
    async fn send(&mut self, input: ExecInput) -> Result<(), RuntimeError> {
        match input {
            ExecInput::Stdin(bytes) if !self.input_closed => {
                self.input.write_all(&bytes).await.map_err(exec_io_error)
            }
            ExecInput::Stdin(_) => Err(RuntimeError::Rejected {
                message: "docker exec standard input is already closed".to_owned(),
            }),
            ExecInput::Resize { columns, rows }
                if matches!(self.mode, ExecMode::Terminal { .. }) =>
            {
                self.client
                    .resize_exec(
                        &self.exec_id,
                        docker::exec::ResizeExecOptions {
                            height: rows,
                            width: columns,
                        },
                    )
                    .await
                    .map_err(stream_error)
            }
            ExecInput::Resize { .. } => Err(RuntimeError::Rejected {
                message: "cannot resize a pipe-mode docker exec session".to_owned(),
            }),
            ExecInput::CloseStdin if !self.input_closed => {
                self.input.shutdown().await.map_err(exec_io_error)?;
                self.input_closed = true;
                Ok(())
            }
            ExecInput::CloseStdin => Ok(()),
        }
    }

    async fn next(&mut self) -> Result<Option<ExecOutput>, RuntimeError> {
        if let Some(output) = self.output.next().await {
            return output.map(exec_output).map(Some).map_err(stream_error);
        }
        if self.exit_emitted {
            Ok(None)
        } else {
            let inspect = self
                .client
                .inspect_exec(&self.exec_id)
                .await
                .map_err(stream_error)?;
            self.exit_emitted = true;
            Ok(Some(ExecOutput::Exited {
                code: inspect.exit_code.and_then(|code| i32::try_from(code).ok()),
            }))
        }
    }
}

pub(crate) fn log_since(cursor: Option<&LogCursor>) -> Result<i32, RuntimeError> {
    cursor.map_or(Ok(0), |cursor| {
        let timestamp = OffsetDateTime::parse(cursor.as_str(), &Rfc3339).map_err(|error| {
            RuntimeError::Stream {
                message: format!("docker log cursor is invalid: {error}"),
            }
        })?;
        i32::try_from(timestamp.unix_timestamp()).map_err(|_| RuntimeError::Stream {
            message: "docker log cursor is outside the daemon timestamp range".to_owned(),
        })
    })
}

pub(crate) fn decode_log(output: LogOutput) -> Result<LogFrame, RuntimeError> {
    let source = match output {
        LogOutput::StdErr { .. } => LogSource::Stderr,
        LogOutput::StdOut { .. } | LogOutput::StdIn { .. } | LogOutput::Console { .. } => {
            LogSource::Stdout
        }
    };
    let bytes = output.into_bytes();
    let separator =
        bytes
            .iter()
            .position(|byte| *byte == b' ')
            .ok_or_else(|| RuntimeError::Stream {
                message: "docker timestamped log frame omitted its timestamp separator".to_owned(),
            })?;
    let timestamp_bytes = bytes
        .as_ref()
        .get(..separator)
        .ok_or_else(|| RuntimeError::Stream {
            message: "docker log timestamp boundary is invalid".to_owned(),
        })?;
    let payload = bytes
        .as_ref()
        .get(separator.saturating_add(1)..)
        .ok_or_else(|| RuntimeError::Stream {
            message: "docker log payload boundary is invalid".to_owned(),
        })?;
    let timestamp = std::str::from_utf8(timestamp_bytes).map_err(|error| RuntimeError::Stream {
        message: format!("docker log timestamp is not UTF-8: {error}"),
    })?;
    OffsetDateTime::parse(timestamp, &Rfc3339).map_err(|error| RuntimeError::Stream {
        message: format!("docker log timestamp is invalid: {error}"),
    })?;
    Ok(LogFrame {
        cursor: LogCursor::new(timestamp),
        source,
        payload: payload.to_vec(),
    })
}

fn exec_output(output: LogOutput) -> ExecOutput {
    match output {
        LogOutput::StdErr { message } => ExecOutput::Stderr(message.to_vec()),
        LogOutput::StdOut { message }
        | LogOutput::StdIn { message }
        | LogOutput::Console { message } => ExecOutput::Stdout(message.to_vec()),
    }
}

fn exec_io_error(error: std::io::Error) -> RuntimeError {
    RuntimeError::Stream {
        message: format!("docker exec input failed: {error}"),
    }
}
