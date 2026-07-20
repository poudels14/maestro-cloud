use std::collections::VecDeque;

use async_trait::async_trait;

use crate::{
    ExecInput, ExecOutput, ExecSession, LogFrame, LogStream, RuntimeError, RuntimeEvent,
    RuntimeEventStream,
};

pub(crate) struct FakeEventStream {
    pub(crate) events: VecDeque<RuntimeEvent>,
}

#[async_trait]
impl RuntimeEventStream for FakeEventStream {
    async fn next(&mut self) -> Result<Option<RuntimeEvent>, RuntimeError> {
        Ok(self.events.pop_front())
    }
}

pub(crate) struct FakeLogStream {
    pub(crate) frames: VecDeque<LogFrame>,
}

#[async_trait]
impl LogStream for FakeLogStream {
    async fn next(&mut self) -> Result<Option<LogFrame>, RuntimeError> {
        Ok(self.frames.pop_front())
    }
}

pub(crate) struct FakeExecSession {
    pub(crate) outputs: VecDeque<ExecOutput>,
}

#[async_trait]
impl ExecSession for FakeExecSession {
    async fn send(&mut self, input: ExecInput) -> Result<(), RuntimeError> {
        if let ExecInput::Stdin(bytes) = input {
            self.outputs.push_front(ExecOutput::Stdout(bytes));
        }
        Ok(())
    }

    async fn next(&mut self) -> Result<Option<ExecOutput>, RuntimeError> {
        Ok(self.outputs.pop_front())
    }
}

pub(crate) fn parse_cursor(cursor: Option<&str>) -> Result<u64, RuntimeError> {
    cursor.map_or(Ok(0), cursor_sequence)
}

pub(crate) fn cursor_sequence(cursor: &str) -> Result<u64, RuntimeError> {
    cursor.parse().map_err(|_| RuntimeError::Stream {
        message: format!("fake cursor `{cursor}` is invalid"),
    })
}
