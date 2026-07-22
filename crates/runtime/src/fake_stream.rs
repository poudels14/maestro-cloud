use std::collections::VecDeque;

use async_trait::async_trait;
use kernel_api::{ClusterId, NodeId};
use tokio::sync::broadcast;

use crate::fake_state::FakeEventRecord;
use crate::{
    ExecInput, ExecOutput, ExecSession, LogFrame, LogStream, RuntimeError, RuntimeEvent,
    RuntimeEventStream,
};

pub(crate) struct FakeEventStream {
    pub(crate) events: VecDeque<RuntimeEvent>,
    pub(crate) receiver: broadcast::Receiver<FakeEventRecord>,
    pub(crate) cluster_id: ClusterId,
    pub(crate) node_id: NodeId,
    pub(crate) after: u64,
}

#[async_trait]
impl RuntimeEventStream for FakeEventStream {
    async fn next(&mut self) -> Result<Option<RuntimeEvent>, RuntimeError> {
        if let Some(event) = self.events.pop_front() {
            self.after = cursor_sequence(event.cursor.as_str())?;
            return Ok(Some(event));
        }
        loop {
            match self.receiver.recv().await {
                Ok(record) => {
                    let sequence = cursor_sequence(record.event.cursor.as_str())?;
                    if record.cluster_id == self.cluster_id
                        && record.node_id == self.node_id
                        && sequence > self.after
                    {
                        self.after = sequence;
                        return Ok(Some(record.event));
                    }
                }
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    return Err(RuntimeError::Stream {
                        message: format!(
                            "fake lifecycle subscriber lagged by {skipped} events; re-list required"
                        ),
                    });
                }
                Err(broadcast::error::RecvError::Closed) => return Ok(None),
            }
        }
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

    async fn kill(&mut self) -> Result<(), RuntimeError> {
        self.outputs.clear();
        self.outputs
            .push_back(ExecOutput::Exited { code: Some(137) });
        Ok(())
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
