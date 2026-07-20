use std::collections::VecDeque;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use kernel_api::{ClusterId, NodeId};

use crate::{
    EventCursor, EventRequest, LogCursor, LogFrame, LogMode, LogSource, LogStream, RuntimeClock,
    RuntimeError, RuntimeEvent, RuntimeEventKind, RuntimeEventStream, WorkloadMetadata,
};

const EVENT_CAPACITY: usize = 4_096;
const LOG_CHUNK_BYTES: u64 = 64 * 1_024;

static NEXT_JOURNAL: AtomicU64 = AtomicU64::new(1);

#[derive(Clone)]
pub(crate) struct ProcessEventJournal {
    inner: Arc<Mutex<EventJournalState>>,
}

impl ProcessEventJournal {
    pub(crate) fn new() -> Self {
        let generation = NEXT_JOURNAL.fetch_add(1, Ordering::Relaxed);
        Self {
            inner: Arc::new(Mutex::new(EventJournalState {
                epoch: format!("{}-{generation}", std::process::id()),
                sequence: 0,
                records: VecDeque::new(),
            })),
        }
    }

    pub(crate) fn emit(
        &self,
        metadata: &WorkloadMetadata,
        kind: RuntimeEventKind,
        exit_code: Option<i32>,
    ) -> Result<(), RuntimeError> {
        let mut state = self.lock()?;
        state.sequence = state.sequence.saturating_add(1);
        let cursor = EventCursor::new(format!("{}:{}", state.epoch, state.sequence));
        if state.records.len() == EVENT_CAPACITY {
            state.records.pop_front();
        }
        state.records.push_back(EventRecord {
            cluster_id: metadata.cluster_id.clone(),
            node_id: metadata.node_id.clone(),
            event: RuntimeEvent {
                cursor,
                workload_id: metadata.workload_id.clone(),
                kind,
                exit_code,
            },
        });
        Ok(())
    }

    pub(crate) fn stream(
        &self,
        request: EventRequest,
        clock: Arc<dyn RuntimeClock>,
        poll_interval: Duration,
    ) -> Result<Box<dyn RuntimeEventStream>, RuntimeError> {
        let state = self.lock()?;
        let after = parse_event_cursor(request.after.as_ref(), &state.epoch)?;
        drop(state);
        Ok(Box::new(ProcessEventStream {
            journal: self.clone(),
            cluster_id: request.cluster_id,
            node_id: request.node_id,
            after,
            clock,
            poll_interval,
        }))
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, EventJournalState>, RuntimeError> {
        self.inner.lock().map_err(|_| RuntimeError::Unavailable {
            message: "process event journal lock was poisoned".to_owned(),
        })
    }
}

struct EventJournalState {
    epoch: String,
    sequence: u64,
    records: VecDeque<EventRecord>,
}

struct EventRecord {
    cluster_id: ClusterId,
    node_id: NodeId,
    event: RuntimeEvent,
}

struct ProcessEventStream {
    journal: ProcessEventJournal,
    cluster_id: ClusterId,
    node_id: NodeId,
    after: u64,
    clock: Arc<dyn RuntimeClock>,
    poll_interval: Duration,
}

#[async_trait]
impl RuntimeEventStream for ProcessEventStream {
    async fn next(&mut self) -> Result<Option<RuntimeEvent>, RuntimeError> {
        loop {
            let event = {
                let state = self.journal.lock()?;
                state
                    .records
                    .iter()
                    .find(|record| {
                        record.cluster_id == self.cluster_id
                            && record.node_id == self.node_id
                            && event_sequence(&record.event.cursor, &state.epoch)
                                .is_ok_and(|sequence| sequence > self.after)
                    })
                    .map(|record| record.event.clone())
            };
            if let Some(event) = event {
                self.after = event_sequence(&event.cursor, &self.journal.lock()?.epoch)?;
                return Ok(Some(event));
            }
            let deadline = self.clock.now().saturating_add(self.poll_interval);
            self.clock.sleep_until(deadline).await;
        }
    }
}

pub(crate) struct ProcessLogStream {
    stdout_path: PathBuf,
    stderr_path: PathBuf,
    stdout_offset: u64,
    stderr_offset: u64,
    mode: LogMode,
    clock: Arc<dyn RuntimeClock>,
    poll_interval: Duration,
    buffered: VecDeque<LogFrame>,
}

impl ProcessLogStream {
    pub(crate) fn open(
        stdout_path: PathBuf,
        stderr_path: PathBuf,
        after: Option<&LogCursor>,
        mode: LogMode,
        clock: Arc<dyn RuntimeClock>,
        poll_interval: Duration,
    ) -> Result<Box<dyn LogStream>, RuntimeError> {
        let (stdout_offset, stderr_offset) = parse_log_cursor(after)?;
        Ok(Box::new(Self {
            stdout_path,
            stderr_path,
            stdout_offset,
            stderr_offset,
            mode,
            clock,
            poll_interval,
            buffered: VecDeque::new(),
        }))
    }

    async fn refill(&mut self) -> Result<LogAvailability, RuntimeError> {
        let stdout_path = self.stdout_path.clone();
        let stderr_path = self.stderr_path.clone();
        let stdout_offset = self.stdout_offset;
        let stderr_offset = self.stderr_offset;
        let availability = tokio::task::spawn_blocking(move || {
            let stdout = read_chunk(&stdout_path, stdout_offset)?;
            let stderr = read_chunk(&stderr_path, stderr_offset)?;
            Ok::<_, RuntimeError>((stdout, stderr))
        })
        .await
        .map_err(|error| RuntimeError::Stream {
            message: format!("process log reader task failed: {error}"),
        })??;
        let (stdout, stderr) = availability;
        self.stdout_offset = stdout.offset;
        if !stdout.bytes.is_empty() {
            self.buffered.push_back(LogFrame {
                cursor: log_cursor(self.stdout_offset, self.stderr_offset),
                source: LogSource::Stdout,
                payload: stdout.bytes,
            });
        }
        self.stderr_offset = stderr.offset;
        if !stderr.bytes.is_empty() {
            self.buffered.push_back(LogFrame {
                cursor: log_cursor(self.stdout_offset, self.stderr_offset),
                source: LogSource::Stderr,
                payload: stderr.bytes,
            });
        }
        Ok(LogAvailability {
            any_file_exists: stdout.exists || stderr.exists,
        })
    }
}

#[async_trait]
impl LogStream for ProcessLogStream {
    async fn next(&mut self) -> Result<Option<LogFrame>, RuntimeError> {
        loop {
            if let Some(frame) = self.buffered.pop_front() {
                return Ok(Some(frame));
            }
            let availability = self.refill().await?;
            if let Some(frame) = self.buffered.pop_front() {
                return Ok(Some(frame));
            }
            if self.mode == LogMode::Snapshot || !availability.any_file_exists {
                return Ok(None);
            }
            let deadline = self.clock.now().saturating_add(self.poll_interval);
            self.clock.sleep_until(deadline).await;
        }
    }
}

struct LogChunk {
    bytes: Vec<u8>,
    offset: u64,
    exists: bool,
}

struct LogAvailability {
    any_file_exists: bool,
}

fn read_chunk(path: &Path, requested_offset: u64) -> Result<LogChunk, RuntimeError> {
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(LogChunk {
                bytes: Vec::new(),
                offset: 0,
                exists: false,
            });
        }
        Err(error) => return Err(log_io_error("open", path, error)),
    };
    let length = file
        .metadata()
        .map_err(|error| log_io_error("inspect", path, error))?
        .len();
    let offset = if requested_offset > length {
        0
    } else {
        requested_offset
    };
    file.seek(SeekFrom::Start(offset))
        .map_err(|error| log_io_error("seek", path, error))?;
    let mut bytes = Vec::new();
    file.take(LOG_CHUNK_BYTES)
        .read_to_end(&mut bytes)
        .map_err(|error| log_io_error("read", path, error))?;
    Ok(LogChunk {
        offset: offset.saturating_add(u64::try_from(bytes.len()).unwrap_or(u64::MAX)),
        bytes,
        exists: true,
    })
}

fn parse_event_cursor(cursor: Option<&EventCursor>, epoch: &str) -> Result<u64, RuntimeError> {
    let Some(cursor) = cursor else {
        return Ok(0);
    };
    let Some((observed_epoch, sequence)) = cursor.as_str().split_once(':') else {
        return Err(RuntimeError::Stream {
            message: format!("process event cursor `{}` is invalid", cursor.as_str()),
        });
    };
    if observed_epoch == epoch {
        parse_sequence(sequence, "event", cursor.as_str())
    } else {
        Ok(0)
    }
}

fn event_sequence(cursor: &EventCursor, epoch: &str) -> Result<u64, RuntimeError> {
    parse_event_cursor(Some(cursor), epoch)
}

fn parse_log_cursor(cursor: Option<&LogCursor>) -> Result<(u64, u64), RuntimeError> {
    let Some(cursor) = cursor else {
        return Ok((0, 0));
    };
    let Some((stdout, stderr)) = cursor.as_str().split_once(':') else {
        return Err(RuntimeError::Stream {
            message: format!("process log cursor `{}` is invalid", cursor.as_str()),
        });
    };
    Ok((
        parse_sequence(stdout, "log", cursor.as_str())?,
        parse_sequence(stderr, "log", cursor.as_str())?,
    ))
}

fn parse_sequence(value: &str, kind: &str, cursor: &str) -> Result<u64, RuntimeError> {
    value.parse().map_err(|_| RuntimeError::Stream {
        message: format!("process {kind} cursor `{cursor}` is invalid"),
    })
}

fn log_cursor(stdout_offset: u64, stderr_offset: u64) -> LogCursor {
    LogCursor::new(format!("{stdout_offset}:{stderr_offset}"))
}

fn log_io_error(operation: &str, path: &Path, error: std::io::Error) -> RuntimeError {
    RuntimeError::Stream {
        message: format!(
            "process log {operation} failed for `{}`: {error}",
            path.display()
        ),
    }
}
