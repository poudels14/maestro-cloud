use std::collections::VecDeque;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

use crate::{LogCursor, LogFrame, LogMode, LogSource, LogStream, RuntimeClock, RuntimeError};

const LOG_CHUNK_BYTES: u64 = 64 * 1_024;

pub(crate) struct FileLogStream {
    stdout_path: PathBuf,
    stderr_path: PathBuf,
    stdout_offset: u64,
    stderr_offset: u64,
    mode: LogMode,
    clock: Arc<dyn RuntimeClock>,
    poll_interval: Duration,
    buffered: VecDeque<LogFrame>,
}

impl FileLogStream {
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
            message: format!("file log reader task failed: {error}"),
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
impl LogStream for FileLogStream {
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

fn parse_log_cursor(cursor: Option<&LogCursor>) -> Result<(u64, u64), RuntimeError> {
    let Some(cursor) = cursor else {
        return Ok((0, 0));
    };
    let Some((stdout, stderr)) = cursor.as_str().split_once(':') else {
        return Err(RuntimeError::Stream {
            message: format!("file log cursor `{}` is invalid", cursor.as_str()),
        });
    };
    Ok((
        parse_sequence(stdout, cursor.as_str())?,
        parse_sequence(stderr, cursor.as_str())?,
    ))
}

fn parse_sequence(value: &str, cursor: &str) -> Result<u64, RuntimeError> {
    value.parse().map_err(|_| RuntimeError::Stream {
        message: format!("file log cursor `{cursor}` is invalid"),
    })
}

fn log_cursor(stdout_offset: u64, stderr_offset: u64) -> LogCursor {
    LogCursor::new(format!("{stdout_offset}:{stderr_offset}"))
}

fn log_io_error(operation: &str, path: &Path, error: std::io::Error) -> RuntimeError {
    RuntimeError::Stream {
        message: format!(
            "file log {operation} failed for `{}`: {error}",
            path.display()
        ),
    }
}
