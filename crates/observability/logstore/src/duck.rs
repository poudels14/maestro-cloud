use std::sync::Arc;
use std::thread::JoinHandle;

use async_trait::async_trait;
use logs::{
    DeadLetterStore, DeadLetterStoreError, IngestLogEntry, LogAppendReport, LogDeliveryStore,
    LogDeliveryStoreError, LogSequence, LogSinkId, LogStore, LogStoreError, LogStoreRuntime,
    LogStoreRuntimeError, SequencedLogEntry, SinkDeadLetter, SinkDeadLetterStats,
};
use tokio::sync::{mpsc, oneshot};

use crate::{DuckStoreError, DuckStoreSettings};
use crate::{delivery_schema, schema};

enum Command {
    Append {
        entries: Vec<IngestLogEntry>,
        response: oneshot::Sender<Result<LogAppendReport, LogStoreError>>,
    },
    ReadAfter {
        cursor: Option<LogSequence>,
        limit: usize,
        response: oneshot::Sender<Result<Vec<SequencedLogEntry>, LogDeliveryStoreError>>,
    },
    LoadCursor {
        sink_id: LogSinkId,
        response: oneshot::Sender<Result<Option<LogSequence>, LogDeliveryStoreError>>,
    },
    CommitCursor {
        sink_id: LogSinkId,
        sequence: LogSequence,
        response: oneshot::Sender<Result<(), LogDeliveryStoreError>>,
    },
    RecordDeadLetter {
        dead_letter: SinkDeadLetter,
        response: oneshot::Sender<Result<(), DeadLetterStoreError>>,
    },
    ListDeadLetters {
        sink_id: LogSinkId,
        after: Option<LogSequence>,
        limit: usize,
        response: oneshot::Sender<Result<Vec<SinkDeadLetter>, DeadLetterStoreError>>,
    },
    DeadLetterStats {
        sink_id: LogSinkId,
        response: oneshot::Sender<Result<SinkDeadLetterStats, DeadLetterStoreError>>,
    },
    PurgeDeadLetters {
        sink_id: LogSinkId,
        through: Option<LogSequence>,
        response: oneshot::Sender<Result<u64, DeadLetterStoreError>>,
    },
    Shutdown {
        response: oneshot::Sender<()>,
    },
}

/// Async append handle applying bounded backpressure to one DuckDB owner thread.
pub struct DuckLogStore {
    commands: mpsc::Sender<Command>,
}

/// Explicit lifetime owner for the blocking DuckDB writer thread.
pub struct DuckLogStoreRuntime {
    store: Arc<DuckLogStore>,
    worker: Option<JoinHandle<()>>,
}

impl DuckLogStoreRuntime {
    /// Opens and migrates the database on its dedicated thread before returning.
    pub async fn open(settings: DuckStoreSettings) -> Result<Self, DuckStoreError> {
        let (commands, receiver) = mpsc::channel(settings.queue_capacity);
        let (initialized, initialization) = oneshot::channel();
        let path = settings.path.clone();
        let worker_path = path.clone();
        let worker = std::thread::Builder::new()
            .name("maestro-logstore".to_owned())
            .spawn(move || run_worker(&worker_path, receiver, initialized))
            .map_err(|source| DuckStoreError::Spawn { path, source })?;
        match initialization.await {
            Ok(Ok(())) => Ok(Self {
                store: Arc::new(DuckLogStore { commands }),
                worker: Some(worker),
            }),
            Ok(Err(message)) => {
                join(worker).await?;
                Err(DuckStoreError::Initialize {
                    path: settings.path,
                    message,
                })
            }
            Err(_) => {
                join(worker).await?;
                Err(DuckStoreError::WorkerStopped {
                    action: "reporting initialization",
                })
            }
        }
    }

    /// Returns the append contract shared by runtime and OTLP ingestion.
    pub fn store(&self) -> Arc<DuckLogStore> {
        self.store.clone()
    }

    /// Drains accepted appends, closes DuckDB, and joins the owner thread.
    pub async fn shutdown(mut self) -> Result<(), DuckStoreError> {
        let (shutdown, stopped) = oneshot::channel();
        let lifecycle = match self
            .store
            .commands
            .send(Command::Shutdown { response: shutdown })
            .await
        {
            Ok(()) => stopped.await.map_err(|_| DuckStoreError::WorkerStopped {
                action: "confirming shutdown",
            }),
            Err(_) => Err(DuckStoreError::WorkerStopped {
                action: "requesting shutdown",
            }),
        };
        if let Some(worker) = self.worker.take() {
            join(worker).await?;
        }
        lifecycle
    }
}

impl Drop for DuckLogStoreRuntime {
    fn drop(&mut self) {
        if self.worker.is_some() {
            let (response, _stopped) = oneshot::channel();
            let _ignored = self.store.commands.try_send(Command::Shutdown { response });
        }
    }
}

#[async_trait]
impl LogStore for DuckLogStore {
    async fn append(&self, entries: &[IngestLogEntry]) -> Result<LogAppendReport, LogStoreError> {
        let (response, result) = oneshot::channel();
        self.commands
            .send(Command::Append {
                entries: entries.to_vec(),
                response,
            })
            .await
            .map_err(|_| LogStoreError::Unavailable {
                message: "DuckDB writer stopped before accepting append".to_owned(),
            })?;
        result.await.map_err(|_| LogStoreError::Unavailable {
            message: "DuckDB writer stopped before completing append".to_owned(),
        })?
    }
}

#[async_trait]
impl LogDeliveryStore for DuckLogStore {
    async fn read_after(
        &self,
        cursor: Option<LogSequence>,
        limit: usize,
    ) -> Result<Vec<SequencedLogEntry>, LogDeliveryStoreError> {
        let (response, result) = oneshot::channel();
        self.commands
            .send(Command::ReadAfter {
                cursor,
                limit,
                response,
            })
            .await
            .map_err(|_| delivery_worker_stopped("accepting delivery read"))?;
        result
            .await
            .map_err(|_| delivery_worker_stopped("completing delivery read"))?
    }

    async fn load_sink_cursor(
        &self,
        sink_id: &LogSinkId,
    ) -> Result<Option<LogSequence>, LogDeliveryStoreError> {
        let (response, result) = oneshot::channel();
        self.commands
            .send(Command::LoadCursor {
                sink_id: sink_id.clone(),
                response,
            })
            .await
            .map_err(|_| delivery_worker_stopped("accepting cursor read"))?;
        result
            .await
            .map_err(|_| delivery_worker_stopped("completing cursor read"))?
    }

    async fn commit_sink_cursor(
        &self,
        sink_id: &LogSinkId,
        sequence: LogSequence,
    ) -> Result<(), LogDeliveryStoreError> {
        let (response, result) = oneshot::channel();
        self.commands
            .send(Command::CommitCursor {
                sink_id: sink_id.clone(),
                sequence,
                response,
            })
            .await
            .map_err(|_| delivery_worker_stopped("accepting cursor commit"))?;
        result
            .await
            .map_err(|_| delivery_worker_stopped("completing cursor commit"))?
    }
}

#[async_trait]
impl DeadLetterStore for DuckLogStore {
    async fn record(&self, dead_letter: &SinkDeadLetter) -> Result<(), DeadLetterStoreError> {
        let (response, result) = oneshot::channel();
        self.commands
            .send(Command::RecordDeadLetter {
                dead_letter: dead_letter.clone(),
                response,
            })
            .await
            .map_err(|_| dead_worker_stopped("accepting dead letter"))?;
        result
            .await
            .map_err(|_| dead_worker_stopped("recording dead letter"))?
    }

    async fn list(
        &self,
        sink_id: &LogSinkId,
        after: Option<LogSequence>,
        limit: usize,
    ) -> Result<Vec<SinkDeadLetter>, DeadLetterStoreError> {
        let (response, result) = oneshot::channel();
        self.commands
            .send(Command::ListDeadLetters {
                sink_id: sink_id.clone(),
                after,
                limit,
                response,
            })
            .await
            .map_err(|_| dead_worker_stopped("accepting dead-letter list"))?;
        result
            .await
            .map_err(|_| dead_worker_stopped("listing dead letters"))?
    }

    async fn stats(
        &self,
        sink_id: &LogSinkId,
    ) -> Result<SinkDeadLetterStats, DeadLetterStoreError> {
        let (response, result) = oneshot::channel();
        self.commands
            .send(Command::DeadLetterStats {
                sink_id: sink_id.clone(),
                response,
            })
            .await
            .map_err(|_| dead_worker_stopped("accepting dead-letter stats read"))?;
        result
            .await
            .map_err(|_| dead_worker_stopped("reading dead-letter stats"))?
    }

    async fn purge(
        &self,
        sink_id: &LogSinkId,
        through: Option<LogSequence>,
    ) -> Result<u64, DeadLetterStoreError> {
        let (response, result) = oneshot::channel();
        self.commands
            .send(Command::PurgeDeadLetters {
                sink_id: sink_id.clone(),
                through,
                response,
            })
            .await
            .map_err(|_| dead_worker_stopped("accepting dead-letter purge"))?;
        result
            .await
            .map_err(|_| dead_worker_stopped("purging dead letters"))?
    }
}

#[async_trait]
impl LogStoreRuntime for DuckLogStoreRuntime {
    fn store(&self) -> Arc<dyn LogStore> {
        self.store.clone()
    }

    fn delivery_store(&self) -> Arc<dyn LogDeliveryStore> {
        self.store.clone()
    }

    fn dead_letter_store(&self) -> Arc<dyn DeadLetterStore> {
        self.store.clone()
    }

    async fn shutdown(self: Box<Self>) -> Result<(), LogStoreRuntimeError> {
        DuckLogStoreRuntime::shutdown(*self)
            .await
            .map_err(|error| LogStoreRuntimeError {
                message: error.to_string(),
            })
    }
}

fn run_worker(
    path: &std::path::Path,
    mut commands: mpsc::Receiver<Command>,
    initialized: oneshot::Sender<Result<(), String>>,
) {
    let mut connection = match schema::open(path) {
        Ok(connection) => {
            if initialized.send(Ok(())).is_err() {
                return;
            }
            connection
        }
        Err(error) => {
            let _ignored = initialized.send(Err(error));
            return;
        }
    };
    while let Some(command) = commands.blocking_recv() {
        match command {
            Command::Append { entries, response } => {
                let _ignored = response.send(schema::append(&mut connection, &entries));
            }
            Command::ReadAfter {
                cursor,
                limit,
                response,
            } => {
                let _ignored =
                    response.send(delivery_schema::read_after(&connection, cursor, limit));
            }
            Command::LoadCursor { sink_id, response } => {
                let _ignored = response.send(delivery_schema::load_cursor(&connection, &sink_id));
            }
            Command::CommitCursor {
                sink_id,
                sequence,
                response,
            } => {
                let _ignored = response.send(delivery_schema::commit_cursor(
                    &mut connection,
                    &sink_id,
                    sequence,
                ));
            }
            Command::RecordDeadLetter {
                dead_letter,
                response,
            } => {
                let _ignored = response.send(delivery_schema::record_dead_letter(
                    &mut connection,
                    &dead_letter,
                ));
            }
            Command::ListDeadLetters {
                sink_id,
                after,
                limit,
                response,
            } => {
                let _ignored = response.send(delivery_schema::list_dead_letters(
                    &connection,
                    &sink_id,
                    after,
                    limit,
                ));
            }
            Command::DeadLetterStats { sink_id, response } => {
                let _ignored =
                    response.send(delivery_schema::dead_letter_stats(&connection, &sink_id));
            }
            Command::PurgeDeadLetters {
                sink_id,
                through,
                response,
            } => {
                let _ignored = response.send(delivery_schema::purge_dead_letters(
                    &connection,
                    &sink_id,
                    through,
                ));
            }
            Command::Shutdown { response } => {
                drop(connection);
                let _ignored = response.send(());
                return;
            }
        }
    }
}

fn delivery_worker_stopped(action: &'static str) -> LogDeliveryStoreError {
    LogDeliveryStoreError::Unavailable {
        message: format!("DuckDB worker stopped before {action}"),
    }
}

fn dead_worker_stopped(action: &'static str) -> DeadLetterStoreError {
    DeadLetterStoreError::Unavailable {
        message: format!("DuckDB worker stopped before {action}"),
    }
}

async fn join(worker: JoinHandle<()>) -> Result<(), DuckStoreError> {
    tokio::task::spawn_blocking(move || worker.join())
        .await
        .map_err(|_| DuckStoreError::WorkerPanicked)?
        .map_err(|_| DuckStoreError::WorkerPanicked)
}
