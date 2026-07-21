use std::sync::Arc;
use std::thread::JoinHandle;

use async_trait::async_trait;
use logs::{IngestLogEntry, LogAppendReport, LogStore, LogStoreError};
use tokio::sync::{mpsc, oneshot};

use crate::schema;
use crate::{DuckLogStoreError, DuckLogStoreSettings};

enum Command {
    Append {
        entries: Vec<IngestLogEntry>,
        response: oneshot::Sender<Result<LogAppendReport, LogStoreError>>,
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
    pub async fn open(settings: DuckLogStoreSettings) -> Result<Self, DuckLogStoreError> {
        let (commands, receiver) = mpsc::channel(settings.queue_capacity);
        let (initialized, initialization) = oneshot::channel();
        let path = settings.path.clone();
        let worker_path = path.clone();
        let worker = std::thread::Builder::new()
            .name("maestro-logstore".to_owned())
            .spawn(move || run_worker(&worker_path, receiver, initialized))
            .map_err(|source| DuckLogStoreError::Spawn { path, source })?;
        match initialization.await {
            Ok(Ok(())) => Ok(Self {
                store: Arc::new(DuckLogStore { commands }),
                worker: Some(worker),
            }),
            Ok(Err(message)) => {
                join(worker).await?;
                Err(DuckLogStoreError::Initialize {
                    path: settings.path,
                    message,
                })
            }
            Err(_) => {
                join(worker).await?;
                Err(DuckLogStoreError::WorkerStopped {
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
    pub async fn shutdown(mut self) -> Result<(), DuckLogStoreError> {
        let (shutdown, stopped) = oneshot::channel();
        let lifecycle = match self
            .store
            .commands
            .send(Command::Shutdown { response: shutdown })
            .await
        {
            Ok(()) => stopped.await.map_err(|_| DuckLogStoreError::WorkerStopped {
                action: "confirming shutdown",
            }),
            Err(_) => Err(DuckLogStoreError::WorkerStopped {
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
            Command::Shutdown { response } => {
                drop(connection);
                let _ignored = response.send(());
                return;
            }
        }
    }
}

async fn join(worker: JoinHandle<()>) -> Result<(), DuckLogStoreError> {
    tokio::task::spawn_blocking(move || worker.join())
        .await
        .map_err(|_| DuckLogStoreError::WorkerPanicked)?
        .map_err(|_| DuckLogStoreError::WorkerPanicked)
}
