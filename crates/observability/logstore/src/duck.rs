use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread::JoinHandle;

use async_trait::async_trait;
use kernel_api::Timestamp;
use logs::{
    DeadLetterStore, DeadLetterStoreError, IngestLogEntry, LogAppendReport, LogDeliveryStore,
    LogDeliveryStoreError, LogHistogramBucket, LogHistogramQuery, LogQueryStoreError, LogReadQuery,
    LogSequence, LogSinkId, LogSpoolStats, LogStatsStore, LogStatsStoreError, LogStore,
    LogStoreError, LogStoreRuntime, LogStoreRuntimeError, SequencedLogEntry, SinkDeadLetter,
    SinkDeadLetterStats,
};
use tokio::sync::{mpsc, oneshot};

use crate::duck_worker::{
    archive_worker_stopped, backup_worker_stopped, dead_worker_stopped, delivery_worker_stopped,
    retention_worker_stopped, run_worker, stats_worker_stopped,
};
use crate::log_backup_schema::PendingLogBackupPartition;
use crate::{
    DuckStoreError, DuckStoreSettings, LogArchiveError, LogBackupError, LogRetentionError,
    LogRetentionReport, LogRolloverReport,
};

pub(crate) enum Command {
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
    StatsSnapshot {
        sink_ids: Vec<LogSinkId>,
        response: oneshot::Sender<Result<LogSpoolStats, LogStatsStoreError>>,
    },
    Rollover {
        before: Timestamp,
        response: oneshot::Sender<Result<LogRolloverReport, LogArchiveError>>,
    },
    PendingBackups {
        response: oneshot::Sender<Result<Vec<PendingLogBackupPartition>, LogBackupError>>,
    },
    MarkBackedUp {
        partition: PendingLogBackupPartition,
        updated_at: Timestamp,
        response: oneshot::Sender<Result<(), LogBackupError>>,
    },
    LoadBackupStats {
        response: oneshot::Sender<Result<Option<logs::BackupStatsSnapshot>, LogBackupError>>,
    },
    SaveBackupStats {
        stats: logs::BackupStatsSnapshot,
        updated_at: Timestamp,
        response: oneshot::Sender<Result<(), LogBackupError>>,
    },
    PruneBackedUp {
        cutoff: chrono::NaiveDate,
        response: oneshot::Sender<Result<LogRetentionReport, LogRetentionError>>,
    },
    QueryLogs {
        query: LogReadQuery,
        response: oneshot::Sender<Result<Vec<SequencedLogEntry>, LogQueryStoreError>>,
    },
    QueryHistogram {
        query: LogHistogramQuery,
        response: oneshot::Sender<Result<Vec<LogHistogramBucket>, LogQueryStoreError>>,
    },
    Shutdown {
        response: oneshot::Sender<()>,
    },
}

/// Async append handle applying bounded backpressure to one DuckDB owner thread.
pub struct DuckLogStore {
    pub(crate) commands: mpsc::Sender<Command>,
    cold_root: PathBuf,
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
        let cold_root = path.with_extension("parts");
        let worker_path = path.clone();
        let worker_cold_root = cold_root.clone();
        let worker = std::thread::Builder::new()
            .name("maestro-logstore".to_owned())
            .spawn(move || {
                run_worker(&worker_path, &worker_cold_root, receiver, initialized);
            })
            .map_err(|source| DuckStoreError::Spawn { path, source })?;
        match initialization.await {
            Ok(Ok(())) => Ok(Self {
                store: Arc::new(DuckLogStore {
                    commands,
                    cold_root,
                }),
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

impl DuckLogStore {
    /// Returns the node-local root containing hive-partitioned Parquet objects.
    pub fn cold_root(&self) -> &Path {
        &self.cold_root
    }

    /// Seals every complete UTC hour before `before` into verified ZSTD Parquet.
    pub async fn rollover_before(
        &self,
        before: Timestamp,
    ) -> Result<LogRolloverReport, LogArchiveError> {
        let (response, result) = oneshot::channel();
        self.commands
            .send(Command::Rollover { before, response })
            .await
            .map_err(|_| archive_worker_stopped("accepting rollover"))?;
        result
            .await
            .map_err(|_| archive_worker_stopped("completing rollover"))?
    }

    pub(crate) async fn pending_backup_partitions(
        &self,
    ) -> Result<Vec<PendingLogBackupPartition>, LogBackupError> {
        let (response, result) = oneshot::channel();
        self.commands
            .send(Command::PendingBackups { response })
            .await
            .map_err(|_| backup_worker_stopped("accepting pending-partition read"))?;
        result
            .await
            .map_err(|_| backup_worker_stopped("completing pending-partition read"))?
    }

    pub(crate) async fn mark_partition_backed_up(
        &self,
        partition: &PendingLogBackupPartition,
        updated_at: Timestamp,
    ) -> Result<(), LogBackupError> {
        let (response, result) = oneshot::channel();
        self.commands
            .send(Command::MarkBackedUp {
                partition: partition.clone(),
                updated_at,
                response,
            })
            .await
            .map_err(|_| backup_worker_stopped("accepting backup commit"))?;
        result
            .await
            .map_err(|_| backup_worker_stopped("completing backup commit"))?
    }

    /// Loads the most recently persisted backup health snapshot, if one exists.
    pub async fn load_backup_stats(
        &self,
    ) -> Result<Option<logs::BackupStatsSnapshot>, LogBackupError> {
        let (response, result) = oneshot::channel();
        self.commands
            .send(Command::LoadBackupStats { response })
            .await
            .map_err(|_| backup_worker_stopped("accepting backup stats read"))?;
        result
            .await
            .map_err(|_| backup_worker_stopped("completing backup stats read"))?
    }

    /// Atomically replaces the singleton backup health snapshot.
    pub async fn save_backup_stats(
        &self,
        stats: &logs::BackupStatsSnapshot,
        updated_at: Timestamp,
    ) -> Result<(), LogBackupError> {
        let (response, result) = oneshot::channel();
        self.commands
            .send(Command::SaveBackupStats {
                stats: stats.clone(),
                updated_at,
                response,
            })
            .await
            .map_err(|_| backup_worker_stopped("accepting backup stats write"))?;
        result
            .await
            .map_err(|_| backup_worker_stopped("completing backup stats write"))?
    }

    /// Removes cold partitions strictly before `cutoff` only after every object is backed up.
    pub async fn prune_backed_up_before(
        &self,
        cutoff: chrono::NaiveDate,
    ) -> Result<LogRetentionReport, LogRetentionError> {
        let (response, result) = oneshot::channel();
        self.commands
            .send(Command::PruneBackedUp { cutoff, response })
            .await
            .map_err(|_| retention_worker_stopped("accepting retention prune"))?;
        result
            .await
            .map_err(|_| retention_worker_stopped("completing retention prune"))?
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
impl LogStatsStore for DuckLogStore {
    async fn stats_snapshot(
        &self,
        sink_ids: &[LogSinkId],
    ) -> Result<LogSpoolStats, LogStatsStoreError> {
        let (response, result) = oneshot::channel();
        self.commands
            .send(Command::StatsSnapshot {
                sink_ids: sink_ids.to_vec(),
                response,
            })
            .await
            .map_err(|_| stats_worker_stopped("accepting stats snapshot"))?;
        result
            .await
            .map_err(|_| stats_worker_stopped("completing stats snapshot"))?
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

    fn stats_store(&self) -> Arc<dyn LogStatsStore> {
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

async fn join(worker: JoinHandle<()>) -> Result<(), DuckStoreError> {
    tokio::task::spawn_blocking(move || worker.join())
        .await
        .map_err(|_| DuckStoreError::WorkerPanicked)?
        .map_err(|_| DuckStoreError::WorkerPanicked)
}
