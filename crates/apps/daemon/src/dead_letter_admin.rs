use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use logs::{
    DeadLetterStore, DeadLetterStoreError, LogSequence, LogSinkId, SinkDeadLetter,
    SinkDeadLetterMetadata,
};
use logstore::{DuckLogStoreRuntime, DuckStoreError, DuckStoreSettings};
use serde::Serialize;
use tokio::io::AsyncWriteExt;

use crate::{DaemonLaunchError, load_launch_document};

const ADMIN_QUEUE_CAPACITY: usize = 32;
const EXPORT_PAGE_SIZE: usize = 256;
const TEMP_FILE_ATTEMPTS: u8 = 16;

/// One explicit dead-letter administration operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeadLetterAdminCommand {
    /// Lists bounded metadata and aggregate storage use without exposing payloads.
    List { sink_id: LogSinkId, limit: usize },
    /// Exports every retained payload as ordered JSON Lines without overwriting a file.
    Export { sink_id: LogSinkId, output: PathBuf },
    /// Purges through an inclusive source sequence, or all records when absent.
    Purge {
        sink_id: LogSinkId,
        through: Option<LogSequence>,
    },
}

/// Successful operator-facing dead-letter command output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeadLetterAdminOutput {
    /// Pretty JSON list document intended for standard output.
    Listed(String),
    /// Completed no-clobber JSONL export.
    Exported {
        sink_id: LogSinkId,
        count: u64,
        output: PathBuf,
    },
    /// Completed explicit purge.
    Purged { sink_id: LogSinkId, count: u64 },
}

/// Opens the existing node-local log store, performs one operation, and closes it cleanly.
pub async fn administer_dead_letters(
    launch_config: &Path,
    command: DeadLetterAdminCommand,
) -> Result<DeadLetterAdminOutput, DeadLetterAdminError> {
    let config = load_launch_document(launch_config)?;
    let database = config.data_directory.join("agent").join("logs.duckdb");
    administer_database(&database, command).await
}

pub(crate) async fn administer_database(
    database: &Path,
    command: DeadLetterAdminCommand,
) -> Result<DeadLetterAdminOutput, DeadLetterAdminError> {
    if !tokio::fs::try_exists(database)
        .await
        .map_err(|source| io_error("inspect", database, source))?
    {
        return Err(DeadLetterAdminError::DatabaseMissing {
            path: database.to_path_buf(),
        });
    }
    let settings = DuckStoreSettings::new(database.to_path_buf(), ADMIN_QUEUE_CAPACITY)?;
    let runtime = DuckLogStoreRuntime::open(settings).await?;
    let store: Arc<dyn DeadLetterStore> = runtime.store();
    let operation = execute(store, command).await;
    let shutdown = runtime.shutdown().await;
    match (operation, shutdown) {
        (Ok(output), Ok(())) => Ok(output),
        (Err(error), Ok(())) => Err(error),
        (Ok(_), Err(error)) => Err(error.into()),
        (Err(operation), Err(shutdown)) => Err(DeadLetterAdminError::OperationAndShutdown {
            operation: operation.to_string(),
            shutdown: shutdown.to_string(),
        }),
    }
}

pub(crate) async fn execute(
    store: Arc<dyn DeadLetterStore>,
    command: DeadLetterAdminCommand,
) -> Result<DeadLetterAdminOutput, DeadLetterAdminError> {
    match command {
        DeadLetterAdminCommand::List { sink_id, limit } => {
            if limit == 0 {
                return Err(DeadLetterAdminError::InvalidLimit);
            }
            let stats = store.stats(&sink_id).await?;
            let entries = store
                .list(&sink_id, None, limit)
                .await?
                .iter()
                .map(SinkDeadLetter::metadata)
                .collect();
            let document = serde_json::to_string_pretty(&DeadLetterListDocument {
                sink_id,
                count: stats.count,
                payload_bytes: stats.payload_bytes,
                entries,
            })?;
            Ok(DeadLetterAdminOutput::Listed(document))
        }
        DeadLetterAdminCommand::Export { sink_id, output } => {
            let count = export(store.as_ref(), &sink_id, &output).await?;
            Ok(DeadLetterAdminOutput::Exported {
                sink_id,
                count,
                output,
            })
        }
        DeadLetterAdminCommand::Purge { sink_id, through } => {
            let count = store.purge(&sink_id, through).await?;
            Ok(DeadLetterAdminOutput::Purged { sink_id, count })
        }
    }
}

async fn export(
    store: &dyn DeadLetterStore,
    sink_id: &LogSinkId,
    output: &Path,
) -> Result<u64, DeadLetterAdminError> {
    if tokio::fs::try_exists(output)
        .await
        .map_err(|source| io_error("inspect export target", output, source))?
    {
        return Err(DeadLetterAdminError::OutputExists {
            path: output.to_path_buf(),
        });
    }
    let (temporary, mut file) = create_temporary_export(output).await?;
    let write_result = async {
        let count = write_export(store, sink_id, &temporary, &mut file).await?;
        file.flush()
            .await
            .map_err(|source| io_error("flush temporary export", &temporary, source))?;
        file.sync_all()
            .await
            .map_err(|source| io_error("sync temporary export", &temporary, source))?;
        Ok::<_, DeadLetterAdminError>(count)
    }
    .await;
    let count = match write_result {
        Ok(count) => count,
        Err(error) => {
            drop(file);
            let _ignored = tokio::fs::remove_file(&temporary).await;
            return Err(error);
        }
    };
    drop(file);
    if let Err(source) = tokio::fs::hard_link(&temporary, output).await {
        let _ignored = tokio::fs::remove_file(&temporary).await;
        if source.kind() == std::io::ErrorKind::AlreadyExists {
            return Err(DeadLetterAdminError::OutputExists {
                path: output.to_path_buf(),
            });
        }
        return Err(io_error("commit export", output, source));
    }
    tokio::fs::remove_file(&temporary)
        .await
        .map_err(|source| io_error("remove temporary export", &temporary, source))?;
    Ok(count)
}

async fn write_export(
    store: &dyn DeadLetterStore,
    sink_id: &LogSinkId,
    temporary: &Path,
    file: &mut tokio::fs::File,
) -> Result<u64, DeadLetterAdminError> {
    let mut after = None;
    let mut exported = 0_u64;
    loop {
        let records = store.list(sink_id, after, EXPORT_PAGE_SIZE).await?;
        if records.is_empty() {
            return Ok(exported);
        }
        let record_count = records.len();
        for record in &records {
            let encoded = serde_json::to_vec(&export_record(record))?;
            file.write_all(&encoded)
                .await
                .map_err(|source| io_error("write temporary export", temporary, source))?;
            file.write_all(b"\n")
                .await
                .map_err(|source| io_error("write temporary export", temporary, source))?;
            exported = exported.saturating_add(1);
        }
        let next = records
            .last()
            .map(|record| record.source_sequence)
            .ok_or(DeadLetterAdminError::InvalidPage)?;
        if after.is_some_and(|after| next <= after) {
            return Err(DeadLetterAdminError::InvalidPage);
        }
        after = Some(next);
        if record_count < EXPORT_PAGE_SIZE {
            return Ok(exported);
        }
    }
}

async fn create_temporary_export(
    output: &Path,
) -> Result<(PathBuf, tokio::fs::File), DeadLetterAdminError> {
    for attempt in 0..TEMP_FILE_ATTEMPTS {
        let temporary = temporary_path(output, attempt)?;
        let mut options = tokio::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            options.mode(0o600);
        }
        match options.open(&temporary).await {
            Ok(file) => return Ok((temporary, file)),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(source) => return Err(io_error("create temporary export", &temporary, source)),
        }
    }
    Err(DeadLetterAdminError::TemporaryFilesExhausted {
        path: output.to_path_buf(),
    })
}

fn temporary_path(output: &Path, attempt: u8) -> Result<PathBuf, DeadLetterAdminError> {
    let file_name = output
        .file_name()
        .ok_or_else(|| DeadLetterAdminError::InvalidOutput {
            path: output.to_path_buf(),
        })?;
    let mut temporary_name = OsString::from(".");
    temporary_name.push(file_name);
    temporary_name.push(format!(".{}.{attempt}.tmp", std::process::id()));
    Ok(output.with_file_name(temporary_name))
}

fn export_record(record: &SinkDeadLetter) -> DeadLetterExportRecord {
    let (payload_encoding, payload) = match serde_json::from_slice(&record.payload) {
        Ok(payload) => ("json", payload),
        Err(_) => (
            "bytes",
            serde_json::Value::Array(
                record
                    .payload
                    .iter()
                    .copied()
                    .map(serde_json::Value::from)
                    .collect(),
            ),
        ),
    };
    DeadLetterExportRecord {
        metadata: record.metadata(),
        payload_encoding,
        payload,
    }
}

fn io_error(action: &'static str, path: &Path, source: std::io::Error) -> DeadLetterAdminError {
    DeadLetterAdminError::Io {
        action,
        path: path.to_path_buf(),
        source,
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct DeadLetterListDocument {
    sink_id: LogSinkId,
    count: u64,
    payload_bytes: u64,
    entries: Vec<SinkDeadLetterMetadata>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct DeadLetterExportRecord {
    #[serde(flatten)]
    metadata: SinkDeadLetterMetadata,
    payload_encoding: &'static str,
    payload: serde_json::Value,
}

/// A protected dead-letter administration operation failed.
#[derive(Debug, thiserror::Error)]
pub enum DeadLetterAdminError {
    /// Protected launch configuration could not be loaded or validated.
    #[error(transparent)]
    Launch(#[from] DaemonLaunchError),
    /// The node-local log store does not exist and must not be created by an admin command.
    #[error("node-local log store does not exist at `{}`", path.display())]
    DatabaseMissing { path: PathBuf },
    /// The list limit must be positive.
    #[error("dead-letter list limit must be non-zero")]
    InvalidLimit,
    /// The export target already exists and was not overwritten.
    #[error("dead-letter export target already exists at `{}`", path.display())]
    OutputExists { path: PathBuf },
    /// The output does not name a file.
    #[error("dead-letter export target must name a file: `{}`", path.display())]
    InvalidOutput { path: PathBuf },
    /// Every bounded temporary filename was already occupied.
    #[error("could not reserve a temporary export beside `{}`", path.display())]
    TemporaryFilesExhausted { path: PathBuf },
    /// A store violated ordered cursor paging and export stopped to avoid a loop.
    #[error("dead-letter store returned an invalid export page")]
    InvalidPage,
    /// DuckDB lifecycle or configuration failed.
    #[error(transparent)]
    Store(#[from] DuckStoreError),
    /// Dead-letter storage rejected or could not complete the operation.
    #[error(transparent)]
    DeadLetters(#[from] DeadLetterStoreError),
    /// JSON list or export encoding failed.
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    /// Export filesystem access failed.
    #[error("failed to {action} `{}`: {source}", path.display())]
    Io {
        action: &'static str,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    /// Both the requested operation and owned store shutdown failed.
    #[error("dead-letter operation failed: {operation}; store shutdown also failed: {shutdown}")]
    OperationAndShutdown { operation: String, shutdown: String },
}
