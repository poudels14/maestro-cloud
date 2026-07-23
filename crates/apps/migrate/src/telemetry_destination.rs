use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Component, Path, PathBuf};

use duckdb::{AccessMode, Config, Connection};
use serde::Serialize;
use tempfile::NamedTempFile;

use crate::telemetry_apply::{
    CompletionMarker, LegacyTelemetryDestination, LegacyTelemetryMigrationError,
    LegacyTelemetryStreamVerification, LegacyTelemetryVerification, StreamAccumulator,
};

const MARKER_MAXIMUM_BYTES: u64 = 1024 * 1024;
const INTENT_FILE: &str = ".legacy-telemetry-intent.json";
const COMPLETION_FILE: &str = ".legacy-telemetry-complete.json";

#[derive(Clone)]
pub(crate) struct DestinationPaths {
    agent: PathBuf,
    pub(crate) logs: PathBuf,
    pub(crate) metrics: PathBuf,
    intent: PathBuf,
    completion: PathBuf,
}

impl DestinationPaths {
    fn new(root: &Path) -> Self {
        let agent = root.join("agent");
        Self {
            logs: agent.join("logs.duckdb"),
            metrics: agent.join("metrics.duckdb"),
            intent: agent.join(INTENT_FILE),
            completion: agent.join(COMPLETION_FILE),
            agent,
        }
    }
}

pub(crate) async fn prepare_destination(
    root: &Path,
    destination: &LegacyTelemetryDestination,
) -> Result<DestinationPaths, LegacyTelemetryMigrationError> {
    let root = root.to_path_buf();
    let destination = destination.clone();
    tokio::task::spawn_blocking(move || prepare_destination_sync(&root, &destination))
        .await
        .map_err(|error| LegacyTelemetryMigrationError::Worker(error.to_string()))?
}

fn prepare_destination_sync(
    root: &Path,
    destination: &LegacyTelemetryDestination,
) -> Result<DestinationPaths, LegacyTelemetryMigrationError> {
    validate_destination_root(root, true)?;
    let paths = DestinationPaths::new(root);
    if paths.intent.exists() {
        validate_agent(&paths)?;
        let actual: LegacyTelemetryDestination = read_private_json(&paths.intent)?;
        if actual != *destination {
            return Err(LegacyTelemetryMigrationError::IntentMismatch);
        }
    } else {
        if std::fs::symlink_metadata(&paths.agent).is_ok() {
            return Err(LegacyTelemetryMigrationError::UnownedDestination { path: paths.agent });
        }
        std::fs::create_dir_all(&paths.agent).map_err(io("create", &paths.agent))?;
        protect_directory(&paths.agent)?;
        write_new_private_json(&paths.intent, destination)?;
    }
    Ok(paths)
}

pub(crate) async fn existing_destination(
    root: &Path,
    destination: &LegacyTelemetryDestination,
) -> Result<DestinationPaths, LegacyTelemetryMigrationError> {
    let root = root.to_path_buf();
    let destination = destination.clone();
    tokio::task::spawn_blocking(move || {
        validate_destination_root(&root, false)?;
        let paths = DestinationPaths::new(&root);
        validate_agent(&paths)?;
        let actual: LegacyTelemetryDestination = read_private_json(&paths.intent)?;
        if actual != destination {
            return Err(LegacyTelemetryMigrationError::IntentMismatch);
        }
        Ok(paths)
    })
    .await
    .map_err(|error| LegacyTelemetryMigrationError::Worker(error.to_string()))?
}

fn validate_destination_root(
    root: &Path,
    create: bool,
) -> Result<(), LegacyTelemetryMigrationError> {
    if !root.is_absolute()
        || root
            .components()
            .any(|component| matches!(component, Component::ParentDir))
    {
        return Err(LegacyTelemetryMigrationError::InvalidDestination {
            path: root.to_path_buf(),
        });
    }
    if create {
        std::fs::create_dir_all(root).map_err(io("create", root))?;
    }
    let canonical = std::fs::canonicalize(root).map_err(io("canonicalize", root))?;
    if canonical != root {
        return Err(LegacyTelemetryMigrationError::NonCanonicalDestination {
            path: root.to_path_buf(),
            canonical,
        });
    }
    Ok(())
}

fn validate_agent(paths: &DestinationPaths) -> Result<(), LegacyTelemetryMigrationError> {
    let metadata = std::fs::symlink_metadata(&paths.agent).map_err(io("inspect", &paths.agent))?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(LegacyTelemetryMigrationError::UnsafeDestination {
            path: paths.agent.clone(),
        });
    }
    validate_private(&metadata, &paths.agent)?;
    let canonical =
        std::fs::canonicalize(&paths.agent).map_err(io("canonicalize", &paths.agent))?;
    if canonical != paths.agent {
        return Err(LegacyTelemetryMigrationError::UnsafeDestination {
            path: paths.agent.clone(),
        });
    }
    for path in [&paths.logs, &paths.metrics] {
        match std::fs::symlink_metadata(path) {
            Ok(metadata) if metadata.is_file() && !metadata.file_type().is_symlink() => {}
            Ok(_) => {
                return Err(LegacyTelemetryMigrationError::UnsafeDestination {
                    path: path.clone(),
                });
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(io("inspect", path)(error)),
        }
    }
    Ok(())
}

#[cfg(unix)]
fn protect_directory(path: &Path) -> Result<(), LegacyTelemetryMigrationError> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))
        .map_err(io("protect", path))
}

#[cfg(not(unix))]
fn protect_directory(_path: &Path) -> Result<(), LegacyTelemetryMigrationError> {
    Ok(())
}

pub(crate) async fn read_completion(
    paths: &DestinationPaths,
) -> Result<Option<CompletionMarker>, LegacyTelemetryMigrationError> {
    let path = paths.completion.clone();
    tokio::task::spawn_blocking(move || match read_private_json(&path) {
        Ok(marker) => Ok(Some(marker)),
        Err(LegacyTelemetryMigrationError::Io { source, .. })
            if source.kind() == std::io::ErrorKind::NotFound =>
        {
            Ok(None)
        }
        Err(error) => Err(error),
    })
    .await
    .map_err(|error| LegacyTelemetryMigrationError::Worker(error.to_string()))?
}

pub(crate) async fn write_completion(
    paths: &DestinationPaths,
    completion: &CompletionMarker,
) -> Result<(), LegacyTelemetryMigrationError> {
    let path = paths.completion.clone();
    let completion = completion.clone();
    tokio::task::spawn_blocking(move || write_new_private_json(&path, &completion))
        .await
        .map_err(|error| LegacyTelemetryMigrationError::Worker(error.to_string()))?
}

fn read_private_json<Value: serde::de::DeserializeOwned>(
    path: &Path,
) -> Result<Value, LegacyTelemetryMigrationError> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    }
    let file = options.open(path).map_err(io("open", path))?;
    let metadata = file.metadata().map_err(io("inspect", path))?;
    if !metadata.is_file() || metadata.len() > MARKER_MAXIMUM_BYTES {
        return Err(LegacyTelemetryMigrationError::InvalidMarker {
            path: path.to_path_buf(),
        });
    }
    validate_private(&metadata, path)?;
    let mut value = Vec::new();
    file.take(MARKER_MAXIMUM_BYTES.saturating_add(1))
        .read_to_end(&mut value)
        .map_err(io("read", path))?;
    let length =
        u64::try_from(value.len()).map_err(|_| LegacyTelemetryMigrationError::InvalidMarker {
            path: path.to_path_buf(),
        })?;
    if length > MARKER_MAXIMUM_BYTES {
        return Err(LegacyTelemetryMigrationError::InvalidMarker {
            path: path.to_path_buf(),
        });
    }
    serde_json::from_slice(&value).map_err(Into::into)
}

fn write_new_private_json<Value: Serialize>(
    path: &Path,
    value: &Value,
) -> Result<(), LegacyTelemetryMigrationError> {
    let parent =
        path.parent()
            .ok_or_else(|| LegacyTelemetryMigrationError::InvalidDestination {
                path: path.to_path_buf(),
            })?;
    let mut encoded = serde_json::to_vec_pretty(value)?;
    encoded.push(b'\n');
    let mut temporary = NamedTempFile::new_in(parent).map_err(io("create", path))?;
    protect_file(temporary.as_file(), path)?;
    temporary
        .write_all(&encoded)
        .and_then(|()| temporary.as_file_mut().sync_all())
        .map_err(io("write", path))?;
    temporary
        .persist_noclobber(path)
        .map_err(|error| io("install", path)(error.error))?;
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(io("sync parent of", path))
}

#[cfg(unix)]
fn validate_private(
    metadata: &std::fs::Metadata,
    path: &Path,
) -> Result<(), LegacyTelemetryMigrationError> {
    use std::os::unix::fs::PermissionsExt;
    if metadata.permissions().mode() & 0o077 == 0 {
        Ok(())
    } else {
        Err(LegacyTelemetryMigrationError::PublicMarker {
            path: path.to_path_buf(),
        })
    }
}

#[cfg(not(unix))]
fn validate_private(
    _metadata: &std::fs::Metadata,
    _path: &Path,
) -> Result<(), LegacyTelemetryMigrationError> {
    Ok(())
}

#[cfg(unix)]
fn protect_file(file: &File, path: &Path) -> Result<(), LegacyTelemetryMigrationError> {
    use std::os::unix::fs::PermissionsExt;
    file.set_permissions(std::fs::Permissions::from_mode(0o600))
        .map_err(io("protect", path))
}

#[cfg(not(unix))]
fn protect_file(_file: &File, _path: &Path) -> Result<(), LegacyTelemetryMigrationError> {
    Ok(())
}

pub(crate) async fn inspect_destination(
    paths: &DestinationPaths,
) -> Result<LegacyTelemetryVerification, LegacyTelemetryMigrationError> {
    let paths = paths.clone();
    tokio::task::spawn_blocking(move || inspect_destination_sync(&paths))
        .await
        .map_err(|error| LegacyTelemetryMigrationError::Worker(error.to_string()))?
}

fn inspect_destination_sync(
    paths: &DestinationPaths,
) -> Result<LegacyTelemetryVerification, LegacyTelemetryMigrationError> {
    let logs = open_read_only(&paths.logs)?;
    let metrics = open_read_only(&paths.metrics)?;
    ensure_empty(
        &logs,
        &[
            "SELECT COUNT(*) FROM sink_cursors",
            "SELECT COUNT(*) FROM sink_dead_letters",
            "SELECT COUNT(*) FROM log_partitions",
        ],
    )?;
    ensure_empty(
        &metrics,
        &[
            "SELECT COUNT(*) FROM metric_sink_cursors",
            "SELECT COUNT(*) FROM host_metric_sink_cursors",
        ],
    )?;
    let normalized_logs = digest_query(
        &logs,
        "SELECT entry_json FROM normalized_logs ORDER BY sequence",
    )?;
    let query_logs = digest_query(&logs, "SELECT entry_json FROM query_logs ORDER BY sequence")?;
    if normalized_logs != query_logs {
        return Err(LegacyTelemetryMigrationError::QueryTierMismatch);
    }
    let workload_metrics = digest_query(
        &metrics,
        "SELECT point_json FROM normalized_metrics ORDER BY sequence",
    )?;
    let host_metrics = digest_query(
        &metrics,
        "SELECT point_json FROM host_metrics ORDER BY delivery_sequence",
    )?;
    let operational_metrics = digest_stats(&logs)?;
    let backup_stats = digest_optional(
        &logs,
        "SELECT value_json FROM backup_stats WHERE singleton = TRUE",
    )?;
    reject_cold_objects(&paths.logs.with_extension("parts"))?;
    Ok(LegacyTelemetryVerification {
        logs: normalized_logs,
        workload_metrics,
        host_metrics,
        operational_metrics,
        backup_stats,
    })
}

fn open_read_only(path: &Path) -> Result<Connection, LegacyTelemetryMigrationError> {
    let config = Config::default()
        .enable_autoload_extension(false)
        .map_err(|error| LegacyTelemetryMigrationError::Database(error.to_string()))?
        .access_mode(AccessMode::ReadOnly)
        .map_err(|error| LegacyTelemetryMigrationError::Database(error.to_string()))?;
    Connection::open_with_flags(path, config)
        .map_err(|error| LegacyTelemetryMigrationError::Database(error.to_string()))
}

fn digest_query(
    connection: &Connection,
    sql: &str,
) -> Result<LegacyTelemetryStreamVerification, LegacyTelemetryMigrationError> {
    let mut statement = connection
        .prepare(sql)
        .map_err(|error| LegacyTelemetryMigrationError::Database(error.to_string()))?;
    let rows = statement
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(|error| LegacyTelemetryMigrationError::Database(error.to_string()))?;
    let mut digest = StreamAccumulator::default();
    for row in rows {
        digest.update_json(
            row.map_err(|error| LegacyTelemetryMigrationError::Database(error.to_string()))?
                .as_bytes(),
        )?;
    }
    Ok(digest.finish())
}

fn digest_stats(
    connection: &Connection,
) -> Result<LegacyTelemetryStreamVerification, LegacyTelemetryMigrationError> {
    let mut statement = connection
        .prepare(
            "SELECT ts, name, value, labels_json
             FROM stats_metrics ORDER BY ts, name, labels_json",
        )
        .map_err(|error| LegacyTelemetryMigrationError::Database(error.to_string()))?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, f64>(2)?,
                row.get::<_, String>(3)?,
            ))
        })
        .map_err(|error| LegacyTelemetryMigrationError::Database(error.to_string()))?;
    let mut digest = StreamAccumulator::default();
    for row in rows {
        let (ts, name, value, labels_json) =
            row.map_err(|error| LegacyTelemetryMigrationError::Database(error.to_string()))?;
        let labels = serde_json::from_str(&labels_json)?;
        digest.update_all(&[logs::StatsMetricPoint {
            ts,
            name,
            value,
            labels,
        }])?;
    }
    Ok(digest.finish())
}

fn digest_optional(
    connection: &Connection,
    sql: &str,
) -> Result<LegacyTelemetryStreamVerification, LegacyTelemetryMigrationError> {
    let mut digest = StreamAccumulator::default();
    match connection.query_row(sql, [], |row| row.get::<_, String>(0)) {
        Ok(value) => digest.update_json(value.as_bytes())?,
        Err(duckdb::Error::QueryReturnedNoRows) => {}
        Err(error) => return Err(LegacyTelemetryMigrationError::Database(error.to_string())),
    }
    Ok(digest.finish())
}

fn ensure_empty(
    connection: &Connection,
    queries: &[&str],
) -> Result<(), LegacyTelemetryMigrationError> {
    for query in queries {
        let count = connection
            .query_row(query, [], |row| row.get::<_, i64>(0))
            .map_err(|error| LegacyTelemetryMigrationError::Database(error.to_string()))?;
        if count != 0 {
            return Err(LegacyTelemetryMigrationError::UnexpectedDestinationState);
        }
    }
    Ok(())
}

fn reject_cold_objects(root: &Path) -> Result<(), LegacyTelemetryMigrationError> {
    if !root.exists() {
        return Ok(());
    }
    for entry in std::fs::read_dir(root).map_err(io("read", root))? {
        let entry = entry.map_err(io("read", root))?;
        let path = entry.path();
        let file_type = entry.file_type().map_err(io("inspect", &path))?;
        if file_type.is_symlink() || (!file_type.is_dir() && !file_type.is_file()) {
            return Err(LegacyTelemetryMigrationError::UnsafeDestination { path });
        }
        if file_type.is_dir() {
            reject_cold_objects(&path)?;
        } else if path
            .extension()
            .is_some_and(|extension| extension == "parquet")
        {
            return Err(LegacyTelemetryMigrationError::UnexpectedDestinationState);
        }
    }
    Ok(())
}

fn io<'a>(
    action: &'static str,
    path: &'a Path,
) -> impl FnOnce(std::io::Error) -> LegacyTelemetryMigrationError + 'a {
    move |source| LegacyTelemetryMigrationError::Io {
        action,
        path: path.to_path_buf(),
        source,
    }
}
