use std::io::Read;
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use duckdb::{Connection, OptionalExt, params};
use kernel_api::Timestamp;
use logs::LogSinkId;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::LogArchiveError;

const HOUR_MILLIS: i64 = 60 * 60 * 1_000;

/// Result of one deterministic hourly hot-to-cold rollover pass.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LogRolloverReport {
    /// Hour partitions sealed by this pass.
    pub partitions: usize,
    /// Hot query rows copied, verified, and evicted.
    pub rows: usize,
    /// Compressed Parquet bytes written or resumed.
    pub bytes: u64,
    /// Archived delivery-spool rows no active sink still needs.
    pub delivery_rows_reclaimed: usize,
    /// DuckDB file bytes physically reclaimed after checkpointed archival.
    pub database_bytes_reclaimed: u64,
}

/// Durable manifest for every committed Parquet object in one hour partition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColdPartitionManifest {
    /// Manifest wire version.
    pub version: u8,
    /// Stable `YYYY-MM-DD/HH` partition identity.
    pub partition_key: String,
    /// Deterministic rollover cutoff that last updated this manifest.
    pub updated_at_ms: i64,
    /// Ordered committed Parquet objects.
    pub parts: Vec<ColdPartitionManifestPart>,
}

/// Integrity and sequence bounds for one immutable Parquet object.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColdPartitionManifestPart {
    /// Basename relative to its partition directory.
    pub file: String,
    /// Records stored in the object.
    pub row_count: u64,
    /// Lowest store-local sequence in the object.
    pub sequence_low: u64,
    /// Highest store-local sequence in the object.
    pub sequence_high: u64,
    /// Lowercase hexadecimal SHA-256 digest.
    pub sha256: String,
    /// Compressed object size.
    pub size_bytes: u64,
}

struct RolloverGroup {
    hour: i64,
    row_count: i64,
    sequence_low: i64,
    sequence_high: i64,
}

impl RolloverGroup {
    fn start_millis(&self) -> Result<i64, LogArchiveError> {
        self.hour
            .checked_mul(HOUR_MILLIS)
            .ok_or_else(|| rejected("hour partition start overflowed"))
    }

    fn end_millis(&self) -> Result<i64, LogArchiveError> {
        self.start_millis()?
            .checked_add(HOUR_MILLIS)
            .ok_or_else(|| rejected("hour partition end overflowed"))
    }

    fn partition_key(&self) -> Result<String, LogArchiveError> {
        let seconds = self
            .hour
            .checked_mul(60 * 60)
            .ok_or_else(|| rejected("hour partition timestamp overflowed"))?;
        let timestamp = DateTime::<Utc>::from_timestamp(seconds, 0)
            .ok_or_else(|| rejected("hour partition is outside the supported UTC range"))?;
        Ok(timestamp.format("%Y-%m-%d/%H").to_string())
    }

    fn directory(&self, cold_root: &Path) -> Result<PathBuf, LogArchiveError> {
        let key = self.partition_key()?;
        let (date, hour) = key
            .split_once('/')
            .ok_or_else(|| rejected("hour partition key is malformed"))?;
        Ok(cold_root
            .join("logs")
            .join(format!("date={date}"))
            .join(format!("hour={hour}")))
    }
}

pub(crate) fn prepare(cold_root: &Path) -> Result<(), LogArchiveError> {
    std::fs::create_dir_all(cold_root).map_err(unavailable("create cold-tier root"))?;
    remove_temporary_files(cold_root)
}

pub(crate) fn rollover_before(
    connection: &mut Connection,
    cold_root: &Path,
    before: Timestamp,
    sink_ids: &[LogSinkId],
) -> Result<LogRolloverReport, LogArchiveError> {
    let cutoff = before.0.div_euclid(HOUR_MILLIS) * HOUR_MILLIS;
    let groups = load_groups(connection, cutoff)?;
    let mut report = LogRolloverReport::default();
    for group in groups {
        let size = rollover_group(connection, cold_root, cutoff, &group)?;
        report.partitions = report.partitions.saturating_add(1);
        report.rows = report.rows.saturating_add(
            usize::try_from(group.row_count)
                .map_err(|_| rejected("partition row count is invalid"))?,
        );
        report.bytes = report.bytes.saturating_add(size);
    }
    report.delivery_rows_reclaimed = prune_delivered_rows(connection, sink_ids)?;
    if report.rows > 0 || report.delivery_rows_reclaimed > 0 {
        connection
            .execute_batch("CHECKPOINT")
            .map_err(unavailable("checkpoint log tiers after rollover"))?;
    }
    Ok(report)
}

fn prune_delivered_rows(
    connection: &Connection,
    sink_ids: &[LogSinkId],
) -> Result<usize, LogArchiveError> {
    let mut delivery_floor = i64::MAX;
    let mut cursor = connection
        .prepare("SELECT last_sequence FROM sink_cursors WHERE sink_id = ?1")
        .map_err(unavailable("prepare delivery cursor read for rollover"))?;
    for sink_id in sink_ids {
        let sequence = cursor
            .query_row(params![sink_id.as_str()], |row| row.get::<_, i64>(0))
            .optional()
            .map_err(unavailable("read delivery cursor for rollover"))?
            .unwrap_or(0);
        delivery_floor = delivery_floor.min(sequence);
    }
    connection
        .execute(
            "DELETE FROM normalized_logs
             WHERE sequence <= ?1
               AND NOT EXISTS (
                   SELECT 1 FROM query_logs
                   WHERE query_logs.sequence = normalized_logs.sequence
               )",
            params![delivery_floor],
        )
        .map_err(unavailable("reclaim archived delivery rows"))
}

fn load_groups(
    connection: &Connection,
    cutoff: i64,
) -> Result<Vec<RolloverGroup>, LogArchiveError> {
    let mut statement = connection
        .prepare(
            "SELECT CAST(FLOOR(event_at_ms / 3600000.0) AS BIGINT) AS hour,
                    COUNT(*), MIN(sequence), MAX(sequence)
             FROM query_logs WHERE event_at_ms < ?1
             GROUP BY hour ORDER BY hour",
        )
        .map_err(unavailable("prepare rollover groups"))?;
    let rows = statement
        .query_map(params![cutoff], |row| {
            Ok(RolloverGroup {
                hour: row.get(0)?,
                row_count: row.get(1)?,
                sequence_low: row.get(2)?,
                sequence_high: row.get(3)?,
            })
        })
        .map_err(unavailable("load rollover groups"))?;
    rows.map(|row| row.map_err(unavailable("decode rollover group")))
        .collect()
}

fn rollover_group(
    connection: &mut Connection,
    cold_root: &Path,
    updated_at_ms: i64,
    group: &RolloverGroup,
) -> Result<u64, LogArchiveError> {
    let partition_key = group.partition_key()?;
    let directory = group.directory(cold_root)?;
    std::fs::create_dir_all(&directory).map_err(unavailable("create hour partition"))?;
    let file_name = format!(
        "part-{}-{}.parquet",
        group.sequence_low, group.sequence_high
    );
    let final_path = directory.join(&file_name);
    let temporary_path = directory.join(format!("{file_name}.tmp-{}", std::process::id()));
    if !final_path.exists() {
        if temporary_path.exists() {
            std::fs::remove_file(&temporary_path)
                .map_err(unavailable("remove stale rollover object"))?;
        }
        export_group(connection, group, &temporary_path)?;
        verify_parquet(connection, &temporary_path, group)?;
        sync_file(&temporary_path)?;
    }
    let source = if final_path.exists() {
        &final_path
    } else {
        &temporary_path
    };
    let sha256 = sha256_file(source)?;
    let size_bytes = source
        .metadata()
        .map_err(unavailable("read rollover object metadata"))?
        .len();
    mark_staging(
        connection,
        &partition_key,
        group,
        &sha256,
        size_bytes,
        updated_at_ms,
    )?;
    if !final_path.exists() {
        std::fs::rename(&temporary_path, &final_path)
            .map_err(unavailable("publish rollover object"))?;
        sync_directory(&directory)?;
    }
    verify_parquet(connection, &final_path, group)?;
    write_manifest(connection, &directory, &partition_key, updated_at_ms)?;
    commit_export(connection, &partition_key, group, updated_at_ms)?;
    Ok(size_bytes)
}

fn export_group(
    connection: &Connection,
    group: &RolloverGroup,
    destination: &Path,
) -> Result<(), LogArchiveError> {
    let destination = destination
        .to_str()
        .ok_or_else(|| rejected("cold-tier path is not UTF-8"))?;
    let sql = format!(
        "COPY (
             SELECT query.sequence, query.event_at_ms, normalized.entry_json
             FROM query_logs AS query
             INNER JOIN normalized_logs AS normalized
               ON normalized.sequence = query.sequence
             WHERE query.event_at_ms >= {} AND query.event_at_ms < {}
               AND query.sequence BETWEEN {} AND {}
             ORDER BY query.sequence
         ) TO {} (FORMAT PARQUET, COMPRESSION ZSTD)",
        group.start_millis()?,
        group.end_millis()?,
        group.sequence_low,
        group.sequence_high,
        sql_string(destination)
    );
    connection
        .execute_batch(&sql)
        .map_err(unavailable("export hour partition to Parquet"))
}

fn verify_parquet(
    connection: &Connection,
    path: &Path,
    group: &RolloverGroup,
) -> Result<(), LogArchiveError> {
    let path = path
        .to_str()
        .ok_or_else(|| rejected("Parquet path is not UTF-8"))?;
    let actual = connection
        .query_row(
            "SELECT COUNT(*), MIN(sequence), MAX(sequence) FROM read_parquet(?1)",
            params![path],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            },
        )
        .map_err(unavailable("verify Parquet sequence bounds"))?;
    let expected = (group.row_count, group.sequence_low, group.sequence_high);
    if actual != expected {
        return Err(rejected(format!(
            "Parquet bounds {actual:?} do not match {expected:?}"
        )));
    }
    Ok(())
}

fn mark_staging(
    connection: &Connection,
    partition_key: &str,
    group: &RolloverGroup,
    sha256: &str,
    size_bytes: u64,
    updated_at_ms: i64,
) -> Result<(), LogArchiveError> {
    connection
        .execute(
            "INSERT INTO log_partitions
             VALUES (?1, 'staging', ?2, ?3, ?4, ?5, ?6, ?7)
             ON CONFLICT (partition_key, sequence_low) DO UPDATE SET
                 state = 'staging', row_count = excluded.row_count,
                 sequence_high = excluded.sequence_high, sha256 = excluded.sha256,
                 size_bytes = excluded.size_bytes, updated_at_ms = excluded.updated_at_ms",
            params![
                partition_key,
                group.row_count,
                group.sequence_low,
                group.sequence_high,
                sha256,
                i64::try_from(size_bytes).map_err(|_| rejected("Parquet object is too large"))?,
                updated_at_ms
            ],
        )
        .map_err(unavailable("record staged hour partition"))?;
    Ok(())
}

fn commit_export(
    connection: &mut Connection,
    partition_key: &str,
    group: &RolloverGroup,
    updated_at_ms: i64,
) -> Result<(), LogArchiveError> {
    let transaction = connection
        .transaction()
        .map_err(unavailable("begin rollover commit"))?;
    transaction
        .execute(
            "UPDATE log_partitions SET state = 'exported', updated_at_ms = ?1
             WHERE partition_key = ?2 AND sequence_low = ?3 AND state = 'staging'",
            params![updated_at_ms, partition_key, group.sequence_low],
        )
        .map_err(unavailable("commit partition state"))?;
    let deleted = transaction
        .execute(
            "DELETE FROM query_logs
             WHERE event_at_ms >= ?1 AND event_at_ms < ?2
               AND sequence BETWEEN ?3 AND ?4",
            params![
                group.start_millis()?,
                group.end_millis()?,
                group.sequence_low,
                group.sequence_high
            ],
        )
        .map_err(unavailable("evict archived hot query rows"))?;
    if deleted != usize::try_from(group.row_count).unwrap_or(usize::MAX) {
        return Err(rejected("hot query rows changed during rollover"));
    }
    transaction
        .commit()
        .map_err(unavailable("commit hour partition rollover"))
}

fn write_manifest(
    connection: &Connection,
    directory: &Path,
    partition_key: &str,
    updated_at_ms: i64,
) -> Result<(), LogArchiveError> {
    let mut paths = std::fs::read_dir(directory)
        .map_err(unavailable("read hour partition"))?
        .map(|entry| entry.map(|entry| entry.path()))
        .collect::<Result<Vec<_>, _>>()
        .map_err(unavailable("read hour partition entry"))?;
    paths.retain(|path| {
        path.extension()
            .is_some_and(|extension| extension == "parquet")
    });
    paths.sort();
    let mut parts = Vec::with_capacity(paths.len());
    for path in paths {
        let path_text = path
            .to_str()
            .ok_or_else(|| rejected("Parquet path is not UTF-8"))?;
        let (row_count, sequence_low, sequence_high) = connection
            .query_row(
                "SELECT COUNT(*), MIN(sequence), MAX(sequence) FROM read_parquet(?1)",
                params![path_text],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, i64>(2)?,
                    ))
                },
            )
            .map_err(unavailable("inspect manifest Parquet object"))?;
        parts.push(ColdPartitionManifestPart {
            file: path
                .file_name()
                .and_then(|name| name.to_str())
                .ok_or_else(|| rejected("Parquet object has no UTF-8 filename"))?
                .to_owned(),
            row_count: u64::try_from(row_count)
                .map_err(|_| rejected("Parquet row count is invalid"))?,
            sequence_low: u64::try_from(sequence_low)
                .map_err(|_| rejected("Parquet sequence is invalid"))?,
            sequence_high: u64::try_from(sequence_high)
                .map_err(|_| rejected("Parquet sequence is invalid"))?,
            sha256: sha256_file(&path)?,
            size_bytes: path
                .metadata()
                .map_err(unavailable("read Parquet object metadata"))?
                .len(),
        });
    }
    let manifest = ColdPartitionManifest {
        version: 1,
        partition_key: partition_key.to_owned(),
        updated_at_ms,
        parts,
    };
    let temporary = directory.join(format!("manifest.json.tmp-{}", std::process::id()));
    std::fs::write(
        &temporary,
        serde_json::to_vec_pretty(&manifest)
            .map_err(|error| unavailable_message(format!("encode partition manifest: {error}")))?,
    )
    .map_err(unavailable("write partition manifest"))?;
    sync_file(&temporary)?;
    std::fs::rename(&temporary, directory.join("manifest.json"))
        .map_err(unavailable("publish partition manifest"))?;
    sync_directory(directory)
}

fn sha256_file(path: &Path) -> Result<String, LogArchiveError> {
    let mut file = std::fs::File::open(path).map_err(unavailable("open Parquet object"))?;
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 1024 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(unavailable("hash Parquet object"))?;
        if read == 0 {
            break;
        }
        let bytes = buffer
            .get(..read)
            .ok_or_else(|| rejected("Parquet hash read exceeded its buffer"))?;
        digest.update(bytes);
    }
    Ok(format!("{:x}", digest.finalize()))
}

fn remove_temporary_files(root: &Path) -> Result<(), LogArchiveError> {
    for entry in std::fs::read_dir(root).map_err(unavailable("inspect cold-tier directory"))? {
        let entry = entry.map_err(unavailable("inspect cold-tier entry"))?;
        let path = entry.path();
        let file_type = entry
            .file_type()
            .map_err(unavailable("inspect cold-tier entry type"))?;
        if file_type.is_dir() {
            remove_temporary_files(&path)?;
        } else if path
            .file_name()
            .is_some_and(|name| name.to_string_lossy().contains(".tmp-"))
        {
            std::fs::remove_file(path).map_err(unavailable("remove temporary cold-tier file"))?;
        }
    }
    Ok(())
}

fn sync_file(path: &Path) -> Result<(), LogArchiveError> {
    std::fs::File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(unavailable("sync cold-tier file"))
}

fn sync_directory(path: &Path) -> Result<(), LogArchiveError> {
    std::fs::File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(unavailable("sync cold-tier directory"))
}

fn sql_string(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn rejected(message: impl Into<String>) -> LogArchiveError {
    LogArchiveError::Rejected {
        message: message.into(),
    }
}

fn unavailable<Error: std::fmt::Display>(
    action: &'static str,
) -> impl FnOnce(Error) -> LogArchiveError {
    move |error| unavailable_message(format!("failed to {action}: {error}"))
}

fn unavailable_message(message: impl Into<String>) -> LogArchiveError {
    LogArchiveError::Unavailable {
        message: message.into(),
    }
}
