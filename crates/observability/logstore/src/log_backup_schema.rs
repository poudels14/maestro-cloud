use std::path::{Path, PathBuf};

use duckdb::{Connection, params};

use crate::LogBackupError;

#[derive(Debug, Clone)]
pub(crate) struct PendingLogBackupPartition {
    pub(crate) partition_key: String,
    pub(crate) files: Vec<PathBuf>,
    pub(crate) exported_sequence_lows: Vec<i64>,
}

pub(crate) fn pending_partitions(
    connection: &Connection,
    cold_root: &Path,
) -> Result<Vec<PendingLogBackupPartition>, LogBackupError> {
    let mut statement = connection
        .prepare(
            "SELECT partition_key, sequence_low, sequence_high, state FROM log_partitions
             WHERE state IN ('exported', 'backed_up') ORDER BY partition_key, sequence_low",
        )
        .map_err(unavailable("prepare pending backup partitions"))?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, String>(3)?,
            ))
        })
        .map_err(unavailable("read pending backup partitions"))?;
    let mut grouped = Vec::<(String, Vec<(i64, i64, String)>)>::new();
    for row in rows {
        let (key, sequence_low, sequence_high, state) =
            row.map_err(unavailable("decode pending backup partition"))?;
        if let Some((_, parts)) = grouped.last_mut().filter(|(existing, _)| existing == &key) {
            parts.push((sequence_low, sequence_high, state));
        } else {
            grouped.push((key, vec![(sequence_low, sequence_high, state)]));
        }
    }
    grouped
        .into_iter()
        .filter(|(_, parts)| parts.iter().any(|(_, _, state)| state == "exported"))
        .map(|(partition_key, parts)| {
            let directory = partition_directory(cold_root, &partition_key)?;
            let exported_sequence_lows = parts
                .iter()
                .filter(|(_, _, state)| state == "exported")
                .map(|(low, _, _)| *low)
                .collect();
            let mut files = parts
                .iter()
                .map(|(low, high, _)| directory.join(format!("part-{low}-{high}.parquet")))
                .collect::<Vec<_>>();
            files.push(directory.join("manifest.json"));
            if let Some(missing) = files.iter().find(|path| !path.is_file()) {
                return Err(rejected(format!(
                    "committed backup object is missing: {}",
                    missing.display()
                )));
            }
            Ok(PendingLogBackupPartition {
                partition_key,
                files,
                exported_sequence_lows,
            })
        })
        .collect()
}

pub(crate) fn mark_backed_up(
    connection: &mut Connection,
    partition: &PendingLogBackupPartition,
    updated_at_ms: i64,
) -> Result<(), LogBackupError> {
    let transaction = connection
        .transaction()
        .map_err(unavailable("begin backup commit"))?;
    for sequence_low in &partition.exported_sequence_lows {
        let updated = transaction
            .execute(
                "UPDATE log_partitions SET state = 'backed_up', updated_at_ms = ?1
                 WHERE partition_key = ?2 AND sequence_low = ?3 AND state = 'exported'",
                params![updated_at_ms, partition.partition_key, sequence_low],
            )
            .map_err(unavailable("mark partition object backed up"))?;
        if updated != 1 {
            return Err(rejected("pending partition changed before backup commit"));
        }
    }
    transaction
        .commit()
        .map_err(unavailable("commit backed-up partition state"))
}

fn partition_directory(cold_root: &Path, key: &str) -> Result<PathBuf, LogBackupError> {
    let (date, hour) = key
        .split_once('/')
        .ok_or_else(|| rejected("stored partition key is malformed"))?;
    if date.is_empty() || hour.len() != 2 || key.matches('/').count() != 1 {
        return Err(rejected("stored partition key is malformed"));
    }
    Ok(cold_root
        .join("logs")
        .join(format!("date={date}"))
        .join(format!("hour={hour}")))
}

fn rejected(message: impl Into<String>) -> LogBackupError {
    LogBackupError::Rejected {
        message: message.into(),
    }
}

fn unavailable<Error: std::fmt::Display>(
    action: &'static str,
) -> impl FnOnce(Error) -> LogBackupError {
    move |error| LogBackupError::Unavailable {
        message: format!("failed to {action}: {error}"),
    }
}
