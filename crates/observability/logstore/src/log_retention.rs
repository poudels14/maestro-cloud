use std::path::{Path, PathBuf};

use chrono::NaiveDate;
use duckdb::{Connection, params};

use crate::LogRetentionError;

/// Filesystem and metadata reclaimed by one explicit retention pass.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LogRetentionReport {
    /// Complete UTC-hour partition directories removed.
    pub partitions: u64,
    /// Committed Parquet objects removed with those partitions.
    pub objects: u64,
    /// Compressed Parquet bytes removed with those partitions.
    pub bytes: u64,
}

struct RetentionCandidate {
    partition_key: String,
    objects: i64,
    bytes: i64,
}

pub(crate) fn prune_backed_up_before(
    connection: &mut Connection,
    cold_root: &Path,
    cutoff: NaiveDate,
) -> Result<LogRetentionReport, LogRetentionError> {
    let candidates = load_candidates(connection)?;
    let mut report = LogRetentionReport::default();
    for candidate in candidates {
        let (date, directory) = partition_directory(cold_root, &candidate.partition_key)?;
        if date >= cutoff {
            continue;
        }
        if directory.exists() {
            validate_real_directory(cold_root)?;
            validate_real_directory(&cold_root.join("logs"))?;
            let parent = directory
                .parent()
                .ok_or_else(|| rejected("retention partition has no parent"))?;
            validate_real_directory(parent)?;
            validate_real_directory(&directory)?;
            std::fs::remove_dir_all(&directory)
                .map_err(unavailable("remove backed-up partition"))?;
            sync_directory(parent)?;
        }
        let transaction = connection
            .transaction()
            .map_err(unavailable("begin retention metadata commit"))?;
        let deleted = transaction
            .execute(
                "DELETE FROM log_partitions
                 WHERE partition_key = ?1 AND state = 'backed_up'",
                params![candidate.partition_key],
            )
            .map_err(unavailable("remove backed-up partition metadata"))?;
        if deleted != usize::try_from(candidate.objects).unwrap_or(usize::MAX) {
            return Err(rejected(
                "partition state changed during retention metadata commit",
            ));
        }
        transaction
            .commit()
            .map_err(unavailable("commit retention metadata"))?;
        report.partitions = report.partitions.saturating_add(1);
        report.objects = report.objects.saturating_add(
            u64::try_from(candidate.objects)
                .map_err(|_| rejected("retention object count is invalid"))?,
        );
        report.bytes = report.bytes.saturating_add(
            u64::try_from(candidate.bytes)
                .map_err(|_| rejected("retention byte count is invalid"))?,
        );
    }
    Ok(report)
}

fn load_candidates(connection: &Connection) -> Result<Vec<RetentionCandidate>, LogRetentionError> {
    let mut statement = connection
        .prepare(
            "SELECT partition_key, COUNT(*), SUM(size_bytes)
             FROM log_partitions
             GROUP BY partition_key
             HAVING COUNT(*) = SUM(CASE WHEN state = 'backed_up' THEN 1 ELSE 0 END)
             ORDER BY partition_key",
        )
        .map_err(unavailable("prepare retention candidates"))?;
    let rows = statement
        .query_map([], |row| {
            Ok(RetentionCandidate {
                partition_key: row.get(0)?,
                objects: row.get(1)?,
                bytes: row.get(2)?,
            })
        })
        .map_err(unavailable("load retention candidates"))?;
    rows.map(|row| row.map_err(unavailable("decode retention candidate")))
        .collect()
}

fn partition_directory(
    cold_root: &Path,
    key: &str,
) -> Result<(NaiveDate, PathBuf), LogRetentionError> {
    let (date, hour) = key
        .split_once('/')
        .ok_or_else(|| rejected("stored retention partition key is malformed"))?;
    if key.matches('/').count() != 1
        || date.len() != 10
        || hour.len() != 2
        || !hour.bytes().all(|byte| byte.is_ascii_digit())
        || hour.parse::<u8>().ok().is_none_or(|hour| hour > 23)
    {
        return Err(rejected("stored retention partition key is malformed"));
    }
    let date = NaiveDate::parse_from_str(date, "%Y-%m-%d")
        .map_err(|_| rejected("stored retention partition date is malformed"))?;
    Ok((
        date,
        cold_root
            .join("logs")
            .join(format!("date={date}"))
            .join(format!("hour={hour}")),
    ))
}

fn validate_real_directory(path: &Path) -> Result<(), LogRetentionError> {
    let metadata =
        std::fs::symlink_metadata(path).map_err(unavailable("inspect retention directory"))?;
    if !metadata.file_type().is_dir() || metadata.file_type().is_symlink() {
        return Err(rejected("retention path is not a real directory"));
    }
    Ok(())
}

fn sync_directory(path: &Path) -> Result<(), LogRetentionError> {
    std::fs::File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(unavailable("sync retained partition parent"))
}

fn rejected(message: impl Into<String>) -> LogRetentionError {
    LogRetentionError::Rejected {
        message: message.into(),
    }
}

fn unavailable<Error: std::fmt::Display>(
    action: &'static str,
) -> impl FnOnce(Error) -> LogRetentionError {
    move |error| LogRetentionError::Unavailable {
        message: format!("failed to {action}: {error}"),
    }
}
