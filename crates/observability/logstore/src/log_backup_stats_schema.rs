use duckdb::{Connection, OptionalExt, params};
use logs::BackupStatsSnapshot;

use crate::LogBackupError;

pub(crate) fn load(connection: &Connection) -> Result<Option<BackupStatsSnapshot>, LogBackupError> {
    let value = connection
        .query_row(
            "SELECT value_json FROM backup_stats WHERE singleton = TRUE",
            [],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(unavailable("load backup stats"))?;
    value
        .map(|value| {
            serde_json::from_str(&value)
                .map_err(|error| rejected(format!("persisted backup stats are invalid: {error}")))
        })
        .transpose()
}

pub(crate) fn save(
    connection: &Connection,
    stats: &BackupStatsSnapshot,
    updated_at_ms: i64,
) -> Result<(), LogBackupError> {
    let value = serde_json::to_string(stats)
        .map_err(|error| rejected(format!("backup stats cannot be encoded: {error}")))?;
    connection
        .execute(
            "INSERT INTO backup_stats (singleton, value_json, updated_at_ms)
             VALUES (TRUE, ?1, ?2)
             ON CONFLICT (singleton) DO UPDATE SET
                 value_json = excluded.value_json,
                 updated_at_ms = excluded.updated_at_ms",
            params![value, updated_at_ms],
        )
        .map_err(unavailable("save backup stats"))?;
    Ok(())
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
