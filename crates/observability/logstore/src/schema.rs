use std::path::Path;

use duckdb::{Connection, OptionalExt, params};
use logs::{IngestLogEntry, LogAppendReport, LogProducer, LogStoreError};

const CURRENT_SCHEMA_VERSION: i64 = 3;

pub(crate) fn open(path: &Path) -> Result<Connection, String> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|error| format!("create database parent directory: {error}"))?;
    }
    let mut connection = Connection::open(path).map_err(|error| error.to_string())?;
    connection
        .execute_batch("CREATE TABLE IF NOT EXISTS schema_version (version BIGINT NOT NULL);")
        .map_err(|error| error.to_string())?;
    let (version_count, version) = connection
        .query_row(
            "SELECT COUNT(*), COALESCE(MAX(version), 0) FROM schema_version",
            [],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
        )
        .map_err(|error| error.to_string())?;
    match (version_count, version) {
        (0, _) => initialize_v3(&mut connection)?,
        (1, CURRENT_SCHEMA_VERSION) => {}
        (1, 1) => {
            migrate_v1_to_v2(&mut connection)?;
            migrate_v2_to_v3(&mut connection)?;
        }
        (1, 2) => migrate_v2_to_v3(&mut connection)?,
        (1, version) => {
            return Err(format!(
                "database schema version {version} is not supported by version {CURRENT_SCHEMA_VERSION}"
            ));
        }
        (count, _) => return Err(format!("schema version table contains {count} rows")),
    }
    Ok(connection)
}

pub(crate) fn append(
    connection: &mut Connection,
    entries: &[IngestLogEntry],
) -> Result<LogAppendReport, LogStoreError> {
    let transaction = connection
        .transaction()
        .map_err(unavailable("begin append transaction"))?;
    let mut last_sequence = transaction
        .query_row(
            "SELECT COALESCE(MAX(sequence), 0) FROM normalized_logs",
            [],
            |row| row.get::<_, i64>(0),
        )
        .map_err(unavailable("read log sequence high watermark"))?;
    let mut report = LogAppendReport::default();
    for entry in entries {
        let encoded = serde_json::to_string(entry).map_err(|error| LogStoreError::Rejected {
            message: format!("normalized entry could not be encoded: {error}"),
        })?;
        let (producer_type, producer_id) = producer_key(&entry.id.producer);
        let existing = transaction
            .query_row(
                "SELECT entry_json FROM normalized_logs
                 WHERE node_id = ?1 AND producer_type = ?2 AND producer_id = ?3 AND cursor = ?4",
                params![
                    entry.id.node_id.as_str(),
                    producer_type,
                    producer_id,
                    entry.id.cursor.as_str()
                ],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(unavailable("read replay identity"))?;
        match existing {
            Some(existing) if existing == encoded => {
                report.deduplicated = report.deduplicated.saturating_add(1);
            }
            Some(_) => {
                return Err(LogStoreError::Rejected {
                    message: format!(
                        "record identity `{producer_type}/{producer_id}/{}` was reused with different content",
                        entry.id.cursor.as_str()
                    ),
                });
            }
            None => {
                let sequence =
                    last_sequence
                        .checked_add(1)
                        .ok_or_else(|| LogStoreError::Rejected {
                            message: "normalized log sequence space is exhausted".to_owned(),
                        })?;
                transaction
                    .execute(
                        "INSERT INTO normalized_logs
                         (sequence, node_id, producer_type, producer_id, cursor, event_at_ms, entry_json)
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                        params![
                            sequence,
                            entry.id.node_id.as_str(),
                            producer_type,
                            producer_id,
                            entry.id.cursor.as_str(),
                            entry.event_at.0,
                            encoded.as_str()
                        ],
                    )
                    .map_err(unavailable("insert normalized log"))?;
                transaction
                    .execute(
                        "INSERT INTO query_logs (sequence, event_at_ms, entry_json)
                         VALUES (?1, ?2, ?3)",
                        params![sequence, entry.event_at.0, encoded.as_str()],
                    )
                    .map_err(unavailable("insert hot query log"))?;
                report.committed = report.committed.saturating_add(1);
                last_sequence = sequence;
            }
        }
    }
    transaction
        .commit()
        .map_err(unavailable("commit append transaction"))?;
    Ok(report)
}

fn initialize_v3(connection: &mut Connection) -> Result<(), String> {
    let transaction = connection
        .transaction()
        .map_err(|error| error.to_string())?;
    transaction
        .execute_batch(
            "CREATE TABLE normalized_logs (
                 sequence BIGINT PRIMARY KEY,
                 node_id VARCHAR NOT NULL,
                 producer_type VARCHAR NOT NULL,
                 producer_id VARCHAR NOT NULL,
                 cursor VARCHAR NOT NULL,
                 event_at_ms BIGINT NOT NULL,
                 entry_json VARCHAR NOT NULL,
                 UNIQUE (node_id, producer_type, producer_id, cursor)
             );
             CREATE TABLE sink_cursors (
                 sink_id VARCHAR PRIMARY KEY,
                 last_sequence BIGINT NOT NULL
             );
             CREATE TABLE sink_dead_letters (
                 sink_id VARCHAR NOT NULL,
                 source_sequence BIGINT NOT NULL,
                 status_code INTEGER,
                 reason VARCHAR NOT NULL,
                 payload BLOB NOT NULL,
                 recorded_at_ms BIGINT NOT NULL,
                 PRIMARY KEY (sink_id, source_sequence)
             );
             CREATE TABLE query_logs (
                 sequence BIGINT PRIMARY KEY,
                 event_at_ms BIGINT NOT NULL,
                 entry_json VARCHAR NOT NULL
             );
             CREATE INDEX query_logs_event_sequence
                 ON query_logs(event_at_ms, sequence);
             CREATE TABLE log_partitions (
                 partition_key VARCHAR NOT NULL,
                 state VARCHAR NOT NULL,
                 row_count BIGINT NOT NULL,
                 sequence_low BIGINT NOT NULL,
                 sequence_high BIGINT NOT NULL,
                 sha256 VARCHAR NOT NULL,
                 size_bytes BIGINT NOT NULL,
                 updated_at_ms BIGINT NOT NULL,
                 PRIMARY KEY (partition_key, sequence_low)
             );
             INSERT INTO schema_version (version) VALUES (3);",
        )
        .map_err(|error| error.to_string())?;
    transaction.commit().map_err(|error| error.to_string())
}

fn migrate_v1_to_v2(connection: &mut Connection) -> Result<(), String> {
    let transaction = connection
        .transaction()
        .map_err(|error| error.to_string())?;
    transaction
        .execute_batch(
            "CREATE TABLE normalized_logs_v2 (
                 sequence BIGINT PRIMARY KEY,
                 node_id VARCHAR NOT NULL,
                 producer_type VARCHAR NOT NULL,
                 producer_id VARCHAR NOT NULL,
                 cursor VARCHAR NOT NULL,
                 event_at_ms BIGINT NOT NULL,
                 entry_json VARCHAR NOT NULL,
                 UNIQUE (node_id, producer_type, producer_id, cursor)
             );
             INSERT INTO normalized_logs_v2
             SELECT ROW_NUMBER() OVER (
                        ORDER BY event_at_ms, node_id, producer_type, producer_id, cursor
                    ),
                    node_id, producer_type, producer_id, cursor, event_at_ms, entry_json
             FROM normalized_logs;
             DROP TABLE normalized_logs;
             ALTER TABLE normalized_logs_v2 RENAME TO normalized_logs;
             CREATE TABLE sink_cursors (
                 sink_id VARCHAR PRIMARY KEY,
                 last_sequence BIGINT NOT NULL
             );
             CREATE TABLE sink_dead_letters (
                 sink_id VARCHAR NOT NULL,
                 source_sequence BIGINT NOT NULL,
                 status_code INTEGER,
                 reason VARCHAR NOT NULL,
                 payload BLOB NOT NULL,
                 recorded_at_ms BIGINT NOT NULL,
                 PRIMARY KEY (sink_id, source_sequence)
             );
             UPDATE schema_version SET version = 2;",
        )
        .map_err(|error| error.to_string())?;
    transaction.commit().map_err(|error| error.to_string())
}

fn migrate_v2_to_v3(connection: &mut Connection) -> Result<(), String> {
    let transaction = connection
        .transaction()
        .map_err(|error| error.to_string())?;
    transaction
        .execute_batch(
            "CREATE TABLE query_logs (
                 sequence BIGINT PRIMARY KEY,
                 event_at_ms BIGINT NOT NULL,
                 entry_json VARCHAR NOT NULL
             );
             INSERT INTO query_logs
             SELECT sequence, event_at_ms, entry_json FROM normalized_logs;
             CREATE INDEX query_logs_event_sequence
                 ON query_logs(event_at_ms, sequence);
             CREATE TABLE log_partitions (
                 partition_key VARCHAR NOT NULL,
                 state VARCHAR NOT NULL,
                 row_count BIGINT NOT NULL,
                 sequence_low BIGINT NOT NULL,
                 sequence_high BIGINT NOT NULL,
                 sha256 VARCHAR NOT NULL,
                 size_bytes BIGINT NOT NULL,
                 updated_at_ms BIGINT NOT NULL,
                 PRIMARY KEY (partition_key, sequence_low)
             );
             UPDATE schema_version SET version = 3;",
        )
        .map_err(|error| error.to_string())?;
    transaction.commit().map_err(|error| error.to_string())
}

fn producer_key(producer: &LogProducer) -> (&'static str, &str) {
    match producer {
        LogProducer::Workload(id) => ("workload", id.as_str()),
        LogProducer::System(component) => ("system", component),
        LogProducer::Build(id) => ("build", id.as_str()),
    }
}

fn unavailable(action: &'static str) -> impl FnOnce(duckdb::Error) -> LogStoreError {
    move |error| LogStoreError::Unavailable {
        message: format!("failed to {action}: {error}"),
    }
}
