use std::path::Path;

use duckdb::{Config, Connection, OptionalExt, params};
use logs::{IngestLogEntry, LogAppendReport, LogProducer, LogStoreError};

const CURRENT_SCHEMA_VERSION: i64 = 7;

pub(crate) fn open(path: &Path) -> Result<Connection, String> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|error| format!("create database parent directory: {error}"))?;
    }
    let config = Config::default()
        .enable_autoload_extension(false)
        .map_err(|error| error.to_string())?;
    let mut connection =
        Connection::open_with_flags(path, config).map_err(|error| error.to_string())?;
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
        (0, _) => initialize_v7(&mut connection)?,
        (1, CURRENT_SCHEMA_VERSION) => {}
        (1, 1) => {
            migrate_v1_to_v2(&mut connection)?;
            migrate_v2_to_v3(&mut connection)?;
            migrate_v3_to_v4(&mut connection)?;
            crate::stats_metric_schema::migrate_v4_to_v5(&mut connection)?;
            crate::otlp_envelope_schema::migrate_v5_to_v6(&mut connection)?;
            migrate_v6_to_v7(&mut connection)?;
        }
        (1, 2) => {
            migrate_v2_to_v3(&mut connection)?;
            migrate_v3_to_v4(&mut connection)?;
            crate::stats_metric_schema::migrate_v4_to_v5(&mut connection)?;
            crate::otlp_envelope_schema::migrate_v5_to_v6(&mut connection)?;
            migrate_v6_to_v7(&mut connection)?;
        }
        (1, 3) => {
            migrate_v3_to_v4(&mut connection)?;
            crate::stats_metric_schema::migrate_v4_to_v5(&mut connection)?;
            crate::otlp_envelope_schema::migrate_v5_to_v6(&mut connection)?;
            migrate_v6_to_v7(&mut connection)?;
        }
        (1, 4) => {
            crate::stats_metric_schema::migrate_v4_to_v5(&mut connection)?;
            crate::otlp_envelope_schema::migrate_v5_to_v6(&mut connection)?;
            migrate_v6_to_v7(&mut connection)?;
        }
        (1, 5) => {
            crate::otlp_envelope_schema::migrate_v5_to_v6(&mut connection)?;
            migrate_v6_to_v7(&mut connection)?;
        }
        (1, 6) => migrate_v6_to_v7(&mut connection)?,
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
            "SELECT last_sequence FROM log_sequence WHERE singleton = TRUE",
            [],
            |row| row.get::<_, i64>(0),
        )
        .map_err(unavailable("read log sequence high watermark"))?;
    let mut report = LogAppendReport::default();
    {
        let mut read_existing = transaction
            .prepare(
                "SELECT entry_json FROM normalized_logs
                 WHERE node_id = ?1 AND producer_type = ?2 AND producer_id = ?3 AND cursor = ?4",
            )
            .map_err(unavailable("prepare replay identity read"))?;
        let mut insert_normalized = transaction
            .prepare(
                        "INSERT INTO normalized_logs
                         (sequence, node_id, producer_type, producer_id, cursor, event_at_ms, entry_json)
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            )
            .map_err(unavailable("prepare normalized log insert"))?;
        let mut insert_query = transaction
            .prepare(
                "INSERT INTO query_logs (sequence, event_at_ms)
                         VALUES (?1, ?2)",
            )
            .map_err(unavailable("prepare hot query log insert"))?;
        for entry in entries {
            let encoded =
                serde_json::to_string(entry).map_err(|error| LogStoreError::Rejected {
                    message: format!("normalized entry could not be encoded: {error}"),
                })?;
            let (producer_type, producer_id) = producer_key(&entry.id.producer);
            let existing = read_existing
                .query_row(
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
                    insert_normalized
                        .execute(params![
                            sequence,
                            entry.id.node_id.as_str(),
                            producer_type,
                            producer_id,
                            entry.id.cursor.as_str(),
                            entry.event_at.0,
                            encoded.as_str()
                        ])
                        .map_err(unavailable("insert normalized log"))?;
                    insert_query
                        .execute(params![sequence, entry.event_at.0])
                        .map_err(unavailable("insert hot query log"))?;
                    report.committed = report.committed.saturating_add(1);
                    last_sequence = sequence;
                }
            }
        }
    }
    transaction
        .execute(
            "UPDATE log_sequence SET last_sequence = ?1 WHERE singleton = TRUE",
            params![last_sequence],
        )
        .map_err(unavailable("advance log sequence high watermark"))?;
    transaction
        .commit()
        .map_err(unavailable("commit append transaction"))?;
    Ok(report)
}

pub(crate) fn append_migration(
    connection: &mut Connection,
    entries: &[IngestLogEntry],
) -> Result<LogAppendReport, LogStoreError> {
    let transaction = connection
        .transaction()
        .map_err(unavailable("begin migration append transaction"))?;
    let mut last_sequence = transaction
        .query_row(
            "SELECT last_sequence FROM log_sequence WHERE singleton = TRUE",
            [],
            |row| row.get::<_, i64>(0),
        )
        .map_err(unavailable("read migration log sequence high watermark"))?;
    let mut report = LogAppendReport::default();
    {
        let mut insert_normalized = transaction
            .prepare(
                "INSERT INTO normalized_logs
                 (sequence, node_id, producer_type, producer_id, cursor, event_at_ms, entry_json)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            )
            .map_err(unavailable("prepare migration normalized log insert"))?;
        let mut insert_query = transaction
            .prepare(
                "INSERT INTO query_logs (sequence, event_at_ms)
                 VALUES (?1, ?2)",
            )
            .map_err(unavailable("prepare migration hot query log insert"))?;
        for entry in entries {
            let encoded =
                serde_json::to_string(entry).map_err(|error| LogStoreError::Rejected {
                    message: format!("normalized entry could not be encoded: {error}"),
                })?;
            let (producer_type, producer_id) = producer_key(&entry.id.producer);
            let sequence = last_sequence
                .checked_add(1)
                .ok_or_else(|| LogStoreError::Rejected {
                    message: "normalized log sequence space is exhausted".to_owned(),
                })?;
            insert_normalized
                .execute(params![
                    sequence,
                    entry.id.node_id.as_str(),
                    producer_type,
                    producer_id,
                    entry.id.cursor.as_str(),
                    entry.event_at.0,
                    encoded.as_str()
                ])
                .map_err(unavailable("insert migration normalized log"))?;
            insert_query
                .execute(params![sequence, entry.event_at.0])
                .map_err(unavailable("insert migration hot query log"))?;
            report.committed = report.committed.saturating_add(1);
            last_sequence = sequence;
        }
    }
    transaction
        .execute(
            "UPDATE log_sequence SET last_sequence = ?1 WHERE singleton = TRUE",
            params![last_sequence],
        )
        .map_err(unavailable("advance migration log sequence high watermark"))?;
    transaction
        .commit()
        .map_err(unavailable("commit migration append transaction"))?;
    Ok(report)
}

fn initialize_v7(connection: &mut Connection) -> Result<(), String> {
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
             CREATE TABLE log_sequence (
                 singleton BOOLEAN PRIMARY KEY CHECK (singleton = TRUE),
                 last_sequence BIGINT NOT NULL
             );
             INSERT INTO log_sequence VALUES (TRUE, 0);
             CREATE TABLE query_logs (
                 sequence BIGINT PRIMARY KEY,
                 event_at_ms BIGINT NOT NULL
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
             CREATE TABLE backup_stats (
                 singleton BOOLEAN PRIMARY KEY CHECK (singleton = TRUE),
                 value_json VARCHAR NOT NULL,
                 updated_at_ms BIGINT NOT NULL
             );
             CREATE TABLE stats_metrics (
                 ts BIGINT NOT NULL,
                 name VARCHAR NOT NULL,
                 value DOUBLE NOT NULL,
                 labels_json VARCHAR NOT NULL,
                 PRIMARY KEY (ts, name, labels_json)
             );
             CREATE INDEX stats_metrics_name_ts
                 ON stats_metrics(name, ts, labels_json);
             CREATE TABLE otlp_envelopes (
                 node_id VARCHAR NOT NULL,
                 workload_id VARCHAR NOT NULL,
                 signal VARCHAR NOT NULL CHECK (signal IN ('metrics', 'traces')),
                 digest BLOB NOT NULL,
                 observed_at_ms BIGINT NOT NULL,
                 metadata_json VARCHAR NOT NULL,
                 payload BLOB NOT NULL,
                 PRIMARY KEY (node_id, workload_id, signal, digest)
             );
             CREATE INDEX otlp_envelopes_observed
                 ON otlp_envelopes(observed_at_ms, signal);
             INSERT INTO schema_version (version) VALUES (7);",
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

fn migrate_v3_to_v4(connection: &mut Connection) -> Result<(), String> {
    let transaction = connection
        .transaction()
        .map_err(|error| error.to_string())?;
    transaction
        .execute_batch(
            "CREATE TABLE backup_stats (
                 singleton BOOLEAN PRIMARY KEY CHECK (singleton = TRUE),
                 value_json VARCHAR NOT NULL,
                 updated_at_ms BIGINT NOT NULL
             );
             UPDATE schema_version SET version = 4;",
        )
        .map_err(|error| error.to_string())?;
    transaction.commit().map_err(|error| error.to_string())
}

fn migrate_v6_to_v7(connection: &mut Connection) -> Result<(), String> {
    let transaction = connection
        .transaction()
        .map_err(|error| error.to_string())?;
    transaction
        .execute_batch(
            "CREATE TABLE log_sequence (
                 singleton BOOLEAN PRIMARY KEY CHECK (singleton = TRUE),
                 last_sequence BIGINT NOT NULL
             );
             INSERT INTO log_sequence
             SELECT TRUE, COALESCE(MAX(sequence), 0) FROM normalized_logs;
             CREATE TABLE query_logs_v7 (
                 sequence BIGINT PRIMARY KEY,
                 event_at_ms BIGINT NOT NULL
             );
             INSERT INTO query_logs_v7
             SELECT sequence, event_at_ms FROM query_logs;
             DROP TABLE query_logs;
             ALTER TABLE query_logs_v7 RENAME TO query_logs;
             CREATE INDEX query_logs_event_sequence
                 ON query_logs(event_at_ms, sequence);
             UPDATE schema_version SET version = 7;",
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
