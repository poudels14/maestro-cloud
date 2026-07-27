use duckdb::Connection;

use crate::database_compaction::{compact_if_needed, open, sidecar};

#[test]
fn compaction_reclaims_deleted_pages_and_preserves_the_complete_database()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let path = temporary.path().join("logs.duckdb");
    let mut connection = open(&path)?;
    connection.execute_batch(
        "CREATE TABLE compaction_sentinel (
             id BIGINT PRIMARY KEY,
             value VARCHAR NOT NULL
         );
         INSERT INTO compaction_sentinel VALUES (7, 'preserved');
         UPDATE log_sequence SET last_sequence = 9 WHERE singleton = TRUE;
         INSERT INTO normalized_logs
         VALUES (9, 'node-1', 'system', 'daemon', 'cursor-9', 123, '{\"body\":\"live\"}');
         INSERT INTO query_logs VALUES (9, 123);
         INSERT INTO sink_cursors VALUES ('datadog', 8);
         CREATE TABLE compaction_churn AS
         SELECT value AS id, repeat(CAST(value AS VARCHAR), 256) AS payload
         FROM range(0, 20000) AS values(value);
         CHECKPOINT;
         DROP TABLE compaction_churn;
         CHECKPOINT;",
    )?;
    let old_size = std::fs::metadata(&path)?.len();

    let reclaimed = compact_if_needed(&mut connection, &path)?;

    assert!(reclaimed > 0);
    assert_eq!(
        std::fs::metadata(&path)?.len(),
        old_size.saturating_sub(reclaimed)
    );
    assert_eq!(
        connection.query_row(
            "SELECT value FROM compaction_sentinel WHERE id = 7",
            [],
            |row| row.get::<_, String>(0),
        )?,
        "preserved"
    );
    assert_eq!(
        connection.query_row("SELECT COUNT(*) FROM normalized_logs", [], |row| {
            row.get::<_, i64>(0)
        })?,
        1
    );
    assert_eq!(
        connection.query_row(
            "SELECT last_sequence FROM sink_cursors WHERE sink_id = 'datadog'",
            [],
            |row| row.get::<_, i64>(0),
        )?,
        8
    );
    drop(connection);

    let reopened = open(&path)?;
    assert_eq!(
        reopened.query_row("SELECT value FROM compaction_sentinel", [], |row| {
            row.get::<_, String>(0)
        })?,
        "preserved"
    );
    assert!(!sidecar(&path, "compact")?.exists());
    assert!(!sidecar(&path, "precompact")?.exists());
    Ok(())
}

#[test]
fn open_physically_compacts_v6_duplicate_query_payloads() -> Result<(), Box<dyn std::error::Error>>
{
    let temporary = tempfile::tempdir()?;
    let path = temporary.path().join("logs.duckdb");
    let connection = crate::schema::open(&path)?;
    connection.execute_batch(
        "DROP TABLE query_logs;
         DROP TABLE log_sequence;
         CREATE TABLE query_logs (
             sequence BIGINT PRIMARY KEY,
             event_at_ms BIGINT NOT NULL,
             entry_json VARCHAR NOT NULL
         );
         CREATE INDEX query_logs_event_sequence
             ON query_logs(event_at_ms, sequence);
         UPDATE schema_version SET version = 6;
         INSERT INTO normalized_logs
         SELECT value + 1, 'node-1', 'system', 'daemon',
                CAST(value AS VARCHAR), value,
                repeat(CAST(value AS VARCHAR), 256)
         FROM range(0, 20000) AS values(value);
         INSERT INTO query_logs
         SELECT sequence, event_at_ms, entry_json FROM normalized_logs;
         CHECKPOINT;",
    )?;
    let duplicated_size = std::fs::metadata(&path)?.len();
    drop(connection);

    let migrated = open(&path)?;
    let compacted_size = std::fs::metadata(&path)?.len();

    assert!(compacted_size < duplicated_size);
    assert_eq!(
        migrated.query_row("SELECT version FROM schema_version", [], |row| {
            row.get::<_, i64>(0)
        })?,
        7
    );
    assert_eq!(
        migrated.query_row(
            "SELECT COUNT(*) FROM pragma_table_info('query_logs')",
            [],
            |row| row.get::<_, i64>(0),
        )?,
        2
    );
    assert_eq!(
        migrated.query_row("SELECT COUNT(*) FROM normalized_logs", [], |row| {
            row.get::<_, i64>(0)
        })?,
        20_000
    );
    Ok(())
}

#[test]
fn open_restores_a_backup_when_publish_was_interrupted() -> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let path = temporary.path().join("logs.duckdb");
    seed(&path)?;
    let backup = sidecar(&path, "precompact")?;
    std::fs::rename(&path, &backup)?;

    let connection = open(&path)?;

    assert_seed(&connection)?;
    assert!(path.exists());
    assert!(!backup.exists());
    Ok(())
}

#[test]
fn open_rolls_back_a_corrupt_published_copy() -> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let path = temporary.path().join("logs.duckdb");
    seed(&path)?;
    let backup = sidecar(&path, "precompact")?;
    std::fs::rename(&path, &backup)?;
    std::fs::write(&path, b"not a DuckDB database")?;

    let connection = open(&path)?;

    assert_seed(&connection)?;
    assert!(!backup.exists());
    assert!(!sidecar(&path, "compact")?.exists());
    Ok(())
}

fn seed(path: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
    let connection = crate::schema::open(path)?;
    connection.execute_batch(
        "CREATE TABLE compaction_sentinel (
             id BIGINT PRIMARY KEY,
             value VARCHAR NOT NULL
         );
         INSERT INTO compaction_sentinel VALUES (7, 'preserved');",
    )?;
    connection.execute_batch("CHECKPOINT")?;
    Ok(())
}

fn assert_seed(connection: &Connection) -> Result<(), Box<dyn std::error::Error>> {
    assert_eq!(
        connection.query_row("SELECT value FROM compaction_sentinel", [], |row| {
            row.get::<_, String>(0)
        })?,
        "preserved"
    );
    Ok(())
}
