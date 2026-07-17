use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Result;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::OptionalExtension;
use tokio::sync::Notify;
use tokio::task;

type ConnPool = r2d2::Pool<SqliteConnectionManager>;
pub const MAX_SINK_DEAD_LETTERS: i64 = 100_000;

fn arc_value_is_null(v: &Arc<serde_json::Value>) -> bool {
    v.is_null()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogOrigin {
    System,
    Build,
    Service,
}

impl LogOrigin {
    pub fn as_str(&self) -> &'static str {
        match self {
            LogOrigin::System => "system",
            LogOrigin::Build => "build",
            LogOrigin::Service => "service",
        }
    }

    fn from_str(s: &str) -> Self {
        match s {
            "build" => LogOrigin::Build,
            "service" => LogOrigin::Service,
            _ => LogOrigin::System,
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct LogEntry {
    #[serde(default)]
    pub seq: i64,
    pub ts: i64,
    pub level: Arc<str>,
    pub stream: Arc<str>,
    pub text: String,
    pub source: Arc<str>,
    #[serde(default = "default_origin")]
    pub origin: LogOrigin,
    #[serde(
        default = "default_null_arc",
        skip_serializing_if = "arc_value_is_null"
    )]
    pub tags: Arc<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub attrs: Vec<(String, String)>,
}

#[derive(Clone)]
pub struct Logger {
    sender: Option<flume::Sender<LogEntry>>,
}

impl Logger {
    pub fn new(sender: Option<flume::Sender<LogEntry>>) -> Self {
        Self { sender }
    }

    pub fn noop() -> Self {
        Self { sender: None }
    }

    pub fn emit(&self, level: &str, text: &str) {
        eprintln!("[maestro]: {text}");
        self.emit_from_source(level, text, "maestro-controller", LogOrigin::System);
    }

    pub fn emit_from_source(&self, level: &str, text: &str, source: &str, origin: LogOrigin) {
        if let Some(sender) = &self.sender {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64;
            let _ = sender.try_send(LogEntry {
                seq: 0,
                ts: now,
                level: Arc::from(level),
                stream: Arc::from("stderr"),
                text: text.to_string(),
                source: Arc::from(source),
                origin,
                tags: Arc::new(serde_json::Value::Array(vec![])),
                attrs: vec![],
            });
        }
    }
}

fn default_origin() -> LogOrigin {
    LogOrigin::System
}

fn default_null_arc() -> Arc<serde_json::Value> {
    Arc::new(serde_json::Value::Null)
}

pub struct LogStore {
    path: PathBuf,
    pool: ConnPool,
    notify: Arc<Notify>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct SinkDeadLetterMetadata {
    pub sink_id: String,
    pub seq: i64,
    pub status: u16,
    pub error: String,
    pub payload_sha256: String,
    pub payload_bytes: u64,
    pub created_at_ms: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SinkDeadLetterStats {
    pub count: u64,
    pub payload_bytes: u64,
}

#[derive(Debug, Clone)]
pub struct LogSpoolStats {
    pub row_count: u64,
    pub high_watermark: i64,
    pub oldest_entry_at_ms: Option<i64>,
    pub database_bytes: u64,
    pub sinks: Vec<LogSinkCursorStats>,
    pub dead_letters: SinkDeadLetterSnapshot,
}

#[derive(Debug, Clone)]
pub struct LogSinkCursorStats {
    pub sink_id: String,
    pub cursor: i64,
    pub pending_entries: u64,
    pub oldest_pending_at_ms: Option<i64>,
}

#[derive(Debug, Clone, Default)]
pub struct SinkDeadLetterSnapshot {
    pub count: u64,
    pub payload_bytes: u64,
    pub latest_at_ms: Option<i64>,
    pub latest_status: Option<u16>,
    pub latest_error: Option<String>,
}

impl LogStore {
    pub fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let manager = SqliteConnectionManager::file(path).with_init(|conn| {
            conn.execute_batch(
                r#"
                    PRAGMA journal_mode = WAL;
                    PRAGMA synchronous = NORMAL;
                    PRAGMA busy_timeout = 5000;
                "#,
            )
        });
        let pool = ConnPool::builder().max_size(8).build(manager)?;

        let conn = pool.get()?;

        let has_origin = conn.prepare("SELECT origin FROM logs LIMIT 0").is_ok();
        if !has_origin {
            conn.execute_batch("DROP TABLE IF EXISTS logs; DROP TABLE IF EXISTS sink_cursors;")?;
        }

        conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS logs (
                seq INTEGER PRIMARY KEY AUTOINCREMENT,
                ts INTEGER NOT NULL,
                level TEXT NOT NULL,
                stream TEXT NOT NULL,
                text TEXT NOT NULL,
                source TEXT NOT NULL,
                origin TEXT NOT NULL DEFAULT 'system',
                tags TEXT NOT NULL DEFAULT '[]',
                attributes TEXT NOT NULL DEFAULT '[]'
            );

            CREATE TABLE IF NOT EXISTS sink_cursors (
                sink_id TEXT PRIMARY KEY,
                last_seq INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS sink_dead_letters (
                sink_id TEXT NOT NULL,
                seq INTEGER NOT NULL,
                status INTEGER NOT NULL,
                error TEXT NOT NULL,
                payload BLOB NOT NULL,
                payload_sha256 TEXT NOT NULL,
                created_at_ms INTEGER NOT NULL,
                PRIMARY KEY (sink_id, seq)
            );

            CREATE INDEX IF NOT EXISTS idx_logs_source_seq ON logs (source, seq);
            CREATE INDEX IF NOT EXISTS idx_logs_ts ON logs (ts);

            -- These tables belonged to the retired probe SQLite telemetry
            -- backend. This database is now the controller delivery spool only.
            DROP TABLE IF EXISTS ingress_traffic;
            DROP TABLE IF EXISTS metrics;
            DROP TABLE IF EXISTS traffic_metrics;
            DROP TABLE IF EXISTS stats_metrics;
            ",
        )?;

        let has_tags = conn.prepare("SELECT tags FROM logs LIMIT 0").is_ok();
        if !has_tags {
            conn.execute_batch(
                "ALTER TABLE logs RENAME COLUMN attributes TO tags;
                 ALTER TABLE logs ADD COLUMN attributes TEXT NOT NULL DEFAULT '[]';",
            )?;
        }

        drop(conn);

        Ok(Self {
            path: path.to_path_buf(),
            pool,
            notify: Arc::new(Notify::new()),
        })
    }

    pub async fn append(&self, entries: &[LogEntry]) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let pool = self.pool.clone();
        let entries = entries.to_vec();
        task::spawn_blocking(move || -> Result<()> {
            let mut conn = pool.get()?;
            let tx = conn.transaction()?;
            let mut stmt = tx.prepare_cached(
                "INSERT INTO logs (ts, level, stream, text, source, origin, tags, attributes)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            )?;
            for entry in &entries {
                let tags_json = serde_json::to_string(&entry.tags).unwrap_or_default();
                let attrs_json =
                    serde_json::to_string(&entry.attrs).unwrap_or_else(|_| "[]".into());
                stmt.execute(rusqlite::params![
                    entry.ts,
                    entry.level,
                    entry.stream,
                    entry.text,
                    entry.source,
                    entry.origin.as_str(),
                    tags_json,
                    attrs_json,
                ])?;
            }
            drop(stmt);
            tx.commit()?;
            Ok(())
        })
        .await??;
        self.notify.notify_waiters();
        Ok(())
    }

    pub async fn read_after(&self, after_seq: i64, limit: usize) -> Result<Vec<LogEntry>> {
        let pool = self.pool.clone();
        task::spawn_blocking(move || -> Result<Vec<LogEntry>> {
            let conn = pool.get()?;
            let mut stmt = conn.prepare_cached(
                "SELECT seq, ts, level, stream, text, source, origin, tags, attributes
                 FROM logs WHERE seq > ?1
                 ORDER BY seq ASC
                 LIMIT ?2",
            )?;
            let rows = stmt.query_map(rusqlite::params![after_seq, limit as i64], |row| {
                Self::row_to_entry(row)
            })?;
            Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
        })
        .await?
    }

    pub async fn get_sink_cursor(&self, sink_id: &str) -> Result<i64> {
        let pool = self.pool.clone();
        let sink_id = sink_id.to_string();
        task::spawn_blocking(move || -> Result<i64> {
            use rusqlite::OptionalExtension;

            let conn = pool.get()?;
            let cursor = conn
                .query_row(
                    "SELECT last_seq FROM sink_cursors WHERE sink_id = ?1",
                    rusqlite::params![sink_id],
                    |row| row.get(0),
                )
                .optional()?;
            Ok(cursor.unwrap_or(0))
        })
        .await?
    }

    pub async fn register_sink(&self, sink_id: &str) -> Result<()> {
        let pool = self.pool.clone();
        let sink_id = sink_id.to_string();
        task::spawn_blocking(move || -> Result<()> {
            let conn = pool.get()?;
            conn.execute(
                "INSERT OR IGNORE INTO sink_cursors (sink_id, last_seq) VALUES (?1, 0)",
                rusqlite::params![sink_id],
            )?;
            Ok(())
        })
        .await?
    }

    pub async fn unregister_sink(&self, sink_id: &str) -> Result<()> {
        let pool = self.pool.clone();
        let sink_id = sink_id.to_string();
        task::spawn_blocking(move || -> Result<()> {
            let conn = pool.get()?;
            conn.execute(
                "DELETE FROM sink_cursors WHERE sink_id = ?1",
                rusqlite::params![sink_id],
            )?;
            Ok(())
        })
        .await?
    }

    pub async fn record_sink_dead_letter(
        &self,
        sink_id: &str,
        seq: i64,
        status: u16,
        error: &str,
        payload: &[u8],
    ) -> Result<()> {
        use sha2::{Digest, Sha256};

        let pool = self.pool.clone();
        let sink_id = sink_id.to_string();
        let error = error.to_string();
        let payload = payload.to_vec();
        let payload_sha256 = format!("{:x}", Sha256::digest(&payload));
        let created_at_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        task::spawn_blocking(move || -> Result<()> {
            use rusqlite::{OptionalExtension, TransactionBehavior};

            let mut conn = pool.get()?;
            let transaction = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let current_count: i64 =
                transaction.query_row("SELECT count(*) FROM sink_dead_letters", [], |row| {
                    row.get(0)
                })?;
            let already_exists = transaction
                .query_row(
                    "SELECT 1 FROM sink_dead_letters WHERE sink_id=?1 AND seq=?2",
                    rusqlite::params![sink_id, seq],
                    |_| Ok(()),
                )
                .optional()?
                .is_some();
            if !already_exists && current_count >= MAX_SINK_DEAD_LETTERS {
                anyhow::bail!(
                    "sink dead-letter storage limit of {} rows has been reached",
                    MAX_SINK_DEAD_LETTERS
                );
            }

            transaction.execute(
                "INSERT INTO sink_dead_letters
                    (sink_id, seq, status, error, payload, payload_sha256, created_at_ms)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                 ON CONFLICT(sink_id, seq) DO UPDATE SET
                    status=excluded.status,
                    error=excluded.error,
                    payload=excluded.payload,
                    payload_sha256=excluded.payload_sha256,
                    created_at_ms=excluded.created_at_ms",
                rusqlite::params![
                    sink_id,
                    seq,
                    i64::from(status),
                    error,
                    payload,
                    payload_sha256,
                    created_at_ms,
                ],
            )?;
            transaction.commit()?;
            Ok(())
        })
        .await?
    }

    pub async fn sink_dead_letter_stats(&self, sink_id: &str) -> Result<SinkDeadLetterStats> {
        let pool = self.pool.clone();
        let sink_id = sink_id.to_string();
        task::spawn_blocking(move || -> Result<SinkDeadLetterStats> {
            let conn = pool.get()?;
            let (count, payload_bytes): (i64, i64) = conn.query_row(
                "SELECT count(*), COALESCE(SUM(length(payload)), 0)
                 FROM sink_dead_letters WHERE sink_id=?1",
                rusqlite::params![sink_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?;
            Ok(SinkDeadLetterStats {
                count: u64::try_from(count).unwrap_or_default(),
                payload_bytes: u64::try_from(payload_bytes).unwrap_or_default(),
            })
        })
        .await?
    }

    #[cfg(test)]
    pub async fn sink_dead_letter_count(&self, sink_id: &str) -> Result<i64> {
        Ok(i64::try_from(self.sink_dead_letter_stats(sink_id).await?.count).unwrap_or(i64::MAX))
    }

    pub async fn list_sink_dead_letters(
        &self,
        sink_id: &str,
        limit: usize,
    ) -> Result<Vec<SinkDeadLetterMetadata>> {
        let pool = self.pool.clone();
        let sink_id = sink_id.to_string();
        task::spawn_blocking(move || -> Result<Vec<SinkDeadLetterMetadata>> {
            let conn = pool.get()?;
            let mut statement = conn.prepare(
                "SELECT sink_id,seq,status,error,payload_sha256,length(payload),created_at_ms
                 FROM sink_dead_letters WHERE sink_id=?1 ORDER BY seq LIMIT ?2",
            )?;
            let rows = statement.query_map(
                rusqlite::params![sink_id, i64::try_from(limit).unwrap_or(i64::MAX)],
                |row| {
                    let status: i64 = row.get(2)?;
                    let payload_bytes: i64 = row.get(5)?;
                    Ok(SinkDeadLetterMetadata {
                        sink_id: row.get(0)?,
                        seq: row.get(1)?,
                        status: u16::try_from(status).unwrap_or_default(),
                        error: row.get(3)?,
                        payload_sha256: row.get(4)?,
                        payload_bytes: u64::try_from(payload_bytes).unwrap_or_default(),
                        created_at_ms: row.get(6)?,
                    })
                },
            )?;
            Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
        })
        .await?
    }

    pub async fn export_sink_dead_letters(&self, sink_id: &str, path: &Path) -> Result<u64> {
        if let (Ok(output_path), Ok(database_path)) = (
            std::fs::canonicalize(path),
            std::fs::canonicalize(&self.path),
        ) && output_path == database_path
        {
            anyhow::bail!("dead-letter export path must not overwrite the log spool database");
        }
        let pool = self.pool.clone();
        let sink_id = sink_id.to_string();
        let path = path.to_path_buf();
        task::spawn_blocking(move || -> Result<u64> {
            use std::io::{BufWriter, Write};

            let conn = pool.get()?;
            let mut statement = conn.prepare(
                "SELECT sink_id,seq,status,error,payload_sha256,created_at_ms,payload
                 FROM sink_dead_letters WHERE sink_id=?1 ORDER BY seq",
            )?;
            let mut rows = statement.query(rusqlite::params![sink_id])?;
            let file = std::fs::File::create(&path)?;
            let mut writer = BufWriter::new(file);
            let mut exported = 0u64;
            while let Some(row) = rows.next()? {
                let payload: Vec<u8> = row.get(6)?;
                let payload: serde_json::Value = serde_json::from_slice(&payload)?;
                let record = serde_json::json!({
                    "sinkId": row.get::<_, String>(0)?,
                    "seq": row.get::<_, i64>(1)?,
                    "status": row.get::<_, i64>(2)?,
                    "error": row.get::<_, String>(3)?,
                    "payloadSha256": row.get::<_, String>(4)?,
                    "createdAtMs": row.get::<_, i64>(5)?,
                    "payload": payload,
                });
                serde_json::to_writer(&mut writer, &record)?;
                writer.write_all(b"\n")?;
                exported += 1;
            }
            writer.flush()?;
            Ok(exported)
        })
        .await?
    }

    pub async fn purge_sink_dead_letters(
        &self,
        sink_id: &str,
        through_seq: Option<i64>,
    ) -> Result<u64> {
        let pool = self.pool.clone();
        let sink_id = sink_id.to_string();
        task::spawn_blocking(move || -> Result<u64> {
            let conn = pool.get()?;
            let deleted = if let Some(seq) = through_seq {
                conn.execute(
                    "DELETE FROM sink_dead_letters WHERE sink_id=?1 AND seq<=?2",
                    rusqlite::params![sink_id, seq],
                )?
            } else {
                conn.execute(
                    "DELETE FROM sink_dead_letters WHERE sink_id=?1",
                    rusqlite::params![sink_id],
                )?
            };
            Ok(u64::try_from(deleted).unwrap_or_default())
        })
        .await?
    }

    pub async fn set_sink_cursor(&self, sink_id: &str, seq: i64) -> Result<()> {
        let pool = self.pool.clone();
        let sink_id = sink_id.to_string();
        task::spawn_blocking(move || -> Result<()> {
            let conn = pool.get()?;
            conn.execute(
                "INSERT INTO sink_cursors (sink_id, last_seq) VALUES (?1, ?2)
                 ON CONFLICT(sink_id) DO UPDATE SET last_seq = ?2",
                rusqlite::params![sink_id, seq],
            )?;
            Ok(())
        })
        .await?
    }

    pub async fn min_sink_cursor(&self) -> Result<Option<i64>> {
        let pool = self.pool.clone();
        task::spawn_blocking(move || -> Result<Option<i64>> {
            let conn = pool.get()?;
            let result = conn.query_row("SELECT MIN(last_seq) FROM sink_cursors", [], |row| {
                row.get::<_, Option<i64>>(0)
            })?;
            Ok(result)
        })
        .await?
    }

    pub async fn delete_before(&self, seq: i64) -> Result<usize> {
        let pool = self.pool.clone();
        task::spawn_blocking(move || -> Result<usize> {
            let conn = pool.get()?;
            let deleted =
                conn.execute("DELETE FROM logs WHERE seq <= ?1", rusqlite::params![seq])?;
            Ok(deleted)
        })
        .await?
    }

    pub async fn stats_snapshot(&self) -> Result<LogSpoolStats> {
        let pool = self.pool.clone();
        let path = self.path.clone();
        task::spawn_blocking(move || -> Result<LogSpoolStats> {
            let conn = pool.get()?;
            let first_entry: Option<(i64, i64)> = conn
                .query_row("SELECT seq, ts FROM logs ORDER BY seq LIMIT 1", [], |row| {
                    Ok((row.get(0)?, row.get(1)?))
                })
                .optional()?;
            let high_watermark: i64 = conn
                .query_row(
                    "SELECT seq FROM logs ORDER BY seq DESC LIMIT 1",
                    [],
                    |row| row.get(0),
                )
                .optional()?
                .unwrap_or(0);
            let row_count = first_entry
                .map(|(first_seq, _)| high_watermark.saturating_sub(first_seq).saturating_add(1))
                .unwrap_or(0);
            let oldest_entry_at_ms = first_entry.map(|(_, timestamp)| timestamp);

            let mut cursor_statement =
                conn.prepare("SELECT sink_id, last_seq FROM sink_cursors ORDER BY sink_id")?;
            let cursors = cursor_statement
                .query_map([], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            let mut sinks = Vec::with_capacity(cursors.len());
            for (sink_id, cursor) in cursors {
                let oldest_pending_at_ms: Option<i64> = conn
                    .query_row(
                        "SELECT ts FROM logs WHERE seq > ?1 ORDER BY seq LIMIT 1",
                        rusqlite::params![cursor],
                        |row| row.get(0),
                    )
                    .optional()?;
                sinks.push(LogSinkCursorStats {
                    sink_id,
                    cursor,
                    pending_entries: u64::try_from(high_watermark.saturating_sub(cursor))
                        .unwrap_or_default(),
                    oldest_pending_at_ms,
                });
            }

            let (dead_letter_count, dead_letter_bytes): (i64, i64) = conn.query_row(
                "SELECT count(*), COALESCE(SUM(length(payload)), 0) FROM sink_dead_letters",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?;
            let latest_dead_letter = conn
                .query_row(
                    "SELECT status, error, created_at_ms
                     FROM sink_dead_letters
                     ORDER BY created_at_ms DESC
                     LIMIT 1",
                    [],
                    |row| {
                        let status: i64 = row.get(0)?;
                        Ok((
                            u16::try_from(status).ok(),
                            row.get::<_, String>(1)?,
                            row.get::<_, i64>(2)?,
                        ))
                    },
                )
                .optional()?;
            let (latest_status, latest_error, latest_at_ms) = latest_dead_letter
                .map(|(status, error, at)| (status, Some(error), Some(at)))
                .unwrap_or((None, None, None));

            Ok(LogSpoolStats {
                row_count: u64::try_from(row_count).unwrap_or_default(),
                high_watermark,
                oldest_entry_at_ms,
                database_bytes: sqlite_file_set_bytes(&path),
                sinks,
                dead_letters: SinkDeadLetterSnapshot {
                    count: u64::try_from(dead_letter_count).unwrap_or_default(),
                    payload_bytes: u64::try_from(dead_letter_bytes).unwrap_or_default(),
                    latest_at_ms,
                    latest_status,
                    latest_error,
                },
            })
        })
        .await?
    }

    pub fn notifier(&self) -> Arc<Notify> {
        self.notify.clone()
    }

    fn row_to_entry(row: &rusqlite::Row) -> rusqlite::Result<LogEntry> {
        let origin_str: String = row.get(6)?;
        let tags_str: String = row.get(7)?;
        let attrs_str: String = row.get(8)?;
        let tags: serde_json::Value =
            serde_json::from_str(&tags_str).unwrap_or(serde_json::Value::Null);
        let attrs: Vec<(String, String)> = serde_json::from_str(&attrs_str).unwrap_or_default();
        Ok(LogEntry {
            seq: row.get(0)?,
            ts: row.get(1)?,
            level: row.get::<_, String>(2)?.into(),
            stream: row.get::<_, String>(3)?.into(),
            text: row.get(4)?,
            source: row.get::<_, String>(5)?.into(),
            origin: LogOrigin::from_str(&origin_str),
            tags: Arc::new(tags),
            attrs,
        })
    }
}

fn sqlite_file_set_bytes(path: &Path) -> u64 {
    ["", "-wal", "-shm"]
        .into_iter()
        .filter_map(|suffix| {
            let mut value = path.as_os_str().to_os_string();
            value.push(suffix);
            std::fs::metadata(PathBuf::from(value)).ok()
        })
        .map(|metadata| metadata.len())
        .sum()
}

#[cfg(test)]
#[path = "../tests/logs/store.rs"]
mod tests;
