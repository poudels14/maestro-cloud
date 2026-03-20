use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Result;
use rusqlite::Connection;
use tokio::sync::{Mutex, Notify};

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
}

#[derive(Clone)]
pub struct SystemLogger {
    sender: Option<flume::Sender<LogEntry>>,
}

impl SystemLogger {
    pub fn new(sender: Option<flume::Sender<LogEntry>>) -> Self {
        Self { sender }
    }

    pub fn emit(&self, level: &str, text: &str) {
        eprintln!("[maestro]: {text}");
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
                source: Arc::from("maestro-controller"),
                origin: LogOrigin::System,
                tags: Arc::new(serde_json::Value::Array(vec![])),
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
    #[allow(dead_code)]
    path: PathBuf,
    conn: Mutex<Connection>,
    notify: Arc<Notify>,
}

impl LogStore {
    pub fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let conn = Connection::open(path)?;
        conn.execute_batch("PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL;")?;

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
                attributes TEXT NOT NULL DEFAULT '{}'
            );

            CREATE TABLE IF NOT EXISTS sink_cursors (
                sink_id TEXT PRIMARY KEY,
                last_seq INTEGER NOT NULL DEFAULT 0
            );

            CREATE INDEX IF NOT EXISTS idx_logs_source_seq ON logs (source, seq);

            CREATE TABLE IF NOT EXISTS metrics (
                ts INTEGER NOT NULL,
                source TEXT NOT NULL,
                cpu_percent REAL NOT NULL,
                memory_bytes INTEGER NOT NULL,
                memory_limit_bytes INTEGER NOT NULL,
                net_rx_bytes INTEGER NOT NULL,
                net_tx_bytes INTEGER NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_metrics_source_ts ON metrics (source, ts);
            ",
        )?;
        Ok(Self {
            path: path.to_path_buf(),
            conn: Mutex::new(conn),
            notify: Arc::new(Notify::new()),
        })
    }

    pub async fn append(&self, entries: &[LogEntry]) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let conn = self.conn.lock().await;
        let mut stmt = conn.prepare_cached(
            "INSERT INTO logs (ts, level, stream, text, source, origin, attributes)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        )?;
        for entry in entries {
            let attrs = serde_json::to_string(&entry.tags).unwrap_or_default();
            stmt.execute(rusqlite::params![
                entry.ts,
                entry.level,
                entry.stream,
                entry.text,
                entry.source,
                entry.origin.as_str(),
                attrs,
            ])?;
        }
        drop(stmt);
        drop(conn);
        self.notify.notify_waiters();
        Ok(())
    }

    pub async fn read_tail_all(&self, limit: usize) -> Result<Vec<LogEntry>> {
        let conn = self.conn.lock().await;
        let mut stmt = conn.prepare_cached(
            "SELECT seq, ts, level, stream, text, source, origin, attributes
             FROM logs ORDER BY seq DESC LIMIT ?1",
        )?;
        let rows = stmt.query_map(rusqlite::params![limit as i64], |row| {
            Self::row_to_entry(row)
        })?;
        let mut entries: Vec<LogEntry> = rows.filter_map(|r| r.ok()).collect();
        entries.reverse();
        Ok(entries)
    }

    pub async fn read_after_all(&self, after_seq: i64, limit: usize) -> Result<Vec<LogEntry>> {
        let conn = self.conn.lock().await;
        let mut stmt = conn.prepare_cached(
            "SELECT seq, ts, level, stream, text, source, origin, attributes
             FROM logs WHERE seq > ?1 ORDER BY seq ASC LIMIT ?2",
        )?;
        let rows = stmt.query_map(rusqlite::params![after_seq, limit as i64], |row| {
            Self::row_to_entry(row)
        })?;
        Ok(rows.filter_map(|r| r.ok()).collect())
    }

    pub async fn read_tail_by_prefix(&self, prefix: &str, limit: usize) -> Result<Vec<LogEntry>> {
        let conn = self.conn.lock().await;
        let pattern = format!("{prefix}%");
        let mut stmt = conn.prepare_cached(
            "SELECT seq, ts, level, stream, text, source, origin, attributes
             FROM logs WHERE source LIKE ?1
             ORDER BY seq DESC
             LIMIT ?2",
        )?;
        let rows = stmt.query_map(rusqlite::params![pattern, limit as i64], |row| {
            Self::row_to_entry(row)
        })?;
        let mut entries: Vec<LogEntry> = rows.filter_map(|r| r.ok()).collect();
        entries.reverse();
        Ok(entries)
    }

    pub async fn read_tail(&self, source: &str, limit: usize) -> Result<Vec<LogEntry>> {
        let conn = self.conn.lock().await;
        let mut stmt = conn.prepare_cached(
            "SELECT seq, ts, level, stream, text, source, origin, attributes
             FROM logs WHERE source = ?1
             ORDER BY seq DESC
             LIMIT ?2",
        )?;
        let rows = stmt.query_map(rusqlite::params![source, limit as i64], |row| {
            Self::row_to_entry(row)
        })?;
        let mut entries: Vec<LogEntry> = rows.filter_map(|r| r.ok()).collect();
        entries.reverse();
        Ok(entries)
    }

    pub async fn read_tail_sources(&self, sources: &[&str], limit: usize) -> Result<Vec<LogEntry>> {
        let conn = self.conn.lock().await;
        let placeholders: Vec<String> = (1..=sources.len()).map(|i| format!("?{i}")).collect();
        let query = format!(
            "SELECT seq, ts, level, stream, text, source, origin, attributes
             FROM logs WHERE source IN ({})
             ORDER BY seq DESC
             LIMIT ?{}",
            placeholders.join(", "),
            sources.len() + 1
        );
        let mut stmt = conn.prepare(&query)?;
        let mut params: Vec<Box<dyn rusqlite::types::ToSql>> = sources
            .iter()
            .map(|s| Box::new(s.to_string()) as Box<dyn rusqlite::types::ToSql>)
            .collect();
        params.push(Box::new(limit as i64));
        let param_refs: Vec<&dyn rusqlite::types::ToSql> =
            params.iter().map(|p| p.as_ref()).collect();
        let rows = stmt.query_map(&*param_refs, |row| Self::row_to_entry(row))?;
        let mut entries: Vec<LogEntry> = rows.filter_map(|r| r.ok()).collect();
        entries.reverse();
        Ok(entries)
    }

    pub async fn read_after(&self, after_seq: i64, limit: usize) -> Result<Vec<LogEntry>> {
        let conn = self.conn.lock().await;
        let mut stmt = conn.prepare_cached(
            "SELECT seq, ts, level, stream, text, source, origin, attributes
             FROM logs WHERE seq > ?1
             ORDER BY seq ASC
             LIMIT ?2",
        )?;
        let rows = stmt.query_map(rusqlite::params![after_seq, limit as i64], |row| {
            Self::row_to_entry(row)
        })?;
        Ok(rows.filter_map(|r| r.ok()).collect())
    }

    pub async fn read_after_for_source(
        &self,
        source: &str,
        after_seq: i64,
        limit: usize,
    ) -> Result<Vec<LogEntry>> {
        let conn = self.conn.lock().await;
        let mut stmt = conn.prepare_cached(
            "SELECT seq, ts, level, stream, text, source, origin, attributes
             FROM logs WHERE source = ?1 AND seq > ?2
             ORDER BY seq ASC
             LIMIT ?3",
        )?;
        let rows = stmt.query_map(rusqlite::params![source, after_seq, limit as i64], |row| {
            Self::row_to_entry(row)
        })?;
        Ok(rows.filter_map(|r| r.ok()).collect())
    }

    pub async fn get_sink_cursor(&self, sink_id: &str) -> Result<i64> {
        let conn = self.conn.lock().await;
        let cursor = conn
            .query_row(
                "SELECT last_seq FROM sink_cursors WHERE sink_id = ?1",
                rusqlite::params![sink_id],
                |row| row.get(0),
            )
            .unwrap_or(0i64);
        Ok(cursor)
    }

    pub async fn set_sink_cursor(&self, sink_id: &str, seq: i64) -> Result<()> {
        let conn = self.conn.lock().await;
        conn.execute(
            "INSERT INTO sink_cursors (sink_id, last_seq) VALUES (?1, ?2)
             ON CONFLICT(sink_id) DO UPDATE SET last_seq = ?2",
            rusqlite::params![sink_id, seq],
        )?;
        Ok(())
    }

    pub async fn min_sink_cursor(&self) -> Result<Option<i64>> {
        let conn = self.conn.lock().await;
        let result = conn.query_row("SELECT MIN(last_seq) FROM sink_cursors", [], |row| {
            row.get::<_, Option<i64>>(0)
        })?;
        Ok(result)
    }

    pub async fn delete_before(&self, seq: i64) -> Result<usize> {
        let conn = self.conn.lock().await;
        let deleted = conn.execute("DELETE FROM logs WHERE seq <= ?1", rusqlite::params![seq])?;
        Ok(deleted)
    }

    pub fn notifier(&self) -> Arc<Notify> {
        self.notify.clone()
    }

    pub async fn append_metrics(&self, entries: &[crate::metrics::MetricPoint]) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let conn = self.conn.lock().await;
        let mut stmt = conn.prepare_cached(
            "INSERT INTO metrics (ts, source, cpu_percent, memory_bytes, memory_limit_bytes, net_rx_bytes, net_tx_bytes)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        )?;
        for entry in entries {
            stmt.execute(rusqlite::params![
                entry.ts,
                entry.source,
                entry.cpu_percent,
                entry.memory_bytes,
                entry.memory_limit_bytes,
                entry.net_rx_bytes,
                entry.net_tx_bytes,
            ])?;
        }
        Ok(())
    }

    pub async fn read_metrics(
        &self,
        source: &str,
        from: i64,
        to: i64,
    ) -> Result<Vec<crate::metrics::MetricPoint>> {
        let conn = self.conn.lock().await;
        let mut stmt = conn.prepare_cached(
            "SELECT ts, source, cpu_percent, memory_bytes, memory_limit_bytes, net_rx_bytes, net_tx_bytes
             FROM metrics WHERE source = ?1 AND ts >= ?2 AND ts <= ?3
             ORDER BY ts ASC",
        )?;
        let rows = stmt.query_map(rusqlite::params![source, from, to], |row| {
            Ok(crate::metrics::MetricPoint {
                ts: row.get(0)?,
                source: row.get(1)?,
                cpu_percent: row.get(2)?,
                memory_bytes: row.get(3)?,
                memory_limit_bytes: row.get(4)?,
                net_rx_bytes: row.get(5)?,
                net_tx_bytes: row.get(6)?,
            })
        })?;
        Ok(rows.filter_map(|r| r.ok()).collect())
    }

    pub async fn read_metrics_by_prefix(
        &self,
        prefix: &str,
        from: i64,
        to: i64,
    ) -> Result<Vec<crate::metrics::MetricPoint>> {
        let conn = self.conn.lock().await;
        let pattern = format!("{prefix}%");
        let mut stmt = conn.prepare_cached(
            "SELECT ts, source, cpu_percent, memory_bytes, memory_limit_bytes, net_rx_bytes, net_tx_bytes
             FROM metrics WHERE source LIKE ?1 AND ts >= ?2 AND ts <= ?3
             ORDER BY ts ASC",
        )?;
        let rows = stmt.query_map(rusqlite::params![pattern, from, to], |row| {
            Ok(crate::metrics::MetricPoint {
                ts: row.get(0)?,
                source: row.get(1)?,
                cpu_percent: row.get(2)?,
                memory_bytes: row.get(3)?,
                memory_limit_bytes: row.get(4)?,
                net_rx_bytes: row.get(5)?,
                net_tx_bytes: row.get(6)?,
            })
        })?;
        Ok(rows.filter_map(|r| r.ok()).collect())
    }

    pub async fn cleanup_old_metrics(&self, max_age_ms: i64) -> Result<usize> {
        let conn = self.conn.lock().await;
        let cutoff = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64
            - max_age_ms;
        let deleted = conn.execute(
            "DELETE FROM metrics WHERE ts < ?1",
            rusqlite::params![cutoff],
        )?;
        Ok(deleted)
    }

    fn row_to_entry(row: &rusqlite::Row) -> rusqlite::Result<LogEntry> {
        let origin_str: String = row.get(6)?;
        let attrs_str: String = row.get(7)?;
        let tags: serde_json::Value =
            serde_json::from_str(&attrs_str).unwrap_or(serde_json::Value::Null);
        Ok(LogEntry {
            seq: row.get(0)?,
            ts: row.get(1)?,
            level: row.get::<_, String>(2)?.into(),
            stream: row.get::<_, String>(3)?.into(),
            text: row.get(4)?,
            source: row.get::<_, String>(5)?.into(),
            origin: LogOrigin::from_str(&origin_str),
            tags: Arc::new(tags),
        })
    }
}
