use duckdb::params;

use super::LogStore;
use super::query::log_source;
use crate::error::{Result, TelemetryError};

pub(super) const SINKS_SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS sink_cursors (
    sink_id VARCHAR PRIMARY KEY,
    last_seq BIGINT NOT NULL
);
CREATE TABLE IF NOT EXISTS sink_dead_letters (
    sink_id VARCHAR NOT NULL,
    seq BIGINT NOT NULL,
    status INTEGER NOT NULL,
    error VARCHAR NOT NULL,
    payload VARCHAR NOT NULL,
    created_at_ms BIGINT NOT NULL,
    PRIMARY KEY (sink_id, seq)
);
"#;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SinkRecord {
    pub seq: i64,
    pub ts: i64,
    pub replica_id: String,
    pub service_id: Option<String>,
    pub deployment_id: Option<String>,
    pub level: String,
    pub line: String,
    pub message: Option<String>,
    pub attributes: String,
}

impl LogStore {
    pub fn sink_cursor(&self, sink_id: &str) -> Result<Option<i64>> {
        let connection = self.db.reader()?;
        let mut statement =
            connection.prepare("SELECT last_seq FROM sink_cursors WHERE sink_id = ?")?;
        let mut rows = statement.query_map(params![sink_id], |row| row.get::<_, i64>(0))?;
        Ok(rows.next().transpose()?)
    }

    pub fn set_sink_cursor(&self, sink_id: &str, last_seq: i64) -> Result<()> {
        self.db.writer()?.execute(
            "INSERT INTO sink_cursors VALUES (?, ?) ON CONFLICT DO UPDATE SET last_seq = excluded.last_seq",
            params![sink_id, last_seq],
        )?;
        Ok(())
    }

    pub fn latest_seq(&self) -> Result<i64> {
        let connection = self.db.reader()?;
        Ok(
            connection.query_row("SELECT coalesce(max(seq), 0) FROM logs", [], |row| {
                row.get(0)
            })?,
        )
    }

    pub async fn records_after(&self, after_seq: i64, limit: usize) -> Result<Vec<SinkRecord>> {
        let db = self.db.clone();
        let parts_root = self.parts_root.clone();
        tokio::task::spawn_blocking(move || {
            let _visibility = db.read_parquet()?;
            let connection = db.reader()?;
            let mut statement = connection.prepare(&format!(
                "SELECT seq, ts, replica_id, service_id, deployment_id, level, line, message, attributes \
                 FROM ({}) q WHERE seq > ? ORDER BY seq LIMIT ?",
                log_source(&parts_root)
            ))?;
            let rows = statement.query_map(params![after_seq, limit as i64], |row| {
                Ok(SinkRecord {
                    seq: row.get(0)?,
                    ts: row.get(1)?,
                    replica_id: row.get(2)?,
                    service_id: row.get(3)?,
                    deployment_id: row.get(4)?,
                    level: row.get(5)?,
                    line: row.get(6)?,
                    message: row.get(7)?,
                    attributes: row.get(8)?,
                })
            })?;
            Ok::<_, TelemetryError>(rows.collect::<duckdb::Result<Vec<_>>>()?)
        })
        .await?
    }

    pub fn dead_letter(
        &self,
        sink_id: &str,
        seq: i64,
        status: u16,
        error: &str,
        payload: &str,
        capacity: usize,
    ) -> Result<()> {
        let connection = self.db.writer()?;
        let count: i64 = connection.query_row(
            "SELECT count(*) FROM sink_dead_letters WHERE sink_id = ?",
            params![sink_id],
            |row| row.get(0),
        )?;
        match count as usize >= capacity {
            true => Err(TelemetryError::Storage(format!(
                "{sink_id} dead letters are full ({capacity})"
            ))),
            false => {
                connection.execute(
                    "INSERT OR REPLACE INTO sink_dead_letters VALUES (?, ?, ?, ?, ?, ?)",
                    params![sink_id, seq, status as i32, error, payload, now_ms()],
                )?;
                Ok(())
            }
        }
    }

    pub fn dead_letter_count(&self, sink_id: &str) -> Result<i64> {
        Ok(self.db.reader()?.query_row(
            "SELECT count(*) FROM sink_dead_letters WHERE sink_id = ?",
            params![sink_id],
            |row| row.get(0),
        )?)
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_millis() as i64)
}
