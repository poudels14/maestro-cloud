use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use duckdb::{params, types::Value};
use serde::Serialize;

use crate::access_log::{INGRESS_REPLICA, parse_access_event};
use crate::error::{Result, TelemetryError};
use crate::log_driver::LogRecord;
use crate::parse::parse_log_line;
use crate::resolver::ReplicaIdentity;
use crate::search::LogSearchQuery;

mod db;
mod ingress;
mod metrics;
mod query;
mod rollover;
mod traffic;

use db::Db;
pub use ingress::{IngressTrafficQuery, TrafficScope};
pub use metrics::MetricsQuery;
use query::query_logs;
use rollover::{remove_stale_exports, rollover_partitions, write_partition_manifest};
pub use traffic::TrafficQuery;

const SCHEMA: &str = r#"
CREATE SEQUENCE IF NOT EXISTS log_seq START 1;
CREATE TABLE IF NOT EXISTS logs (
    seq BIGINT PRIMARY KEY DEFAULT nextval('log_seq'),
    ts BIGINT NOT NULL,
    date DATE NOT NULL,
    replica_id VARCHAR NOT NULL,
    service_id VARCHAR,
    deployment_id VARCHAR,
    stream VARCHAR NOT NULL,
    level VARCHAR NOT NULL,
    line VARCHAR NOT NULL,
    message VARCHAR,
    attributes VARCHAR NOT NULL
);
CREATE TABLE IF NOT EXISTS tail_checkpoints (
    replica_id VARCHAR PRIMARY KEY,
    inode UBIGINT NOT NULL,
    byte_offset UBIGINT NOT NULL,
    updated_at_ms BIGINT NOT NULL
);
CREATE TABLE IF NOT EXISTS partition_state (
    partition_key VARCHAR NOT NULL,
    state VARCHAR NOT NULL,
    row_count BIGINT NOT NULL,
    seq_lo BIGINT NOT NULL,
    seq_hi BIGINT NOT NULL,
    sha256 VARCHAR,
    updated_at_ms BIGINT NOT NULL,
    PRIMARY KEY (partition_key, seq_lo)
);
"#;

const INSERT_LOG: &str = r#"
INSERT INTO logs (
    ts, date, replica_id, service_id, deployment_id, stream, level, line, message, attributes
) VALUES (?, CAST(epoch_ms(?) AS DATE), ?, ?, ?, ?, ?, ?, ?, ?)
"#;

const UPSERT_CHECKPOINT: &str = r#"
INSERT INTO tail_checkpoints VALUES (?, ?, ?, ?)
ON CONFLICT (replica_id) DO UPDATE SET
    inode = excluded.inode,
    byte_offset = excluded.byte_offset,
    updated_at_ms = excluded.updated_at_ms
"#;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TailCheckpoint {
    pub inode: u64,
    pub offset: u64,
}

#[derive(Clone, Debug, Default)]
pub struct LogQuery {
    pub search: Option<LogSearchQuery>,
    pub before: Option<i64>,
    pub before_ts: Option<i64>,
    pub limit: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StoredLog {
    pub seq: i64,
    pub ts: i64,
    pub replica_id: String,
    pub service_id: Option<String>,
    pub deployment_id: Option<String>,
    pub stream: String,
    pub level: String,
    pub line: String,
}

pub struct LogStore {
    db: Arc<Db>,
    parts_root: PathBuf,
    rollover_lock: Arc<Mutex<()>>,
}

impl LogStore {
    pub fn open(data_dir: &Path) -> Result<Self> {
        let parts_root = data_dir.join("parts");
        std::fs::create_dir_all(&parts_root)?;
        remove_stale_exports(&parts_root)?;
        let store = Self {
            db: Arc::new(Db::open(
                &data_dir.join("logs.duckdb"),
                &format!(
                    "{SCHEMA}{}{}{}",
                    metrics::METRICS_SCHEMA,
                    traffic::TRAFFIC_SCHEMA,
                    ingress::INGRESS_SCHEMA
                ),
            )?),
            parts_root,
            rollover_lock: Arc::new(Mutex::new(())),
        };
        store.rebuild_partition_manifests()?;
        Ok(store)
    }

    pub fn ingest(
        &self,
        replica_id: &str,
        identity: &ReplicaIdentity,
        records: &[LogRecord],
        checkpoint: TailCheckpoint,
    ) -> Result<()> {
        let mut connection = self.db.writer()?;
        let transaction = connection.transaction()?;
        {
            let mut statement = transaction.prepare_cached(INSERT_LOG)?;
            records.iter().try_for_each(|record| {
                let parsed = parse_log_line(&record.line);
                let timestamp = i64::try_from(record.ts).unwrap_or(i64::MAX);
                let message = (parsed.message != record.line).then_some(parsed.message);
                statement.execute(params![
                    timestamp,
                    timestamp,
                    replica_id,
                    identity.service_id,
                    identity.deployment_id,
                    record.stream.as_str(),
                    parsed.level,
                    record.line,
                    message,
                    serde_json::Value::Object(parsed.attributes).to_string(),
                ])?;
                Ok::<_, TelemetryError>(())
            })?;
        }
        if replica_id == INGRESS_REPLICA {
            records.iter().try_for_each(|record| {
                let timestamp = i64::try_from(record.ts).unwrap_or(i64::MAX);
                match parse_access_event(&record.line, timestamp) {
                    Some(event) => ingress::record_access(&transaction, &event),
                    None => Ok(()),
                }
            })?;
        }
        transaction.execute(
            UPSERT_CHECKPOINT,
            params![replica_id, checkpoint.inode, checkpoint.offset, now_ms()],
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn checkpoints(&self) -> Result<HashMap<String, TailCheckpoint>> {
        let connection = self.db.reader()?;
        let mut statement =
            connection.prepare("SELECT replica_id, inode, byte_offset FROM tail_checkpoints")?;
        let rows = statement.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                TailCheckpoint {
                    inode: row.get(1)?,
                    offset: row.get(2)?,
                },
            ))
        })?;
        Ok(rows.collect::<duckdb::Result<HashMap<_, _>>>()?)
    }

    pub fn forget_checkpoint(&self, replica_id: &str) -> Result<()> {
        self.db.writer()?.execute(
            "DELETE FROM tail_checkpoints WHERE replica_id = ?",
            params![replica_id],
        )?;
        Ok(())
    }

    pub async fn read_logs(&self, query: LogQuery) -> Result<Vec<StoredLog>> {
        let db = self.db.clone();
        let parts_root = self.parts_root.clone();
        tokio::task::spawn_blocking(move || {
            let _visibility = db.read_parquet()?;
            query_logs(&db, &parts_root, &query)
        })
        .await?
    }

    pub async fn rollover(&self) -> Result<usize> {
        let db = self.db.clone();
        let parts_root = self.parts_root.clone();
        let rollover_lock = self.rollover_lock.clone();
        tokio::task::spawn_blocking(move || {
            let _rollover = rollover_lock
                .lock()
                .map_err(|_| TelemetryError::Storage("rollover lock poisoned".to_string()))?;
            rollover_partitions(&db, &parts_root)
        })
        .await?
    }

    fn rebuild_partition_manifests(&self) -> Result<()> {
        let keys = {
            let connection = self.db.reader()?;
            let mut statement = connection
                .prepare("SELECT DISTINCT partition_key FROM partition_state ORDER BY 1")?;
            let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
            rows.collect::<duckdb::Result<Vec<_>>>()?
        };
        keys.iter()
            .map(|key| (key, partition_directory(&self.parts_root, key)))
            .filter(|(_, directory)| contains_parquet(directory))
            .try_for_each(|(key, directory)| {
                write_partition_manifest(&*self.db.reader()?, &directory, key)
            })
    }
}

fn partition_directory(parts_root: &Path, date: &str) -> PathBuf {
    parts_root.join(format!("date={date}"))
}

fn contains_parquet(root: &Path) -> bool {
    std::fs::read_dir(root).is_ok_and(|entries| {
        entries.filter_map(std::result::Result::ok).any(|entry| {
            let path = entry.path();
            path.extension()
                .is_some_and(|extension| extension == "parquet")
                || path.is_dir() && contains_parquet(&path)
        })
    })
}

fn sql_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn search_value(value: crate::search::LogSearchValue) -> Value {
    match value {
        crate::search::LogSearchValue::Text(text) => Value::Text(text),
        crate::search::LogSearchValue::Number(number) => Value::Double(number),
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}
