use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result, anyhow, bail};
use duckdb::{params, params_from_iter, types::Value};

use super::store::{LogEntry, LogOrigin};

mod compat;
mod db;
mod ingestion;
mod migration;
mod query;
mod rollover;

pub use compat::TelemetryStore;
use db::Db;
use ingestion::{append_log_batch, parse_service_source};
use migration::migrate_sqlite_inner;
use query::{cold_tier_has_seq_after, parquet_glob_if_present, query_logs, service_glob};
use rollover::{remove_stale_exports, rollover_service, rollover_system, write_partition_manifest};

const SERVICE_SCHEMA: &str = r#"
CREATE SEQUENCE IF NOT EXISTS log_seq START 1;
CREATE TABLE IF NOT EXISTS logs (
    seq BIGINT PRIMARY KEY DEFAULT nextval('log_seq'),
    ts BIGINT NOT NULL,
    date DATE NOT NULL,
    service_id VARCHAR NOT NULL,
    deployment_id VARCHAR NOT NULL,
    unit VARCHAR NOT NULL,
    origin VARCHAR NOT NULL,
    level VARCHAR NOT NULL,
    stream VARCHAR NOT NULL,
    text VARCHAR NOT NULL,
    tags VARCHAR[] NOT NULL,
    attributes MAP(VARCHAR, VARCHAR) NOT NULL
);
CREATE INDEX IF NOT EXISTS logs_identity_seq ON logs(service_id, deployment_id, seq);
CREATE TABLE IF NOT EXISTS ingest_offsets (
    node_id VARCHAR PRIMARY KEY,
    last_origin_seq BIGINT NOT NULL,
    updated_at_ms BIGINT NOT NULL
);
CREATE TABLE IF NOT EXISTS partition_state (
    tier VARCHAR NOT NULL,
    partition_key VARCHAR NOT NULL,
    state VARCHAR NOT NULL,
    row_count BIGINT NOT NULL,
    seq_lo BIGINT NOT NULL,
    seq_hi BIGINT NOT NULL,
    sha256 VARCHAR,
    updated_at_ms BIGINT NOT NULL,
    PRIMARY KEY (tier, partition_key, seq_lo)
);
CREATE TABLE IF NOT EXISTS migration_progress (
    source_path VARCHAR NOT NULL,
    table_name VARCHAR NOT NULL,
    last_rowid BIGINT NOT NULL,
    PRIMARY KEY (source_path, table_name)
);
"#;

const SYSTEM_SCHEMA: &str = r#"
CREATE SEQUENCE IF NOT EXISTS log_seq START 1;
CREATE TABLE IF NOT EXISTS logs (
    seq BIGINT PRIMARY KEY DEFAULT nextval('log_seq'),
    ts BIGINT NOT NULL,
    date DATE NOT NULL,
    source VARCHAR NOT NULL,
    origin VARCHAR NOT NULL,
    level VARCHAR NOT NULL,
    stream VARCHAR NOT NULL,
    text VARCHAR NOT NULL,
    tags VARCHAR[] NOT NULL,
    attributes MAP(VARCHAR, VARCHAR) NOT NULL
);
CREATE INDEX IF NOT EXISTS logs_source_seq ON logs(source, seq);
CREATE TABLE IF NOT EXISTS ingest_offsets (
    node_id VARCHAR PRIMARY KEY,
    last_origin_seq BIGINT NOT NULL,
    updated_at_ms BIGINT NOT NULL
);
CREATE TABLE IF NOT EXISTS partition_state (
    tier VARCHAR NOT NULL,
    partition_key VARCHAR NOT NULL,
    state VARCHAR NOT NULL,
    row_count BIGINT NOT NULL,
    seq_lo BIGINT NOT NULL,
    seq_hi BIGINT NOT NULL,
    sha256 VARCHAR,
    updated_at_ms BIGINT NOT NULL,
    PRIMARY KEY (tier, partition_key, seq_lo)
);
CREATE TABLE IF NOT EXISTS migration_progress (
    source_path VARCHAR NOT NULL,
    table_name VARCHAR NOT NULL,
    last_rowid BIGINT NOT NULL,
    PRIMARY KEY (source_path, table_name)
);
"#;

const METRICS_SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS metrics (
    ts BIGINT NOT NULL,
    source VARCHAR NOT NULL,
    cpu_percent DOUBLE NOT NULL,
    memory_bytes BIGINT NOT NULL,
    memory_limit_bytes BIGINT NOT NULL,
    net_rx_bytes BIGINT NOT NULL,
    net_tx_bytes BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS metrics_source_ts ON metrics(source, ts);
CREATE TABLE IF NOT EXISTS traffic_metrics (
    ts BIGINT NOT NULL,
    service_id VARCHAR NOT NULL,
    deployment_id VARCHAR,
    status_code INTEGER NOT NULL,
    method VARCHAR NOT NULL,
    requests BIGINT NOT NULL,
    bytes_in BIGINT NOT NULL,
    bytes_out BIGINT NOT NULL,
    lat_le_1s BIGINT NOT NULL,
    lat_le_5s BIGINT NOT NULL,
    lat_le_10s BIGINT NOT NULL,
    lat_total BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS traffic_service_ts ON traffic_metrics(service_id, ts);
CREATE TABLE IF NOT EXISTS migration_progress (
    source_path VARCHAR NOT NULL,
    table_name VARCHAR NOT NULL,
    last_rowid BIGINT NOT NULL,
    PRIMARY KEY (source_path, table_name)
);
"#;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct IngestLogEntry {
    #[serde(flatten)]
    pub entry: LogEntry,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin_seq: Option<i64>,
}

impl From<LogEntry> for IngestLogEntry {
    fn from(entry: LogEntry) -> Self {
        Self {
            entry,
            node_id: None,
            origin_seq: None,
        }
    }
}

/// Probe-owned DuckDB telemetry databases and their sealed Parquet partitions.
pub struct DuckLogStore {
    service: Arc<Db>,
    system: Arc<Db>,
    metrics: Arc<Db>,
    parts_root: PathBuf,
    rollover_lock: Arc<Mutex<()>>,
}

#[derive(Debug, Clone)]
pub struct BackupPartition {
    pub tier: String,
    pub partition_key: String,
    pub files: Vec<PathBuf>,
    pub seq_los: Vec<i64>,
}

impl DuckLogStore {
    pub fn open(data_root: &Path) -> Result<Self> {
        let db_root = data_root.join("duckdb");
        let parts_root = data_root.join("parts");
        std::fs::create_dir_all(&parts_root)?;
        remove_stale_exports(&parts_root)?;
        let store = Self {
            service: Arc::new(Db::open(
                &db_root.join("service-logs.duckdb"),
                SERVICE_SCHEMA,
            )?),
            system: Arc::new(Db::open(
                &db_root.join("system-logs.duckdb"),
                SYSTEM_SCHEMA,
            )?),
            metrics: Arc::new(Db::open(&db_root.join("metrics.duckdb"), METRICS_SCHEMA)?),
            parts_root,
            rollover_lock: Arc::new(Mutex::new(())),
        };
        store.rebuild_partition_manifests()?;
        Ok(store)
    }

    fn rebuild_partition_manifests(&self) -> Result<()> {
        for (db, tier) in [(&self.service, "service"), (&self.system, "system")] {
            let keys = {
                let conn = db.reader()?;
                let mut stmt = conn.prepare(
                    r#"
                        SELECT DISTINCT partition_key
                        FROM partition_state
                        WHERE tier = ?
                        ORDER BY partition_key
                    "#,
                )?;
                let rows = stmt.query_map(params![tier], |row| row.get::<_, String>(0))?;
                rows.collect::<duckdb::Result<Vec<_>>>()?
            };
            for key in keys {
                let directory = partition_directory(&self.parts_root, tier, &key)?;
                if !contains_parquet(&directory) {
                    continue;
                }
                let conn = db.reader()?;
                write_partition_manifest(&conn, &directory, tier, &key)?;
            }
        }
        Ok(())
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub async fn append(&self, entries: &[LogEntry]) -> Result<()> {
        let entries = entries
            .iter()
            .cloned()
            .map(IngestLogEntry::from)
            .collect::<Vec<_>>();
        self.append_ingest(&entries).await
    }

    pub async fn append_ingest(&self, entries: &[IngestLogEntry]) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let mut service_entries = Vec::new();
        let mut system_entries = Vec::new();
        let mut malformed_service_sources = 0;
        for entry in entries {
            match entry.entry.origin {
                LogOrigin::Service | LogOrigin::Build
                    if parse_service_source(&entry.entry.source).is_some() =>
                {
                    service_entries.push(entry.clone());
                }
                _ => {
                    if matches!(entry.entry.origin, LogOrigin::Service | LogOrigin::Build) {
                        malformed_service_sources += 1;
                    }
                    system_entries.push(entry.clone());
                }
            }
        }
        if malformed_service_sources > 0 {
            eprintln!(
                "routed {malformed_service_sources} logs with malformed service sources to the system tier"
            );
        }
        let service = self.service.clone();
        let system = self.system.clone();
        tokio::task::spawn_blocking(move || -> Result<()> {
            append_log_batch(&service, true, &service_entries)?;
            append_log_batch(&system, false, &system_entries)?;
            Ok(())
        })
        .await??;
        Ok(())
    }

    /// Resumably import a legacy SQLite archive without modifying it. Progress
    /// is committed in the same DuckDB transaction as each imported batch.
    pub async fn migrate_sqlite(&self, source: &Path) -> Result<usize> {
        if !source.exists() {
            return Ok(0);
        }
        let service = self.service.clone();
        let system = self.system.clone();
        let metrics = self.metrics.clone();
        let source = source.to_path_buf();
        tokio::task::spawn_blocking(move || {
            migrate_sqlite_inner(&source, &service, &system, &metrics)
        })
        .await?
    }

    pub async fn read_tail_by_prefix_origin(
        &self,
        prefix: &str,
        origin: Option<LogOrigin>,
        limit: usize,
    ) -> Result<Vec<LogEntry>> {
        self.read_service(prefix, origin, None, None, limit, true)
            .await
    }

    pub async fn read_after_by_prefix_origin(
        &self,
        prefix: &str,
        origin: Option<LogOrigin>,
        after: i64,
        limit: usize,
    ) -> Result<Vec<LogEntry>> {
        self.read_service(prefix, origin, Some(after), None, limit, false)
            .await
    }

    pub async fn read_before_by_prefix_origin(
        &self,
        prefix: &str,
        origin: Option<LogOrigin>,
        before: i64,
        limit: usize,
    ) -> Result<Vec<LogEntry>> {
        self.read_service(prefix, origin, None, Some(before), limit, true)
            .await
    }

    async fn read_service(
        &self,
        prefix: &str,
        origin: Option<LogOrigin>,
        after: Option<i64>,
        before: Option<i64>,
        limit: usize,
        descending: bool,
    ) -> Result<Vec<LogEntry>> {
        let db = self.service.clone();
        let parts = self.parts_root.join("service-logs");
        let prefix = prefix.to_string();
        tokio::task::spawn_blocking(move || {
            let _visibility = db.read_parquet()?;
            let cold_glob = match after {
                Some(cursor) if !cold_tier_has_seq_after(&db, "service", cursor)? => None,
                _ => service_glob(&parts, &prefix),
            };
            query_logs(
                &db,
                true,
                Some(&prefix),
                None,
                origin,
                after,
                before,
                limit,
                descending,
                cold_glob.as_deref(),
            )
        })
        .await?
    }

    pub async fn read_tail(&self, source: &str, limit: usize) -> Result<Vec<LogEntry>> {
        self.read_system(&[source], None, None, limit, true).await
    }

    pub async fn read_after_for_source(
        &self,
        source: &str,
        after: i64,
        limit: usize,
    ) -> Result<Vec<LogEntry>> {
        self.read_system(&[source], Some(after), None, limit, false)
            .await
    }

    pub async fn read_before_for_source(
        &self,
        source: &str,
        before: i64,
        limit: usize,
    ) -> Result<Vec<LogEntry>> {
        self.read_system(&[source], None, Some(before), limit, true)
            .await
    }

    pub async fn read_tail_sources(&self, sources: &[&str], limit: usize) -> Result<Vec<LogEntry>> {
        self.read_system(sources, None, None, limit, true).await
    }

    pub async fn read_after_sources(
        &self,
        sources: &[&str],
        after: i64,
        limit: usize,
    ) -> Result<Vec<LogEntry>> {
        self.read_system(sources, Some(after), None, limit, false)
            .await
    }

    pub async fn read_before_sources(
        &self,
        sources: &[&str],
        before: i64,
        limit: usize,
    ) -> Result<Vec<LogEntry>> {
        self.read_system(sources, None, Some(before), limit, true)
            .await
    }

    async fn read_system(
        &self,
        sources: &[&str],
        after: Option<i64>,
        before: Option<i64>,
        limit: usize,
        descending: bool,
    ) -> Result<Vec<LogEntry>> {
        let db = self.system.clone();
        let sources = sources.iter().map(|s| (*s).to_string()).collect::<Vec<_>>();
        let parts = self.parts_root.join("system-logs");
        tokio::task::spawn_blocking(move || {
            let _visibility = db.read_parquet()?;
            let glob = match after {
                Some(cursor) if !cold_tier_has_seq_after(&db, "system", cursor)? => None,
                _ => parquet_glob_if_present(&parts, "date=*/part-*.parquet"),
            };
            query_logs(
                &db,
                false,
                None,
                Some(&sources),
                None,
                after,
                before,
                limit,
                descending,
                glob.as_deref(),
            )
        })
        .await?
    }

    pub async fn append_metrics(&self, entries: &[crate::metrics::MetricPoint]) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let db = self.metrics.clone();
        let entries = entries.to_vec();
        tokio::task::spawn_blocking(move || {
            let mut conn = db.writer()?;
            let tx = conn.transaction()?;
            {
                let mut stmt = tx.prepare("INSERT INTO metrics VALUES (?, ?, ?, ?, ?, ?, ?)")?;
                for e in entries {
                    stmt.execute(params![
                        e.ts,
                        e.source,
                        e.cpu_percent,
                        e.memory_bytes,
                        e.memory_limit_bytes,
                        e.net_rx_bytes,
                        e.net_tx_bytes
                    ])?;
                }
            }
            tx.commit()?;
            Ok(())
        })
        .await?
    }

    pub async fn read_metrics(
        &self,
        source: &str,
        from: i64,
        to: i64,
    ) -> Result<Vec<crate::metrics::MetricPoint>> {
        self.query_metrics(source, false, from, to).await
    }

    pub async fn read_metrics_by_prefix(
        &self,
        prefix: &str,
        from: i64,
        to: i64,
    ) -> Result<Vec<crate::metrics::MetricPoint>> {
        self.query_metrics(prefix, true, from, to).await
    }

    async fn query_metrics(
        &self,
        source: &str,
        prefix: bool,
        from: i64,
        to: i64,
    ) -> Result<Vec<crate::metrics::MetricPoint>> {
        let db = self.metrics.clone();
        let source = if prefix {
            format!("{source}%")
        } else {
            source.to_string()
        };
        tokio::task::spawn_blocking(move || {
            let conn = db.reader()?;
            let op = if prefix { "LIKE" } else { "=" };
            let sql = format!(
                r#"
                    SELECT
                        ts,
                        source,
                        cpu_percent,
                        memory_bytes,
                        memory_limit_bytes,
                        net_rx_bytes,
                        net_tx_bytes
                    FROM metrics
                    WHERE source {op} ?
                      AND ts >= ?
                      AND ts <= ?
                    ORDER BY ts
                "#
            );
            let mut stmt = conn.prepare(&sql)?;
            let rows = stmt.query_map(params![source, from, to], |r| {
                Ok(crate::metrics::MetricPoint {
                    ts: r.get(0)?,
                    source: r.get(1)?,
                    cpu_percent: r.get(2)?,
                    memory_bytes: r.get(3)?,
                    memory_limit_bytes: r.get(4)?,
                    net_rx_bytes: r.get(5)?,
                    net_tx_bytes: r.get(6)?,
                })
            })?;
            Ok(rows.collect::<duckdb::Result<Vec<_>>>()?)
        })
        .await?
    }

    pub async fn append_traffic_metrics(
        &self,
        entries: &[crate::metrics::TrafficPoint],
    ) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let db = self.metrics.clone();
        let entries = entries.to_vec();
        tokio::task::spawn_blocking(move || {
            let mut conn = db.writer()?;
            let tx = conn.transaction()?;
            {
                let mut stmt =
                    tx.prepare("INSERT INTO traffic_metrics VALUES (?,?,?,?,?,?,?,?,?,?,?,?)")?;
                for e in entries {
                    stmt.execute(params![
                        e.ts,
                        e.service_id,
                        e.deployment_id,
                        e.status_code as i32,
                        e.method,
                        e.requests,
                        e.bytes_in,
                        e.bytes_out,
                        e.lat_le_1s,
                        e.lat_le_5s,
                        e.lat_le_10s,
                        e.lat_total
                    ])?;
                }
            }
            tx.commit()?;
            Ok(())
        })
        .await?
    }

    pub async fn read_traffic_metrics(
        &self,
        service_id: &str,
        from: i64,
        to: i64,
    ) -> Result<Vec<crate::metrics::TrafficPoint>> {
        let db = self.metrics.clone();
        let service_id = service_id.to_string();
        tokio::task::spawn_blocking(move || {
            let conn = db.reader()?;
            let mut stmt = conn.prepare(
                r#"
                    SELECT
                        ts,
                        service_id,
                        deployment_id,
                        status_code,
                        method,
                        requests,
                        bytes_in,
                        bytes_out,
                        lat_le_1s,
                        lat_le_5s,
                        lat_le_10s,
                        lat_total
                    FROM traffic_metrics
                    WHERE service_id = ?
                      AND ts >= ?
                      AND ts <= ?
                    ORDER BY ts
                "#,
            )?;
            let rows = stmt.query_map(params![service_id, from, to], |r| {
                Ok(crate::metrics::TrafficPoint {
                    ts: r.get(0)?,
                    service_id: r.get(1)?,
                    deployment_id: r.get(2)?,
                    status_code: r.get::<_, i32>(3)? as u16,
                    method: r.get(4)?,
                    requests: r.get(5)?,
                    bytes_in: r.get(6)?,
                    bytes_out: r.get(7)?,
                    lat_le_1s: r.get(8)?,
                    lat_le_5s: r.get(9)?,
                    lat_le_10s: r.get(10)?,
                    lat_total: r.get(11)?,
                })
            })?;
            Ok(rows.collect::<duckdb::Result<Vec<_>>>()?)
        })
        .await?
    }

    pub async fn cleanup_old_metrics(&self, max_age_ms: i64) -> Result<usize> {
        let db = self.metrics.clone();
        tokio::task::spawn_blocking(move || {
            let conn = db.writer()?;
            let cutoff = now_ms() - max_age_ms;
            let count: i64 = conn.query_row(
                r#"
                    SELECT
                        (SELECT count(*) FROM metrics WHERE ts < ?)
                        + (SELECT count(*) FROM traffic_metrics WHERE ts < ?)
                "#,
                params![cutoff, cutoff],
                |r| r.get(0),
            )?;
            conn.execute("DELETE FROM metrics WHERE ts < ?", params![cutoff])?;
            conn.execute("DELETE FROM traffic_metrics WHERE ts < ?", params![cutoff])?;
            conn.execute_batch("CHECKPOINT")?;
            Ok(count as usize)
        })
        .await?
    }

    /// Seal every UTC date before today into verified Parquet files.
    pub async fn rollover(&self) -> Result<usize> {
        let service = self.service.clone();
        let system = self.system.clone();
        let parts = self.parts_root.clone();
        let rollover_lock = self.rollover_lock.clone();
        tokio::task::spawn_blocking(move || {
            let _rollover = rollover_lock
                .lock()
                .map_err(|_| anyhow!("DuckDB rollover mutex poisoned"))?;
            let mut count = rollover_service(&service, &parts.join("service-logs"))?;
            count += rollover_system(&system, &parts.join("system-logs"))?;
            Ok(count)
        })
        .await?
    }

    pub async fn pending_backups(&self) -> Result<Vec<BackupPartition>> {
        let service = self.service.clone();
        let system = self.system.clone();
        let parts_root = self.parts_root.clone();
        tokio::task::spawn_blocking(move || {
            let mut pending = pending_backup_keys(&service, "service")?;
            pending.extend(pending_backup_keys(&system, "system")?);
            let mut partitions = Vec::with_capacity(pending.len());
            for (tier, partition_key, seq_los) in pending {
                let result = (|| -> Result<BackupPartition> {
                    let directory = partition_directory(&parts_root, &tier, &partition_key)?;
                    let mut files = std::fs::read_dir(&directory)
                        .with_context(|| format!("read backup partition {}", directory.display()))?
                        .filter_map(Result::ok)
                        .map(|entry| entry.path())
                        .filter(|path| {
                            path.extension().is_some_and(|ext| ext == "parquet")
                                || path.file_name().is_some_and(|name| name == "manifest.json")
                        })
                        .collect::<Vec<_>>();
                    // Parquet objects land first; manifest.json is the partition's
                    // commit marker and must be uploaded last.
                    files.sort_by(|left, right| {
                        let left_manifest = left.file_name().is_some_and(|n| n == "manifest.json");
                        let right_manifest =
                            right.file_name().is_some_and(|n| n == "manifest.json");
                        left_manifest
                            .cmp(&right_manifest)
                            .then_with(|| left.cmp(right))
                    });
                    Ok(BackupPartition {
                        tier,
                        partition_key,
                        files,
                        seq_los,
                    })
                })();
                match result {
                    Ok(partition) => partitions.push(partition),
                    Err(err) => eprintln!("skipping unavailable log backup partition: {err:#}"),
                }
            }
            Ok(partitions)
        })
        .await?
    }

    pub async fn mark_backed_up(
        &self,
        tier: &str,
        partition_key: &str,
        seq_los: &[i64],
    ) -> Result<()> {
        if seq_los.is_empty() {
            return Ok(());
        }
        let db = match tier {
            "service" => self.service.clone(),
            "system" => self.system.clone(),
            _ => bail!("unknown log tier {tier}"),
        };
        let tier = tier.to_string();
        let partition_key = partition_key.to_string();
        let seq_los = seq_los.to_vec();
        tokio::task::spawn_blocking(move || {
            let conn = db.writer()?;
            let placeholders = vec!["?"; seq_los.len()].join(",");
            let sql = format!(
                r#"
                    UPDATE partition_state
                    SET state = 'backed_up',
                        updated_at_ms = ?
                    WHERE tier = ?
                      AND partition_key = ?
                      AND seq_lo IN ({placeholders})
                "#
            );
            let mut values = vec![
                Value::BigInt(now_ms()),
                Value::Text(tier),
                Value::Text(partition_key),
            ];
            values.extend(seq_los.into_iter().map(Value::BigInt));
            conn.execute(&sql, params_from_iter(values.iter()))?;
            Ok(())
        })
        .await?
    }

    /// Explicit retention primitive: only fully backed-up partitions older
    /// than the requested UTC date are eligible for directory removal.
    pub async fn prune_backed_up_before(&self, cutoff: chrono::NaiveDate) -> Result<usize> {
        let service = self.service.clone();
        let system = self.system.clone();
        let parts_root = self.parts_root.clone();
        tokio::task::spawn_blocking(move || {
            let mut removed = prune_tier(&service, &parts_root, "service", cutoff)?;
            removed += prune_tier(&system, &parts_root, "system", cutoff)?;
            Ok(removed)
        })
        .await?
    }
}

fn prune_tier(db: &Db, parts_root: &Path, tier: &str, cutoff: chrono::NaiveDate) -> Result<usize> {
    let candidates = {
        let conn = db.reader()?;
        let mut stmt = conn.prepare(
            r#"
                SELECT
                    partition_key,
                    bool_and(state = 'backed_up')
                FROM partition_state
                WHERE tier = ?
                GROUP BY partition_key
            "#,
        )?;
        let rows = stmt.query_map(params![tier], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, bool>(1)?))
        })?;
        rows.collect::<duckdb::Result<Vec<_>>>()?
    };
    let mut removed = 0;
    for (key, fully_backed_up) in candidates {
        let date = key.rsplit('/').next().unwrap_or(&key);
        let Ok(date) = chrono::NaiveDate::parse_from_str(date, "%Y-%m-%d") else {
            continue;
        };
        if !fully_backed_up || date >= cutoff {
            continue;
        }
        let _visibility = db.write_parquet()?;
        let directory = partition_directory(parts_root, tier, &key)?;
        if directory.exists() {
            std::fs::remove_dir_all(&directory)?;
        }
        let conn = db.writer()?;
        conn.execute(
            "DELETE FROM partition_state WHERE tier=? AND partition_key=? AND state='backed_up'",
            params![tier, key],
        )?;
        removed += 1;
    }
    Ok(removed)
}

fn pending_backup_keys(db: &Db, tier: &str) -> Result<Vec<(String, String, Vec<i64>)>> {
    let conn = db.reader()?;
    let mut stmt = conn.prepare(
        r#"
            SELECT
                tier,
                partition_key,
                seq_lo
            FROM partition_state
            WHERE tier = ?
              AND state = 'exported'
            ORDER BY partition_key, seq_lo
        "#,
    )?;
    let rows = stmt
        .query_map(params![tier], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
            ))
        })?
        .collect::<duckdb::Result<Vec<_>>>()?;
    let mut grouped: Vec<(String, String, Vec<i64>)> = Vec::new();
    for (tier, key, seq_lo) in rows {
        if let Some((_, _, seq_los)) =
            grouped
                .last_mut()
                .filter(|(existing_tier, existing_key, _)| {
                    existing_tier == &tier && existing_key == &key
                })
        {
            seq_los.push(seq_lo);
        } else {
            grouped.push((tier, key, vec![seq_lo]));
        }
    }
    Ok(grouped)
}

fn partition_directory(parts_root: &Path, tier: &str, key: &str) -> Result<PathBuf> {
    match tier {
        "service" => {
            let mut pieces = key.splitn(3, '/');
            let service_id = pieces.next().filter(|value| !value.is_empty());
            let deployment_id = pieces.next().filter(|value| !value.is_empty());
            let date = pieces.next().filter(|value| !value.is_empty());
            match (service_id, deployment_id, date) {
                (Some(service_id), Some(deployment_id), Some(date)) => Ok(parts_root
                    .join("service-logs")
                    .join(format!("service_id={}", hive_component(service_id)))
                    .join(format!("deployment_id={}", hive_component(deployment_id)))
                    .join(format!("date={date}"))),
                _ => bail!("invalid service partition key {key}"),
            }
        }
        "system" => Ok(parts_root.join("system-logs").join(format!("date={key}"))),
        _ => bail!("unknown log tier {tier}"),
    }
}

fn contains_parquet(root: &Path) -> bool {
    let Ok(rd) = std::fs::read_dir(root) else {
        return false;
    };
    rd.filter_map(Result::ok).any(|e| {
        let p = e.path();
        p.extension().is_some_and(|x| x == "parquet") || p.is_dir() && contains_parquet(&p)
    })
}
fn hive_component(s: &str) -> String {
    s.replace('%', "%25")
        .replace('/', "%2F")
        .replace('=', "%3D")
}
fn sql_lit(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}
fn duckdb_i64_or_zero(result: duckdb::Result<i64>) -> Result<i64> {
    match result {
        Ok(value) => Ok(value),
        Err(duckdb::Error::QueryReturnedNoRows) => Ok(0),
        Err(err) => Err(err.into()),
    }
}
fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

#[cfg(test)]
#[path = "../../tests/logs/duck.rs"]
mod tests;
