use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use duckdb::params;

use super::ingestion::{insert_log, parse_service_source};
use super::{Db, duckdb_i64_or_zero, now_ms};
use crate::logs::{LogEntry, LogOrigin};

fn migration_progress(db: &Db, source: &str, table: &str) -> Result<i64> {
    let conn = db.reader()?;
    duckdb_i64_or_zero(conn.query_row(
        "SELECT last_rowid FROM migration_progress WHERE source_path=? AND table_name=?",
        params![source, table],
        |r| r.get(0),
    ))
}

fn migrate_log_tier(
    db: &Db,
    service: bool,
    rows: &[(i64, LogEntry)],
    source: &str,
    batch_hi: i64,
) -> Result<usize> {
    let mut conn = db.writer()?;
    let tx = conn.transaction()?;
    let current = duckdb_i64_or_zero(tx.query_row(
        "SELECT last_rowid FROM migration_progress WHERE source_path=? AND table_name='logs'",
        params![source],
        |r| r.get::<_, i64>(0),
    ))?;
    let mut inserted = 0;
    for (rowid, entry) in rows {
        if *rowid <= current {
            continue;
        }
        let is_service = matches!(entry.origin, LogOrigin::Service | LogOrigin::Build)
            && parse_service_source(&entry.source).is_some();
        if is_service == service {
            insert_log(&tx, service, entry)?;
            inserted += 1;
        }
    }
    tx.execute(
        r#"
            INSERT INTO migration_progress
            VALUES (?, 'logs', ?)
            ON CONFLICT(source_path, table_name) DO UPDATE
            SET last_rowid = greatest(
                migration_progress.last_rowid,
                excluded.last_rowid
            )
        "#,
        params![source, batch_hi],
    )?;
    tx.commit()?;
    Ok(inserted)
}

pub(super) fn migrate_sqlite_inner(
    source: &Path,
    service: &Db,
    system: &Db,
    metrics: &Db,
) -> Result<usize> {
    use rusqlite::OpenFlags;
    let source_key = source
        .canonicalize()
        .unwrap_or_else(|_| source.to_path_buf())
        .to_string_lossy()
        .to_string();
    let conn = rusqlite::Connection::open_with_flags(
        source,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    let has_logs: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='logs')",
        [],
        |r| r.get(0),
    )?;
    let mut imported = 0;
    if has_logs {
        loop {
            let from = migration_progress(service, &source_key, "logs")?.min(migration_progress(
                system,
                &source_key,
                "logs",
            )?);
            let rows = {
                let mut stmt = conn.prepare(
                    r#"
                        SELECT
                            rowid,
                            seq,
                            ts,
                            level,
                            stream,
                            text,
                            source,
                            origin,
                            tags,
                            attributes
                        FROM logs
                        WHERE rowid > ?
                        ORDER BY rowid
                        LIMIT 5000
                    "#,
                )?;
                let rows = stmt.query_map(rusqlite::params![from], |r| {
                    let tags: String = r.get(8)?;
                    let attrs: String = r.get(9)?;
                    let origin: String = r.get(7)?;
                    Ok((
                        r.get(0)?,
                        LogEntry {
                            seq: r.get(1)?,
                            ts: r.get(2)?,
                            level: r.get::<_, String>(3)?.into(),
                            stream: r.get::<_, String>(4)?.into(),
                            text: r.get(5)?,
                            source: r.get::<_, String>(6)?.into(),
                            origin: match origin.as_str() {
                                "build" => LogOrigin::Build,
                                "service" => LogOrigin::Service,
                                _ => LogOrigin::System,
                            },
                            tags: Arc::new(
                                serde_json::from_str(&tags)
                                    .unwrap_or(serde_json::Value::Array(vec![])),
                            ),
                            attrs: serde_json::from_str(&attrs).unwrap_or_default(),
                        },
                    ))
                })?;
                rows.collect::<rusqlite::Result<Vec<_>>>()?
            };
            let Some(batch_hi) = rows.last().map(|r| r.0) else {
                break;
            };
            imported += migrate_log_tier(service, true, &rows, &source_key, batch_hi)?;
            imported += migrate_log_tier(system, false, &rows, &source_key, batch_hi)?;
        }
    }
    imported += migrate_sqlite_metrics(&conn, metrics, &source_key)?;
    Ok(imported)
}

fn migrate_sqlite_metrics(conn: &rusqlite::Connection, db: &Db, source: &str) -> Result<usize> {
    let mut imported = 0;
    let cutoff = now_ms() - 7 * 24 * 60 * 60 * 1000;
    let has_metrics: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='metrics')",
        [],
        |r| r.get(0),
    )?;
    if has_metrics {
        let progress = migration_progress(db, source, "metrics")?;
        let mut stmt = conn.prepare(
            r#"
                SELECT
                    rowid,
                    ts,
                    source,
                    cpu_percent,
                    memory_bytes,
                    memory_limit_bytes,
                    net_rx_bytes,
                    net_tx_bytes
                FROM metrics
                WHERE rowid > ?
                  AND ts >= ?
                ORDER BY rowid
            "#,
        )?;
        let rows = stmt
            .query_map(rusqlite::params![progress, cutoff], |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    crate::metrics::MetricPoint {
                        ts: r.get(1)?,
                        source: r.get(2)?,
                        cpu_percent: r.get(3)?,
                        memory_bytes: r.get(4)?,
                        memory_limit_bytes: r.get(5)?,
                        net_rx_bytes: r.get(6)?,
                        net_tx_bytes: r.get(7)?,
                    },
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        if let Some(last) = rows.last().map(|r| r.0) {
            let mut target = db.writer()?;
            let tx = target.transaction()?;
            {
                let mut insert = tx.prepare("INSERT INTO metrics VALUES (?,?,?,?,?,?,?)")?;
                for (_, e) in &rows {
                    insert.execute(params![
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
            tx.execute(
                r#"
                    INSERT INTO migration_progress
                    VALUES (?, 'metrics', ?)
                    ON CONFLICT DO UPDATE
                    SET last_rowid = excluded.last_rowid
                "#,
                params![source, last],
            )?;
            tx.commit()?;
            imported += rows.len();
        }
    }
    let has_traffic: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='traffic_metrics')",
        [],
        |r| r.get(0),
    )?;
    if has_traffic {
        let progress = migration_progress(db, source, "traffic_metrics")?;
        let mut stmt = conn.prepare(
            r#"
                SELECT
                    rowid,
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
                WHERE rowid > ?
                  AND ts >= ?
                ORDER BY rowid
            "#,
        )?;
        let rows = stmt
            .query_map(rusqlite::params![progress, cutoff], |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    crate::metrics::TrafficPoint {
                        ts: r.get(1)?,
                        service_id: r.get(2)?,
                        deployment_id: r.get(3)?,
                        status_code: r.get::<_, i64>(4)? as u16,
                        method: r.get(5)?,
                        requests: r.get(6)?,
                        bytes_in: r.get(7)?,
                        bytes_out: r.get(8)?,
                        lat_le_1s: r.get(9)?,
                        lat_le_5s: r.get(10)?,
                        lat_le_10s: r.get(11)?,
                        lat_total: r.get(12)?,
                    },
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        if let Some(last) = rows.last().map(|r| r.0) {
            let mut target = db.writer()?;
            let tx = target.transaction()?;
            {
                let mut insert =
                    tx.prepare("INSERT INTO traffic_metrics VALUES (?,?,?,?,?,?,?,?,?,?,?,?)")?;
                for (_, e) in &rows {
                    insert.execute(params![
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
            tx.execute(
                r#"
                    INSERT INTO migration_progress
                    VALUES (?, 'traffic_metrics', ?)
                    ON CONFLICT DO UPDATE
                    SET last_rowid = excluded.last_rowid
                "#,
                params![source, last],
            )?;
            tx.commit()?;
            imported += rows.len();
        }
    }
    Ok(imported)
}
