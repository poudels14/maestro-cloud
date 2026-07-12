use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Result;
use duckdb::{params, params_from_iter, types::Value};

use super::{Db, contains_parquet, hive_component, sql_lit};
use crate::logs::{LogEntry, LogOrigin};

pub(super) fn query_logs(
    db: &Db,
    service: bool,
    prefix: Option<&str>,
    sources: Option<&[String]>,
    origin: Option<LogOrigin>,
    after: Option<i64>,
    before: Option<i64>,
    limit: usize,
    descending: bool,
    cold_glob: Option<&Path>,
) -> Result<Vec<LogEntry>> {
    let projection = if service {
        r#"
            seq,
            ts,
            level,
            stream,
            text,
            service_id || '/' || deployment_id || '/' || unit AS source,
            origin,
            to_json(tags)::VARCHAR,
            to_json(attributes)::VARCHAR
        "#
    } else {
        r#"
            seq,
            ts,
            level,
            stream,
            text,
            source,
            origin,
            to_json(tags)::VARCHAR,
            to_json(attributes)::VARCHAR
        "#
    };
    let mut arms = vec![format!(
        r#"
            SELECT {projection}
            FROM logs
        "#
    )];
    if let Some(glob) = cold_glob {
        arms.push(format!(
            r#"
                SELECT {projection}
                FROM read_parquet(
                    {},
                    hive_partitioning = true,
                    union_by_name = true
                )
            "#,
            sql_lit(&glob.to_string_lossy())
        ));
    }
    let mut sql = format!("SELECT * FROM ({}) q WHERE true", arms.join(" UNION ALL "));
    let mut vals: Vec<Value> = Vec::new();
    if let Some(p) = prefix {
        sql.push_str(" AND source LIKE ?");
        vals.push(Value::Text(format!("{p}%")));
    }
    if let Some(s) = sources {
        sql.push_str(&format!(
            " AND source IN ({})",
            vec!["?"; s.len()].join(",")
        ));
        vals.extend(s.iter().cloned().map(Value::Text));
    }
    if let Some(o) = origin {
        sql.push_str(" AND origin=?");
        vals.push(Value::Text(o.as_str().into()));
    }
    if let Some(a) = after {
        sql.push_str(" AND seq>?");
        vals.push(Value::BigInt(a));
    }
    if let Some(b) = before {
        sql.push_str(" AND seq<?");
        vals.push(Value::BigInt(b));
    }
    sql.push_str(if descending {
        " ORDER BY seq DESC"
    } else {
        " ORDER BY seq ASC"
    });
    sql.push_str(" LIMIT ?");
    vals.push(Value::BigInt(limit as i64));
    let conn = db.reader()?;
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map(params_from_iter(vals.iter()), row_to_entry)?;
    let mut out = rows.collect::<duckdb::Result<Vec<_>>>()?;
    if descending {
        out.reverse();
    }
    Ok(out)
}

fn row_to_entry(row: &duckdb::Row<'_>) -> duckdb::Result<LogEntry> {
    let origin: String = row.get(6)?;
    let tags_json: String = row.get(7)?;
    let attrs_json: String = row.get(8)?;
    let attrs_value: serde_json::Value = serde_json::from_str(&attrs_json).unwrap_or_default();
    let attrs = attrs_value
        .as_object()
        .map(|m| {
            m.iter()
                .filter_map(|(k, v)| v.as_str().map(|v| (k.clone(), v.to_string())))
                .collect()
        })
        .unwrap_or_default();
    Ok(LogEntry {
        seq: row.get(0)?,
        ts: row.get(1)?,
        level: row.get::<_, String>(2)?.into(),
        stream: row.get::<_, String>(3)?.into(),
        text: row.get(4)?,
        source: row.get::<_, String>(5)?.into(),
        origin: match origin.as_str() {
            "build" => LogOrigin::Build,
            "service" => LogOrigin::Service,
            _ => LogOrigin::System,
        },
        tags: Arc::new(
            serde_json::from_str(&tags_json).unwrap_or(serde_json::Value::Array(vec![])),
        ),
        attrs,
    })
}

pub(super) fn service_glob(root: &Path, prefix: &str) -> Option<PathBuf> {
    let parts = prefix.trim_end_matches('/').split('/').collect::<Vec<_>>();
    let (base, suffix) = match parts.as_slice() {
        [sid, did] => (
            root.join(format!("service_id={}", hive_component(sid)))
                .join(format!("deployment_id={}", hive_component(did))),
            "date=*/part-*.parquet",
        ),
        [sid] => (
            root.join(format!("service_id={}", hive_component(sid))),
            "deployment_id=*/date=*/part-*.parquet",
        ),
        _ => (
            root.to_path_buf(),
            "service_id=*/deployment_id=*/date=*/part-*.parquet",
        ),
    };
    parquet_glob_if_present(&base, suffix)
}
pub(super) fn parquet_glob_if_present(root: &Path, suffix: &str) -> Option<PathBuf> {
    if contains_parquet(root) {
        Some(root.join(suffix))
    } else {
        None
    }
}
pub(super) fn cold_tier_has_seq_after(db: &Db, tier: &str, after: i64) -> Result<bool> {
    let conn = db.reader()?;
    let max_seq: i64 = conn.query_row(
        "SELECT coalesce(max(seq_hi), 0) FROM partition_state WHERE tier=?",
        params![tier],
        |row| row.get(0),
    )?;
    Ok(max_seq > after)
}
