use std::path::Path;

use duckdb::{params_from_iter, types::Value};

use super::{Db, LogQuery, StoredLog, contains_parquet, search_value, sql_literal};
use crate::error::Result;

const PROJECTION: &str = r#"
    seq,
    ts,
    replica_id,
    service_id,
    deployment_id,
    stream,
    level,
    line,
    message,
    attributes
"#;

pub(super) fn query_logs(db: &Db, parts_root: &Path, query: &LogQuery) -> Result<Vec<StoredLog>> {
    let union = log_source(parts_root);
    let mut sql = format!("SELECT {PROJECTION} FROM ({union}) q WHERE true");
    let mut values = Vec::new();
    if let Some(search) = &query.search {
        let compiled = search.compile();
        sql.push_str(" AND ");
        sql.push_str(&compiled.sql);
        values.extend(compiled.values.into_iter().map(search_value));
    }
    if let Some(before) = query.before {
        sql.push_str(" AND seq < ?");
        values.push(Value::BigInt(before));
    }
    if let Some(before_ts) = query.before_ts {
        sql.push_str(" AND ts < ?");
        values.push(Value::BigInt(before_ts));
    }
    if let Some(after_ts) = query.after_ts {
        sql.push_str(" AND ts >= ?");
        values.push(Value::BigInt(after_ts));
    }
    sql.push_str(" ORDER BY ts DESC, seq DESC LIMIT ?");
    values.push(Value::BigInt(
        i64::try_from(query.limit).unwrap_or(i64::MAX),
    ));
    let connection = db.reader()?;
    let mut statement = connection.prepare(&sql)?;
    let rows = statement.query_map(params_from_iter(values.iter()), |row| {
        Ok(StoredLog {
            seq: row.get(0)?,
            ts: row.get(1)?,
            replica_id: row.get(2)?,
            service_id: row.get(3)?,
            deployment_id: row.get(4)?,
            stream: row.get(5)?,
            level: row.get(6)?,
            line: row.get(7)?,
        })
    })?;
    Ok(rows.collect::<duckdb::Result<Vec<_>>>()?)
}

pub(super) fn log_source(parts_root: &Path) -> String {
    let hot = format!("SELECT {PROJECTION} FROM logs");
    match contains_parquet(parts_root) {
        true => {
            let glob = parts_root.join("date=*").join("part-*.parquet");
            format!(
                r#"
                    {hot}
                    UNION ALL
                    SELECT {PROJECTION}
                    FROM read_parquet(
                        {},
                        hive_partitioning = true,
                        union_by_name = true
                    )
                "#,
                sql_literal(&glob.to_string_lossy())
            )
        }
        false => hot,
    }
}
