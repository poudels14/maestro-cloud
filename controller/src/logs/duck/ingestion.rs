use std::collections::{BTreeMap, HashMap};

use anyhow::{Result, anyhow};
use duckdb::{Connection, params, params_from_iter, types::Value};

use super::{Db, IngestLogEntry, duckdb_i64_or_zero, now_ms};
use crate::logs::LogEntry;

pub(super) fn append_log_batch(db: &Db, service: bool, entries: &[IngestLogEntry]) -> Result<()> {
    if entries.is_empty() {
        return Ok(());
    }
    let mut conn = db.writer()?;
    let tx = conn.transaction()?;
    let mut offsets: HashMap<String, i64> = HashMap::new();
    for item in entries {
        if let (Some(node), Some(origin_seq)) = (&item.node_id, item.origin_seq) {
            let current = if let Some(value) = offsets.get(node) {
                *value
            } else {
                duckdb_i64_or_zero(tx.query_row(
                    "SELECT last_origin_seq FROM ingest_offsets WHERE node_id=?",
                    params![node],
                    |r| r.get(0),
                ))?
            };
            if origin_seq <= current {
                continue;
            }
            offsets.insert(node.clone(), origin_seq);
        }
        insert_log(&tx, service, &item.entry)?;
    }
    for (node, seq) in offsets {
        tx.execute(
            r#"
                INSERT INTO ingest_offsets
                VALUES (?, ?, ?)
                ON CONFLICT(node_id) DO UPDATE SET
                    last_origin_seq = greatest(
                        ingest_offsets.last_origin_seq,
                        excluded.last_origin_seq
                    ),
                    updated_at_ms = excluded.updated_at_ms
            "#,
            params![node, seq, now_ms()],
        )?;
    }
    tx.commit()?;
    Ok(())
}

pub(super) fn insert_log(conn: &Connection, service: bool, entry: &LogEntry) -> Result<i64> {
    let tags = entry
        .tags
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|v| v.as_str().map(str::to_owned))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let attrs = entry.attrs.iter().cloned().collect::<BTreeMap<_, _>>();
    let mut values = vec![Value::BigInt(entry.ts)];
    let columns = if service {
        let (sid, did, unit) = parse_service_source(&entry.source)
            .ok_or_else(|| anyhow!("invalid service log source {}", entry.source))?;
        values.extend([Value::Text(sid), Value::Text(did), Value::Text(unit)]);
        "ts,date,service_id,deployment_id,unit,origin,level,stream,text,tags,attributes"
    } else {
        values.push(Value::Text(entry.source.to_string()));
        "ts,date,source,origin,level,stream,text,tags,attributes"
    };
    values.extend([
        Value::Text(entry.origin.as_str().into()),
        Value::Text(entry.level.to_string()),
        Value::Text(entry.stream.to_string()),
        Value::Text(entry.text.clone()),
    ]);
    let tags_sql = if tags.is_empty() {
        "[]::VARCHAR[]".to_string()
    } else {
        format!("[{}]", vec!["?"; tags.len()].join(","))
    };
    values.extend(tags.into_iter().map(Value::Text));
    let attrs_sql = if attrs.is_empty() {
        "MAP([]::VARCHAR[], []::VARCHAR[])".to_string()
    } else {
        let placeholders = vec!["?"; attrs.len()].join(",");
        for key in attrs.keys() {
            values.push(Value::Text(key.clone()));
        }
        for value in attrs.values() {
            values.push(Value::Text(value.clone()));
        }
        format!("MAP([{placeholders}], [{placeholders}])")
    };
    let fixed = if service {
        "?,CAST(epoch_ms(?) AS DATE),?,?,?,?,?,?,?"
    } else {
        "?,CAST(epoch_ms(?) AS DATE),?,?,?,?,?"
    };
    // ts is bound twice: once as the stored value and once to derive the UTC date.
    values.insert(1, Value::BigInt(entry.ts));
    let sql = format!(
        "INSERT INTO logs ({columns}) VALUES ({fixed},{tags_sql},{attrs_sql}) RETURNING seq"
    );
    let seq = conn.query_row(&sql, params_from_iter(values.iter()), |row| row.get(0))?;
    Ok(seq)
}

pub(super) fn parse_service_source(source: &str) -> Option<(String, String, String)> {
    let mut parts = source.split('/');
    let sid = parts.next()?;
    let did = parts.next()?;
    let unit = parts.next()?;
    if sid.is_empty() || did.is_empty() || unit.is_empty() || parts.next().is_some() {
        return None;
    }
    Some((sid.into(), did.into(), unit.into()))
}
