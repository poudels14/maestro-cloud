use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Result;
use duckdb::{params, params_from_iter, types::Value};

use super::{Db, contains_parquet, hive_component, sql_lit};
use crate::logs::{
    LogEntry, LogHistogramBucket, LogHistogramGroupBy, LogOrigin, LogSearchQuery, LogSearchValue,
    SqlDialect, http_status_class_expression, sql_like_prefix,
};

#[derive(Clone, Copy)]
enum IngressTrafficScope<'a> {
    Service(&'a str),
    Cluster,
    Blocked,
}

pub(super) fn query_ingress_traffic(
    db: &Db,
    service_id: &str,
    from: i64,
    to: i64,
    limit: usize,
) -> Result<crate::logs::IngressTrafficBreakdown> {
    let scope = IngressTrafficScope::Service(service_id);
    Ok(crate::logs::IngressTrafficBreakdown {
        by_ip: query_ingress_dimension(db, scope, from, to, limit, "ip")?,
        by_path: query_ingress_dimension(db, scope, from, to, limit, "path")?,
    })
}

pub(super) fn query_cluster_ingress_traffic(
    db: &Db,
    from: i64,
    to: i64,
    limit: usize,
) -> Result<crate::logs::IngressTrafficBreakdown> {
    Ok(crate::logs::IngressTrafficBreakdown {
        by_ip: query_ingress_dimension(db, IngressTrafficScope::Cluster, from, to, limit, "ip")?,
        by_path: query_ingress_dimension(
            db,
            IngressTrafficScope::Cluster,
            from,
            to,
            limit,
            "path",
        )?,
    })
}

pub(super) fn query_blocked_ingress_traffic(
    db: &Db,
    from: i64,
    to: i64,
    limit: usize,
) -> Result<crate::logs::IngressTrafficBreakdown> {
    Ok(crate::logs::IngressTrafficBreakdown {
        by_ip: query_ingress_dimension(db, IngressTrafficScope::Blocked, from, to, limit, "ip")?,
        by_path: query_ingress_dimension(
            db,
            IngressTrafficScope::Blocked,
            from,
            to,
            limit,
            "path",
        )?,
    })
}

fn query_ingress_dimension(
    db: &Db,
    scope: IngressTrafficScope<'_>,
    from: i64,
    to: i64,
    limit: usize,
    dimension: &str,
) -> Result<Vec<crate::logs::TrafficBreakdownEntry>> {
    let router_filter = match scope {
        IngressTrafficScope::Service(_) => {
            "router = ? OR (starts_with(router, ?) AND ends_with(router, '@etcd'))"
        }
        IngressTrafficScope::Cluster => "NOT starts_with(router, ?)",
        IngressTrafficScope::Blocked => "starts_with(router, ?) AND ends_with(router, '@etcd')",
    };
    let sql = format!(
        r#"
            WITH grouped AS (
                SELECT value, status_code, sum(requests)::BIGINT AS requests,
                       max(last_seen_at_ms) AS last_seen_at_ms
                FROM ingress_traffic
                WHERE bucket_at_ms >= ? AND bucket_at_ms <= ?
                  AND dimension = ?
                  AND ({router_filter})
                GROUP BY value, status_code
            ),
            top_values AS (
                SELECT value, sum(requests)::BIGINT AS total, max(last_seen_at_ms) AS last_seen_at_ms
                FROM grouped
                GROUP BY value
                ORDER BY total DESC, last_seen_at_ms DESC, value ASC
                LIMIT ?
            )
            SELECT grouped.value, grouped.status_code, grouped.requests, grouped.last_seen_at_ms
            FROM grouped
            INNER JOIN top_values USING (value)
            ORDER BY top_values.total DESC, top_values.last_seen_at_ms DESC,
                     grouped.value ASC, grouped.status_code ASC
        "#
    );
    let mut values = vec![
        Value::BigInt(from - from.rem_euclid(60_000)),
        Value::BigInt(to),
        Value::Text(dimension.to_string()),
    ];
    match scope {
        IngressTrafficScope::Service(service_id) => values.extend([
            Value::Text(format!("{service_id}@etcd")),
            Value::Text(format!("{service_id}-aff-")),
        ]),
        IngressTrafficScope::Cluster | IngressTrafficScope::Blocked => {
            values.push(Value::Text(
                crate::deployment::ingress_blocklist::ROUTER_LABEL_PREFIX.to_string(),
            ));
        }
    }
    values.push(Value::BigInt(i64::try_from(limit).unwrap_or(i64::MAX)));
    let conn = db.reader()?;
    let mut statement = conn.prepare(&sql)?;
    let rows = statement.query_map(params_from_iter(values.iter()), |row| {
        let status_code = row.get::<_, i32>(1)?;
        let requests = row.get::<_, i64>(2)?;
        Ok(crate::logs::TrafficBreakdownEntry {
            value: row.get(0)?,
            status_code: u16::try_from(status_code).unwrap_or_default(),
            requests: u64::try_from(requests).unwrap_or_default(),
            last_seen_at_ms: row.get(3)?,
        })
    })?;
    Ok(rows.collect::<duckdb::Result<Vec<_>>>()?)
}

pub(super) fn query_logs(
    db: &Db,
    service: bool,
    prefix: Option<&str>,
    sources: Option<&[String]>,
    origin: Option<LogOrigin>,
    search: Option<&LogSearchQuery>,
    time_range: Option<(i64, i64)>,
    after: Option<i64>,
    before: Option<i64>,
    limit: usize,
    descending: bool,
    cold_globs: &[PathBuf],
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
            to_json(tags)::VARCHAR AS tags_json,
            to_json(attributes)::VARCHAR AS attributes_json
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
            to_json(tags)::VARCHAR AS tags_json,
            to_json(attributes)::VARCHAR AS attributes_json
        "#
    };
    let mut arms = vec![format!(
        r#"
            SELECT {projection}
            FROM logs
        "#
    )];
    if !cold_globs.is_empty() {
        let parquet_sources = cold_globs
            .iter()
            .map(|glob| sql_lit(&glob.to_string_lossy()))
            .collect::<Vec<_>>();
        let parquet_sources = if parquet_sources.len() == 1 {
            parquet_sources[0].clone()
        } else {
            format!("[{}]", parquet_sources.join(", "))
        };
        arms.push(format!(
            r#"
                SELECT {projection}
                FROM read_parquet(
                    {parquet_sources},
                    hive_partitioning = true,
                    union_by_name = true
                )
            "#,
        ));
    }
    let mut sql = format!("SELECT * FROM ({}) q WHERE true", arms.join(" UNION ALL "));
    let mut vals: Vec<Value> = Vec::new();
    if let Some(p) = prefix {
        sql.push_str(" AND source LIKE ? ESCAPE '\\'");
        vals.push(Value::Text(sql_like_prefix(p)));
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
    if let Some(search) = search {
        let compiled = search.compile(SqlDialect::DuckDb);
        sql.push_str(" AND ");
        sql.push_str(&compiled.sql);
        vals.extend(compiled.values.into_iter().map(|value| match value {
            LogSearchValue::Text(value) => Value::Text(value),
            LogSearchValue::Number(value) => Value::Double(value),
        }));
    }
    if let Some((from, to)) = time_range {
        sql.push_str(" AND ts >= ? AND ts < ?");
        vals.extend([Value::BigInt(from), Value::BigInt(to)]);
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

#[allow(clippy::too_many_arguments)]
pub(super) fn query_log_histogram(
    db: &Db,
    service: bool,
    prefix: Option<&str>,
    sources: Option<&[String]>,
    origin: Option<LogOrigin>,
    search: Option<&LogSearchQuery>,
    from: i64,
    to: i64,
    bucket_ms: i64,
    group_by: LogHistogramGroupBy,
    cold_globs: &[PathBuf],
) -> Result<Vec<LogHistogramBucket>> {
    if bucket_ms <= 0 {
        return Err(anyhow::anyhow!("log histogram bucket must be positive"));
    }
    let projection = if service {
        r#"
            ts,
            level,
            text,
            service_id || '/' || deployment_id || '/' || unit AS source,
            origin,
            to_json(tags)::VARCHAR AS tags_json,
            to_json(attributes)::VARCHAR AS attributes_json
        "#
    } else {
        r#"
            ts,
            level,
            text,
            source,
            origin,
            to_json(tags)::VARCHAR AS tags_json,
            to_json(attributes)::VARCHAR AS attributes_json
        "#
    };
    let mut arms = vec![format!("SELECT {projection} FROM logs")];
    if !cold_globs.is_empty() {
        let parquet_sources = cold_globs
            .iter()
            .map(|glob| sql_lit(&glob.to_string_lossy()))
            .collect::<Vec<_>>();
        let parquet_sources = if parquet_sources.len() == 1 {
            parquet_sources[0].clone()
        } else {
            format!("[{}]", parquet_sources.join(", "))
        };
        arms.push(format!(
            r#"
                SELECT {projection}
                FROM read_parquet(
                    {parquet_sources},
                    hive_partitioning = true,
                    union_by_name = true
                )
            "#,
        ));
    }
    let group_expr = match group_by {
        LogHistogramGroupBy::Level => "lower(level)".to_string(),
        LogHistogramGroupBy::HttpStatusClass => http_status_class_expression(SqlDialect::DuckDb),
    };
    let mut sql = format!(
        "SELECT ts - (ts % ?) AS bucket_at_ms, {group_expr} AS grp,
                count(*)::BIGINT AS count
         FROM ({}) q WHERE true",
        arms.join(" UNION ALL ")
    );
    if group_by == LogHistogramGroupBy::HttpStatusClass {
        sql.push_str(&format!(" AND {group_expr} IS NOT NULL"));
    }
    let mut values = vec![Value::BigInt(bucket_ms)];
    if let Some(prefix) = prefix {
        sql.push_str(" AND source LIKE ? ESCAPE '\\'");
        values.push(Value::Text(sql_like_prefix(prefix)));
    }
    if let Some(sources) = sources {
        if sources.is_empty() {
            return Ok(Vec::new());
        }
        sql.push_str(&format!(
            " AND source IN ({})",
            vec!["?"; sources.len()].join(",")
        ));
        values.extend(sources.iter().cloned().map(Value::Text));
    }
    if let Some(origin) = origin {
        sql.push_str(" AND origin = ?");
        values.push(Value::Text(origin.as_str().into()));
    }
    if let Some(search) = search {
        let compiled = search.compile(SqlDialect::DuckDb);
        sql.push_str(" AND ");
        sql.push_str(&compiled.sql);
        values.extend(compiled.values.into_iter().map(|value| match value {
            LogSearchValue::Text(value) => Value::Text(value),
            LogSearchValue::Number(value) => Value::Double(value),
        }));
    }
    sql.push_str(
        " AND ts >= ? AND ts < ?
         GROUP BY bucket_at_ms, grp ORDER BY bucket_at_ms, grp",
    );
    values.extend([Value::BigInt(from), Value::BigInt(to)]);

    let conn = db.reader()?;
    let mut statement = conn.prepare(&sql)?;
    let rows = statement.query_map(params_from_iter(values.iter()), |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, i64>(2)?,
        ))
    })?;
    let mut buckets = std::collections::BTreeMap::<i64, LogHistogramBucket>::new();
    for row in rows {
        let (ts, level, count) = row?;
        let count = u64::try_from(count).unwrap_or_default();
        let bucket = buckets.entry(ts).or_insert_with(|| LogHistogramBucket {
            ts,
            count: 0,
            levels: std::collections::BTreeMap::new(),
        });
        bucket.count = bucket.count.saturating_add(count);
        *bucket.levels.entry(level).or_default() += count;
    }
    Ok(buckets.into_values().collect())
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

pub(super) fn service_globs_for_range(
    root: &Path,
    prefix: &str,
    from: i64,
    to: i64,
) -> Vec<PathBuf> {
    let dates = log_date_keys(from, to);
    let parts = prefix.trim_end_matches('/').split('/').collect::<Vec<_>>();
    match parts.as_slice() {
        [service_id, deployment_id] => {
            let base = root
                .join(format!("service_id={}", hive_component(service_id)))
                .join(format!("deployment_id={}", hive_component(deployment_id)));
            dates
                .into_iter()
                .filter_map(|date| {
                    parquet_glob_if_present(&base.join(format!("date={date}")), "part-*.parquet")
                })
                .collect()
        }
        [service_id] => {
            let base = root.join(format!("service_id={}", hive_component(service_id)));
            dates
                .into_iter()
                .filter(|date| service_date_has_parquet(&base, date))
                .map(|date| {
                    base.join("deployment_id=*")
                        .join(format!("date={date}"))
                        .join("part-*.parquet")
                })
                .collect()
        }
        _ => service_glob(root, prefix).into_iter().collect(),
    }
}

pub(super) fn system_globs_for_range(root: &Path, from: i64, to: i64) -> Vec<PathBuf> {
    log_date_keys(from, to)
        .into_iter()
        .filter_map(|date| {
            parquet_glob_if_present(&root.join(format!("date={date}")), "part-*.parquet")
        })
        .collect()
}

fn service_date_has_parquet(service_root: &Path, date: &str) -> bool {
    std::fs::read_dir(service_root)
        .ok()
        .into_iter()
        .flatten()
        .filter_map(|entry| entry.ok())
        .any(|entry| contains_parquet(&entry.path().join(format!("date={date}"))))
}

fn log_date_keys(from: i64, to: i64) -> Vec<String> {
    let Some(mut date) =
        chrono::DateTime::from_timestamp_millis(from).map(|value| value.date_naive())
    else {
        return Vec::new();
    };
    let Some(last) = chrono::DateTime::from_timestamp_millis(to.saturating_sub(1))
        .map(|value| value.date_naive())
    else {
        return Vec::new();
    };
    let mut dates = Vec::new();
    while date <= last {
        dates.push(date.format("%Y-%m-%d").to_string());
        let Some(next) = date.succ_opt() else {
            break;
        };
        date = next;
    }
    dates
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
