use std::collections::BTreeMap;
use std::path::Path;

use duckdb::{Connection, params_from_iter, types::Value};
use logql::LogQuery;
use logs::{
    LogHistogramBucket, LogHistogramGroupBy, LogHistogramQuery, LogQueryScope, LogQueryStoreError,
    LogReadCursor, LogReadOrder, LogReadQuery, LogSequence, SequencedLogEntry,
};

use crate::duck_query_compiler::{
    CompiledPredicate, DuckDbLogQlCompiler, QueryValue, compile_histogram_status,
    compile_json_value,
};

pub(crate) fn query_logs(
    connection: &Connection,
    cold_root: &Path,
    query: &LogReadQuery,
) -> Result<Vec<SequencedLogEntry>, LogQueryStoreError> {
    let source = query_source(cold_root)?;
    let filters = query_filters(query.scope(), query.search(), query.from(), query.to())?;
    let mut sql = format!(
        "SELECT sequence, entry_json FROM ({}) AS stored_logs WHERE {}",
        source.sql, filters.sql
    );
    let mut values = source.values;
    values.extend(filters.values);
    if let Some(cursor) = query.cursor() {
        let sequence = match cursor {
            LogReadCursor::After(sequence) => {
                sql.push_str(" AND sequence > ?");
                sequence
            }
            LogReadCursor::Before(sequence) => {
                sql.push_str(" AND sequence < ?");
                sequence
            }
        };
        let sequence = i64::try_from(sequence.0)
            .map_err(|_| rejected("log query cursor exceeds the store sequence space"))?;
        values.push(QueryValue::Integer(sequence));
    }
    sql.push_str(match query.order() {
        LogReadOrder::OldestFirst => " ORDER BY sequence ASC",
        LogReadOrder::NewestFirst => " ORDER BY sequence DESC",
    });
    sql.push_str(" LIMIT ?");
    values.push(QueryValue::Integer(query.limit() as i64));

    let values = duck_values(values)?;
    let mut statement = connection
        .prepare(&sql)
        .map_err(unavailable("prepare log query"))?;
    let rows = statement
        .query_map(params_from_iter(values.iter()), |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(unavailable("execute log query"))?;
    let mut entries = Vec::new();
    for row in rows {
        let (sequence, entry_json) = row.map_err(unavailable("read log query row"))?;
        let sequence =
            u64::try_from(sequence).map_err(|_| rejected("stored log sequence is negative"))?;
        let entry = serde_json::from_str(&entry_json)
            .map_err(|_| rejected("stored normalized log is invalid"))?;
        entries.push(SequencedLogEntry {
            sequence: LogSequence(sequence),
            entry,
        });
    }
    Ok(entries)
}

pub(crate) fn query_histogram(
    connection: &Connection,
    cold_root: &Path,
    query: &LogHistogramQuery,
) -> Result<Vec<LogHistogramBucket>, LogQueryStoreError> {
    let source = query_source(cold_root)?;
    let filters = query_filters(
        query.scope(),
        query.search(),
        Some(query.from()),
        Some(query.to()),
    )?;
    let group = match query.group_by() {
        LogHistogramGroupBy::Level => compile_json_value("$.severity"),
        LogHistogramGroupBy::HttpStatusClass => compile_histogram_status(),
    };
    let normalize_group = match query.group_by() {
        LogHistogramGroupBy::Level => "LOWER(group_value)",
        LogHistogramGroupBy::HttpStatusClass => {
            "CASE WHEN TRY_CAST(group_value AS INTEGER) BETWEEN 100 AND 599 \
             THEN substr(group_value, 1, 1) || 'xx' END"
        }
    };
    let sql = format!(
        "WITH filtered AS (
             SELECT event_at_ms, {} AS group_value
             FROM ({}) AS stored_logs WHERE {}
         ), bucketed AS (
             SELECT event_at_ms - (event_at_ms % ?) AS bucket_at_ms,
                    {normalize_group} AS normalized_group
             FROM filtered
         )
         SELECT bucket_at_ms, normalized_group, COUNT(*)::BIGINT
         FROM bucketed WHERE normalized_group IS NOT NULL
         GROUP BY bucket_at_ms, normalized_group
         ORDER BY bucket_at_ms ASC, normalized_group ASC",
        group.sql, source.sql, filters.sql
    );
    let mut values = group.values;
    values.extend(source.values);
    values.extend(filters.values);
    values.push(QueryValue::Integer(query.bucket_ms()));
    let values = duck_values(values)?;
    let mut statement = connection
        .prepare(&sql)
        .map_err(unavailable("prepare log histogram"))?;
    let rows = statement
        .query_map(params_from_iter(values.iter()), |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
            ))
        })
        .map_err(unavailable("execute log histogram"))?;
    let mut buckets = BTreeMap::<i64, LogHistogramBucket>::new();
    for row in rows {
        let (bucket_at, group, count) = row.map_err(unavailable("read log histogram row"))?;
        let count =
            u64::try_from(count).map_err(|_| rejected("stored log histogram count is negative"))?;
        let bucket = buckets
            .entry(bucket_at)
            .or_insert_with(|| LogHistogramBucket {
                bucket_at: kernel_api::Timestamp(bucket_at),
                count: 0,
                groups: BTreeMap::new(),
            });
        bucket.count = bucket.count.saturating_add(count);
        bucket.groups.insert(group, count);
    }
    Ok(buckets.into_values().collect())
}

fn query_filters(
    scope: &LogQueryScope,
    search: Option<&LogQuery>,
    from: Option<kernel_api::Timestamp>,
    to: Option<kernel_api::Timestamp>,
) -> Result<CompiledPredicate, LogQueryStoreError> {
    let mut filter = scope_filter(scope);
    if let Some(search) = search {
        let compiled = match search.compile_with(&DuckDbLogQlCompiler) {
            Ok(compiled) => compiled,
            Err(never) => match never {},
        };
        filter.sql.push_str(" AND ");
        filter.sql.push_str(&compiled.sql);
        filter.values.extend(compiled.values);
    }
    if let Some(from) = from {
        filter.sql.push_str(" AND event_at_ms >= ?");
        filter.values.push(QueryValue::Integer(from.0));
    }
    if let Some(to) = to {
        filter.sql.push_str(" AND event_at_ms < ?");
        filter.values.push(QueryValue::Integer(to.0));
    }
    Ok(filter)
}

fn scope_filter(scope: &LogQueryScope) -> CompiledPredicate {
    let (path, expected) = match scope {
        LogQueryScope::All => {
            return CompiledPredicate {
                sql: "TRUE".to_owned(),
                values: Vec::new(),
            };
        }
        LogQueryScope::Service(service_id) => ("$.origin.metadata.serviceId", service_id.as_str()),
        LogQueryScope::Deployment(deployment_id) => {
            ("$.origin.metadata.deploymentId", deployment_id.as_str())
        }
        LogQueryScope::System => ("$.origin.type", "system"),
        LogQueryScope::SystemComponent(component) => ("$.origin.component", component.as_str()),
        LogQueryScope::Build(build_id) => ("$.origin.buildId", build_id.as_str()),
    };
    CompiledPredicate {
        sql: "json_extract_string(entry_json, ?) = ?".to_owned(),
        values: vec![
            QueryValue::Text(path.to_owned()),
            QueryValue::Text(expected.to_owned()),
        ],
    }
}

pub(crate) fn query_source(cold_root: &Path) -> Result<CompiledPredicate, LogQueryStoreError> {
    let mut source = CompiledPredicate {
        sql: "SELECT query.sequence, query.event_at_ms, normalized.entry_json
              FROM query_logs AS query
              INNER JOIN normalized_logs AS normalized
                ON normalized.sequence = query.sequence"
            .to_owned(),
        values: Vec::new(),
    };
    if contains_parquet(cold_root)? {
        let glob = cold_root.join("logs/date=*/hour=*/part-*.parquet");
        let glob = glob
            .to_str()
            .ok_or_else(|| rejected("cold-tier path is not UTF-8"))?;
        source
            .sql
            .push_str(" UNION ALL SELECT sequence, event_at_ms, entry_json FROM read_parquet(?)");
        source.values.push(QueryValue::Text(glob.to_owned()));
    }
    Ok(source)
}

fn contains_parquet(root: &Path) -> Result<bool, LogQueryStoreError> {
    if !root.exists() {
        return Ok(false);
    }
    for entry in std::fs::read_dir(root).map_err(io_unavailable("inspect cold-tier directory"))? {
        let entry = entry.map_err(io_unavailable("inspect cold-tier entry"))?;
        let file_type = entry
            .file_type()
            .map_err(io_unavailable("inspect cold-tier entry type"))?;
        if file_type.is_file()
            && entry
                .path()
                .extension()
                .is_some_and(|value| value == "parquet")
        {
            return Ok(true);
        }
        if file_type.is_dir() && contains_parquet(&entry.path())? {
            return Ok(true);
        }
    }
    Ok(false)
}

pub(crate) fn duck_values(values: Vec<QueryValue>) -> Result<Vec<Value>, LogQueryStoreError> {
    values
        .into_iter()
        .map(|value| match value {
            QueryValue::Text(value) => Ok(Value::Text(value)),
            QueryValue::Integer(value) => Ok(Value::BigInt(value)),
            QueryValue::Number(value) if value.is_finite() => Ok(Value::Double(value)),
            QueryValue::Number(_) => Err(rejected("log query contains a non-finite number")),
        })
        .collect()
}

fn rejected(message: impl Into<String>) -> LogQueryStoreError {
    LogQueryStoreError::Rejected {
        message: message.into(),
    }
}

fn unavailable(action: &'static str) -> impl FnOnce(duckdb::Error) -> LogQueryStoreError {
    move |error| LogQueryStoreError::Unavailable {
        message: format!("{action}: {error}"),
    }
}

fn io_unavailable(action: &'static str) -> impl FnOnce(std::io::Error) -> LogQueryStoreError {
    move |error| LogQueryStoreError::Unavailable {
        message: format!("{action}: {error}"),
    }
}
