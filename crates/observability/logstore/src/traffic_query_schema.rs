use std::path::Path;

use duckdb::{Connection, params_from_iter};
use logs::{
    IngressTrafficBreakdown, IngressTrafficQuery, IngressTrafficScope, ServiceTrafficQuery,
    TrafficBreakdownEntry, TrafficMetricPoint, TrafficQueryError,
};

use crate::duck_query_compiler::QueryValue;
use crate::log_query_schema::{duck_values, query_source};

pub(crate) fn query_ingress_traffic(
    connection: &Connection,
    cold_root: &Path,
    query: &IngressTrafficQuery,
) -> Result<IngressTrafficBreakdown, TrafficQueryError> {
    Ok(IngressTrafficBreakdown {
        by_ip: query_breakdown_dimension(connection, cold_root, query, Dimension::ClientIp)?,
        by_path: query_breakdown_dimension(connection, cold_root, query, Dimension::Path)?,
    })
}

pub(crate) fn query_service_traffic(
    connection: &Connection,
    cold_root: &Path,
    query: &ServiceTrafficQuery,
) -> Result<Vec<TrafficMetricPoint>, TrafficQueryError> {
    let (access_sql, mut values) = access_source(
        cold_root,
        query.from().0,
        query.to().0,
        RouterSelection::Includes(query.router_prefix()),
    )?;
    let sql = format!(
        "WITH access AS ({access_sql}),
         bucketed AS (
             SELECT event_at_ms - (((event_at_ms % 5000) + 5000) % 5000) AS bucket_at_ms,
                    status_code,
                    COALESCE(substr(method, 1, 32), '') AS method,
                    GREATEST(COALESCE(TRY_CAST(bytes_in AS BIGINT), 0), 0) AS bytes_in,
                    GREATEST(COALESCE(TRY_CAST(bytes_out AS BIGINT), 0), 0) AS bytes_out,
                    TRY_CAST(duration_ns AS BIGINT) AS duration_ns
             FROM access
         )
         SELECT bucket_at_ms, status_code, method, COUNT(*)::BIGINT AS requests,
                LEAST(SUM(bytes_in::HUGEINT), 9223372036854775807)::BIGINT AS bytes_in,
                LEAST(SUM(bytes_out::HUGEINT), 9223372036854775807)::BIGINT AS bytes_out,
                SUM(CASE WHEN duration_ns BETWEEN 0 AND 1000000000 THEN 1 ELSE 0 END)::BIGINT,
                SUM(CASE WHEN duration_ns BETWEEN 0 AND 5000000000 THEN 1 ELSE 0 END)::BIGINT,
                SUM(CASE WHEN duration_ns BETWEEN 0 AND 10000000000 THEN 1 ELSE 0 END)::BIGINT,
                SUM(CASE WHEN duration_ns >= 0 THEN 1 ELSE 0 END)::BIGINT
         FROM bucketed
         GROUP BY bucket_at_ms, status_code, method
         ORDER BY bucket_at_ms ASC, status_code ASC, method ASC
         LIMIT ?"
    );
    values.push(QueryValue::Integer(limit_i64(query.limit())?));
    let values = traffic_values(values)?;
    let mut statement = connection
        .prepare(&sql)
        .map_err(unavailable("prepare service traffic query"))?;
    let rows = statement
        .query_map(params_from_iter(values.iter()), |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, i64>(4)?,
                row.get::<_, i64>(5)?,
                row.get::<_, i64>(6)?,
                row.get::<_, i64>(7)?,
                row.get::<_, i64>(8)?,
                row.get::<_, i64>(9)?,
            ))
        })
        .map_err(unavailable("execute service traffic query"))?;
    let service_id = query.service_id().as_str();
    let mut points = Vec::new();
    for row in rows {
        let (
            ts,
            status_code,
            method,
            requests,
            bytes_in,
            bytes_out,
            lat_le_1s,
            lat_le_5s,
            lat_le_10s,
            lat_total,
        ) = row.map_err(unavailable("read service traffic row"))?;
        points.push(TrafficMetricPoint {
            ts,
            service_id: service_id.to_owned(),
            deployment_id: None,
            status_code: u16::try_from(status_code)
                .map_err(|_| rejected("stored traffic status is invalid"))?,
            method,
            requests,
            bytes_in,
            bytes_out,
            lat_le_1s,
            lat_le_5s,
            lat_le_10s,
            lat_total,
        });
    }
    Ok(points)
}

#[derive(Clone, Copy)]
enum Dimension {
    ClientIp,
    Path,
}

impl Dimension {
    fn expression(self) -> &'static str {
        match self {
            Self::ClientIp => "client_ip",
            Self::Path => {
                "CASE WHEN substr(path, 1, 2048) = '' THEN '/' ELSE substr(path, 1, 2048) END"
            }
        }
    }
}

fn query_breakdown_dimension(
    connection: &Connection,
    cold_root: &Path,
    query: &IngressTrafficQuery,
    dimension: Dimension,
) -> Result<Vec<TrafficBreakdownEntry>, TrafficQueryError> {
    let selection = match query.scope() {
        IngressTrafficScope::Cluster {
            blocked_router_prefix,
        } => RouterSelection::Excludes(blocked_router_prefix),
        IngressTrafficScope::Blocked { router_prefix }
        | IngressTrafficScope::Service { router_prefix } => {
            RouterSelection::Includes(router_prefix)
        }
    };
    let (access_sql, mut values) =
        access_source(cold_root, query.from().0, query.to().0, selection)?;
    let expression = dimension.expression();
    let sql = format!(
        "WITH access AS ({access_sql}),
         grouped AS (
             SELECT {expression} AS value, status_code, COUNT(*)::BIGINT AS requests,
                    MAX(event_at_ms) AS last_seen_at_ms
             FROM access
             GROUP BY value, status_code
         ),
         totals AS (
             SELECT value, SUM(requests) AS total_requests,
                    MAX(last_seen_at_ms) AS last_seen_at_ms
             FROM grouped GROUP BY value
         ),
         selected AS (
             SELECT value,
                    ROW_NUMBER() OVER (
                        ORDER BY total_requests DESC, last_seen_at_ms DESC, value ASC
                    ) AS value_rank
             FROM totals
             ORDER BY total_requests DESC, last_seen_at_ms DESC, value ASC
             LIMIT ?
         )
         SELECT grouped.value, grouped.status_code, grouped.requests, grouped.last_seen_at_ms
         FROM grouped JOIN selected USING (value)
         ORDER BY selected.value_rank ASC, grouped.status_code ASC"
    );
    values.push(QueryValue::Integer(limit_i64(query.limit())?));
    let values = traffic_values(values)?;
    let mut statement = connection
        .prepare(&sql)
        .map_err(unavailable("prepare ingress traffic query"))?;
    let rows = statement
        .query_map(params_from_iter(values.iter()), |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, i64>(3)?,
            ))
        })
        .map_err(unavailable("execute ingress traffic query"))?;
    let mut entries = Vec::new();
    for row in rows {
        let (value, status_code, requests, last_seen_at_ms) =
            row.map_err(unavailable("read ingress traffic row"))?;
        entries.push(TrafficBreakdownEntry {
            value,
            status_code: u16::try_from(status_code)
                .map_err(|_| rejected("stored traffic status is invalid"))?,
            requests: u64::try_from(requests)
                .map_err(|_| rejected("stored traffic count is negative"))?,
            last_seen_at_ms,
        });
    }
    Ok(entries)
}

#[derive(Clone, Copy)]
enum RouterSelection<'a> {
    Includes(&'a str),
    Excludes(&'a str),
}

fn access_source(
    cold_root: &Path,
    from: i64,
    to: i64,
    router_selection: RouterSelection<'_>,
) -> Result<(String, Vec<QueryValue>), TrafficQueryError> {
    let source = query_source(cold_root).map_err(map_log_query_error)?;
    let router_filter = match router_selection {
        RouterSelection::Includes(_) => "starts_with(router, ?)",
        RouterSelection::Excludes(_) => "NOT starts_with(router, ?)",
    };
    let prefix = match router_selection {
        RouterSelection::Includes(prefix) | RouterSelection::Excludes(prefix) => prefix,
    };
    let sql = format!(
        "SELECT event_at_ms,
                json_extract_string(entry_json, '$.attributes.RouterName') AS router,
                json_extract_string(entry_json, '$.attributes.\"maestro.client_ip\"') AS client_ip,
                json_extract_string(entry_json, '$.attributes.RequestPath') AS path,
                json_extract_string(entry_json, '$.attributes.RequestMethod') AS method,
                TRY_CAST(json_extract_string(entry_json, '$.attributes.DownstreamStatus') AS BIGINT)
                    AS status_code,
                json_extract_string(entry_json, '$.attributes.RequestContentSize') AS bytes_in,
                json_extract_string(entry_json, '$.attributes.DownstreamContentSize') AS bytes_out,
                json_extract_string(entry_json, '$.attributes.Duration') AS duration_ns
         FROM ({}) AS stored_logs
         WHERE event_at_ms >= ? AND event_at_ms <= ?
           AND json_extract_string(entry_json, '$.origin.type') = 'system'
           AND json_extract_string(entry_json, '$.origin.component') IN ('ingress', 'maestro-ingress')
           AND json_extract_string(entry_json, '$.attributes.\"maestro.log_type\"') = 'ingress_access'
           AND router IS NOT NULL AND client_ip IS NOT NULL AND path IS NOT NULL
           AND method IS NOT NULL AND status_code BETWEEN 100 AND 599
           AND {router_filter}",
        source.sql
    );
    let mut values = source.values;
    values.extend([
        QueryValue::Integer(from),
        QueryValue::Integer(to),
        QueryValue::Text(prefix.to_owned()),
    ]);
    Ok((sql, values))
}

fn traffic_values(values: Vec<QueryValue>) -> Result<Vec<duckdb::types::Value>, TrafficQueryError> {
    duck_values(values).map_err(map_log_query_error)
}

fn limit_i64(limit: usize) -> Result<i64, TrafficQueryError> {
    i64::try_from(limit).map_err(|_| rejected("traffic limit exceeds storage bounds"))
}

fn map_log_query_error(error: logs::LogQueryStoreError) -> TrafficQueryError {
    match error {
        logs::LogQueryStoreError::Rejected { message } => TrafficQueryError::Rejected { message },
        logs::LogQueryStoreError::Unavailable { message } => {
            TrafficQueryError::Unavailable { message }
        }
    }
}

fn rejected(message: impl Into<String>) -> TrafficQueryError {
    TrafficQueryError::Rejected {
        message: message.into(),
    }
}

fn unavailable(action: &'static str) -> impl FnOnce(duckdb::Error) -> TrafficQueryError {
    move |error| TrafficQueryError::Unavailable {
        message: format!("{action}: {error}"),
    }
}
