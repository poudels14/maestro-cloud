use duckdb::{Connection, OptionalExt, params};
use logs::{
    StatsMetricAppendReport, StatsMetricPoint, StatsMetricQuery, StatsMetricStoreError,
    validate_stats_metric_point,
};

pub(crate) fn append(
    connection: &mut Connection,
    points: &[StatsMetricPoint],
) -> Result<StatsMetricAppendReport, StatsMetricStoreError> {
    for point in points {
        validate_stats_metric_point(point)?;
    }
    let transaction = connection
        .transaction()
        .map_err(unavailable("begin stats metric append"))?;
    let mut report = StatsMetricAppendReport::default();
    for point in points {
        let labels_json = serde_json::to_string(&point.labels).map_err(|error| {
            StatsMetricStoreError::Rejected {
                message: format!("stats metric labels could not be encoded: {error}"),
            }
        })?;
        let existing = transaction
            .query_row(
                "SELECT value FROM stats_metrics
                 WHERE ts = ?1 AND name = ?2 AND labels_json = ?3",
                params![point.ts, point.name.as_str(), labels_json.as_str()],
                |row| row.get::<_, f64>(0),
            )
            .optional()
            .map_err(unavailable("read stats metric replay identity"))?;
        match existing {
            Some(existing) if existing.to_bits() == point.value.to_bits() => {
                report.deduplicated = report.deduplicated.saturating_add(1);
            }
            Some(_) => {
                return Err(StatsMetricStoreError::Rejected {
                    message: "stats metric identity was reused with a different value".to_owned(),
                });
            }
            None => {
                transaction
                    .execute(
                        "INSERT INTO stats_metrics (ts, name, value, labels_json)
                         VALUES (?1, ?2, ?3, ?4)",
                        params![
                            point.ts,
                            point.name.as_str(),
                            point.value,
                            labels_json.as_str()
                        ],
                    )
                    .map_err(unavailable("insert stats metric"))?;
                report.committed = report.committed.saturating_add(1);
            }
        }
    }
    transaction
        .commit()
        .map_err(unavailable("commit stats metric append"))?;
    Ok(report)
}

pub(crate) fn query(
    connection: &Connection,
    query: &StatsMetricQuery,
) -> Result<Vec<StatsMetricPoint>, StatsMetricStoreError> {
    let limit = i64::try_from(query.limit()).map_err(|_| StatsMetricStoreError::Rejected {
        message: "stats metric query limit exceeds database range".to_owned(),
    })?;
    let rows = if let Some(name) = query.name() {
        let mut statement = connection
            .prepare(
                "SELECT ts, name, value, labels_json
                 FROM stats_metrics
                 WHERE name = ?1 AND ts >= ?2 AND ts <= ?3
                 ORDER BY ts, name, labels_json
                 LIMIT ?4",
            )
            .map_err(unavailable("prepare named stats metric query"))?;
        statement
            .query_map(params![name, query.from(), query.to(), limit], read_row)
            .map_err(unavailable("query named stats metrics"))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(unavailable("read named stats metric rows"))?
    } else {
        let mut statement = connection
            .prepare(
                "SELECT ts, name, value, labels_json
                 FROM stats_metrics
                 WHERE ts >= ?1 AND ts <= ?2
                 ORDER BY ts, name, labels_json
                 LIMIT ?3",
            )
            .map_err(unavailable("prepare stats metric query"))?;
        statement
            .query_map(params![query.from(), query.to(), limit], read_row)
            .map_err(unavailable("query stats metrics"))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(unavailable("read stats metric rows"))?
    };
    rows.into_iter()
        .map(|(ts, name, value, labels_json)| {
            let labels = serde_json::from_str(&labels_json).map_err(|error| {
                StatsMetricStoreError::Unavailable {
                    message: format!("stored stats metric labels are invalid: {error}"),
                }
            })?;
            let point = StatsMetricPoint {
                ts,
                name,
                value,
                labels,
            };
            validate_stats_metric_point(&point).map_err(|error| {
                StatsMetricStoreError::Unavailable {
                    message: format!("stored stats metric is invalid: {error}"),
                }
            })?;
            Ok(point)
        })
        .collect()
}

pub(crate) fn migrate_v4_to_v5(connection: &mut Connection) -> Result<(), String> {
    let transaction = connection
        .transaction()
        .map_err(|error| error.to_string())?;
    transaction
        .execute_batch(
            "CREATE TABLE stats_metrics (
                 ts BIGINT NOT NULL,
                 name VARCHAR NOT NULL,
                 value DOUBLE NOT NULL,
                 labels_json VARCHAR NOT NULL,
                 PRIMARY KEY (ts, name, labels_json)
             );
             CREATE INDEX stats_metrics_name_ts
                 ON stats_metrics(name, ts, labels_json);
             UPDATE schema_version SET version = 5;",
        )
        .map_err(|error| error.to_string())?;
    transaction.commit().map_err(|error| error.to_string())
}

fn read_row(row: &duckdb::Row<'_>) -> duckdb::Result<(i64, String, f64, String)> {
    Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
}

fn unavailable(action: &'static str) -> impl FnOnce(duckdb::Error) -> StatsMetricStoreError {
    move |error| StatsMetricStoreError::Unavailable {
        message: format!("failed to {action}: {error}"),
    }
}
