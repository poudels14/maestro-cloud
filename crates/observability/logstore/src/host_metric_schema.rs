use duckdb::{Connection, OptionalExt, params};
use metrics::{
    HostMetricComponent, HostMetricHistoryPoint, HostMetricPoint, HostMetricQuery,
    HostMetricQueryStoreError, LatestHostMetricQuery, MetricAppendReport, MetricStoreError,
};

pub(crate) fn append(
    connection: &mut Connection,
    points: &[HostMetricPoint],
) -> Result<MetricAppendReport, MetricStoreError> {
    let transaction = connection
        .transaction()
        .map_err(store_unavailable("begin host metric append transaction"))?;
    let mut delivery_sequence = transaction
        .query_row(
            "SELECT COALESCE(MAX(delivery_sequence), 0) FROM host_metrics",
            [],
            |row| row.get::<_, i64>(0),
        )
        .map_err(store_unavailable(
            "read latest host metric delivery sequence",
        ))?;
    let mut report = MetricAppendReport::default();
    for point in points {
        point
            .validate()
            .map_err(|error| MetricStoreError::Rejected {
                message: error.to_string(),
            })?;
        let encoded = serde_json::to_string(point).map_err(|error| MetricStoreError::Rejected {
            message: format!("normalized host metric point could not be encoded: {error}"),
        })?;
        let existing = transaction
            .query_row(
                "SELECT point_json FROM host_metrics
                 WHERE cluster_id = ?1 AND node_id = ?2 AND collected_at_ms = ?3",
                params![
                    point.id.cluster_id.as_str(),
                    point.id.node_id.as_str(),
                    point.id.collected_at.0
                ],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(store_unavailable("read host metric replay identity"))?;
        match existing {
            Some(existing) if existing == encoded => {
                report.deduplicated = report.deduplicated.saturating_add(1);
            }
            Some(_) => {
                return Err(MetricStoreError::Rejected {
                    message: format!(
                        "host sample identity `{}/{}/{}` was reused with different content",
                        point.id.cluster_id, point.id.node_id, point.id.collected_at.0
                    ),
                });
            }
            None => {
                let previous_resources = if point.resources.is_some() {
                    transaction
                        .query_row(
                            "SELECT delivery_sequence FROM host_metrics
                             WHERE cluster_id = ?1 AND node_id = ?2 AND has_resources
                             ORDER BY delivery_sequence DESC LIMIT 1",
                            params![point.id.cluster_id.as_str(), point.id.node_id.as_str()],
                            |row| row.get::<_, i64>(0),
                        )
                        .optional()
                        .map_err(store_unavailable("read host metric resource baseline"))?
                } else {
                    None
                };
                delivery_sequence =
                    delivery_sequence
                        .checked_add(1)
                        .ok_or_else(|| MetricStoreError::Rejected {
                            message: "host metric delivery sequence space is exhausted".to_owned(),
                        })?;
                transaction
                    .execute(
                        "INSERT INTO host_metrics
                         (delivery_sequence, previous_resource_sequence, cluster_id, node_id,
                          collected_at_ms, point_json, has_resources, has_disks)
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                        params![
                            delivery_sequence,
                            previous_resources,
                            point.id.cluster_id.as_str(),
                            point.id.node_id.as_str(),
                            point.id.collected_at.0,
                            encoded,
                            point.resources.is_some(),
                            point.disks.is_some()
                        ],
                    )
                    .map_err(store_unavailable("insert normalized host metric"))?;
                report.committed = report.committed.saturating_add(1);
            }
        }
    }
    transaction
        .commit()
        .map_err(store_unavailable("commit host metric append transaction"))?;
    Ok(report)
}

pub(crate) fn query(
    connection: &Connection,
    query: &HostMetricQuery,
) -> Result<Vec<HostMetricHistoryPoint>, HostMetricQueryStoreError> {
    let limit =
        i64::try_from(query.limit()).map_err(|_| query_unavailable("convert query limit"))?;
    let component = component_code(query.component());
    // Keep the range and result bound on the left side of the temporal join. A correlated
    // predecessor search can be decorrelated into an intermediate result that grows with the
    // complete history before either bound is applied.
    let mut statement = connection
        .prepare(
            "WITH current_rows AS MATERIALIZED (
                 SELECT cluster_id, node_id, collected_at_ms, point_json
                 FROM host_metrics
                 WHERE cluster_id = ?1
                   AND (?2 IS NULL OR node_id = ?2)
                   AND collected_at_ms >= ?3 AND collected_at_ms <= ?4
                   AND (?5 = 0 OR (?5 = 1 AND has_resources)
                        OR (?5 = 2 AND has_disks))
                 ORDER BY node_id, collected_at_ms
                 LIMIT ?6
             )
             SELECT current.cluster_id, current.node_id, current.collected_at_ms,
                    current.point_json, previous.cluster_id, previous.node_id,
                    previous.collected_at_ms, previous.point_json
             FROM current_rows AS current
             ASOF LEFT JOIN (
                 SELECT cluster_id, node_id, collected_at_ms, point_json
                 FROM host_metrics
                 WHERE cluster_id = ?1
                   AND (?2 IS NULL OR node_id = ?2)
                   AND collected_at_ms < ?4
                   AND (?5 = 0 OR (?5 = 1 AND has_resources)
                        OR (?5 = 2 AND has_disks))
             ) AS previous
               ON current.cluster_id = previous.cluster_id
              AND current.node_id = previous.node_id
              AND current.collected_at_ms > previous.collected_at_ms
             ORDER BY current.node_id, current.collected_at_ms
            ",
        )
        .map_err(query_failed("prepare host history query"))?;
    let rows = statement
        .query_map(
            params![
                query.cluster_id().as_str(),
                query.node_id().map(|node| node.as_str()),
                query.from().0,
                query.to().0,
                component,
                limit,
            ],
            |row| {
                let previous = if let Some(encoded) = row.get::<_, Option<String>>(7)? {
                    Some((
                        row.get::<_, String>(4)?,
                        row.get::<_, String>(5)?,
                        row.get::<_, i64>(6)?,
                        encoded,
                    ))
                } else {
                    None
                };
                Ok((stored_row(row)?, previous))
            },
        )
        .map_err(query_failed("execute host history query"))?;
    let rows = rows
        .collect::<Result<Vec<_>, _>>()
        .map_err(query_failed("read host history query row"))?;
    rows.into_iter()
        .map(|(current, previous)| {
            let current = decode_stored_row(current, "host history")?;
            let previous = previous
                .map(|previous| decode_stored_row(previous, "host history baseline"))
                .transpose()?;
            if previous.as_ref().is_some_and(|previous| {
                previous.id.cluster_id != current.id.cluster_id
                    || previous.id.node_id != current.id.node_id
                    || previous.id.collected_at.0 >= current.id.collected_at.0
            }) {
                return Err(query_unavailable("validate host history baseline"));
            }
            Ok(HostMetricHistoryPoint {
                point: current,
                previous,
            })
        })
        .collect()
}

pub(crate) fn latest(
    connection: &Connection,
    query: &LatestHostMetricQuery,
) -> Result<Vec<HostMetricPoint>, HostMetricQueryStoreError> {
    let limit =
        i64::try_from(query.limit()).map_err(|_| query_unavailable("convert query limit"))?;
    let component = component_code(query.component());
    let mut statement = connection
        .prepare(
            "WITH ranked AS (
                 SELECT cluster_id, node_id, collected_at_ms, point_json,
                        ROW_NUMBER() OVER (
                            PARTITION BY node_id ORDER BY collected_at_ms DESC
                        ) AS rank
                 FROM host_metrics
                 WHERE cluster_id = ?1
                   AND (?2 = 0 OR (?2 = 1 AND has_resources) OR (?2 = 2 AND has_disks))
             )
             SELECT cluster_id, node_id, collected_at_ms, point_json FROM ranked
             WHERE rank = 1
             ORDER BY node_id
             LIMIT ?3",
        )
        .map_err(query_failed("prepare latest host query"))?;
    let rows = statement
        .query_map(
            params![query.cluster_id().as_str(), component, limit],
            stored_row,
        )
        .map_err(query_failed("execute latest host query"))?;
    let encoded = rows
        .collect::<Result<Vec<_>, _>>()
        .map_err(query_failed("read latest host query row"))?;
    decode_rows(encoded, "latest host")
}

pub(crate) fn decode_row(
    cluster_id: &str,
    node_id: &str,
    collected_at: i64,
    encoded: &str,
    kind: &'static str,
) -> Result<HostMetricPoint, HostMetricQueryStoreError> {
    let point: HostMetricPoint =
        serde_json::from_str(encoded).map_err(|error| HostMetricQueryStoreError::Unavailable {
            message: format!("failed to decode {kind} metric point: {error}"),
        })?;
    point
        .validate()
        .map_err(|error| HostMetricQueryStoreError::Unavailable {
            message: format!("failed to validate {kind} metric point: {error}"),
        })?;
    if point.id.cluster_id.as_str() != cluster_id
        || point.id.node_id.as_str() != node_id
        || point.id.collected_at.0 != collected_at
    {
        return Err(HostMetricQueryStoreError::Unavailable {
            message: format!("failed to validate {kind} metric identity"),
        });
    }
    Ok(point)
}

fn stored_row(row: &duckdb::Row<'_>) -> duckdb::Result<(String, String, i64, String)> {
    Ok((
        row.get::<_, String>(0)?,
        row.get::<_, String>(1)?,
        row.get::<_, i64>(2)?,
        row.get::<_, String>(3)?,
    ))
}

fn decode_stored_row(
    stored: (String, String, i64, String),
    kind: &'static str,
) -> Result<HostMetricPoint, HostMetricQueryStoreError> {
    decode_row(&stored.0, &stored.1, stored.2, &stored.3, kind)
}

fn component_code(component: HostMetricComponent) -> i8 {
    match component {
        HostMetricComponent::Any => 0,
        HostMetricComponent::Resources => 1,
        HostMetricComponent::Disks => 2,
    }
}

fn decode_rows(
    rows: Vec<(String, String, i64, String)>,
    kind: &'static str,
) -> Result<Vec<HostMetricPoint>, HostMetricQueryStoreError> {
    rows.into_iter()
        .map(|(cluster_id, node_id, collected_at, encoded)| {
            decode_row(&cluster_id, &node_id, collected_at, &encoded, kind)
        })
        .collect()
}

fn store_unavailable(action: &'static str) -> impl FnOnce(duckdb::Error) -> MetricStoreError {
    move |error| MetricStoreError::Unavailable {
        message: format!("failed to {action}: {error}"),
    }
}

fn query_failed(action: &'static str) -> impl FnOnce(duckdb::Error) -> HostMetricQueryStoreError {
    move |error| HostMetricQueryStoreError::Unavailable {
        message: format!("failed to {action}: {error}"),
    }
}

fn query_unavailable(action: &'static str) -> HostMetricQueryStoreError {
    HostMetricQueryStoreError::Unavailable {
        message: format!("failed to {action}"),
    }
}
