use duckdb::{Connection, params};
use metrics::{
    WorkloadMetricHistoryPoint, WorkloadMetricPoint, WorkloadMetricQuery,
    WorkloadMetricQueryStoreError,
};

pub(crate) fn query(
    connection: &Connection,
    query: &WorkloadMetricQuery,
) -> Result<Vec<WorkloadMetricHistoryPoint>, WorkloadMetricQueryStoreError> {
    let limit =
        i64::try_from(query.limit()).map_err(|_| unavailable_value("convert query limit"))?;
    let mut statement = connection
        .prepare(
            "SELECT current.node_id, current.workload_id, current.collected_at_ms,
                    current.cluster_id, current.service_id, current.deployment_id,
                    current.point_json, current.previous_sequence,
                    previous.node_id, previous.workload_id, previous.collected_at_ms,
                    previous.cluster_id, previous.service_id, previous.deployment_id,
                    previous.point_json
             FROM normalized_metrics AS current
             LEFT JOIN normalized_metrics AS previous
               ON previous.sequence = current.previous_sequence
             WHERE current.cluster_id = ?1
               AND (?2 IS NULL OR current.node_id = ?2)
               AND (?3 IS NULL OR current.service_id = ?3)
               AND (?4 IS NULL OR current.deployment_id = ?4)
               AND current.collected_at_ms >= ?5 AND current.collected_at_ms <= ?6
             ORDER BY current.node_id, current.workload_id, current.collected_at_ms
             LIMIT ?7",
        )
        .map_err(unavailable("prepare workload metric query"))?;
    let rows = statement
        .query_map(
            params![
                query.cluster_id().as_str(),
                query.node_id().map(|node| node.as_str()),
                query.service_id().map(|service| service.as_str()),
                query.deployment_id().map(|deployment| deployment.as_str()),
                query.from().0,
                query.to().0,
                limit,
            ],
            |row| {
                let previous_sequence = row.get::<_, Option<i64>>(7)?;
                let previous_json = row.get::<_, Option<String>>(14)?;
                let previous = if let Some(encoded) = previous_json {
                    Some(StoredWorkloadRow {
                        node_id: row.get::<_, String>(8)?,
                        workload_id: row.get::<_, String>(9)?,
                        collected_at: row.get::<_, i64>(10)?,
                        cluster_id: row.get::<_, String>(11)?,
                        service_id: row.get::<_, String>(12)?,
                        deployment_id: row.get::<_, String>(13)?,
                        encoded,
                    })
                } else {
                    None
                };
                Ok((
                    StoredWorkloadRow {
                        node_id: row.get::<_, String>(0)?,
                        workload_id: row.get::<_, String>(1)?,
                        collected_at: row.get::<_, i64>(2)?,
                        cluster_id: row.get::<_, String>(3)?,
                        service_id: row.get::<_, String>(4)?,
                        deployment_id: row.get::<_, String>(5)?,
                        encoded: row.get::<_, String>(6)?,
                    },
                    previous_sequence,
                    previous,
                ))
            },
        )
        .map_err(unavailable("execute workload metric query"))?;
    rows.map(|row| {
        let (current, previous_sequence, previous) =
            row.map_err(unavailable("read workload metric query row"))?;
        if previous_sequence.is_some() != previous.is_some() {
            return Err(unavailable_value(
                "resolve workload metric history baseline",
            ));
        }
        let current = decode_row(&current, "workload history")?;
        let previous = previous
            .as_ref()
            .map(|previous| decode_row(previous, "workload history baseline"))
            .transpose()?;
        if previous.as_ref().is_some_and(|previous| {
            previous.id.node_id != current.id.node_id
                || previous.id.workload_id != current.id.workload_id
        }) {
            return Err(unavailable_value(
                "validate workload metric history baseline ownership",
            ));
        }
        Ok(WorkloadMetricHistoryPoint {
            point: current,
            previous,
        })
    })
    .collect()
}

struct StoredWorkloadRow {
    node_id: String,
    workload_id: String,
    collected_at: i64,
    cluster_id: String,
    service_id: String,
    deployment_id: String,
    encoded: String,
}

fn decode_row(
    stored: &StoredWorkloadRow,
    kind: &'static str,
) -> Result<WorkloadMetricPoint, WorkloadMetricQueryStoreError> {
    let point: WorkloadMetricPoint = serde_json::from_str(&stored.encoded).map_err(|error| {
        WorkloadMetricQueryStoreError::Unavailable {
            message: format!("failed to decode {kind} point: {error}"),
        }
    })?;
    point
        .validate()
        .map_err(|error| WorkloadMetricQueryStoreError::Unavailable {
            message: format!("failed to validate {kind} point: {error}"),
        })?;
    if point.id.node_id.as_str() != stored.node_id
        || point.id.workload_id.as_str() != stored.workload_id
        || point.id.collected_at.0 != stored.collected_at
        || point.metadata.cluster_id.as_str() != stored.cluster_id
        || point.metadata.service_id.as_str() != stored.service_id
        || point.metadata.deployment_id.as_str() != stored.deployment_id
    {
        return Err(unavailable_value(
            "validate workload metric stored identity",
        ));
    }
    Ok(point)
}

fn unavailable(
    action: &'static str,
) -> impl FnOnce(duckdb::Error) -> WorkloadMetricQueryStoreError {
    move |error| WorkloadMetricQueryStoreError::Unavailable {
        message: format!("failed to {action}: {error}"),
    }
}

fn unavailable_value(action: &'static str) -> WorkloadMetricQueryStoreError {
    WorkloadMetricQueryStoreError::Unavailable {
        message: format!("failed to {action}"),
    }
}
