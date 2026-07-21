use std::path::Path;

use duckdb::{Connection, OptionalExt, params};
use metrics::{MetricAppendReport, MetricStoreError, WorkloadMetricPoint};

const CURRENT_SCHEMA_VERSION: i64 = 1;

pub(crate) fn open(path: &Path) -> Result<Connection, String> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|error| format!("create database parent directory: {error}"))?;
    }
    let mut connection = Connection::open(path).map_err(|error| error.to_string())?;
    connection
        .execute_batch("CREATE TABLE IF NOT EXISTS schema_version (version BIGINT NOT NULL);")
        .map_err(|error| error.to_string())?;
    let (version_count, version) = connection
        .query_row(
            "SELECT COUNT(*), COALESCE(MAX(version), 0) FROM schema_version",
            [],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
        )
        .map_err(|error| error.to_string())?;
    match (version_count, version) {
        (0, _) => initialize_v1(&mut connection)?,
        (1, CURRENT_SCHEMA_VERSION) => {}
        (1, version) => {
            return Err(format!(
                "database schema version {version} is not supported by version {CURRENT_SCHEMA_VERSION}"
            ));
        }
        (count, _) => return Err(format!("schema version table contains {count} rows")),
    }
    Ok(connection)
}

pub(crate) fn append(
    connection: &mut Connection,
    points: &[WorkloadMetricPoint],
) -> Result<MetricAppendReport, MetricStoreError> {
    let transaction = connection
        .transaction()
        .map_err(unavailable("begin append transaction"))?;
    let mut report = MetricAppendReport::default();
    for point in points {
        let encoded = serde_json::to_string(point).map_err(|error| MetricStoreError::Rejected {
            message: format!("normalized metric point could not be encoded: {error}"),
        })?;
        let existing = transaction
            .query_row(
                "SELECT point_json FROM normalized_metrics
                 WHERE node_id = ?1 AND workload_id = ?2 AND collected_at_ms = ?3",
                params![
                    point.id.node_id.as_str(),
                    point.id.workload_id.as_str(),
                    point.id.collected_at.0
                ],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(unavailable("read metric replay identity"))?;
        match existing {
            Some(existing) if existing == encoded => {
                report.deduplicated = report.deduplicated.saturating_add(1);
            }
            Some(_) => {
                return Err(MetricStoreError::Rejected {
                    message: format!(
                        "sample identity `{}/{}/{}` was reused with different content",
                        point.id.node_id, point.id.workload_id, point.id.collected_at.0
                    ),
                });
            }
            None => {
                transaction
                    .execute(
                        "INSERT INTO normalized_metrics
                         (node_id, workload_id, collected_at_ms, service_id, deployment_id, point_json)
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                        params![
                            point.id.node_id.as_str(),
                            point.id.workload_id.as_str(),
                            point.id.collected_at.0,
                            point.metadata.service_id.as_str(),
                            point.metadata.deployment_id.as_str(),
                            encoded
                        ],
                    )
                    .map_err(unavailable("insert normalized metric"))?;
                report.committed = report.committed.saturating_add(1);
            }
        }
    }
    transaction
        .commit()
        .map_err(unavailable("commit append transaction"))?;
    Ok(report)
}

fn initialize_v1(connection: &mut Connection) -> Result<(), String> {
    let transaction = connection
        .transaction()
        .map_err(|error| error.to_string())?;
    transaction
        .execute_batch(
            "CREATE TABLE normalized_metrics (
                 node_id VARCHAR NOT NULL,
                 workload_id VARCHAR NOT NULL,
                 collected_at_ms BIGINT NOT NULL,
                 service_id VARCHAR NOT NULL,
                 deployment_id VARCHAR NOT NULL,
                 point_json VARCHAR NOT NULL,
                 PRIMARY KEY (node_id, workload_id, collected_at_ms)
             );
             CREATE INDEX normalized_metrics_service_time
                 ON normalized_metrics (service_id, collected_at_ms);
             CREATE INDEX normalized_metrics_deployment_time
                 ON normalized_metrics (deployment_id, collected_at_ms);
             INSERT INTO schema_version (version) VALUES (1);",
        )
        .map_err(|error| error.to_string())?;
    transaction.commit().map_err(|error| error.to_string())
}

fn unavailable(action: &'static str) -> impl FnOnce(duckdb::Error) -> MetricStoreError {
    move |error| MetricStoreError::Unavailable {
        message: format!("failed to {action}: {error}"),
    }
}
