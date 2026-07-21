use std::path::Path;

use duckdb::{Connection, OptionalExt, params};
use metrics::{MetricAppendReport, MetricStoreError, WorkloadMetricPoint};

const CURRENT_SCHEMA_VERSION: i64 = 4;

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
        (0, _) => initialize_v4(&mut connection)?,
        (1, 1) => {
            migrate_v1_to_v2(&mut connection)?;
            migrate_v2_to_v3(&mut connection)?;
            migrate_v3_to_v4(&mut connection)?;
        }
        (1, 2) => {
            migrate_v2_to_v3(&mut connection)?;
            migrate_v3_to_v4(&mut connection)?;
        }
        (1, 3) => migrate_v3_to_v4(&mut connection)?,
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
    let mut sequence = transaction
        .query_row(
            "SELECT COALESCE(MAX(sequence), 0) FROM normalized_metrics",
            [],
            |row| row.get::<_, i64>(0),
        )
        .map_err(unavailable("read latest metric sequence"))?;
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
                let previous_sequence = transaction
                    .query_row(
                        "SELECT sequence FROM normalized_metrics
                         WHERE node_id = ?1 AND workload_id = ?2
                         ORDER BY sequence DESC LIMIT 1",
                        params![point.id.node_id.as_str(), point.id.workload_id.as_str()],
                        |row| row.get::<_, i64>(0),
                    )
                    .optional()
                    .map_err(unavailable("read metric rate baseline"))?;
                sequence = sequence
                    .checked_add(1)
                    .ok_or_else(|| MetricStoreError::Rejected {
                        message: "metric delivery sequence space is exhausted".to_owned(),
                    })?;
                transaction
                    .execute(
                        "INSERT INTO normalized_metrics
                         (sequence, previous_sequence, node_id, workload_id, collected_at_ms,
                          service_id, deployment_id, point_json)
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                        params![
                            sequence,
                            previous_sequence,
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

fn initialize_v4(connection: &mut Connection) -> Result<(), String> {
    let transaction = connection
        .transaction()
        .map_err(|error| error.to_string())?;
    transaction
        .execute_batch(
            "CREATE TABLE normalized_metrics (
                 sequence BIGINT NOT NULL UNIQUE,
                 previous_sequence BIGINT,
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
             CREATE TABLE metric_sink_cursors (
                 sink_id VARCHAR PRIMARY KEY,
                 last_sequence BIGINT NOT NULL
             );
             CREATE TABLE host_metrics (
                 cluster_id VARCHAR NOT NULL,
                 node_id VARCHAR NOT NULL,
                 collected_at_ms BIGINT NOT NULL,
                 point_json VARCHAR NOT NULL,
                 has_resources BOOLEAN NOT NULL,
                 has_disks BOOLEAN NOT NULL,
                 PRIMARY KEY (cluster_id, node_id, collected_at_ms)
             );
             CREATE INDEX host_metrics_node_time
                 ON host_metrics (cluster_id, node_id, collected_at_ms);
             INSERT INTO schema_version (version) VALUES (4);",
        )
        .map_err(|error| error.to_string())?;
    transaction.commit().map_err(|error| error.to_string())
}

fn migrate_v3_to_v4(connection: &mut Connection) -> Result<(), String> {
    let transaction = connection
        .transaction()
        .map_err(|error| error.to_string())?;
    transaction
        .execute_batch(
            "CREATE TABLE host_metrics_v4 (
                 cluster_id VARCHAR NOT NULL,
                 node_id VARCHAR NOT NULL,
                 collected_at_ms BIGINT NOT NULL,
                 point_json VARCHAR NOT NULL,
                 has_resources BOOLEAN NOT NULL,
                 has_disks BOOLEAN NOT NULL,
                 PRIMARY KEY (cluster_id, node_id, collected_at_ms)
             );",
        )
        .map_err(|error| error.to_string())?;
    let encoded = {
        let mut statement = transaction
            .prepare(
                "SELECT cluster_id, node_id, collected_at_ms, point_json
                 FROM host_metrics",
            )
            .map_err(|error| error.to_string())?;
        let rows = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, String>(3)?,
                ))
            })
            .map_err(|error| error.to_string())?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|error| error.to_string())?
    };
    for (cluster_id, node_id, collected_at, encoded) in encoded {
        let point = crate::host_metric_schema::decode_row(
            &cluster_id,
            &node_id,
            collected_at,
            &encoded,
            "v3 migration",
        )
        .map_err(|error| error.to_string())?;
        transaction
            .execute(
                "INSERT INTO host_metrics_v4
                 (cluster_id, node_id, collected_at_ms, point_json,
                  has_resources, has_disks)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    cluster_id,
                    node_id,
                    collected_at,
                    encoded,
                    point.resources.is_some(),
                    point.disks.is_some(),
                ],
            )
            .map_err(|error| error.to_string())?;
    }
    transaction
        .execute_batch(
            "DROP TABLE host_metrics;
             ALTER TABLE host_metrics_v4 RENAME TO host_metrics;
             CREATE INDEX host_metrics_node_time
                 ON host_metrics (cluster_id, node_id, collected_at_ms);
             UPDATE schema_version SET version = 4;",
        )
        .map_err(|error| error.to_string())?;
    transaction.commit().map_err(|error| error.to_string())
}

fn migrate_v2_to_v3(connection: &mut Connection) -> Result<(), String> {
    let transaction = connection
        .transaction()
        .map_err(|error| error.to_string())?;
    transaction
        .execute_batch(
            "CREATE TABLE host_metrics (
                 cluster_id VARCHAR NOT NULL,
                 node_id VARCHAR NOT NULL,
                 collected_at_ms BIGINT NOT NULL,
                 point_json VARCHAR NOT NULL,
                 PRIMARY KEY (cluster_id, node_id, collected_at_ms)
             );
             CREATE INDEX host_metrics_node_time
                 ON host_metrics (cluster_id, node_id, collected_at_ms);
             UPDATE schema_version SET version = 3;",
        )
        .map_err(|error| error.to_string())?;
    transaction.commit().map_err(|error| error.to_string())
}

fn migrate_v1_to_v2(connection: &mut Connection) -> Result<(), String> {
    let transaction = connection
        .transaction()
        .map_err(|error| error.to_string())?;
    transaction
        .execute_batch(
            "CREATE TABLE normalized_metrics_v2 (
                 sequence BIGINT NOT NULL UNIQUE,
                 previous_sequence BIGINT,
                 node_id VARCHAR NOT NULL,
                 workload_id VARCHAR NOT NULL,
                 collected_at_ms BIGINT NOT NULL,
                 service_id VARCHAR NOT NULL,
                 deployment_id VARCHAR NOT NULL,
                 point_json VARCHAR NOT NULL,
                 PRIMARY KEY (node_id, workload_id, collected_at_ms)
             );
             WITH ordered AS (
                 SELECT *, ROW_NUMBER() OVER (
                     ORDER BY collected_at_ms, node_id, workload_id
                 ) AS sequence
                 FROM normalized_metrics
             ), linked AS (
                 SELECT *, LAG(sequence) OVER (
                     PARTITION BY node_id, workload_id ORDER BY sequence
                 ) AS previous_sequence
                 FROM ordered
             )
             INSERT INTO normalized_metrics_v2
                 (sequence, previous_sequence, node_id, workload_id, collected_at_ms,
                  service_id, deployment_id, point_json)
             SELECT sequence, previous_sequence, node_id, workload_id, collected_at_ms,
                    service_id, deployment_id, point_json
             FROM linked;
             DROP TABLE normalized_metrics;
             ALTER TABLE normalized_metrics_v2 RENAME TO normalized_metrics;
             CREATE INDEX normalized_metrics_service_time
                 ON normalized_metrics (service_id, collected_at_ms);
             CREATE INDEX normalized_metrics_deployment_time
                 ON normalized_metrics (deployment_id, collected_at_ms);
             CREATE TABLE metric_sink_cursors (
                 sink_id VARCHAR PRIMARY KEY,
                 last_sequence BIGINT NOT NULL
             );
             UPDATE schema_version SET version = 2;",
        )
        .map_err(|error| error.to_string())?;
    transaction.commit().map_err(|error| error.to_string())
}

fn unavailable(action: &'static str) -> impl FnOnce(duckdb::Error) -> MetricStoreError {
    move |error| MetricStoreError::Unavailable {
        message: format!("failed to {action}: {error}"),
    }
}
