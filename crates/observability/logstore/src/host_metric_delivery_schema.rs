use duckdb::{Connection, OptionalExt, params};
use metrics::{
    HostMetricDeliveryStoreError, HostMetricSequence, MetricSinkId, SequencedHostMetricPoint,
};

pub(crate) fn read_after(
    connection: &Connection,
    cursor: Option<HostMetricSequence>,
    limit: usize,
) -> Result<Vec<SequencedHostMetricPoint>, HostMetricDeliveryStoreError> {
    if limit == 0 {
        return Err(rejected("host metric delivery read limit must be non-zero"));
    }
    let cursor = cursor.map(sequence_to_i64).transpose()?;
    let limit = i64::try_from(limit)
        .map_err(|_| rejected("host metric delivery read limit exceeds storage range"))?;
    let mut statement = connection
        .prepare(
            "SELECT current.delivery_sequence, current.cluster_id, current.node_id,
                    current.collected_at_ms, current.point_json,
                    previous.cluster_id, previous.node_id, previous.collected_at_ms,
                    previous.point_json
             FROM host_metrics AS current
             LEFT JOIN host_metrics AS previous
               ON previous.delivery_sequence = current.previous_resource_sequence
             WHERE (?1 IS NULL OR current.delivery_sequence > ?1)
             ORDER BY current.delivery_sequence
             LIMIT ?2",
        )
        .map_err(unavailable("prepare host metric delivery read"))?;
    let rows = statement
        .query_map(params![cursor, limit], |row| {
            let previous = match row.get::<_, Option<String>>(8)? {
                Some(encoded) => Some((
                    row.get::<_, String>(5)?,
                    row.get::<_, String>(6)?,
                    row.get::<_, i64>(7)?,
                    encoded,
                )),
                None => None,
            };
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, String>(4)?,
                previous,
            ))
        })
        .map_err(unavailable("read host metric delivery rows"))?;
    rows.map(|row| {
        let (sequence, cluster, node, collected_at, encoded, previous) =
            row.map_err(unavailable("decode host metric delivery row"))?;
        let point = crate::host_metric_schema::decode_row(
            &cluster,
            &node,
            collected_at,
            &encoded,
            "host delivery",
        )
        .map_err(|error| unavailable_value(error.to_string()))?;
        let previous_resources = previous
            .map(|(cluster, node, collected_at, encoded)| {
                crate::host_metric_schema::decode_row(
                    &cluster,
                    &node,
                    collected_at,
                    &encoded,
                    "host delivery baseline",
                )
                .map_err(|error| unavailable_value(error.to_string()))
            })
            .transpose()?;
        if previous_resources.as_ref().is_some_and(|previous| {
            previous.resources.is_none()
                || previous.id.cluster_id != point.id.cluster_id
                || previous.id.node_id != point.id.node_id
                || previous.id.collected_at.0 >= point.id.collected_at.0
        }) {
            return Err(unavailable_value(
                "stored host delivery baseline is invalid",
            ));
        }
        Ok(SequencedHostMetricPoint {
            sequence: i64_to_sequence(sequence)?,
            point,
            previous_resources,
        })
    })
    .collect()
}

pub(crate) fn load_cursor(
    connection: &Connection,
    sink_id: &MetricSinkId,
) -> Result<Option<HostMetricSequence>, HostMetricDeliveryStoreError> {
    connection
        .query_row(
            "SELECT last_sequence FROM host_metric_sink_cursors WHERE sink_id = ?1",
            params![sink_id.as_str()],
            |row| row.get::<_, i64>(0),
        )
        .optional()
        .map_err(unavailable("load host metric sink cursor"))?
        .map(i64_to_sequence)
        .transpose()
}

pub(crate) fn commit_cursor(
    connection: &mut Connection,
    sink_id: &MetricSinkId,
    sequence: HostMetricSequence,
) -> Result<(), HostMetricDeliveryStoreError> {
    let sequence = sequence_to_i64(sequence)?;
    let transaction = connection
        .transaction()
        .map_err(unavailable("begin host metric cursor transaction"))?;
    let exists = transaction
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM host_metrics WHERE delivery_sequence = ?1)",
            params![sequence],
            |row| row.get::<_, bool>(0),
        )
        .map_err(unavailable("validate host metric sink cursor"))?;
    if !exists {
        return Err(rejected(
            "host metric sink cursor does not identify a stored point",
        ));
    }
    let current = transaction
        .query_row(
            "SELECT last_sequence FROM host_metric_sink_cursors WHERE sink_id = ?1",
            params![sink_id.as_str()],
            |row| row.get::<_, i64>(0),
        )
        .optional()
        .map_err(unavailable("read current host metric sink cursor"))?;
    if current.is_some_and(|current| sequence < current) {
        return Err(rejected("host metric sink cursor cannot regress"));
    }
    transaction
        .execute(
            "INSERT INTO host_metric_sink_cursors (sink_id, last_sequence) VALUES (?1, ?2)
             ON CONFLICT (sink_id) DO UPDATE SET last_sequence = excluded.last_sequence",
            params![sink_id.as_str(), sequence],
        )
        .map_err(unavailable("write host metric sink cursor"))?;
    transaction
        .commit()
        .map_err(unavailable("commit host metric cursor transaction"))
}

pub(crate) fn migrate_v5_to_v6(connection: &mut Connection) -> Result<(), String> {
    let transaction = connection
        .transaction()
        .map_err(|error| error.to_string())?;
    transaction
        .execute_batch(
            "CREATE TABLE host_metrics_v6 (
                 delivery_sequence BIGINT NOT NULL UNIQUE,
                 previous_resource_sequence BIGINT,
                 cluster_id VARCHAR NOT NULL,
                 node_id VARCHAR NOT NULL,
                 collected_at_ms BIGINT NOT NULL,
                 point_json VARCHAR NOT NULL,
                 has_resources BOOLEAN NOT NULL,
                 has_disks BOOLEAN NOT NULL,
                 PRIMARY KEY (cluster_id, node_id, collected_at_ms)
             );
             INSERT INTO host_metrics_v6
                 (delivery_sequence, cluster_id, node_id, collected_at_ms, point_json,
                  has_resources, has_disks)
             SELECT ROW_NUMBER() OVER (
                        ORDER BY collected_at_ms, cluster_id, node_id
                    ), cluster_id, node_id, collected_at_ms, point_json,
                    has_resources, has_disks
             FROM host_metrics;
             UPDATE host_metrics_v6 AS current
             SET previous_resource_sequence = (
                 SELECT previous.delivery_sequence
                 FROM host_metrics_v6 AS previous
                 WHERE previous.cluster_id = current.cluster_id
                   AND previous.node_id = current.node_id
                   AND previous.has_resources
                   AND previous.delivery_sequence < current.delivery_sequence
                 ORDER BY previous.delivery_sequence DESC LIMIT 1
             )
             WHERE current.has_resources;
             DROP TABLE host_metrics;
             ALTER TABLE host_metrics_v6 RENAME TO host_metrics;
             CREATE INDEX host_metrics_node_time
                 ON host_metrics (cluster_id, node_id, collected_at_ms);
             CREATE TABLE host_metric_sink_cursors (
                 sink_id VARCHAR PRIMARY KEY,
                 last_sequence BIGINT NOT NULL
             );
             UPDATE schema_version SET version = 6;",
        )
        .map_err(|error| error.to_string())?;
    transaction.commit().map_err(|error| error.to_string())
}

fn sequence_to_i64(sequence: HostMetricSequence) -> Result<i64, HostMetricDeliveryStoreError> {
    i64::try_from(sequence.0)
        .map_err(|_| rejected("host metric sequence exceeds durable storage range"))
}

fn i64_to_sequence(sequence: i64) -> Result<HostMetricSequence, HostMetricDeliveryStoreError> {
    u64::try_from(sequence)
        .map(HostMetricSequence)
        .map_err(|_| unavailable_value("stored host metric sequence is negative"))
}

fn rejected(message: impl Into<String>) -> HostMetricDeliveryStoreError {
    HostMetricDeliveryStoreError::Rejected {
        message: message.into(),
    }
}

fn unavailable_value(message: impl Into<String>) -> HostMetricDeliveryStoreError {
    HostMetricDeliveryStoreError::Unavailable {
        message: message.into(),
    }
}

fn unavailable(action: &'static str) -> impl FnOnce(duckdb::Error) -> HostMetricDeliveryStoreError {
    move |error| HostMetricDeliveryStoreError::Unavailable {
        message: format!("failed to {action}: {error}"),
    }
}
