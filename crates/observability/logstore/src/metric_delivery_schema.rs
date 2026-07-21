use duckdb::{Connection, OptionalExt, params};
use metrics::{
    MetricDeliveryStoreError, MetricSequence, MetricSinkId, SequencedMetricPoint,
    WorkloadMetricPoint,
};

pub(crate) fn read_after(
    connection: &Connection,
    cursor: Option<MetricSequence>,
    limit: usize,
) -> Result<Vec<SequencedMetricPoint>, MetricDeliveryStoreError> {
    if limit == 0 {
        return Err(rejected("metric delivery read limit must be non-zero"));
    }
    let cursor = cursor.map(sequence_to_i64).transpose()?;
    let limit = i64::try_from(limit)
        .map_err(|_| rejected("metric delivery read limit exceeds storage range"))?;
    let mut statement = connection
        .prepare(
            "SELECT current.sequence, current.point_json, previous.point_json
             FROM normalized_metrics AS current
             LEFT JOIN normalized_metrics AS previous
               ON previous.sequence = current.previous_sequence
             WHERE (?1 IS NULL OR current.sequence > ?1)
             ORDER BY current.sequence
             LIMIT ?2",
        )
        .map_err(unavailable("prepare metric delivery read"))?;
    let rows = statement
        .query_map(params![cursor, limit], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<String>>(2)?,
            ))
        })
        .map_err(unavailable("read metric delivery rows"))?;
    rows.map(|row| {
        let (sequence, point, previous) = row.map_err(unavailable("decode metric delivery row"))?;
        Ok(SequencedMetricPoint {
            sequence: i64_to_sequence(sequence)?,
            point: decode_point(&point)?,
            previous: previous.as_deref().map(decode_point).transpose()?,
        })
    })
    .collect()
}

pub(crate) fn load_cursor(
    connection: &Connection,
    sink_id: &MetricSinkId,
) -> Result<Option<MetricSequence>, MetricDeliveryStoreError> {
    connection
        .query_row(
            "SELECT last_sequence FROM metric_sink_cursors WHERE sink_id = ?1",
            params![sink_id.as_str()],
            |row| row.get::<_, i64>(0),
        )
        .optional()
        .map_err(unavailable("load metric sink cursor"))?
        .map(i64_to_sequence)
        .transpose()
}

pub(crate) fn commit_cursor(
    connection: &mut Connection,
    sink_id: &MetricSinkId,
    sequence: MetricSequence,
) -> Result<(), MetricDeliveryStoreError> {
    let sequence = sequence_to_i64(sequence)?;
    let transaction = connection
        .transaction()
        .map_err(unavailable("begin metric cursor transaction"))?;
    let exists = transaction
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM normalized_metrics WHERE sequence = ?1)",
            params![sequence],
            |row| row.get::<_, bool>(0),
        )
        .map_err(unavailable("validate metric sink cursor"))?;
    if !exists {
        return Err(rejected(
            "metric sink cursor does not identify a stored point",
        ));
    }
    let current = transaction
        .query_row(
            "SELECT last_sequence FROM metric_sink_cursors WHERE sink_id = ?1",
            params![sink_id.as_str()],
            |row| row.get::<_, i64>(0),
        )
        .optional()
        .map_err(unavailable("read current metric sink cursor"))?;
    if current.is_some_and(|current| sequence < current) {
        return Err(rejected("metric sink cursor cannot regress"));
    }
    transaction
        .execute(
            "INSERT INTO metric_sink_cursors (sink_id, last_sequence) VALUES (?1, ?2)
             ON CONFLICT (sink_id) DO UPDATE SET last_sequence = excluded.last_sequence",
            params![sink_id.as_str(), sequence],
        )
        .map_err(unavailable("write metric sink cursor"))?;
    transaction
        .commit()
        .map_err(unavailable("commit metric cursor transaction"))
}

fn decode_point(encoded: &str) -> Result<WorkloadMetricPoint, MetricDeliveryStoreError> {
    serde_json::from_str(encoded).map_err(|error| MetricDeliveryStoreError::Unavailable {
        message: format!("stored normalized metric point is invalid: {error}"),
    })
}

fn sequence_to_i64(sequence: MetricSequence) -> Result<i64, MetricDeliveryStoreError> {
    i64::try_from(sequence.0)
        .map_err(|_| rejected("metric sequence exceeds the durable storage range"))
}

fn i64_to_sequence(sequence: i64) -> Result<MetricSequence, MetricDeliveryStoreError> {
    u64::try_from(sequence)
        .map(MetricSequence)
        .map_err(|_| unavailable_value("stored metric sequence is negative"))
}

fn rejected(message: impl Into<String>) -> MetricDeliveryStoreError {
    MetricDeliveryStoreError::Rejected {
        message: message.into(),
    }
}

fn unavailable_value(message: impl Into<String>) -> MetricDeliveryStoreError {
    MetricDeliveryStoreError::Unavailable {
        message: message.into(),
    }
}

fn unavailable(action: &'static str) -> impl FnOnce(duckdb::Error) -> MetricDeliveryStoreError {
    move |error| MetricDeliveryStoreError::Unavailable {
        message: format!("failed to {action}: {error}"),
    }
}
