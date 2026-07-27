use std::collections::BTreeMap;

use duckdb::Row;
use kernel_api::{AssignmentId, DeploymentId, ServiceId, Timestamp, WorkloadId};
use logs::{IngestLogEntry, LogBody, LogOrigin, LogProducer, LogRecordId, LogStream, OriginCursor};
use metrics::{
    HostMetricPoint, HostMetricRecordId, HostResourceMetricPoint, MetricRecordId,
    WorkloadMetricPoint,
};
use runtime::WorkloadMetadata;
use sha2::{Digest, Sha256};

use crate::LegacyTelemetryPlan;

const LEGACY_LABEL: &str = "migration.maestro.dev/legacy-unit";

pub(crate) fn service_log_row(row: &Row<'_>) -> duckdb::Result<LegacyServiceLogRow> {
    Ok(LegacyServiceLogRow {
        sequence: row.get(0)?,
        timestamp: row.get(1)?,
        service_id: row.get(2)?,
        deployment_id: row.get(3)?,
        unit: row.get(4)?,
        origin: row.get(5)?,
        level: row.get(6)?,
        stream: row.get(7)?,
        text: row.get(8)?,
        tags_json: row.get(9)?,
        attributes_json: row.get(10)?,
    })
}

pub(crate) fn system_log_row(row: &Row<'_>) -> duckdb::Result<LegacySystemLogRow> {
    Ok(LegacySystemLogRow {
        sequence: row.get(0)?,
        timestamp: row.get(1)?,
        source: row.get(2)?,
        origin: row.get(3)?,
        level: row.get(4)?,
        stream: row.get(5)?,
        text: row.get(6)?,
        tags_json: row.get(7)?,
        attributes_json: row.get(8)?,
    })
}

pub(crate) fn metric_row(row: &Row<'_>) -> duckdb::Result<LegacyMetricRow> {
    Ok(LegacyMetricRow {
        row_id: row.get(0)?,
        timestamp: row.get(1)?,
        source: row.get(2)?,
        cpu_percent: row.get(3)?,
        memory_bytes: row.get(4)?,
        memory_limit_bytes: row.get(5)?,
        network_receive_bytes: row.get(6)?,
        network_transmit_bytes: row.get(7)?,
    })
}

pub(crate) fn service_entry(
    plan: &LegacyTelemetryPlan,
    row: LegacyServiceLogRow,
    owner: LegacyOwner,
) -> Result<IngestLogEntry, TelemetryProjectionError> {
    let metadata = metadata(plan, &row.unit, owner)?;
    let mut attributes = attributes(&row.attributes_json, &row.tags_json)?;
    attributes.insert("migration.maestro.dev/legacy-origin".to_owned(), row.origin);
    attributes.insert(
        "migration.maestro.dev/legacy-sequence".to_owned(),
        row.sequence.to_string(),
    );
    Ok(IngestLogEntry {
        id: LogRecordId {
            node_id: plan.node_id.clone(),
            producer: LogProducer::Workload(metadata.workload_id.clone()),
            cursor: OriginCursor::new(format!("legacy-service/{}", row.sequence)),
        },
        observed_at: Timestamp(row.timestamp),
        event_at: Timestamp(row.timestamp),
        severity: row.level,
        stream: stream(&row.stream),
        origin: LogOrigin::Workload { metadata },
        body: LogBody::Text(row.text),
        attributes,
    })
}

pub(crate) fn system_entry(
    plan: &LegacyTelemetryPlan,
    row: LegacySystemLogRow,
) -> Result<IngestLogEntry, TelemetryProjectionError> {
    if row.source.is_empty() {
        return Err(TelemetryProjectionError::InvalidLog(
            "system log source is empty".to_owned(),
        ));
    }
    let mut attributes = attributes(&row.attributes_json, &row.tags_json)?;
    attributes.insert("migration.maestro.dev/legacy-origin".to_owned(), row.origin);
    attributes.insert(
        "migration.maestro.dev/legacy-sequence".to_owned(),
        row.sequence.to_string(),
    );
    Ok(IngestLogEntry {
        id: LogRecordId {
            node_id: plan.node_id.clone(),
            producer: LogProducer::System(row.source.clone()),
            cursor: OriginCursor::new(format!("legacy-system/{}", row.sequence)),
        },
        observed_at: Timestamp(row.timestamp),
        event_at: Timestamp(row.timestamp),
        severity: row.level,
        stream: stream(&row.stream),
        origin: LogOrigin::System {
            cluster_id: plan.cluster_id.clone(),
            node_id: Some(plan.node_id.clone()),
            component: row.source,
        },
        body: LogBody::Text(row.text),
        attributes,
    })
}

pub(crate) fn parse_map(
    encoded: &str,
    kind: &str,
) -> Result<BTreeMap<String, String>, TelemetryProjectionError> {
    serde_json::from_str(encoded)
        .map_err(|error| TelemetryProjectionError::Decode(format!("invalid {kind}: {error}")))
}

fn attributes(
    attributes_json: &str,
    tags_json: &str,
) -> Result<BTreeMap<String, String>, TelemetryProjectionError> {
    let mut attributes = parse_map(attributes_json, "log attributes")?;
    let tags: Vec<String> = serde_json::from_str(tags_json)
        .map_err(|error| TelemetryProjectionError::Decode(error.to_string()))?;
    for (index, tag) in tags.into_iter().enumerate() {
        attributes.insert(format!("migration.maestro.dev/legacy-tag-{index}"), tag);
    }
    Ok(attributes)
}

fn stream(value: &str) -> LogStream {
    match value.to_ascii_lowercase().as_str() {
        "stdout" => LogStream::Stdout,
        "stderr" => LogStream::Stderr,
        _ => LogStream::System,
    }
}

pub(crate) fn workload_metric(
    plan: &LegacyTelemetryPlan,
    unit: &str,
    owner: LegacyOwner,
    row: LegacyMetricRow,
    mut state: WorkloadCounter,
) -> Result<(WorkloadMetricPoint, WorkloadCounter), TelemetryProjectionError> {
    validate_metric(&row)?;
    if state.timestamp == Some(row.timestamp) {
        return Err(TelemetryProjectionError::DuplicateMetric {
            metric_source: row.source,
            timestamp: row.timestamp,
        });
    }
    state.usage = integrate_cpu(
        state.usage,
        state.timestamp,
        row.timestamp,
        row.cpu_percent,
        &row.source,
    )?;
    state.timestamp = Some(row.timestamp);
    let metadata = metadata(plan, unit, owner)?;
    let memory = nonnegative(row.memory_bytes, &row.source, "memory bytes")?;
    let memory_limit = nonnegative(row.memory_limit_bytes, &row.source, "memory limit bytes")?;
    let receive = nonnegative(
        row.network_receive_bytes,
        &row.source,
        "network receive bytes",
    )?;
    let transmit = nonnegative(
        row.network_transmit_bytes,
        &row.source,
        "network transmit bytes",
    )?;
    Ok((
        WorkloadMetricPoint {
            id: MetricRecordId {
                node_id: plan.node_id.clone(),
                workload_id: metadata.workload_id.clone(),
                collected_at: Timestamp(row.timestamp),
            },
            metadata,
            cpu_usage_usec: state.usage,
            cpu_user_usec: state.usage,
            cpu_system_usec: 0,
            cpu_periods: 0,
            cpu_throttled_periods: 0,
            cpu_throttled_usec: 0,
            memory_current_bytes: memory,
            memory_maximum_bytes: (memory_limit > 0).then_some(memory_limit),
            memory_out_of_memory_kills: 0,
            memory_low_events: 0,
            memory_high_events: 0,
            memory_maximum_events: 0,
            memory_out_of_memory_events: 0,
            memory_out_of_memory_group_kills: 0,
            io_read_bytes: 0,
            io_write_bytes: 0,
            io_read_operations: 0,
            io_write_operations: 0,
            io_discarded_bytes: 0,
            io_discard_operations: 0,
            network_receive_bytes: Some(receive),
            network_transmit_bytes: Some(transmit),
            processes_current: 0,
            processes_maximum: None,
        },
        state,
    ))
}

pub(crate) fn host_metric(
    plan: &LegacyTelemetryPlan,
    row: LegacyMetricRow,
    mut state: HostCounter,
) -> Result<(HostMetricPoint, HostCounter), TelemetryProjectionError> {
    validate_metric(&row)?;
    if row.cpu_percent > 100.0 {
        return Err(TelemetryProjectionError::InvalidMetric {
            metric_source: row.source,
            message: "host CPU percent exceeds 100".to_owned(),
        });
    }
    if state.timestamp == Some(row.timestamp) {
        return Err(TelemetryProjectionError::DuplicateMetric {
            metric_source: row.source,
            timestamp: row.timestamp,
        });
    }
    if let Some(previous) = state.timestamp {
        let elapsed = positive_elapsed(previous, row.timestamp, &row.source)?;
        let total_delta = elapsed.saturating_mul(1_000);
        let busy_delta = percent_delta(total_delta, row.cpu_percent, &row.source)?;
        state.total = state.total.saturating_add(total_delta);
        state.idle = state
            .idle
            .saturating_add(total_delta.saturating_sub(busy_delta));
    }
    state.timestamp = Some(row.timestamp);
    let memory = nonnegative(row.memory_bytes, &row.source, "memory bytes")?;
    let memory_total = nonnegative(row.memory_limit_bytes, &row.source, "memory limit bytes")?;
    if memory_total == 0 || memory > memory_total {
        return Err(TelemetryProjectionError::InvalidMetric {
            metric_source: row.source,
            message: "host memory values are invalid".to_owned(),
        });
    }
    Ok((
        HostMetricPoint {
            id: HostMetricRecordId {
                cluster_id: plan.cluster_id.clone(),
                node_id: plan.node_id.clone(),
                collected_at: Timestamp(row.timestamp),
            },
            resources: Some(HostResourceMetricPoint {
                cpu_total_ticks: state.total,
                cpu_idle_ticks: state.idle,
                memory_used_bytes: memory,
                memory_total_bytes: memory_total,
                network_receive_bytes: nonnegative(
                    row.network_receive_bytes,
                    &row.source,
                    "network receive bytes",
                )?,
                network_transmit_bytes: nonnegative(
                    row.network_transmit_bytes,
                    &row.source,
                    "network transmit bytes",
                )?,
            }),
            disks: None,
        },
        state,
    ))
}

fn metadata(
    plan: &LegacyTelemetryPlan,
    unit: &str,
    owner: LegacyOwner,
) -> Result<WorkloadMetadata, TelemetryProjectionError> {
    let workload_id = WorkloadId::new(unit).or_else(|_| {
        WorkloadId::new(format!("legacy-{}", hex::encode(hash(&["workload", unit]))))
    })?;
    let assignment_id = AssignmentId::from_sha256(hash(&[
        plan.cluster_id.as_str(),
        plan.node_id.as_str(),
        unit,
    ]));
    Ok(WorkloadMetadata {
        cluster_id: plan.cluster_id.clone(),
        node_id: plan.node_id.clone(),
        service_id: owner.service_id,
        deployment_id: owner.deployment_id,
        assignment_id,
        workload_id,
        labels: BTreeMap::from([(LEGACY_LABEL.to_owned(), unit.to_owned())]),
    })
}

fn integrate_cpu(
    current: u64,
    previous: Option<i64>,
    timestamp: i64,
    cpu_percent: f64,
    source: &str,
) -> Result<u64, TelemetryProjectionError> {
    let Some(previous) = previous else {
        return Ok(current);
    };
    let elapsed_ms = positive_elapsed(previous, timestamp, source)?;
    let elapsed_us = elapsed_ms.saturating_mul(1_000);
    Ok(current.saturating_add(percent_delta(elapsed_us, cpu_percent, source)?))
}

fn positive_elapsed(
    previous: i64,
    current: i64,
    source: &str,
) -> Result<u64, TelemetryProjectionError> {
    current
        .checked_sub(previous)
        .and_then(|elapsed| u64::try_from(elapsed).ok())
        .filter(|elapsed| *elapsed > 0)
        .ok_or_else(|| TelemetryProjectionError::InvalidMetric {
            metric_source: source.to_owned(),
            message: "timestamps are not strictly increasing".to_owned(),
        })
}

fn percent_delta(total: u64, percent: f64, source: &str) -> Result<u64, TelemetryProjectionError> {
    let total = total
        .to_string()
        .parse::<f64>()
        .map_err(|error| TelemetryProjectionError::Conversion(error.to_string()))?;
    let value = total * percent / 100.0;
    if !value.is_finite() || value < 0.0 {
        return Err(TelemetryProjectionError::InvalidMetric {
            metric_source: source.to_owned(),
            message: "CPU percentage cannot be integrated safely".to_owned(),
        });
    }
    format!("{value:.0}")
        .parse()
        .map_err(
            |error: std::num::ParseIntError| TelemetryProjectionError::InvalidMetric {
                metric_source: source.to_owned(),
                message: format!("CPU percentage cannot be integrated safely: {error}"),
            },
        )
}

fn validate_metric(row: &LegacyMetricRow) -> Result<(), TelemetryProjectionError> {
    if row.timestamp < 0 || !row.cpu_percent.is_finite() || row.cpu_percent < 0.0 {
        return Err(TelemetryProjectionError::InvalidMetric {
            metric_source: row.source.clone(),
            message: format!("row {} has an invalid timestamp or CPU value", row.row_id),
        });
    }
    Ok(())
}

fn nonnegative(value: i64, source: &str, field: &str) -> Result<u64, TelemetryProjectionError> {
    u64::try_from(value).map_err(|_| TelemetryProjectionError::InvalidMetric {
        metric_source: source.to_owned(),
        message: format!("{field} is negative"),
    })
}

fn hash(parts: &[&str]) -> [u8; 32] {
    let mut digest = Sha256::new();
    for part in parts {
        digest.update(part.len().to_be_bytes());
        digest.update(part.as_bytes());
    }
    digest.finalize().into()
}

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct LegacyOwner {
    service_id: ServiceId,
    deployment_id: DeploymentId,
}

impl LegacyOwner {
    pub(crate) fn new(
        service_id: &str,
        deployment_id: &str,
    ) -> Result<Self, TelemetryProjectionError> {
        Ok(Self {
            service_id: ServiceId::new(service_id)?,
            deployment_id: DeploymentId::new(deployment_id)?,
        })
    }

    pub(crate) fn inferred(unit: &str) -> Result<Self, TelemetryProjectionError> {
        let service_id = ServiceId::new(inferred_service(unit)).or_else(|_| {
            ServiceId::new(format!("legacy-{}", hex::encode(hash(&["service", unit]))))
        })?;
        let deployment_id = DeploymentId::new(format!(
            "legacy-{}",
            hex::encode(hash(&["deployment", unit]))
        ))?;
        Ok(Self {
            service_id,
            deployment_id,
        })
    }
}

impl std::fmt::Display for LegacyOwner {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}/{}", self.service_id, self.deployment_id)
    }
}

fn inferred_service(unit: &str) -> &str {
    let without_node = unit
        .rsplit_once("-node-")
        .filter(|(_, port)| port.parse::<u16>().is_ok())
        .map_or(unit, |(base, _)| base);
    let without_replica = without_node
        .rsplit_once('-')
        .filter(|(_, replica)| replica.parse::<u32>().is_ok())
        .map_or(without_node, |(base, _)| base);
    without_replica
        .rsplit_once('-')
        .filter(|(_, suffix)| {
            suffix.len() == 6 && suffix.bytes().all(|byte| byte.is_ascii_alphanumeric())
        })
        .map_or(without_replica, |(service, _)| service)
}

#[derive(Default)]
pub(crate) struct HostCounter {
    timestamp: Option<i64>,
    total: u64,
    idle: u64,
}

pub(crate) struct WorkloadCounter {
    pub(crate) unit: String,
    timestamp: Option<i64>,
    usage: u64,
}

impl WorkloadCounter {
    pub(crate) fn new(unit: &str) -> Self {
        Self {
            unit: unit.to_owned(),
            timestamp: None,
            usage: 0,
        }
    }
}

pub(crate) struct LegacyServiceLogRow {
    sequence: i64,
    timestamp: i64,
    pub(crate) service_id: String,
    pub(crate) deployment_id: String,
    pub(crate) unit: String,
    origin: String,
    level: String,
    stream: String,
    text: String,
    tags_json: String,
    attributes_json: String,
}

pub(crate) struct LegacySystemLogRow {
    sequence: i64,
    timestamp: i64,
    source: String,
    origin: String,
    level: String,
    stream: String,
    text: String,
    tags_json: String,
    attributes_json: String,
}

pub(crate) struct LegacyMetricRow {
    row_id: i64,
    timestamp: i64,
    pub(crate) source: String,
    cpu_percent: f64,
    memory_bytes: i64,
    memory_limit_bytes: i64,
    network_receive_bytes: i64,
    network_transmit_bytes: i64,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum TelemetryProjectionError {
    #[error("legacy telemetry DuckDB read failed: {0}")]
    Database(String),
    #[error("legacy telemetry JSON decode failed: {0}")]
    Decode(String),
    #[error("legacy telemetry identifier is invalid: {0}")]
    Identifier(#[from] kernel_api::InvalidIdentifier),
    #[error("legacy telemetry numeric conversion failed: {0}")]
    Conversion(String),
    #[error("legacy log cannot be normalized: {0}")]
    InvalidLog(String),
    #[error("legacy metric source `{metric_source}` is invalid: {message}")]
    InvalidMetric {
        metric_source: String,
        message: String,
    },
    #[error("legacy metric source `{metric_source}` contains duplicate timestamp {timestamp}")]
    DuplicateMetric {
        metric_source: String,
        timestamp: i64,
    },
    #[error("legacy telemetry resume cursor is invalid: {message}")]
    InvalidResume { message: String },
    #[error("legacy telemetry projection consumer stopped")]
    ConsumerStopped,
    #[error("legacy telemetry projection worker failed: {0}")]
    Worker(String),
}
