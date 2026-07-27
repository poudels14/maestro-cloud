use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use duckdb::{AccessMode, Config, Connection};
use logs::{BackupStatsSnapshot, IngestLogEntry, StatsMetricPoint};
use metrics::{HostMetricPoint, WorkloadMetricPoint};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::LegacyTelemetryPlan;
use crate::telemetry_conversion::{
    HostCounter, LegacyOwner, TelemetryProjectionError, WorkloadCounter, host_metric, metric_row,
    parse_map, service_entry, service_log_row, system_entry, system_log_row, workload_metric,
};

const BATCH_SIZE: usize = 8_192;
/// A missing value records that legacy deployments reused one unit name.
type LegacyOwners = BTreeMap<String, Option<LegacyOwner>>;

pub(crate) enum ProjectionBatch {
    Logs(Vec<IngestLogEntry>),
    WorkloadMetrics(Vec<WorkloadMetricPoint>),
    HostMetrics(Vec<HostMetricPoint>),
    OperationalMetrics(Vec<StatsMetricPoint>),
    BackupStats(Option<BackupStatsSnapshot>),
}

pub(crate) struct TelemetryProjection {
    receiver: mpsc::Receiver<Result<ProjectionBatch, TelemetryProjectionError>>,
    worker: JoinHandle<Result<(), TelemetryProjectionError>>,
}

impl TelemetryProjection {
    pub(crate) fn spawn(plan: LegacyTelemetryPlan, source: PathBuf, skip_logs: u64) -> Self {
        let (sender, receiver) = mpsc::channel(2);
        let worker =
            tokio::task::spawn_blocking(move || project(&plan, &source, skip_logs, &sender));
        Self { receiver, worker }
    }

    pub(crate) async fn next(
        &mut self,
    ) -> Option<Result<ProjectionBatch, TelemetryProjectionError>> {
        self.receiver.recv().await
    }

    pub(crate) fn cancel(&mut self) {
        self.receiver.close();
    }

    pub(crate) async fn finish(self) -> Result<(), TelemetryProjectionError> {
        self.worker
            .await
            .map_err(|error| TelemetryProjectionError::Worker(error.to_string()))?
    }
}

fn project(
    plan: &LegacyTelemetryPlan,
    source: &Path,
    mut skip_logs: u64,
    sender: &mpsc::Sender<Result<ProjectionBatch, TelemetryProjectionError>>,
) -> Result<(), TelemetryProjectionError> {
    let service = open(&source.join("duckdb/service-logs.duckdb"))?;
    let system = open(&source.join("duckdb/system-logs.duckdb"))?;
    let metrics = open(&source.join("duckdb/metrics.duckdb"))?;
    let mut owners = BTreeMap::new();
    project_service_logs(plan, source, &service, &mut owners, &mut skip_logs, sender)?;
    project_system_logs(plan, source, &system, &mut skip_logs, sender)?;
    if skip_logs != 0 {
        return Err(TelemetryProjectionError::InvalidResume {
            message: format!(
                "destination log high-water mark exceeds the projected source by {skip_logs} records"
            ),
        });
    }
    project_workload_metrics(plan, &metrics, &owners, sender)?;
    project_host_metrics(plan, &metrics, sender)?;
    project_operational_metrics(&metrics, sender)?;
    project_backup_stats(&metrics, sender)
}

fn project_service_logs(
    plan: &LegacyTelemetryPlan,
    source: &Path,
    connection: &Connection,
    owners: &mut LegacyOwners,
    skip_logs: &mut u64,
    sender: &mpsc::Sender<Result<ProjectionBatch, TelemetryProjectionError>>,
) -> Result<(), TelemetryProjectionError> {
    project_service_query(
        plan,
        connection,
        "SELECT seq, ts, service_id, deployment_id, unit, origin, level, stream, text,
                to_json(tags)::VARCHAR, to_json(attributes)::VARCHAR
         FROM logs ORDER BY seq",
        None,
        owners,
        skip_logs,
        sender,
    )?;
    for file in plan.files.iter().filter(|file| {
        file.path.starts_with("parts/service-logs/") && file.path.ends_with(".parquet")
    }) {
        let path = source.join(&file.path);
        let path = path.to_string_lossy();
        project_service_query(
            plan,
            connection,
            "SELECT seq, ts, service_id, deployment_id, unit, origin, level, stream, text,
                    to_json(tags)::VARCHAR, to_json(attributes)::VARCHAR
             FROM read_parquet(?1, hive_partitioning = true) ORDER BY seq",
            Some(path.as_ref()),
            owners,
            skip_logs,
            sender,
        )?;
    }
    Ok(())
}

fn project_service_query(
    plan: &LegacyTelemetryPlan,
    connection: &Connection,
    sql: &str,
    parameter: Option<&str>,
    owners: &mut LegacyOwners,
    skip_logs: &mut u64,
    sender: &mpsc::Sender<Result<ProjectionBatch, TelemetryProjectionError>>,
) -> Result<(), TelemetryProjectionError> {
    let mut statement = connection.prepare(sql).map_err(database)?;
    let rows = match parameter {
        Some(parameter) => statement.query_map([parameter], service_log_row),
        None => statement.query_map([], service_log_row),
    }
    .map_err(database)?;
    let mut batch = Vec::with_capacity(BATCH_SIZE);
    for row in rows {
        let row = row.map_err(database)?;
        let owner = LegacyOwner::new(&row.service_id, &row.deployment_id)?;
        record_owner(owners, &row.unit, &owner);
        let entry = service_entry(plan, row, owner)?;
        if *skip_logs != 0 {
            *skip_logs = skip_logs.saturating_sub(1);
            continue;
        }
        batch.push(entry);
        send_full(&mut batch, sender, ProjectionBatch::Logs)?;
    }
    send_remaining(batch, sender, ProjectionBatch::Logs)
}

fn project_system_logs(
    plan: &LegacyTelemetryPlan,
    source: &Path,
    connection: &Connection,
    skip_logs: &mut u64,
    sender: &mpsc::Sender<Result<ProjectionBatch, TelemetryProjectionError>>,
) -> Result<(), TelemetryProjectionError> {
    project_system_query(
        plan,
        connection,
        "SELECT seq, ts, source, origin, level, stream, text,
                to_json(tags)::VARCHAR, to_json(attributes)::VARCHAR
         FROM logs ORDER BY seq",
        None,
        skip_logs,
        sender,
    )?;
    for file in plan.files.iter().filter(|file| {
        file.path.starts_with("parts/system-logs/") && file.path.ends_with(".parquet")
    }) {
        let path = source.join(&file.path);
        let path = path.to_string_lossy();
        project_system_query(
            plan,
            connection,
            "SELECT seq, ts, source, origin, level, stream, text,
                    to_json(tags)::VARCHAR, to_json(attributes)::VARCHAR
             FROM read_parquet(?1) ORDER BY seq",
            Some(path.as_ref()),
            skip_logs,
            sender,
        )?;
    }
    Ok(())
}

fn project_system_query(
    plan: &LegacyTelemetryPlan,
    connection: &Connection,
    sql: &str,
    parameter: Option<&str>,
    skip_logs: &mut u64,
    sender: &mpsc::Sender<Result<ProjectionBatch, TelemetryProjectionError>>,
) -> Result<(), TelemetryProjectionError> {
    let mut statement = connection.prepare(sql).map_err(database)?;
    let rows = match parameter {
        Some(parameter) => statement.query_map([parameter], system_log_row),
        None => statement.query_map([], system_log_row),
    }
    .map_err(database)?;
    let mut batch = Vec::with_capacity(BATCH_SIZE);
    for row in rows {
        let entry = system_entry(plan, row.map_err(database)?)?;
        if *skip_logs != 0 {
            *skip_logs = skip_logs.saturating_sub(1);
            continue;
        }
        batch.push(entry);
        send_full(&mut batch, sender, ProjectionBatch::Logs)?;
    }
    send_remaining(batch, sender, ProjectionBatch::Logs)
}

fn project_workload_metrics(
    plan: &LegacyTelemetryPlan,
    connection: &Connection,
    owners: &LegacyOwners,
    sender: &mpsc::Sender<Result<ProjectionBatch, TelemetryProjectionError>>,
) -> Result<(), TelemetryProjectionError> {
    let mut statement = connection
        .prepare(
            "SELECT rowid, ts, source, cpu_percent, memory_bytes, memory_limit_bytes,
                    net_rx_bytes, net_tx_bytes
             FROM metrics WHERE starts_with(source, 'container:')
             ORDER BY source, ts, rowid",
        )
        .map_err(database)?;
    let rows = statement.query_map([], metric_row).map_err(database)?;
    let mut batch = Vec::with_capacity(BATCH_SIZE);
    let mut previous: Option<WorkloadCounter> = None;
    for row in rows {
        let row = row.map_err(database)?;
        let unit = row
            .source
            .strip_prefix("container:")
            .map(str::to_owned)
            .ok_or_else(|| TelemetryProjectionError::InvalidMetric {
                metric_source: row.source.clone(),
                message: "container source prefix is missing".to_owned(),
            })?;
        let owner = owners
            .get(&unit)
            .and_then(Clone::clone)
            .map_or_else(|| LegacyOwner::inferred(&unit), Ok)?;
        let state = match previous.take() {
            Some(state) if state.unit == unit => state,
            Some(_) | None => WorkloadCounter::new(&unit),
        };
        let (point, state) = workload_metric(plan, &unit, owner, row, state)?;
        previous = Some(state);
        batch.push(point);
        send_full(&mut batch, sender, ProjectionBatch::WorkloadMetrics)?;
    }
    send_remaining(batch, sender, ProjectionBatch::WorkloadMetrics)
}

fn record_owner(owners: &mut LegacyOwners, unit: &str, owner: &LegacyOwner) {
    owners
        .entry(unit.to_owned())
        .and_modify(|existing| {
            if existing.as_ref() != Some(owner) {
                *existing = None;
            }
        })
        .or_insert_with(|| Some(owner.clone()));
}

fn project_host_metrics(
    plan: &LegacyTelemetryPlan,
    connection: &Connection,
    sender: &mpsc::Sender<Result<ProjectionBatch, TelemetryProjectionError>>,
) -> Result<(), TelemetryProjectionError> {
    let mut statement = connection
        .prepare(
            "SELECT rowid, ts, source, cpu_percent, memory_bytes, memory_limit_bytes,
                    net_rx_bytes, net_tx_bytes
             FROM metrics WHERE source = 'node' ORDER BY ts, rowid",
        )
        .map_err(database)?;
    let rows = statement.query_map([], metric_row).map_err(database)?;
    let mut batch = Vec::with_capacity(BATCH_SIZE);
    let mut state = HostCounter::default();
    for row in rows {
        let (point, next) = host_metric(plan, row.map_err(database)?, state)?;
        state = next;
        batch.push(point);
        send_full(&mut batch, sender, ProjectionBatch::HostMetrics)?;
    }
    send_remaining(batch, sender, ProjectionBatch::HostMetrics)
}

fn project_operational_metrics(
    connection: &Connection,
    sender: &mpsc::Sender<Result<ProjectionBatch, TelemetryProjectionError>>,
) -> Result<(), TelemetryProjectionError> {
    let mut statement = connection
        .prepare(
            "SELECT ts, name, value, labels_json
             FROM stats_metrics ORDER BY ts, name, labels_json",
        )
        .map_err(database)?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, f64>(2)?,
                row.get::<_, String>(3)?,
            ))
        })
        .map_err(database)?;
    let mut batch = Vec::with_capacity(BATCH_SIZE);
    for row in rows {
        let (ts, name, value, labels) = row.map_err(database)?;
        batch.push(StatsMetricPoint {
            ts,
            name,
            value,
            labels: parse_map(&labels, "operational metric labels")?,
        });
        send_full(&mut batch, sender, ProjectionBatch::OperationalMetrics)?;
    }
    send_remaining(batch, sender, ProjectionBatch::OperationalMetrics)
}

fn project_backup_stats(
    connection: &Connection,
    sender: &mpsc::Sender<Result<ProjectionBatch, TelemetryProjectionError>>,
) -> Result<(), TelemetryProjectionError> {
    let value = match connection.query_row(
        "SELECT value_json FROM probe_state WHERE key = 'backup-stats'",
        [],
        |row| row.get::<_, String>(0),
    ) {
        Ok(value) => Some(
            serde_json::from_str(&value)
                .map_err(|error| TelemetryProjectionError::Decode(error.to_string()))?,
        ),
        Err(duckdb::Error::QueryReturnedNoRows) => None,
        Err(error) => return Err(database(error)),
    };
    send(sender, ProjectionBatch::BackupStats(value))
}

fn send_full<Value>(
    batch: &mut Vec<Value>,
    sender: &mpsc::Sender<Result<ProjectionBatch, TelemetryProjectionError>>,
    wrap: impl FnOnce(Vec<Value>) -> ProjectionBatch,
) -> Result<(), TelemetryProjectionError> {
    if batch.len() < BATCH_SIZE {
        return Ok(());
    }
    let full = std::mem::replace(batch, Vec::with_capacity(BATCH_SIZE));
    send(sender, wrap(full))
}

fn send_remaining<Value>(
    batch: Vec<Value>,
    sender: &mpsc::Sender<Result<ProjectionBatch, TelemetryProjectionError>>,
    wrap: impl FnOnce(Vec<Value>) -> ProjectionBatch,
) -> Result<(), TelemetryProjectionError> {
    if batch.is_empty() {
        Ok(())
    } else {
        send(sender, wrap(batch))
    }
}

fn send(
    sender: &mpsc::Sender<Result<ProjectionBatch, TelemetryProjectionError>>,
    batch: ProjectionBatch,
) -> Result<(), TelemetryProjectionError> {
    sender
        .blocking_send(Ok(batch))
        .map_err(|_| TelemetryProjectionError::ConsumerStopped)
}

fn open(path: &Path) -> Result<Connection, TelemetryProjectionError> {
    let config = Config::default()
        .enable_autoload_extension(false)
        .map_err(database)?
        .access_mode(AccessMode::ReadOnly)
        .map_err(database)?;
    Connection::open_with_flags(path, config).map_err(database)
}

fn database(error: duckdb::Error) -> TelemetryProjectionError {
    TelemetryProjectionError::Database(error.to_string())
}
