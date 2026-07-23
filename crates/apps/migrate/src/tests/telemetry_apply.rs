use std::path::Path;

use duckdb::Connection;
use kernel_api::{ClusterId, NodeId};
use metrics::{HostMetricPoint, WorkloadMetricPoint};

use super::telemetry_plan::{seed_databases, seed_service_partition};
use crate::{
    LegacyTelemetryApplyOutcome, LegacyTelemetryMigrationError, LegacyTelemetryPlan,
    apply_legacy_telemetry, verify_legacy_telemetry,
};

type TestResult<Value = ()> = Result<Value, Box<dyn std::error::Error>>;

#[tokio::test]
async fn apply_is_exact_verifiable_and_replay_safe() -> TestResult {
    let directory = tempfile::tempdir()?;
    let source = directory.path().join("probe-data");
    let destination = directory.path().join("rewrite-data");
    seed_databases(&source)?;
    seed_service_partition(&source)?;
    append_metric_samples(&source)?;
    let plan = LegacyTelemetryPlan::capture(
        &source,
        ClusterId::new("cluster-a")?,
        NodeId::new("node-a")?,
    )?;

    let first = apply_legacy_telemetry(&plan, &source, &destination).await?;

    assert_eq!(first.outcome, LegacyTelemetryApplyOutcome::Applied);
    assert_eq!(first.verification.logs.records, 3);
    assert_eq!(first.verification.workload_metrics.records, 2);
    assert_eq!(first.verification.host_metrics.records, 2);
    assert_eq!(first.verification.operational_metrics.records, 1);
    assert_eq!(first.verification.backup_stats.records, 1);
    assert_eq!(
        verify_legacy_telemetry(&plan, &source, &destination).await?,
        first.verification
    );
    assert_converted_counters(&destination)?;

    let replay = apply_legacy_telemetry(&plan, &source, &destination).await?;
    assert_eq!(replay.outcome, LegacyTelemetryApplyOutcome::AlreadyComplete);
    assert_eq!(replay.verification, first.verification);
    #[cfg(unix)]
    assert_destination_symlink_is_rejected(&plan, &source, &destination).await?;
    Ok(())
}

#[tokio::test]
async fn apply_rejects_a_source_changed_after_review() -> TestResult {
    let directory = tempfile::tempdir()?;
    let source = directory.path().join("probe-data");
    let destination = directory.path().join("rewrite-data");
    seed_databases(&source)?;
    let plan = LegacyTelemetryPlan::capture(
        &source,
        ClusterId::new("cluster-a")?,
        NodeId::new("node-a")?,
    )?;
    let metrics = Connection::open(source.join("duckdb/metrics.duckdb"))?;
    metrics.execute(
        "INSERT INTO stats_metrics VALUES (2, 'changed', 1, '{}')",
        [],
    )?;
    drop(metrics);

    let error = apply_legacy_telemetry(&plan, &source, &destination)
        .await
        .expect_err("a changed source must be refused");

    assert!(matches!(error, LegacyTelemetryMigrationError::Plan(_)));
    assert!(!destination.exists());
    Ok(())
}

#[tokio::test]
async fn verification_requires_an_existing_destination_without_creating_it() -> TestResult {
    let directory = tempfile::tempdir()?;
    let source = directory.path().join("probe-data");
    let destination = directory.path().join("missing-rewrite-data");
    seed_databases(&source)?;
    let plan = LegacyTelemetryPlan::capture(
        &source,
        ClusterId::new("cluster-a")?,
        NodeId::new("node-a")?,
    )?;

    verify_legacy_telemetry(&plan, &source, &destination)
        .await
        .expect_err("verification must require an existing destination");

    assert!(!destination.exists());
    Ok(())
}

fn append_metric_samples(source: &Path) -> TestResult {
    let metrics = Connection::open(source.join("duckdb/metrics.duckdb"))?;
    metrics.execute_batch(
        "INSERT INTO metrics VALUES
             (1001, 'node', 25, 2, 3, 14, 15),
             (1001, 'container:api-b', 50, 2, 3, 14, 15);",
    )?;
    Ok(())
}

fn assert_converted_counters(destination: &Path) -> TestResult {
    let metrics = Connection::open_with_flags(
        destination.join("agent/metrics.duckdb"),
        duckdb::Config::default().access_mode(duckdb::AccessMode::ReadOnly)?,
    )?;
    let workload: WorkloadMetricPoint = serde_json::from_str(&metrics.query_row(
        "SELECT point_json FROM normalized_metrics ORDER BY sequence DESC LIMIT 1",
        [],
        |row| row.get::<_, String>(0),
    )?)?;
    assert_eq!(workload.cpu_usage_usec, 500_000);
    assert_eq!(workload.network_receive_bytes, Some(14));
    let host: HostMetricPoint = serde_json::from_str(&metrics.query_row(
        "SELECT point_json FROM host_metrics ORDER BY delivery_sequence DESC LIMIT 1",
        [],
        |row| row.get::<_, String>(0),
    )?)?;
    let resources = host
        .resources
        .ok_or_else(|| std::io::Error::other("host resources should be present"))?;
    assert_eq!(resources.cpu_total_ticks, 1_000_000);
    assert_eq!(resources.cpu_idle_ticks, 750_000);
    Ok(())
}

#[cfg(unix)]
async fn assert_destination_symlink_is_rejected(
    plan: &LegacyTelemetryPlan,
    source: &Path,
    destination: &Path,
) -> TestResult {
    use std::os::unix::fs::symlink;

    let metrics = destination.join("agent/metrics.duckdb");
    std::fs::remove_file(&metrics)?;
    symlink(source.join("duckdb/metrics.duckdb"), &metrics)?;
    let error = verify_legacy_telemetry(plan, source, destination)
        .await
        .expect_err("a destination store symlink must be refused");
    assert!(matches!(
        error,
        LegacyTelemetryMigrationError::UnsafeDestination { .. }
    ));
    Ok(())
}
