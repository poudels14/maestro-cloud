use std::fs::File;
use std::io::Read;
use std::path::Path;

use duckdb::Connection;
use kernel_api::{ClusterId, NodeId};
use sha2::{Digest, Sha256};

use crate::{LegacyTelemetryPlan, LegacyTelemetryPlanError};

type TestResult<Value = ()> = Result<Value, Box<dyn std::error::Error>>;

#[test]
fn plan_fences_hot_and_manifest_committed_cold_rows() -> TestResult {
    let directory = tempfile::tempdir()?;
    let root = directory.path().join("probe-data");
    seed_databases(&root)?;
    seed_service_partition(&root)?;

    let plan =
        LegacyTelemetryPlan::capture(&root, ClusterId::new("cluster-a")?, NodeId::new("node-a")?)?;

    assert_eq!(plan.schema_version, 1);
    assert_eq!(plan.counts.service_logs, 2);
    assert_eq!(plan.counts.system_logs, 1);
    assert_eq!(plan.counts.host_metrics, 1);
    assert_eq!(plan.counts.container_metrics, 1);
    assert_eq!(plan.counts.derived_metric_aggregates, 2);
    assert_eq!(plan.counts.operational_metrics, 1);
    assert_eq!(plan.counts.retired_traffic_metrics, 1);
    assert!(plan.counts.backup_stats);
    assert_eq!(plan.counts.cold_partitions, 1);
    assert_eq!(plan.counts.parquet_files, 1);
    assert_eq!(plan.files.len(), 5);
    assert_eq!(plan.source_sha256.len(), 64);
    Ok(())
}

#[test]
fn plan_rejects_parquet_not_committed_by_a_manifest() -> TestResult {
    let directory = tempfile::tempdir()?;
    let root = directory.path().join("probe-data");
    seed_databases(&root)?;
    let partition = root.join("parts/system-logs/date=2026-07-20");
    std::fs::create_dir_all(&partition)?;
    let database = Connection::open(root.join("duckdb/system-logs.duckdb"))?;
    database.execute_batch(&format!(
        "COPY (SELECT * FROM logs) TO '{}' (FORMAT PARQUET)",
        partition.join("part-1-1.parquet").display()
    ))?;

    let error =
        LegacyTelemetryPlan::capture(&root, ClusterId::new("cluster-a")?, NodeId::new("node-a")?)
            .expect_err("uncommitted Parquet must fail");
    assert!(matches!(
        error,
        LegacyTelemetryPlanError::UncommittedPartition { .. }
    ));
    Ok(())
}

fn seed_databases(root: &Path) -> TestResult {
    let databases = root.join("duckdb");
    std::fs::create_dir_all(&databases)?;
    std::fs::create_dir_all(root.join("parts"))?;
    let service = Connection::open(databases.join("service-logs.duckdb"))?;
    service.execute_batch(
        "CREATE TABLE logs (
             seq BIGINT, ts BIGINT, date DATE, service_id VARCHAR,
             deployment_id VARCHAR, unit VARCHAR, origin VARCHAR, level VARCHAR,
             stream VARCHAR, text VARCHAR, tags VARCHAR[],
             attributes MAP(VARCHAR, VARCHAR)
         );
         INSERT INTO logs VALUES (
             2, 2000, DATE '2026-07-22', 'api', 'dep-b', 'api-b',
             'service', 'info', 'stdout', 'hot', [], MAP([], [])
         );",
    )?;
    let system = Connection::open(databases.join("system-logs.duckdb"))?;
    system.execute_batch(
        "CREATE TABLE logs (
             seq BIGINT, ts BIGINT, date DATE, source VARCHAR, origin VARCHAR,
             level VARCHAR, stream VARCHAR, text VARCHAR, tags VARCHAR[],
             attributes MAP(VARCHAR, VARCHAR)
         );
         INSERT INTO logs VALUES (
             1, 1000, DATE '2026-07-22', 'maestro-controller',
             'system', 'info', 'stderr', 'ready', [], MAP([], [])
         );",
    )?;
    let metrics = Connection::open(databases.join("metrics.duckdb"))?;
    metrics.execute_batch(
        "CREATE TABLE metrics (
             ts BIGINT, source VARCHAR, cpu_percent DOUBLE, memory_bytes BIGINT,
             memory_limit_bytes BIGINT, net_rx_bytes BIGINT, net_tx_bytes BIGINT
         );
         INSERT INTO metrics VALUES
             (1, 'node', 1, 2, 3, 4, 5),
             (1, 'container:api-b', 1, 2, 3, 4, 5),
             (1, 'cluster', 1, 2, 3, 4, 5),
             (1, 'service:api', 1, 2, 3, 4, 5);
         CREATE TABLE stats_metrics (
             ts BIGINT, name VARCHAR, value DOUBLE, labels_json VARCHAR
         );
         INSERT INTO stats_metrics VALUES (1, 'ready', 1, '{}');
         CREATE TABLE traffic_metrics (value BIGINT);
         INSERT INTO traffic_metrics VALUES (1);
         CREATE TABLE probe_state (
             key VARCHAR, value_json VARCHAR, updated_at_ms BIGINT
         );
         INSERT INTO probe_state VALUES ('backup-stats', '{}', 1);",
    )?;
    Ok(())
}

fn seed_service_partition(root: &Path) -> TestResult {
    let partition =
        root.join("parts/service-logs/service_id=api/deployment_id=dep-a/date=2026-07-20");
    std::fs::create_dir_all(&partition)?;
    let part = partition.join("part-1-1.parquet");
    let database = Connection::open(root.join("duckdb/service-logs.duckdb"))?;
    database.execute_batch(&format!(
        "COPY (
             SELECT 1::BIGINT AS seq, 1000::BIGINT AS ts, 'api-a'::VARCHAR AS unit,
                    'service'::VARCHAR AS origin, 'info'::VARCHAR AS level,
                    'stdout'::VARCHAR AS stream, 'cold'::VARCHAR AS text,
                    []::VARCHAR[] AS tags, MAP([], []) AS attributes
         ) TO '{}' (FORMAT PARQUET)",
        part.display()
    ))?;
    let (size_bytes, sha256) = digest(&part)?;
    let manifest = serde_json::json!({
        "version": 1,
        "tier": "service",
        "partition_key": "api/dep-a/2026-07-20",
        "updated_at_ms": 1,
        "parts": [{
            "file": "part-1-1.parquet",
            "row_count": 1,
            "seq_lo": 1,
            "seq_hi": 1,
            "sha256": sha256,
            "size_bytes": size_bytes
        }]
    });
    std::fs::write(
        partition.join("manifest.json"),
        serde_json::to_vec_pretty(&manifest)?,
    )?;
    Ok(())
}

fn digest(path: &Path) -> TestResult<(u64, String)> {
    let mut file = File::open(path)?;
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 8192];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        digest.update(
            buffer
                .get(..read)
                .expect("Read guarantees the returned count fits the provided buffer"),
        );
    }
    Ok((file.metadata()?.len(), hex::encode(digest.finalize())))
}
