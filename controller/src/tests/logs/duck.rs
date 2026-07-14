use super::*;
use crate::logs::LogStore;
use std::time::{SystemTime, UNIX_EPOCH};

fn temp_root(label: &str) -> PathBuf {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "maestro-duck-test-{label}-{}-{unique}",
        std::process::id()
    ))
}

fn entry(ts: i64, source: &str, origin: LogOrigin, text: &str) -> LogEntry {
    LogEntry {
        seq: 0,
        ts,
        level: Arc::from("info"),
        stream: Arc::from("stdout"),
        text: text.into(),
        source: Arc::from(source),
        origin,
        tags: Arc::new(serde_json::json!(["service:api", "replica:0"])),
        attrs: vec![("request_id".into(), "abc".into())],
    }
}

fn ingress_entry(ts: i64, router: &str, ip: &str, path: &str, status: u16) -> LogEntry {
    let mut entry = entry(ts, "maestro-ingress", LogOrigin::System, "access");
    entry.attrs = vec![
        ("RouterName".into(), router.into()),
        ("maestro.client_ip".into(), ip.into()),
        ("RequestPath".into(), path.into()),
        ("DownstreamStatus".into(), status.to_string()),
    ];
    entry
}

#[test]
fn ingest_wire_format_is_backward_compatible() {
    let legacy = serde_json::to_value(entry(
        1_700_000_000_000,
        "api/dep/replica0",
        LogOrigin::Service,
        "legacy",
    ))
    .expect("legacy JSON");
    let parsed: IngestLogEntry = serde_json::from_value(legacy).expect("legacy ingest");
    assert!(parsed.node_id.is_none());
    assert!(parsed.origin_seq.is_none());

    let idempotent = IngestLogEntry {
        entry: parsed.entry,
        node_id: Some("node-a".into()),
        origin_seq: Some(7),
    };
    let value = serde_json::to_value(idempotent).expect("new JSON");
    assert_eq!(value["node_id"], "node-a");
    assert_eq!(value["origin_seq"], 7);
    let old_probe: LogEntry = serde_json::from_value(value).expect("old probe ignores metadata");
    assert_eq!(old_probe.text, "legacy");
}

#[test]
fn missing_offsets_default_to_zero_but_other_database_errors_propagate() {
    assert_eq!(
        duckdb_i64_or_zero(Err(duckdb::Error::QueryReturnedNoRows)).expect("missing row"),
        0
    );
    assert!(duckdb_i64_or_zero(Err(duckdb::Error::InvalidQuery)).is_err());
}

#[tokio::test]
async fn ingress_traffic_groups_ip_path_and_status() {
    let root = temp_root("ingress-traffic");
    let store = DuckLogStore::open(&root).expect("open");
    store
        .append(&[
            ingress_entry(
                1_700_000_000_000,
                "api@etcd",
                "203.0.113.9",
                "/login?token=secret",
                200,
            ),
            ingress_entry(
                1_700_000_000_100,
                "api-aff-node1@etcd",
                "203.0.113.9",
                "/login?token=other",
                401,
            ),
            ingress_entry(1_700_000_000_200, "api@etcd", "198.51.100.8", "/.env", 403),
            ingress_entry(
                1_700_000_000_250,
                "maestro.internal-blocked-deadbeef-0@etcd",
                "2001:db8::9",
                "/wp-admin?probe=1",
                403,
            ),
            ingress_entry(
                1_700_000_000_300,
                "other@etcd",
                "192.0.2.1",
                "/ignored",
                404,
            ),
        ])
        .await
        .expect("append");

    let traffic = store
        .read_ingress_traffic("api", 1_699_999_999_000, 1_700_000_001_000, 100)
        .await
        .expect("traffic query");
    assert_eq!(
        traffic.by_ip,
        vec![
            crate::logs::TrafficBreakdownEntry {
                value: "203.0.113.9".into(),
                status_code: 200,
                requests: 1,
                last_seen_at_ms: 1_700_000_000_000,
            },
            crate::logs::TrafficBreakdownEntry {
                value: "203.0.113.9".into(),
                status_code: 401,
                requests: 1,
                last_seen_at_ms: 1_700_000_000_100,
            },
            crate::logs::TrafficBreakdownEntry {
                value: "198.51.100.8".into(),
                status_code: 403,
                requests: 1,
                last_seen_at_ms: 1_700_000_000_200,
            },
        ]
    );
    assert_eq!(traffic.by_path[0].value, "/login");
    assert_eq!(traffic.by_path[0].status_code, 200);
    assert_eq!(traffic.by_path[1].value, "/login");
    assert_eq!(traffic.by_path[1].status_code, 401);
    assert_eq!(traffic.by_path[2].value, "/.env");
    let blocked = store
        .read_blocked_ingress_traffic(1_699_999_999_000, 1_700_000_001_000, 100)
        .await
        .expect("blocked traffic query");
    assert_eq!(
        blocked.by_ip,
        vec![crate::logs::TrafficBreakdownEntry {
            value: "2001:db8::9".into(),
            status_code: 403,
            requests: 1,
            last_seen_at_ms: 1_700_000_000_250,
        }]
    );
    assert_eq!(blocked.by_path[0].value, "/wp-admin");
    assert!(
        store
            .read_tail("maestro-ingress", 10)
            .await
            .expect("read raw ingress logs")
            .is_empty(),
        "access records should be discarded after aggregation"
    );
    store
        .append(&[entry(
            1_700_000_000_400,
            "maestro-ingress",
            LogOrigin::System,
            "provider configuration reloaded",
        )])
        .await
        .expect("append ingress diagnostic");
    assert_eq!(
        store
            .read_tail("maestro-ingress", 10)
            .await
            .expect("read ingress diagnostic")[0]
            .text,
        "provider configuration reloaded"
    );

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn stats_metrics_and_backup_state_survive_reopen() {
    let root = temp_root("stats-persistence");
    let store = DuckLogStore::open(&root).expect("open");
    let point = crate::cluster_stats::StatsMetricPoint {
        ts: 1_700_000_000_000,
        name: "logs.spool.bytes".to_string(),
        value: 42.0,
        labels: std::collections::BTreeMap::new(),
    };
    store
        .append_stats_metrics(std::slice::from_ref(&point))
        .await
        .expect("append stats metric");
    let backup = crate::cluster_stats::BackupStatsSnapshot {
        configured: true,
        last_success_at_ms: Some(1_700_000_000_000),
        pending_partitions: 3,
        pending_bytes: 99,
        ..crate::cluster_stats::BackupStatsSnapshot::default()
    };
    store
        .save_backup_stats(&backup)
        .await
        .expect("save backup stats");
    drop(store);

    let reopened = DuckLogStore::open(&root).expect("reopen");
    let points = reopened
        .read_stats_metrics(
            Some("logs.spool.bytes"),
            1_699_999_999_999,
            1_700_000_000_001,
        )
        .await
        .expect("read stats metrics");
    assert_eq!(points, vec![point]);
    let restored = reopened
        .load_backup_stats()
        .await
        .expect("load backup stats")
        .expect("saved backup stats");
    assert!(restored.configured);
    assert_eq!(restored.last_success_at_ms, Some(1_700_000_000_000));
    assert_eq!(restored.pending_partitions, 3);
    assert_eq!(restored.pending_bytes, 99);

    drop(reopened);
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn slow_query_locks_do_not_block_ingest() {
    let root = temp_root("connection-pool");
    let store = DuckLogStore::open(&root).expect("open");
    let db = store.system.clone();
    let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel(0);
    let (release_tx, release_rx) = std::sync::mpsc::sync_channel(0);
    let query = std::thread::spawn(move || {
        let _visibility = db.read_parquet().expect("visibility");
        let _reader = db.reader().expect("reader connection");
        ready_tx.send(()).expect("signal query started");
        release_rx.recv().expect("release query");
    });
    ready_rx.recv().expect("query lock acquired");
    tokio::time::timeout(
        std::time::Duration::from_millis(500),
        store.append(&[entry(
            now_ms(),
            "maestro-probe",
            LogOrigin::System,
            "ingest-during-query",
        )]),
    )
    .await
    .expect("slow query must not block ingest")
    .expect("append");
    release_tx.send(()).expect("release query");
    query.join().expect("query thread");
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn rollover_and_ingest_can_run_concurrently_without_losing_rows() {
    let root = temp_root("concurrent-rollover");
    let store = DuckLogStore::open(&root).expect("open");
    let old_entries = (0..200)
        .map(|index| {
            entry(
                1_700_000_000_000,
                "api/dep/replica0",
                LogOrigin::Service,
                &format!("old-{index}"),
            )
        })
        .collect::<Vec<_>>();
    store.append(&old_entries).await.expect("append old rows");
    let current = entry(now_ms(), "api/dep/replica0", LogOrigin::Service, "current");
    let current = [current];
    let (rolled, appended) = tokio::join!(store.rollover(), store.append(&current));
    assert_eq!(rolled.expect("rollover"), old_entries.len());
    appended.expect("concurrent append");

    let rows = store
        .read_tail_by_prefix_origin("api/dep/", None, 500)
        .await
        .expect("read hot and cold rows");
    assert_eq!(rows.len(), old_entries.len() + 1);
    assert_eq!(rows.iter().filter(|row| row.text == "current").count(), 1);
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn routes_and_reads_service_and_system_logs() {
    let root = temp_root("routes");
    let store = DuckLogStore::open(&root).expect("open");
    let now = now_ms();
    store
        .append(&[
            entry(now, "api/dep/replica0", LogOrigin::Service, "service"),
            entry(now, "maestro-probe", LogOrigin::System, "system"),
        ])
        .await
        .expect("append");

    let service = store
        .read_tail_by_prefix_origin("api/dep/", Some(LogOrigin::Service), 10)
        .await
        .expect("read service");
    assert_eq!(service.len(), 1);
    assert_eq!(service[0].text, "service");
    assert_eq!(service[0].attrs, vec![("request_id".into(), "abc".into())]);

    let system = store
        .read_tail("maestro-probe", 10)
        .await
        .expect("read system");
    assert_eq!(system.len(), 1);
    assert_eq!(system[0].text, "system");
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn duplicate_origin_sequence_is_a_noop() {
    let root = temp_root("dedup");
    let store = DuckLogStore::open(&root).expect("open");
    let item = IngestLogEntry {
        entry: entry(now_ms(), "api/dep/replica0", LogOrigin::Service, "once"),
        node_id: Some("node-a".into()),
        origin_seq: Some(42),
    };
    store
        .append_ingest(std::slice::from_ref(&item))
        .await
        .expect("first");
    store.append_ingest(&[item]).await.expect("retry");
    let rows = store
        .read_tail_by_prefix_origin("api/", None, 10)
        .await
        .expect("read");
    assert_eq!(rows.len(), 1);
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn rollover_keeps_old_logs_queryable_from_parquet() {
    let root = temp_root("rollover");
    let store = DuckLogStore::open(&root).expect("open");
    store
        .append(&[entry(
            1_700_000_000_000,
            "api/dep/replica0",
            LogOrigin::Service,
            "cold",
        )])
        .await
        .expect("append");
    assert_eq!(store.rollover().await.expect("rollover"), 1);

    let rows = store
        .read_tail_by_prefix_origin("api/dep/", None, 10)
        .await
        .expect("cold read");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].text, "cold");
    assert!(
        !cold_tier_has_seq_after(&store.service, "service", rows[0].seq)
            .expect("cold cursor bound")
    );
    assert!(contains_parquet(&root.join("parts/service-logs")));
    assert!(
        root.join(
            "parts/service-logs/service_id=api/deployment_id=dep/date=2023-11-14/manifest.json"
        )
        .exists()
    );
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn rollover_exports_late_arrivals_as_additional_parts() {
    let root = temp_root("late");
    let store = DuckLogStore::open(&root).expect("open");
    let old_ts = 1_700_000_000_000;
    store
        .append(&[entry(
            old_ts,
            "api/dep/replica0",
            LogOrigin::Service,
            "first",
        )])
        .await
        .expect("first append");
    store.rollover().await.expect("first rollover");
    store
        .append(&[entry(
            old_ts,
            "api/dep/replica0",
            LogOrigin::Service,
            "late",
        )])
        .await
        .expect("late append");
    store.rollover().await.expect("second rollover");

    let rows = store
        .read_tail_by_prefix_origin("api/dep/", None, 10)
        .await
        .expect("read both parts");
    assert_eq!(
        rows.iter().map(|row| row.text.as_str()).collect::<Vec<_>>(),
        vec!["first", "late"]
    );
    let partition =
        root.join("parts/service-logs/service_id=api/deployment_id=dep/date=2023-11-14");
    let part_count = std::fs::read_dir(partition)
        .expect("partition")
        .filter_map(Result::ok)
        .filter(|entry| {
            entry
                .path()
                .extension()
                .is_some_and(|extension| extension == "parquet")
        })
        .count();
    assert_eq!(part_count, 2);
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn rollover_replaces_an_uncommitted_export_before_including_late_rows() {
    let root = temp_root("orphan-export");
    let store = DuckLogStore::open(&root).expect("open");
    let old_ts = 1_700_000_000_000;
    store
        .append(&[entry(
            old_ts,
            "api/dep/replica0",
            LogOrigin::Service,
            "first",
        )])
        .await
        .expect("first append");

    let partition =
        root.join("parts/service-logs/service_id=api/deployment_id=dep/date=2023-11-14");
    std::fs::create_dir_all(&partition).expect("partition directory");
    let orphan = partition.join("part-1-1.parquet");
    {
        let conn = store.service.writer().expect("connection");
        conn.execute_batch(&format!(
            r#"
                COPY (
                    SELECT
                        seq,
                        ts,
                        unit,
                        origin,
                        level,
                        stream,
                        text,
                        tags,
                        attributes
                    FROM logs
                    ORDER BY seq
                ) TO {} (
                    FORMAT PARQUET,
                    COMPRESSION ZSTD
                )
            "#,
            sql_lit(&orphan.to_string_lossy())
        ))
        .expect("simulate renamed export before failed ledger transaction");
    }
    store
        .append(&[entry(
            old_ts,
            "api/dep/replica0",
            LogOrigin::Service,
            "late",
        )])
        .await
        .expect("late append");

    assert_eq!(store.rollover().await.expect("rollover recovery"), 2);
    assert!(!orphan.exists());
    assert!(partition.join("part-1-2.parquet").exists());
    let rows = store
        .read_tail_by_prefix_origin("api/dep/", None, 10)
        .await
        .expect("cold read");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].seq, 1);
    assert_eq!(rows[1].seq, 2);
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn sqlite_migration_resumes_without_duplicates() {
    let root = temp_root("migration");
    std::fs::create_dir_all(&root).expect("root");
    let sqlite_path = root.join("logs.db");
    let sqlite = LogStore::open(&sqlite_path).expect("SQLite open");
    sqlite
        .append(&[
            entry(
                1_700_000_000_000,
                "api/dep/replica0",
                LogOrigin::Service,
                "service",
            ),
            entry(
                1_700_000_000_001,
                "maestro-probe",
                LogOrigin::System,
                "system",
            ),
        ])
        .await
        .expect("SQLite append");
    drop(sqlite);

    let duck = DuckLogStore::open(&root.join("new")).expect("DuckDB open");
    assert_eq!(duck.migrate_sqlite(&sqlite_path).await.expect("migrate"), 2);
    assert_eq!(
        duck.migrate_sqlite(&sqlite_path)
            .await
            .expect("resume migration"),
        0
    );
    assert_eq!(
        duck.read_tail_by_prefix_origin("api/", None, 10)
            .await
            .expect("service read")
            .len(),
        1
    );
    assert_eq!(
        duck.read_tail("maestro-probe", 10)
            .await
            .expect("system read")
            .len(),
        1
    );
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn retention_only_removes_partitions_marked_backed_up() {
    let root = temp_root("retention");
    let store = DuckLogStore::open(&root).expect("open");
    store
        .append(&[entry(
            1_700_000_000_000,
            "api/dep/replica0",
            LogOrigin::Service,
            "cold",
        )])
        .await
        .expect("append");
    store.rollover().await.expect("rollover");

    let pending = store.pending_backups().await.expect("pending backups");
    assert_eq!(pending.len(), 1);
    assert!(
        pending[0]
            .files
            .iter()
            .any(|path| path.file_name().is_some_and(|name| name == "manifest.json"))
    );
    assert!(
        pending[0]
            .files
            .last()
            .is_some_and(|path| path.file_name().is_some_and(|name| name == "manifest.json"))
    );
    assert_eq!(
        store
            .prune_backed_up_before(chrono::Utc::now().date_naive())
            .await
            .expect("not backed up"),
        0
    );
    store
        .mark_backed_up(
            &pending[0].tier,
            &pending[0].partition_key,
            &pending[0].seq_los,
        )
        .await
        .expect("mark backed up");
    let partition_directory = pending[0]
        .files
        .first()
        .and_then(|path| path.parent())
        .expect("partition directory")
        .to_path_buf();
    assert_eq!(
        store
            .prune_backed_up_before(chrono::Utc::now().date_naive())
            .await
            .expect("prune"),
        1
    );
    assert!(!partition_directory.exists());
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn missing_backup_partition_does_not_starve_available_partitions() {
    let root = temp_root("missing-backup-partition");
    let store = DuckLogStore::open(&root).expect("open");
    store
        .append(&[
            entry(
                1_700_000_000_000,
                "api/dep/replica0",
                LogOrigin::Service,
                "api",
            ),
            entry(
                1_700_000_000_000,
                "web/dep/replica0",
                LogOrigin::Service,
                "web",
            ),
        ])
        .await
        .expect("append");
    store.rollover().await.expect("rollover");
    std::fs::remove_dir_all(
        root.join("parts/service-logs/service_id=api/deployment_id=dep/date=2023-11-14"),
    )
    .expect("remove one partition");

    let pending = store.pending_backups().await.expect("pending backups");
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].partition_key, "web/dep/2023-11-14");
    std::fs::remove_dir_all(root).ok();
}
