use std::path::Path;

use maestro_telemetry::{
    HistogramGroup, HistogramQuery, IngressTrafficQuery, LogQuery, LogRecord, LogStore,
    MetricPoint, MetricsQuery, ReplicaIdentity, StoredLog, Stream, TailCheckpoint, TrafficScope,
};

const OLD_TS: u64 = 1_700_000_000_000;
const OLD_PARTITION: &str = "service_id=api/deployment_id=dep1/date=2023-11-14";

fn record(ts: u64, line: &str) -> LogRecord {
    LogRecord {
        ts,
        stream: Stream::Stdout,
        line: line.to_string(),
    }
}

fn identity(service_id: &str, deployment_id: &str) -> ReplicaIdentity {
    ReplicaIdentity {
        service_id: Some(service_id.to_string()),
        deployment_id: Some(deployment_id.to_string()),
    }
}

fn ingest(store: &LogStore, replica_id: &str, identity: &ReplicaIdentity, records: &[LogRecord]) {
    store
        .ingest(
            replica_id,
            identity,
            records,
            TailCheckpoint {
                inode: 1,
                offset: records.len() as u64,
            },
        )
        .unwrap();
}

async fn search(store: &LogStore, query: &str) -> Vec<StoredLog> {
    store
        .read_logs(LogQuery {
            before_ts: None,
            after_ts: None,
            search: Some(query.parse().unwrap()),
            before: None,
            limit: 100,
        })
        .await
        .unwrap()
}

fn lines(logs: &[StoredLog]) -> Vec<&str> {
    logs.iter().map(|log| log.line.as_str()).collect()
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

fn parquet_files(directory: &Path) -> Vec<String> {
    let mut names = std::fs::read_dir(directory)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().to_string())
        .filter(|name| name.ends_with(".parquet"))
        .collect::<Vec<_>>();
    names.sort();
    names
}

#[tokio::test]
async fn reads_newest_first_with_identity_and_cursor() {
    let directory = tempfile::tempdir().unwrap();
    let store = LogStore::open(directory.path()).unwrap();
    let api = identity("api", "dep1");
    ingest(
        &store,
        "dep1-0",
        &api,
        &(0..5)
            .map(|index| record(now_ms(), &format!("line {index}")))
            .collect::<Vec<_>>(),
    );

    let first_page = store
        .read_logs(LogQuery {
            before_ts: None,
            after_ts: None,
            search: None,
            before: None,
            limit: 2,
        })
        .await
        .unwrap();
    assert_eq!(lines(&first_page), ["line 4", "line 3"]);
    assert_eq!(first_page[0].service_id.as_deref(), Some("api"));
    assert_eq!(first_page[0].deployment_id.as_deref(), Some("dep1"));
    assert_eq!(first_page[0].replica_id, "dep1-0");
    assert_eq!(first_page[0].stream, "stdout");

    let second_page = store
        .read_logs(LogQuery {
            before_ts: None,
            after_ts: None,
            search: None,
            before: Some(first_page[1].seq),
            limit: 10,
        })
        .await
        .unwrap();
    assert_eq!(lines(&second_page), ["line 2", "line 1", "line 0"]);
}

#[tokio::test]
async fn ingest_persists_checkpoints_atomically() {
    let directory = tempfile::tempdir().unwrap();
    let store = LogStore::open(directory.path()).unwrap();
    store
        .ingest(
            "dep1-0",
            &ReplicaIdentity::default(),
            &[record(now_ms(), "hello")],
            TailCheckpoint {
                inode: 42,
                offset: 128,
            },
        )
        .unwrap();
    drop(store);

    let reopened = LogStore::open(directory.path()).unwrap();
    assert_eq!(
        reopened.checkpoints().unwrap().get("dep1-0"),
        Some(&TailCheckpoint {
            inode: 42,
            offset: 128
        })
    );
    reopened.forget_checkpoint("dep1-0").unwrap();
    assert!(reopened.checkpoints().unwrap().is_empty());
    let logs = reopened
        .read_logs(LogQuery {
            before_ts: None,
            after_ts: None,
            limit: 10,
            ..LogQuery::default()
        })
        .await
        .unwrap();
    assert_eq!(lines(&logs), ["hello"]);
    assert_eq!(logs[0].service_id, None);
}

#[tokio::test]
async fn query_language_filters_fields_text_and_attributes() {
    let directory = tempfile::tempdir().unwrap();
    let store = LogStore::open(directory.path()).unwrap();
    let now = now_ms();
    ingest(
        &store,
        "dep1-0",
        &identity("api", "dep1"),
        &[
            record(
                now,
                r#"{"level":"error","msg":"upstream request failed","DownstreamStatus":503,"user":{"id":"u1"}}"#,
            ),
            record(
                now,
                r#"{"level":"info","msg":"request ok","http.status_code":200,"duration_ms":12}"#,
            ),
            record(
                now,
                r#"{"level":"warn","msg":"slow request","http":{"status_code":429},"duration_ms":900}"#,
            ),
            record(now, "plain health check"),
        ],
    );
    store
        .ingest(
            "dep2-0",
            &identity("worker", "dep2"),
            &[LogRecord {
                ts: now,
                stream: Stream::Stderr,
                line: "2026-03-15T20:28:36Z ERR worker crashed".to_string(),
            }],
            TailCheckpoint {
                inode: 2,
                offset: 1,
            },
        )
        .unwrap();

    assert_eq!(
        lines(&search(&store, "level:error").await),
        [
            "2026-03-15T20:28:36Z ERR worker crashed",
            r#"{"level":"error","msg":"upstream request failed","DownstreamStatus":503,"user":{"id":"u1"}}"#,
        ]
    );
    assert_eq!(search(&store, "service:api").await.len(), 4);
    assert_eq!(search(&store, "service:work*").await.len(), 1);
    assert_eq!(search(&store, "deployment:dep2").await.len(), 1);
    assert_eq!(search(&store, "replica:dep1-0").await.len(), 4);
    assert_eq!(search(&store, "source:dep2-0 stream:stderr").await.len(), 1);
    assert_eq!(search(&store, "REQUEST").await.len(), 3);
    assert_eq!(search(&store, r#""health check""#).await.len(), 1);
    assert_eq!(search(&store, "message:*crashed").await.len(), 1);
    assert_eq!(search(&store, "request -failed").await.len(), 2);
    assert_eq!(search(&store, "request AND NOT slow").await.len(), 2);
    assert_eq!(search(&store, "crashed OR \"request ok\"").await.len(), 2);
    assert_eq!(
        search(&store, "@http.status_code:[500 TO 599]").await.len(),
        1
    );
    assert_eq!(search(&store, "@http.status_code:429").await.len(), 1);
    assert_eq!(search(&store, "@http.status_code:>=200").await.len(), 3);
    assert_eq!(search(&store, "@duration_ms:>100").await.len(), 1);
    assert_eq!(search(&store, "@duration_ms:<=12").await.len(), 1);
    assert_eq!(search(&store, "@user.id:u1").await.len(), 1);
    assert_eq!(search(&store, "@user.id:*").await.len(), 1);
    assert_eq!(
        search(&store, "(level:warn OR level:error) service:api")
            .await
            .len(),
        2
    );
}

#[tokio::test]
async fn rollover_keeps_old_logs_queryable_from_parquet() {
    let directory = tempfile::tempdir().unwrap();
    let store = LogStore::open(directory.path()).unwrap();
    ingest(
        &store,
        "dep1-0",
        &identity("api", "dep1"),
        &[record(
            OLD_TS,
            r#"{"level":"error","msg":"cold failure","DownstreamStatus":"503"}"#,
        )],
    );
    assert_eq!(store.rollover().await.unwrap(), 1);
    let partition = directory.path().join("parts").join(OLD_PARTITION);
    assert!(partition.join("manifest.json").exists());
    assert_eq!(parquet_files(&partition), ["part-1-1.parquet"]);

    ingest(
        &store,
        "dep1-0",
        &identity("api", "dep1"),
        &[record(
            now_ms(),
            r#"{"level":"info","msg":"hot success","http.response.status_code":200}"#,
        )],
    );
    let failures = search(&store, "failure AND @http.status_code:[500 TO 599]").await;
    assert_eq!(failures.len(), 1);
    assert_eq!(failures[0].service_id.as_deref(), Some("api"));
    let everything = store
        .read_logs(LogQuery {
            before_ts: None,
            after_ts: None,
            limit: 10,
            ..LogQuery::default()
        })
        .await
        .unwrap();
    assert_eq!(everything.len(), 2);
    assert!(everything[0].seq > everything[1].seq);
}

#[tokio::test]
async fn rollover_exports_late_arrivals_as_additional_parts() {
    let directory = tempfile::tempdir().unwrap();
    let store = LogStore::open(directory.path()).unwrap();
    let api = identity("api", "dep1");
    ingest(&store, "dep1-0", &api, &[record(OLD_TS, "first")]);
    store.rollover().await.unwrap();
    ingest(&store, "dep1-0", &api, &[record(OLD_TS, "late")]);
    store.rollover().await.unwrap();

    let logs = store
        .read_logs(LogQuery {
            before_ts: None,
            after_ts: None,
            limit: 10,
            ..LogQuery::default()
        })
        .await
        .unwrap();
    assert_eq!(lines(&logs), ["late", "first"]);
    assert_eq!(
        parquet_files(&directory.path().join("parts").join(OLD_PARTITION)),
        ["part-1-1.parquet", "part-2-2.parquet"]
    );
}

#[tokio::test]
async fn rollover_replaces_an_uncommitted_export() {
    let directory = tempfile::tempdir().unwrap();
    let store = LogStore::open(directory.path()).unwrap();
    let api = identity("api", "dep1");
    ingest(&store, "dep1-0", &api, &[record(OLD_TS, "first")]);
    let partition = directory.path().join("parts").join(OLD_PARTITION);
    std::fs::create_dir_all(&partition).unwrap();
    let orphan = partition.join("part-1-1.parquet");
    duckdb::Connection::open_in_memory()
        .unwrap()
        .execute_batch(&format!(
            "COPY (SELECT 1::BIGINT AS seq) TO '{}' (FORMAT PARQUET)",
            orphan.display()
        ))
        .unwrap();
    ingest(&store, "dep1-0", &api, &[record(OLD_TS, "late")]);

    assert_eq!(store.rollover().await.unwrap(), 2);
    assert!(!orphan.exists());
    assert_eq!(parquet_files(&partition), ["part-1-2.parquet"]);
    let logs = store
        .read_logs(LogQuery {
            before_ts: None,
            after_ts: None,
            limit: 10,
            ..LogQuery::default()
        })
        .await
        .unwrap();
    assert_eq!(lines(&logs), ["late", "first"]);
}

#[tokio::test]
async fn rollover_and_ingest_can_run_concurrently_without_losing_rows() {
    let directory = tempfile::tempdir().unwrap();
    let store = std::sync::Arc::new(LogStore::open(directory.path()).unwrap());
    let api = identity("api", "dep1");
    let old_records = (0..200)
        .map(|index| record(OLD_TS, &format!("old-{index}")))
        .collect::<Vec<_>>();
    ingest(&store, "dep1-0", &api, &old_records);

    let ingesting = {
        let store = store.clone();
        tokio::task::spawn_blocking(move || {
            ingest(&store, "dep1-0", &api, &[record(now_ms(), "current")])
        })
    };
    let (rolled, ingested) = tokio::join!(store.rollover(), ingesting);
    assert_eq!(rolled.unwrap(), 200);
    ingested.unwrap();

    let logs = store
        .read_logs(LogQuery {
            before_ts: None,
            after_ts: None,
            limit: 500,
            ..LogQuery::default()
        })
        .await
        .unwrap();
    assert_eq!(logs.len(), 201);
    assert_eq!(logs.iter().filter(|log| log.line == "current").count(), 1);
}

#[tokio::test]
async fn reopen_cleans_stale_temp_exports() {
    let directory = tempfile::tempdir().unwrap();
    let store = LogStore::open(directory.path()).unwrap();
    ingest(
        &store,
        "dep1-0",
        &identity("api", "dep1"),
        &[record(OLD_TS, "cold")],
    );
    store.rollover().await.unwrap();
    drop(store);
    let partition = directory.path().join("parts").join(OLD_PARTITION);
    let stale = partition.join("part-9-9.parquet.tmp-1234");
    std::fs::write(&stale, b"partial").unwrap();

    let reopened = LogStore::open(directory.path()).unwrap();
    assert!(!stale.exists());
    assert_eq!(search(&reopened, "cold").await.len(), 1);
}

#[tokio::test]
async fn stores_filters_and_prunes_metrics() {
    let directory = tempfile::tempdir().unwrap();
    let store = LogStore::open(directory.path()).unwrap();
    let point = |ts: i64, source: &str| MetricPoint {
        ts,
        source: source.to_string(),
        cpu_percent: 12.5,
        memory_bytes: 1024,
        memory_limit_bytes: 4096,
        net_rx_bytes: 1,
        net_tx_bytes: 2,
    };
    store
        .insert_metrics(&[
            point(100, "node"),
            point(100, "container:d1-0"),
            point(200, "container:d1-1"),
            point(300, "node"),
        ])
        .unwrap();
    let node = store
        .read_metrics(MetricsQuery {
            source: Some("node".to_string()),
            source_prefix: None,
            from: 0,
            to: 250,
        })
        .await
        .unwrap();
    assert_eq!(node, [point(100, "node")]);
    let containers = store
        .read_metrics(MetricsQuery {
            source: None,
            source_prefix: Some("container:".to_string()),
            from: 0,
            to: 1000,
        })
        .await
        .unwrap();
    assert_eq!(containers.len(), 2);
    assert_eq!(store.prune_metrics(250).unwrap(), 3);
}

#[tokio::test]
async fn counts_ingress_traffic_by_ip_and_path_per_scope() {
    let directory = tempfile::tempdir().unwrap();
    let store = LogStore::open(directory.path()).unwrap();
    let access = |router: &str, ip: &str, path: &str, status: u16| {
        format!(
            r#"{{"ClientHost":"{ip}","DownstreamStatus":{status},"RouterName":"{router}","RequestPath":"{path}"}}"#
        )
    };
    let lines = [
        access("svc-web@file", "203.0.113.1", "/a?x=1", 200),
        access("svc-web@file", "203.0.113.1", "/a", 200),
        access("svc-web-dmv7n1a4xao07-1@file", "203.0.113.2", "/b", 500),
        access("svc-webapp@file", "203.0.113.3", "/c", 200),
        access("maestro-blocked-0@file", "198.51.100.9", "/", 403),
        "not an access log".to_string(),
    ];
    let records = lines
        .iter()
        .map(|line| record(OLD_TS, line))
        .collect::<Vec<_>>();
    store
        .ingest(
            "maestro-traefik",
            &ReplicaIdentity::default(),
            &records,
            TailCheckpoint {
                inode: 1,
                offset: 1,
            },
        )
        .unwrap();
    let query = |scope: TrafficScope, service: Option<&str>| IngressTrafficQuery {
        scope,
        service_id: service.map(str::to_string),
        from: 0,
        to: i64::MAX,
        limit: 10,
    };

    let web = store
        .read_ingress_traffic(query(TrafficScope::Service, Some("web")))
        .await
        .unwrap();
    let ips = web
        .by_ip
        .iter()
        .map(|entry| (entry.value.as_str(), entry.status_code, entry.requests))
        .collect::<Vec<_>>();
    assert_eq!(ips, [("203.0.113.1", 200, 2), ("203.0.113.2", 500, 1)]);
    assert_eq!(web.by_path[0].value, "/a");

    let cluster = store
        .read_ingress_traffic(query(TrafficScope::Cluster, None))
        .await
        .unwrap();
    assert_eq!(cluster.by_ip.len(), 3);
    let blocked = store
        .read_ingress_traffic(query(TrafficScope::Blocked, None))
        .await
        .unwrap();
    assert_eq!(blocked.by_ip[0].value, "198.51.100.9");
    assert_eq!(blocked.by_ip[0].status_code, 403);
}

#[tokio::test]
async fn buckets_log_counts_by_level_and_status_class() {
    let directory = tempfile::tempdir().unwrap();
    let store = LogStore::open(directory.path()).unwrap();
    let lines = [
        r#"{"level":"error","msg":"boom"}"#.to_string(),
        r#"{"level":"info","msg":"ok","status":200}"#.to_string(),
        r#"{"level":"info","msg":"missing","DownstreamStatus":404}"#.to_string(),
    ];
    let records = lines
        .iter()
        .enumerate()
        .map(|(index, line)| record(OLD_TS + index as u64 * 61_000, line))
        .collect::<Vec<_>>();
    store
        .ingest(
            "d1-0",
            &ReplicaIdentity::default(),
            &records,
            TailCheckpoint {
                inode: 1,
                offset: 1,
            },
        )
        .unwrap();
    let from = OLD_TS as i64;
    let query = |group_by: HistogramGroup| HistogramQuery {
        search: None,
        from,
        to: from + 180_000,
        bucket_ms: 60_000,
        group_by,
    };
    let levels = store
        .log_histogram(query(HistogramGroup::Level))
        .await
        .unwrap();
    assert_eq!(
        levels
            .buckets
            .iter()
            .map(|bucket| bucket.count)
            .sum::<u64>(),
        3
    );
    assert_eq!(levels.buckets[0].levels.get("error"), Some(&1));
    let statuses = store
        .log_histogram(query(HistogramGroup::Status))
        .await
        .unwrap();
    let classes = statuses
        .buckets
        .iter()
        .flat_map(|bucket| bucket.levels.keys().cloned())
        .collect::<Vec<_>>();
    assert_eq!(classes, ["2xx", "4xx"]);
}

#[tokio::test]
async fn partitions_parquet_by_service_deployment_and_date() {
    let directory = tempfile::tempdir().unwrap();
    let store = LogStore::open(directory.path()).unwrap();
    ingest(
        &store,
        "dep1-0",
        &identity("api", "dep1"),
        &[record(OLD_TS, "api line")],
    );
    ingest(
        &store,
        "dep2-0",
        &identity("worker", "dep2"),
        &[record(OLD_TS, "worker line")],
    );
    ingest(
        &store,
        "maestro-traefik",
        &ReplicaIdentity::default(),
        &[record(OLD_TS, "system line")],
    );
    store.rollover().await.unwrap();

    let parts = directory.path().join("parts");
    for partition in [
        "service_id=api/deployment_id=dep1/date=2023-11-14",
        "service_id=worker/deployment_id=dep2/date=2023-11-14",
        "service_id=_system/deployment_id=_none/date=2023-11-14",
    ] {
        assert_eq!(
            parquet_files(&parts.join(partition)).len(),
            1,
            "{partition}"
        );
    }
    let logs = store
        .read_logs(LogQuery {
            search: None,
            before: None,
            before_ts: None,
            after_ts: None,
            limit: 10,
        })
        .await
        .unwrap();
    assert_eq!(logs.len(), 3);
}
