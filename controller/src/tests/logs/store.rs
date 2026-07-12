use super::*;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

fn temp_db_path(label: &str) -> std::path::PathBuf {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "maestro-logs-test-{label}-{}-{unique}.sqlite",
        std::process::id()
    ))
}

fn sample_entry() -> LogEntry {
    LogEntry {
        seq: 0,
        ts: 1_700_000_000_000,
        level: Arc::from("info"),
        stream: Arc::from("stdout"),
        text: "request".to_string(),
        source: Arc::from("app/abc/replica0"),
        origin: LogOrigin::Service,
        tags: Arc::new(serde_json::json!(["service:app", "replica:0"])),
        attrs: vec![
            ("http.method".to_string(), "GET".to_string()),
            ("http.status_code".to_string(), "200".to_string()),
            ("duration".to_string(), "933354417".to_string()),
        ],
    }
}

#[tokio::test]
async fn append_and_read_preserves_attrs_and_tags() {
    let path = temp_db_path("roundtrip");
    let store = LogStore::open(&path).expect("open store");

    store.append(&[sample_entry()]).await.expect("append");

    let entries = store.read_tail_all(10).await.expect("read");
    assert_eq!(entries.len(), 1);
    let got = &entries[0];

    assert_eq!(got.text, "request");
    assert_eq!(got.level.as_ref(), "info");
    assert_eq!(got.source.as_ref(), "app/abc/replica0");

    assert_eq!(
        got.attrs,
        vec![
            ("http.method".to_string(), "GET".to_string()),
            ("http.status_code".to_string(), "200".to_string()),
            ("duration".to_string(), "933354417".to_string()),
        ]
    );

    let tags_arr = got.tags.as_array().expect("tags is array");
    assert_eq!(tags_arr.len(), 2);
    assert_eq!(tags_arr[0], serde_json::json!("service:app"));

    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn entry_without_attrs_round_trips_with_empty_vec() {
    let path = temp_db_path("noattrs");
    let store = LogStore::open(&path).expect("open store");

    let mut entry = sample_entry();
    entry.attrs = vec![];
    store.append(&[entry]).await.expect("append");

    let entries = store.read_tail_all(10).await.expect("read");
    assert_eq!(entries.len(), 1);
    assert!(entries[0].attrs.is_empty());

    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn spool_cleanup_is_bounded_by_the_slowest_registered_sink() {
    let path = temp_db_path("sink-watermark");
    let store = LogStore::open(&path).expect("open store");
    store.register_sink("controller").await.expect("probe sink");
    store.register_sink("datadog").await.expect("Datadog sink");

    let mut entries = Vec::new();
    for index in 1..=3 {
        let mut entry = sample_entry();
        entry.text = format!("request-{index}");
        entries.push(entry);
    }
    store.append(&entries).await.expect("append");
    store
        .set_sink_cursor("controller", 3)
        .await
        .expect("probe acknowledged");
    store
        .set_sink_cursor("datadog", 1)
        .await
        .expect("Datadog acknowledged one");

    assert_eq!(store.min_sink_cursor().await.expect("watermark"), Some(1));
    store
        .register_sink("temporary")
        .await
        .expect("temporary sink");
    assert_eq!(store.min_sink_cursor().await.expect("watermark"), Some(0));
    store
        .unregister_sink("temporary")
        .await
        .expect("remove disabled sink");
    assert_eq!(store.min_sink_cursor().await.expect("watermark"), Some(1));
    assert_eq!(store.delete_before(1).await.expect("cleanup"), 1);
    let remaining = store.read_tail_all(10).await.expect("remaining");
    assert_eq!(
        remaining.iter().map(|entry| entry.seq).collect::<Vec<_>>(),
        vec![2, 3]
    );

    // Re-registering a sink must never reset its durable cursor.
    store.register_sink("datadog").await.expect("re-register");
    assert_eq!(store.get_sink_cursor("datadog").await.expect("cursor"), 1);
    store
        .set_sink_cursor("datadog", 3)
        .await
        .expect("Datadog caught up");
    assert_eq!(store.min_sink_cursor().await.expect("watermark"), Some(3));
    assert_eq!(store.delete_before(3).await.expect("cleanup"), 2);
    assert!(store.read_tail_all(10).await.expect("empty").is_empty());

    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn stats_snapshot_reports_spool_sink_and_dead_letter_state() {
    let path = temp_db_path("stats-snapshot");
    let store = LogStore::open(&path).expect("open store");
    store.register_sink("controller").await.expect("probe sink");
    store.register_sink("datadog").await.expect("Datadog sink");

    store
        .append(&[sample_entry(), sample_entry(), sample_entry()])
        .await
        .expect("append");
    store
        .set_sink_cursor("controller", 2)
        .await
        .expect("probe cursor");
    store
        .set_sink_cursor("datadog", 1)
        .await
        .expect("Datadog cursor");
    store
        .record_sink_dead_letter("datadog", 1, 413, "too large", b"payload")
        .await
        .expect("dead letter");

    let stats = store.stats_snapshot().await.expect("stats snapshot");
    assert_eq!(stats.row_count, 3);
    assert_eq!(stats.high_watermark, 3);
    assert_eq!(stats.oldest_entry_at_ms, Some(1_700_000_000_000));
    assert!(stats.database_bytes > 0);
    assert_eq!(stats.sinks.len(), 2);
    assert_eq!(stats.sinks[0].sink_id, "controller");
    assert_eq!(stats.sinks[0].pending_entries, 1);
    assert_eq!(stats.sinks[1].sink_id, "datadog");
    assert_eq!(stats.sinks[1].pending_entries, 2);
    assert_eq!(stats.dead_letters.count, 1);
    assert_eq!(stats.dead_letters.payload_bytes, 7);
    assert_eq!(stats.dead_letters.latest_status, Some(413));
    assert_eq!(
        stats.dead_letters.latest_error.as_deref(),
        Some("too large")
    );

    drop(store);
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn sqlite_stats_metrics_round_trip_with_labels() {
    let path = temp_db_path("stats-metrics");
    let store = LogStore::open(&path).expect("open store");
    let point = crate::cluster_stats::StatsMetricPoint {
        ts: 1_700_000_000_000,
        name: "logs.sink.pending_entries".to_string(),
        value: 12.0,
        labels: std::collections::BTreeMap::from([("sink".to_string(), "datadog".to_string())]),
    };
    store
        .append_stats_metrics(std::slice::from_ref(&point))
        .await
        .expect("append stats metric");
    store
        .append_stats_metrics(std::slice::from_ref(&point))
        .await
        .expect("duplicate is idempotent");

    let points = store
        .read_stats_metrics(
            Some("logs.sink.pending_entries"),
            1_699_999_999_999,
            1_700_000_000_001,
        )
        .await
        .expect("read stats metrics");
    assert_eq!(points, vec![point]);

    drop(store);
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn sink_cursor_defaults_only_when_the_sink_row_is_missing() {
    let path = temp_db_path("sink-cursor-errors");
    let store = LogStore::open(&path).expect("open store");

    assert_eq!(
        store
            .get_sink_cursor("not-registered")
            .await
            .expect("missing cursor"),
        0
    );

    {
        let conn = rusqlite::Connection::open(&path).expect("inspect store");
        conn.execute_batch("DROP TABLE sink_cursors")
            .expect("break cursor storage");
    }

    let error = store
        .get_sink_cursor("datadog")
        .await
        .expect_err("database errors must propagate");
    assert!(error.to_string().contains("sink_cursors"));

    drop(store);
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn sink_reads_propagate_row_decode_errors() {
    let path = temp_db_path("sink-row-errors");
    let store = LogStore::open(&path).expect("open store");
    store.append(&[sample_entry()]).await.expect("append");

    {
        let conn = rusqlite::Connection::open(&path).expect("inspect store");
        conn.execute("UPDATE logs SET ts = 'invalid'", [])
            .expect("corrupt row type");
    }

    store
        .read_after(0, 10)
        .await
        .expect_err("row decode errors must propagate");

    drop(store);
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn sink_dead_letters_are_durable_and_idempotent_by_sink_sequence() {
    let path = temp_db_path("dead-letter");
    let store = LogStore::open(&path).expect("open store");
    store
        .record_sink_dead_letter("datadog", 42, 400, "malformed", br#"[{"message":"bad"}]"#)
        .await
        .expect("record dead letter");
    store
        .record_sink_dead_letter("datadog", 42, 413, "too large", br#"[{"message":"bad"}]"#)
        .await
        .expect("update dead letter");
    assert_eq!(
        store
            .sink_dead_letter_count("datadog")
            .await
            .expect("count"),
        1
    );

    let conn = rusqlite::Connection::open(&path).expect("inspect dead letter");
    let (status, error, payload): (i64, String, Vec<u8>) = conn
        .query_row(
            r#"
                SELECT
                    status,
                    error,
                    payload
                FROM sink_dead_letters
                WHERE sink_id = 'datadog'
                  AND seq = 42
            "#,
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .expect("dead letter row");
    assert_eq!(status, 413);
    assert_eq!(error, "too large");
    assert_eq!(payload, br#"[{"message":"bad"}]"#);

    let stats = store
        .sink_dead_letter_stats("datadog")
        .await
        .expect("dead-letter stats");
    assert_eq!(stats.count, 1);
    assert_eq!(stats.payload_bytes, payload.len() as u64);
    let listed = store
        .list_sink_dead_letters("datadog", 10)
        .await
        .expect("list dead letters");
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].seq, 42);
    assert_eq!(listed[0].status, 413);

    let export_path = path.with_extension("jsonl");
    let overwrite_error = store
        .export_sink_dead_letters("datadog", &path)
        .await
        .expect_err("export must not overwrite the spool database");
    assert!(overwrite_error.to_string().contains("must not overwrite"));
    assert_eq!(
        store
            .export_sink_dead_letters("datadog", &export_path)
            .await
            .expect("export dead letters"),
        1
    );
    let exported = std::fs::read_to_string(&export_path).expect("read export");
    let exported: serde_json::Value = serde_json::from_str(exported.trim()).expect("JSONL row");
    assert_eq!(exported["seq"], 42);
    assert_eq!(exported["payload"][0]["message"], "bad");
    assert_eq!(
        store
            .purge_sink_dead_letters("datadog", Some(42))
            .await
            .expect("purge dead letters"),
        1
    );
    assert_eq!(
        store
            .sink_dead_letter_count("datadog")
            .await
            .expect("empty count"),
        0
    );
    drop(conn);
    drop(store);
    let _ = std::fs::remove_file(export_path);
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn sink_dead_letter_cap_pins_new_entries_but_allows_idempotent_updates() {
    let path = temp_db_path("dead-letter-cap");
    let store = LogStore::open(&path).expect("open store");
    {
        let conn = rusqlite::Connection::open(&path).expect("seed dead letters");
        conn.execute_batch(
            "WITH RECURSIVE n(seq) AS (
                VALUES(1) UNION ALL SELECT seq + 1 FROM n WHERE seq < 100000
             )
             INSERT INTO sink_dead_letters
                (sink_id,seq,status,error,payload,payload_sha256,created_at_ms)
             SELECT 'datadog',seq,400,'bad',X'5B5D','hash',0 FROM n;",
        )
        .expect("fill dead-letter table");
    }

    store
        .record_sink_dead_letter("datadog", 1, 413, "updated", b"[]")
        .await
        .expect("existing row can be updated at the cap");
    let error = store
        .record_sink_dead_letter("datadog", 100_001, 400, "new", b"[]")
        .await
        .expect_err("new row must be rejected at the cap");
    assert!(error.to_string().contains("100000 rows"));

    drop(store);
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn migration_renames_legacy_attributes_column_to_tags() {
    let path = temp_db_path("migration");

    {
        let conn = rusqlite::Connection::open(&path).expect("open raw conn");
        conn.execute_batch(
            "CREATE TABLE logs (
                seq INTEGER PRIMARY KEY AUTOINCREMENT,
                ts INTEGER NOT NULL,
                level TEXT NOT NULL,
                stream TEXT NOT NULL,
                text TEXT NOT NULL,
                source TEXT NOT NULL,
                origin TEXT NOT NULL DEFAULT 'system',
                attributes TEXT NOT NULL DEFAULT '{}'
            );
            INSERT INTO logs (ts, level, stream, text, source, origin, attributes)
            VALUES (
                1700000000000,
                'info',
                'stdout',
                'legacy',
                'svc/x/replica0',
                'service',
                '[\"service:svc\"]'
            );",
        )
        .expect("seed legacy table");
    }

    let store = LogStore::open(&path).expect("open migrates schema");
    let entries = store.read_tail_all(10).await.expect("read after migration");
    assert_eq!(entries.len(), 1);

    let got = &entries[0];
    assert_eq!(got.text, "legacy");
    let tags_arr = got.tags.as_array().expect("tags is array");
    assert_eq!(tags_arr[0], serde_json::json!("service:svc"));
    assert!(got.attrs.is_empty());

    store
        .append(&[sample_entry()])
        .await
        .expect("append after migration");
    let entries = store.read_tail_all(10).await.expect("read again");
    assert_eq!(entries.len(), 2);
    let fresh = entries.iter().find(|e| e.text == "request").expect("fresh");
    assert_eq!(fresh.attrs.len(), 3);

    let _ = std::fs::remove_file(&path);
}
