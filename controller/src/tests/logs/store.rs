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
            VALUES (1700000000000, 'info', 'stdout', 'legacy', 'svc/x/replica0', 'service', '[\"service:svc\"]');",
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
