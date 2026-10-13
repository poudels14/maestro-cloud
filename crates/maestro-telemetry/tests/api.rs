use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use maestro_telemetry::{
    DeploymentFromReplicaId, LogRecord, LogStore, ReplicaIdentity, Stream, TailCheckpoint,
    TelemetrySettings, router, run,
};
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tower::ServiceExt;

fn seeded_store(directory: &Path) -> Arc<LogStore> {
    let store = LogStore::open(directory).unwrap();
    let records = (0..5)
        .map(|index| LogRecord {
            ts: 1_800_000_000_000 + index,
            stream: Stream::Stdout,
            line: match index % 2 {
                0 => format!(r#"{{"level":"error","msg":"failure {index}","status":500}}"#),
                _ => format!("info line {index}"),
            },
        })
        .collect::<Vec<_>>();
    store
        .ingest(
            "dep1-0",
            &ReplicaIdentity {
                service_id: Some("api".to_string()),
                deployment_id: Some("dep1".to_string()),
            },
            &records,
            TailCheckpoint {
                inode: 1,
                offset: 1,
            },
        )
        .unwrap();
    Arc::new(store)
}

async fn get(app: &axum::Router, uri: &str) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(Request::get(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

fn lines(page: &Value) -> Vec<&str> {
    page["records"]
        .as_array()
        .unwrap()
        .iter()
        .map(|record| record["line"].as_str().unwrap())
        .collect()
}

#[tokio::test]
async fn health_reports_ok() {
    let directory = tempfile::tempdir().unwrap();
    let app = router(seeded_store(directory.path()));
    let (status, body) = get(&app, "/health").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "ok");
}

#[tokio::test]
async fn pages_newest_first_with_a_cursor() {
    let directory = tempfile::tempdir().unwrap();
    let app = router(seeded_store(directory.path()));

    let (status, first) = get(&app, "/logs?limit=2").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        lines(&first),
        [
            "{\"level\":\"error\",\"msg\":\"failure 4\",\"status\":500}",
            "info line 3"
        ]
    );
    assert_eq!(first["records"][0]["serviceId"], "api");
    assert_eq!(first["records"][0]["deploymentId"], "dep1");
    assert_eq!(first["records"][0]["replicaId"], "dep1-0");
    assert_eq!(first["records"][0]["level"], "error");
    let cursor = first["nextCursor"].as_i64().unwrap();

    let (_, second) = get(&app, &format!("/logs?limit=2&before={cursor}")).await;
    assert_eq!(lines(&second).len(), 2);
    let cursor = second["nextCursor"].as_i64().unwrap();
    let (_, last) = get(&app, &format!("/logs?limit=2&before={cursor}")).await;
    assert_eq!(lines(&last).len(), 1);
    assert!(last["nextCursor"].is_null());
}

#[tokio::test]
async fn filters_with_the_query_language() {
    let directory = tempfile::tempdir().unwrap();
    let app = router(seeded_store(directory.path()));

    let (status, errors) = get(&app, "/logs?query=level%3Aerror%20%40status%3A%3E%3D500").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(lines(&errors).len(), 3);
    let (_, phrase) = get(&app, "/logs?query=%22info%20line%201%22").await;
    assert_eq!(lines(&phrase), ["info line 1"]);
    let (_, blank) = get(&app, "/logs?query=%20%20").await;
    assert_eq!(lines(&blank).len(), 5);
}

#[tokio::test]
async fn rejects_invalid_queries() {
    let directory = tempfile::tempdir().unwrap();
    let app = router(seeded_store(directory.path()));

    let (status, body) = get(&app, "/logs?query=unknown%3Avalue").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(
        body["error"]
            .as_str()
            .unwrap()
            .contains("unknown reserved log field")
    );
    let (status, _) = get(&app, "/logs?before=not-a-number").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn run_tails_logs_and_serves_them_over_the_unix_socket() {
    let root = tempfile::tempdir().unwrap();
    let socket = root.path().join("run").join("telemetry.sock");
    let log_dir = root.path().join("logs");
    let settings = TelemetrySettings {
        log_dir: log_dir.clone(),
        data_dir: root.path().join("telemetry"),
        socket: socket.clone(),
        poll_interval: Duration::from_millis(20),
        resolver: Arc::new(DeploymentFromReplicaId),
        ..TelemetrySettings::default()
    };
    let telemetry = tokio::spawn(run(settings));

    let record = LogRecord {
        ts: 1_800_000_000_000,
        stream: Stream::Stderr,
        line: "hello from the container".to_string(),
    };
    std::fs::create_dir_all(log_dir.join("dep9-0")).unwrap();
    std::fs::File::create(log_dir.join("dep9-0").join("current.log"))
        .unwrap()
        .write_all(format!("{}\n", serde_json::to_string(&record).unwrap()).as_bytes())
        .unwrap();

    let page = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            match socket_get(&socket, "/logs?query=replica%3Adep9-0").await {
                Some(page) if !page["records"].as_array().unwrap().is_empty() => break page,
                _ => tokio::time::sleep(Duration::from_millis(20)).await,
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(lines(&page), ["hello from the container"]);
    assert_eq!(page["records"][0]["deploymentId"], "dep9");
    assert_eq!(page["records"][0]["stream"], "stderr");
    assert_eq!(
        socket_get(&socket, "/health").await.unwrap()["status"],
        "ok"
    );
    telemetry.abort();
}

async fn socket_get(socket: &Path, uri: &str) -> Option<Value> {
    let mut stream = tokio::net::UnixStream::connect(socket).await.ok()?;
    let request = format!("GET {uri} HTTP/1.1\r\nHost: telemetry\r\nConnection: close\r\n\r\n");
    stream.write_all(request.as_bytes()).await.ok()?;
    let mut response = String::new();
    stream.read_to_string(&mut response).await.ok()?;
    let (_, body) = response.split_once("\r\n\r\n")?;
    serde_json::from_str(body).ok()
}
