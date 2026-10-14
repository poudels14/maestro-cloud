use std::io::Read;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::body::Bytes;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::post;
use maestro_telemetry::{
    DatadogConfig, LogRecord, LogStore, ReplicaIdentity, Stream, TailCheckpoint, run_datadog_logs,
};

type Received = Arc<Mutex<Vec<serde_json::Value>>>;

async fn intake(State(received): State<Received>, body: Bytes) -> StatusCode {
    let mut decoded = String::new();
    flate2::read::GzDecoder::new(&body[..])
        .read_to_string(&mut decoded)
        .unwrap();
    let entries: Vec<serde_json::Value> = serde_json::from_str(&decoded).unwrap();
    match entries.iter().any(|entry| entry["message"] == "poison") {
        true => StatusCode::BAD_REQUEST,
        false => {
            received.lock().unwrap().extend(entries);
            StatusCode::ACCEPTED
        }
    }
}

fn record(ts: u64, line: &str) -> LogRecord {
    LogRecord {
        ts,
        stream: Stream::Stdout,
        line: line.to_string(),
    }
}

#[tokio::test]
async fn ships_service_logs_and_dead_letters_rejected_entries() {
    let received = Received::default();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let app = axum::Router::new()
        .route("/logs", post(intake))
        .with_state(received.clone());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let directory = tempfile::tempdir().unwrap();
    let store = Arc::new(LogStore::open(directory.path()).unwrap());
    let config = DatadogConfig {
        api_key: "key".to_string(),
        logs_url: format!("http://{address}/logs"),
        metrics_url: format!("http://{address}/series"),
        tags: vec!["env:test".to_string()],
        cluster: "dev".to_string(),
        node: "node1".to_string(),
        include_ingress_logs: false,
        include_tailscale_logs: false,
        include_healthcheck: true,
        include_metrics: false,
    };
    let sink = tokio::spawn(run_datadog_logs(
        config,
        store.clone(),
        Arc::new(std::collections::HashMap::<String, ReplicaIdentity>::new()),
    ));
    tokio::time::sleep(Duration::from_millis(200)).await;
    let identity = ReplicaIdentity {
        service_id: Some("api".to_string()),
        deployment_id: Some("d1".to_string()),
        healthcheck_path: None,
    };
    let now = 1_800_000_000_000;
    store
        .ingest(
            "d1-0",
            &identity,
            &[
                record(now, "hello"),
                record(now + 1, "poison"),
                record(now + 2, "world"),
            ],
            TailCheckpoint {
                inode: 1,
                offset: 3,
            },
        )
        .unwrap();
    store
        .ingest(
            "maestro-traefik",
            &ReplicaIdentity::default(),
            &[record(now + 3, "ingress line")],
            TailCheckpoint {
                inode: 2,
                offset: 1,
            },
        )
        .unwrap();
    tokio::time::sleep(Duration::from_secs(7)).await;
    sink.abort();

    let messages = received
        .lock()
        .unwrap()
        .iter()
        .map(|entry| entry["message"].as_str().unwrap().to_string())
        .collect::<Vec<_>>();
    assert_eq!(messages, ["hello", "world"]);
    let first = received.lock().unwrap()[0].clone();
    assert_eq!(first["service"], "api");
    assert_eq!(first["ddsource"], "maestro");
    assert!(first["ddtags"].as_str().unwrap().contains("env:test"));
    assert!(
        first["ddtags"]
            .as_str()
            .unwrap()
            .contains("deployment_id:d1")
    );
    assert_eq!(store.dead_letter_count("datadog").unwrap(), 1);
}
