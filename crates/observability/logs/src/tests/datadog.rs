use std::collections::{BTreeMap, VecDeque};
use std::io::Read;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use flate2::read::GzDecoder;
use kernel_api::{AssignmentId, ClusterId, DeploymentId, NodeId, ServiceId, Timestamp, WorkloadId};
use runtime::{HEALTHCHECK_PATH_LABEL, WorkloadMetadata};

use crate::{
    DatadogLogSink, DatadogLogSinkSettings, HttpRequest, HttpResponse, HttpTransport,
    HttpTransportError, InMemoryDeadLetterStore, IngestLogEntry, LogBody, LogFilterKind, LogOrigin,
    LogProducer, LogRecordId, LogSequence, LogSink, LogSinkError, LogStream, OriginCursor,
    SequencedLogEntry,
};

#[tokio::test]
async fn datadog_request_has_exact_headers_and_normalized_payload()
-> Result<(), Box<dyn std::error::Error>> {
    let transport = Arc::new(FakeHttpTransport::new([accepted()]));
    let sink = make_sink(
        settings()?,
        transport.clone(),
        Arc::new(InMemoryDeadLetterStore::default()),
    );
    let mut entry = workload_entry(1, "api", "slow request");
    entry.entry.severity = "warn".to_owned();
    entry
        .entry
        .attributes
        .insert("request_id".to_owned(), "r1".to_owned());
    let LogOrigin::Workload { metadata } = &mut entry.entry.origin else {
        return Err("expected workload origin".into());
    };
    metadata
        .labels
        .insert("environment".to_owned(), "production".to_owned());
    metadata
        .labels
        .insert("maestro.internal".to_owned(), "hidden".to_owned());

    let outcome = sink.send(&[entry]).await?;
    assert_eq!(outcome.filtered_entries, 0);
    let requests = transport.requests()?;
    let request = requests.first().ok_or("missing request")?;
    assert_eq!(request.url, "https://intake.example/v2/logs");
    assert_eq!(
        request.headers,
        BTreeMap::from([
            ("Content-Encoding".to_owned(), "gzip".to_owned()),
            ("Content-Type".to_owned(), "application/json".to_owned()),
            ("DD-API-KEY".to_owned(), "test-key".to_owned()),
        ])
    );
    assert_eq!(
        decode_json(&request.body)?,
        serde_json::json!([{
            "message": "slow request",
            "hostname": "workload-1",
            "service": "api",
            "ddsource": "maestro",
            "ddtags": "environment:production,cluster:cluster-1,deployment:deployment-1,workload:workload-1",
            "status": "warn",
            "request_id": "r1"
        }])
    );
    Ok(())
}

#[tokio::test]
async fn datadog_origin_and_healthcheck_filters_are_counted_without_requests()
-> Result<(), Box<dyn std::error::Error>> {
    let transport = Arc::new(FakeHttpTransport::new([]));
    let settings = settings()?.filters(vec![LogFilterKind::SuccessfulHealthcheck]);
    let sink = make_sink(
        settings,
        transport.clone(),
        Arc::new(InMemoryDeadLetterStore::default()),
    );
    let mut health = workload_entry(1, "api", "request");
    let LogOrigin::Workload { metadata } = &mut health.entry.origin else {
        return Err("expected workload origin".into());
    };
    metadata
        .labels
        .insert(HEALTHCHECK_PATH_LABEL.to_owned(), "/health".to_owned());
    health.entry.attributes = BTreeMap::from([
        ("http.method".to_owned(), "GET".to_owned()),
        ("http.status_code".to_owned(), "200".to_owned()),
        ("http.url_details.path".to_owned(), "/health".to_owned()),
    ]);
    let entries = [
        health,
        workload_entry(2, "ingress", "access"),
        workload_entry(3, "tailscale", "connected"),
        system_entry(4, "daemon", "startup"),
    ];

    let outcome = sink.send(&entries).await?;
    assert_eq!(outcome.filtered_entries, 4);
    assert!(transport.requests()?.is_empty());
    Ok(())
}

#[tokio::test]
async fn included_ingress_logs_use_the_traefik_source() -> Result<(), Box<dyn std::error::Error>> {
    let transport = Arc::new(FakeHttpTransport::new([accepted()]));
    let sink = make_sink(
        settings()?.include_ingress_logs(true),
        transport.clone(),
        Arc::new(InMemoryDeadLetterStore::default()),
    );

    sink.send(&[workload_entry(1, "ingress", "access")]).await?;
    let requests = transport.requests()?;
    let payload = decode_json(&requests.first().ok_or("missing request")?.body)?;
    assert_eq!(
        payload.pointer("/0/ddsource"),
        Some(&serde_json::json!("traefik"))
    );
    Ok(())
}

#[tokio::test]
async fn oversized_batches_are_split_before_transport() -> Result<(), Box<dyn std::error::Error>> {
    let transport = Arc::new(FakeHttpTransport::new([accepted(), accepted()]));
    let sink = make_sink(
        settings()?,
        transport.clone(),
        Arc::new(InMemoryDeadLetterStore::default()),
    );

    sink.send(&[
        workload_entry(1, "api", &"a".repeat(2_500_000)),
        workload_entry(2, "api", &"b".repeat(2_500_000)),
    ])
    .await?;
    assert_eq!(transport.requests()?.len(), 2);
    Ok(())
}

#[tokio::test]
async fn payload_too_large_is_bisected_or_quarantined_at_one_record()
-> Result<(), Box<dyn std::error::Error>> {
    let transport = Arc::new(FakeHttpTransport::new([
        rejected(413),
        accepted(),
        accepted(),
    ]));
    let dead_letters = Arc::new(InMemoryDeadLetterStore::default());
    let sink = make_sink(settings()?, transport.clone(), dead_letters.clone());
    let outcome = sink
        .send(&[
            workload_entry(1, "api", "first"),
            workload_entry(2, "api", "second"),
        ])
        .await?;
    assert_eq!(transport.requests()?.len(), 3);
    assert_eq!(outcome.quarantined_entries, 0);

    let transport = Arc::new(FakeHttpTransport::new([rejected(413)]));
    let sink = make_sink(settings()?, transport, dead_letters.clone());
    let outcome = sink.send(&[workload_entry(3, "api", "poison")]).await?;
    assert_eq!(outcome.quarantined_entries, 1);
    assert_eq!(
        crate::DeadLetterStore::stats(dead_letters.as_ref(), sink.id())
            .await?
            .count,
        1
    );
    Ok(())
}

#[tokio::test]
async fn isolated_bad_request_is_quarantined_but_global_bad_request_pins_cursor()
-> Result<(), Box<dyn std::error::Error>> {
    let dead_letters = Arc::new(InMemoryDeadLetterStore::default());
    let transport = Arc::new(FakeHttpTransport::new([
        rejected(400),
        rejected(400),
        accepted(),
    ]));
    let sink = make_sink(settings()?, transport, dead_letters.clone());
    let outcome = sink
        .send(&[
            workload_entry(11, "api", "first"),
            workload_entry(12, "api", "second"),
        ])
        .await?;
    assert_eq!(outcome.quarantined_entries, 1);
    assert_eq!(
        crate::DeadLetterStore::stats(dead_letters.as_ref(), sink.id())
            .await?
            .count,
        1
    );

    let clean_dead_letters = Arc::new(InMemoryDeadLetterStore::default());
    let transport = Arc::new(FakeHttpTransport::new([
        rejected(400),
        rejected(400),
        rejected(400),
    ]));
    let sink = make_sink(settings()?, transport, clean_dead_letters.clone());
    assert!(matches!(
        sink.send(&[
            workload_entry(21, "api", "first"),
            workload_entry(22, "api", "second"),
        ])
        .await,
        Err(LogSinkError::Rejected { .. })
    ));
    assert_eq!(
        crate::DeadLetterStore::stats(clean_dead_letters.as_ref(), sink.id())
            .await?
            .count,
        0
    );
    Ok(())
}

#[tokio::test]
async fn operational_failures_are_retryable_and_never_quarantined()
-> Result<(), Box<dyn std::error::Error>> {
    for status in [403, 429, 503] {
        let dead_letters = Arc::new(InMemoryDeadLetterStore::default());
        let transport = Arc::new(FakeHttpTransport::new([rejected(status)]));
        let sink = make_sink(settings()?, transport, dead_letters.clone());

        assert!(matches!(
            sink.send(&[workload_entry(u64::from(status), "api", "retry me")])
                .await,
            Err(LogSinkError::Unavailable { .. })
        ));
        assert_eq!(
            crate::DeadLetterStore::stats(dead_letters.as_ref(), sink.id())
                .await?
                .count,
            0
        );
    }
    Ok(())
}

#[test]
fn settings_and_transport_bounds_fail_closed() {
    assert!(DatadogLogSinkSettings::new("", "datadoghq.com").is_err());
    assert!(DatadogLogSinkSettings::new("key", "https://evil.example").is_err());
    assert!(crate::ReqwestHttpTransport::new(std::time::Duration::ZERO).is_err());
}

struct CapturedRequest {
    url: String,
    headers: BTreeMap<String, String>,
    body: Vec<u8>,
}

struct FakeHttpTransport {
    responses: Mutex<VecDeque<Result<HttpResponse, HttpTransportError>>>,
    requests: Mutex<Vec<CapturedRequest>>,
}

impl FakeHttpTransport {
    fn new(responses: impl IntoIterator<Item = Result<HttpResponse, HttpTransportError>>) -> Self {
        Self {
            responses: Mutex::new(responses.into_iter().collect()),
            requests: Mutex::new(Vec::new()),
        }
    }

    fn requests(&self) -> Result<std::sync::MutexGuard<'_, Vec<CapturedRequest>>, &'static str> {
        self.requests.lock().map_err(|_| "request lock poisoned")
    }
}

#[async_trait]
impl HttpTransport for FakeHttpTransport {
    async fn send(&self, request: HttpRequest) -> Result<HttpResponse, HttpTransportError> {
        self.requests
            .lock()
            .map_err(|_| HttpTransportError::Unavailable {
                message: "request lock poisoned".to_owned(),
            })?
            .push(CapturedRequest {
                url: request.url,
                headers: request.headers,
                body: request.body,
            });
        self.responses
            .lock()
            .map_err(|_| HttpTransportError::Unavailable {
                message: "response lock poisoned".to_owned(),
            })?
            .pop_front()
            .unwrap_or_else(accepted)
    }
}

fn make_sink(
    settings: DatadogLogSinkSettings,
    transport: Arc<dyn HttpTransport>,
    dead_letters: Arc<dyn crate::DeadLetterStore>,
) -> DatadogLogSink {
    DatadogLogSink::new(settings, transport, dead_letters)
}

fn settings() -> Result<DatadogLogSinkSettings, crate::DatadogLogSinkSettingsError> {
    DatadogLogSinkSettings::with_endpoint("test-key", "https://intake.example/v2/logs".to_owned())
}

fn accepted() -> Result<HttpResponse, HttpTransportError> {
    Ok(HttpResponse {
        status: 202,
        body: String::new(),
    })
}

fn rejected(status: u16) -> Result<HttpResponse, HttpTransportError> {
    Ok(HttpResponse {
        status,
        body: "test rejection".to_owned(),
    })
}

fn decode_json(body: &[u8]) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    let mut decoder = GzDecoder::new(body);
    let mut decoded = Vec::new();
    decoder.read_to_end(&mut decoded)?;
    Ok(serde_json::from_slice(&decoded)?)
}

fn workload_entry(sequence: u64, service_id: &str, body: &str) -> SequencedLogEntry {
    let cluster_id = ClusterId::new("cluster-1").unwrap();
    let node_id = NodeId::new("node-1").unwrap();
    let workload_id = WorkloadId::new(format!("workload-{sequence}")).unwrap();
    SequencedLogEntry {
        sequence: LogSequence(sequence),
        entry: IngestLogEntry {
            id: LogRecordId {
                node_id: node_id.clone(),
                producer: LogProducer::Workload(workload_id.clone()),
                cursor: OriginCursor::new(format!("cursor-{sequence}")),
            },
            observed_at: Timestamp(i64::try_from(sequence).unwrap_or(i64::MAX)),
            event_at: Timestamp(i64::try_from(sequence).unwrap_or(i64::MAX)),
            severity: "info".to_owned(),
            stream: LogStream::Stdout,
            origin: LogOrigin::Workload {
                metadata: WorkloadMetadata {
                    cluster_id,
                    node_id,
                    service_id: ServiceId::new(service_id).unwrap(),
                    deployment_id: DeploymentId::new("deployment-1").unwrap(),
                    assignment_id: AssignmentId::new("assignment-1").unwrap(),
                    workload_id,
                    labels: BTreeMap::new(),
                },
            },
            body: LogBody::Text(body.to_owned()),
            attributes: BTreeMap::new(),
        },
    }
}

fn system_entry(sequence: u64, component: &str, body: &str) -> SequencedLogEntry {
    let cluster_id = ClusterId::new("cluster-1").unwrap();
    let node_id = NodeId::new("node-1").unwrap();
    SequencedLogEntry {
        sequence: LogSequence(sequence),
        entry: IngestLogEntry {
            id: LogRecordId {
                node_id: node_id.clone(),
                producer: LogProducer::System(component.to_owned()),
                cursor: OriginCursor::new(format!("cursor-{sequence}")),
            },
            observed_at: Timestamp(i64::try_from(sequence).unwrap_or(i64::MAX)),
            event_at: Timestamp(i64::try_from(sequence).unwrap_or(i64::MAX)),
            severity: "info".to_owned(),
            stream: LogStream::System,
            origin: LogOrigin::System {
                cluster_id,
                node_id: Some(node_id),
                component: component.to_owned(),
            },
            body: LogBody::Text(body.to_owned()),
            attributes: BTreeMap::new(),
        },
    }
}
