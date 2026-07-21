use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use kernel_api::{NodeId, Timestamp, WorkloadId};

use crate::{
    DatadogMetricSink, DatadogMetricSinkSettings, HostMetricSequence, HostMetricSink,
    MetricHttpRequest, MetricHttpResponse, MetricHttpTransport, MetricHttpTransportError,
    MetricSequence, MetricSink, MetricSinkError, SequencedHostMetricPoint, SequencedMetricPoint,
};

#[tokio::test]
async fn datadog_request_preserves_legacy_gauges_deltas_and_tags()
-> Result<(), Box<dyn std::error::Error>> {
    let transport = Arc::new(RecordingTransport::new([]));
    let sink = DatadogMetricSink::new(settings()?, transport.clone());
    let points = points()?;

    sink.send(&points).await?;
    sink.send(&points).await?;

    let requests = transport.requests()?;
    assert_eq!(requests.len(), 2);
    let first = requests.first().ok_or("Datadog request missing")?;
    let second = requests.get(1).ok_or("replayed Datadog request missing")?;
    assert!(first == second);
    assert_eq!(first.url, "http://datadog.test/api/v2/series");
    assert_eq!(
        first.headers,
        BTreeMap::from([
            ("Content-Type".to_owned(), "application/json".to_owned()),
            ("DD-API-KEY".to_owned(), "secret".to_owned()),
        ])
    );
    let tags = serde_json::json!([
        "cluster:prod",
        "host:node-one",
        "env:test",
        "service:api",
        "deployment:deployment-1",
        "replica:2"
    ]);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&first.body)?,
        serde_json::json!({
            "series": [
                {
                    "metric": "maestro.service.memory.bytes",
                    "type": 3,
                    "points": [{"timestamp": 1_750_000_000, "value": 1024.0}],
                    "tags": tags,
                    "unit": "byte"
                },
                {
                    "metric": "maestro.service.cpu.percent",
                    "type": 3,
                    "points": [{"timestamp": 1_750_000_001, "value": 50.0}],
                    "tags": tags,
                    "unit": "percent"
                },
                {
                    "metric": "maestro.service.memory.bytes",
                    "type": 3,
                    "points": [{"timestamp": 1_750_000_001, "value": 2048.0}],
                    "tags": tags,
                    "unit": "byte"
                },
                {
                    "metric": "maestro.service.network.rx.bytes_per_sec",
                    "type": 3,
                    "points": [{"timestamp": 1_750_000_001, "value": 200.0}],
                    "tags": tags,
                    "unit": "byte"
                },
                {
                    "metric": "maestro.service.network.tx.bytes_per_sec",
                    "type": 3,
                    "points": [{"timestamp": 1_750_000_001, "value": 300.0}],
                    "tags": tags,
                    "unit": "byte"
                }
            ]
        })
    );
    Ok(())
}

#[tokio::test]
async fn datadog_counter_resets_emit_zero_rates_without_losing_memory()
-> Result<(), Box<dyn std::error::Error>> {
    let transport = Arc::new(RecordingTransport::new([]));
    let sink = DatadogMetricSink::new(settings()?, transport.clone());
    let mut points = points()?;
    let current = points.get_mut(1).ok_or("current metric point missing")?;
    current.point.cpu_usage_usec = 1;
    current.point.network_receive_bytes = Some(1);
    current.point.network_transmit_bytes = Some(1);

    sink.send(&points).await?;

    let requests = transport.requests()?;
    let request = requests.first().ok_or("Datadog request missing")?;
    let document = serde_json::from_slice::<serde_json::Value>(&request.body)?;
    let values = document
        .get("series")
        .and_then(serde_json::Value::as_array)
        .ok_or("Datadog series missing")?
        .iter()
        .filter_map(|series| series.get("points")?.get(0)?.get("value")?.as_f64())
        .collect::<Vec<_>>();
    assert_eq!(values, vec![1_024.0, 0.0, 2_048.0, 0.0, 0.0]);
    Ok(())
}

#[tokio::test]
async fn datadog_rejects_mismatched_baselines_before_transport()
-> Result<(), Box<dyn std::error::Error>> {
    let transport = Arc::new(RecordingTransport::new([]));
    let sink = DatadogMetricSink::new(settings()?, transport.clone());
    let mut points = points()?;
    points
        .get_mut(1)
        .and_then(|point| point.previous.as_mut())
        .ok_or("rate baseline missing")?
        .id
        .workload_id = WorkloadId::new("another-workload")?;

    assert!(matches!(
        sink.send(&points).await,
        Err(MetricSinkError::Rejected { .. })
    ));
    assert!(transport.requests()?.is_empty());
    Ok(())
}

#[tokio::test]
async fn datadog_host_request_preserves_legacy_gauges_rates_and_tags()
-> Result<(), Box<dyn std::error::Error>> {
    let transport = Arc::new(RecordingTransport::new([]));
    let sink = DatadogMetricSink::new(settings()?, transport.clone());
    let points = host_points()?;

    sink.send_host_metrics(&points).await?;
    sink.send_host_metrics(&points).await?;

    let requests = transport.requests()?;
    assert_eq!(requests.len(), 2);
    let first = requests.first().ok_or("Datadog host request missing")?;
    let second = requests.get(1).ok_or("replayed host request missing")?;
    assert!(first == second);
    let tags = serde_json::json!(["cluster:prod", "host:node-one", "env:test"]);
    let disk_tags = serde_json::json!(["cluster:prod", "host:node-one", "env:test", "mount:/"]);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&first.body)?,
        serde_json::json!({
            "series": [
                {
                    "metric": "maestro.node.cpu.percent",
                    "type": 3,
                    "points": [{"timestamp": 1_750_000_001, "value": 70.0}],
                    "tags": tags,
                    "unit": "percent"
                },
                {
                    "metric": "maestro.node.memory.bytes",
                    "type": 3,
                    "points": [{"timestamp": 1_750_000_001, "value": 2048.0}],
                    "tags": tags,
                    "unit": "byte"
                },
                {
                    "metric": "maestro.node.network.rx.bytes_per_sec",
                    "type": 3,
                    "points": [{"timestamp": 1_750_000_001, "value": 200.0}],
                    "tags": tags,
                    "unit": "byte"
                },
                {
                    "metric": "maestro.node.network.tx.bytes_per_sec",
                    "type": 3,
                    "points": [{"timestamp": 1_750_000_001, "value": 300.0}],
                    "tags": tags,
                    "unit": "byte"
                },
                {
                    "metric": "maestro.node.disk.used.bytes",
                    "type": 3,
                    "points": [{"timestamp": 1_750_000_001, "value": 6000.0}],
                    "tags": disk_tags,
                    "unit": "byte"
                },
                {
                    "metric": "maestro.node.disk.total.bytes",
                    "type": 3,
                    "points": [{"timestamp": 1_750_000_001, "value": 10000.0}],
                    "tags": disk_tags,
                    "unit": "byte"
                },
                {
                    "metric": "maestro.node.disk.usage.percent",
                    "type": 3,
                    "points": [{"timestamp": 1_750_000_001, "value": 60.0}],
                    "tags": disk_tags,
                    "unit": "percent"
                }
            ]
        })
    );
    Ok(())
}

#[tokio::test]
async fn datadog_host_counter_resets_emit_zero_rates() -> Result<(), Box<dyn std::error::Error>> {
    let transport = Arc::new(RecordingTransport::new([]));
    let sink = DatadogMetricSink::new(settings()?, transport.clone());
    let mut points = host_points()?;
    let resources = points
        .first_mut()
        .and_then(|point| point.point.resources.as_mut())
        .ok_or("host resources missing")?;
    resources.cpu_total_ticks = 10;
    resources.cpu_idle_ticks = 5;
    resources.network_receive_bytes = 1;
    resources.network_transmit_bytes = 1;

    sink.send_host_metrics(&points).await?;

    let requests = transport.requests()?;
    let request = requests.first().ok_or("Datadog host request missing")?;
    let document = serde_json::from_slice::<serde_json::Value>(&request.body)?;
    let values = document
        .get("series")
        .and_then(serde_json::Value::as_array)
        .ok_or("Datadog host series missing")?
        .iter()
        .filter_map(|series| series.get("points")?.get(0)?.get("value")?.as_f64())
        .collect::<Vec<_>>();
    assert_eq!(
        values,
        vec![0.0, 2_048.0, 0.0, 0.0, 6_000.0, 10_000.0, 60.0]
    );
    Ok(())
}

#[tokio::test]
async fn datadog_rejects_mismatched_host_baselines_before_transport()
-> Result<(), Box<dyn std::error::Error>> {
    let transport = Arc::new(RecordingTransport::new([]));
    let sink = DatadogMetricSink::new(settings()?, transport.clone());
    let mut points = host_points()?;
    points
        .first_mut()
        .and_then(|point| point.previous_resources.as_mut())
        .ok_or("host rate baseline missing")?
        .id
        .node_id = NodeId::new("another-node")?;

    assert!(matches!(
        sink.send_host_metrics(&points).await,
        Err(MetricSinkError::Rejected { .. })
    ));
    assert!(transport.requests()?.is_empty());
    Ok(())
}

#[tokio::test]
async fn datadog_classifies_operational_and_permanent_http_failures()
-> Result<(), Box<dyn std::error::Error>> {
    let transport = Arc::new(RecordingTransport::new([
        Ok(MetricHttpResponse {
            status: 503,
            body: "unavailable".to_owned(),
        }),
        Ok(MetricHttpResponse {
            status: 400,
            body: "bad series".to_owned(),
        }),
        Err(MetricHttpTransportError::Unavailable {
            message: "offline".to_owned(),
        }),
    ]));
    let sink = DatadogMetricSink::new(settings()?, transport);
    let points = points()?;

    assert!(matches!(
        sink.send(&points).await,
        Err(MetricSinkError::Unavailable { .. })
    ));
    assert!(matches!(
        sink.send(&points).await,
        Err(MetricSinkError::Rejected { .. })
    ));
    assert!(matches!(
        sink.send(&points).await,
        Err(MetricSinkError::Unavailable { .. })
    ));
    Ok(())
}

#[test]
fn datadog_settings_reject_unsafe_credentials_sites_identities_and_tags() {
    assert!(DatadogMetricSinkSettings::new("", "datadoghq.com", "prod", "node", vec![]).is_err());
    assert!(
        DatadogMetricSinkSettings::new("key", ".datadoghq.com", "prod", "node", vec![]).is_err()
    );
    assert!(DatadogMetricSinkSettings::new("key", "datadoghq.com", "", "node", vec![]).is_err());
    assert!(
        DatadogMetricSinkSettings::new(
            "key",
            "datadoghq.com",
            "prod",
            "node",
            vec!["bad\ntag".to_owned()],
        )
        .is_err()
    );
}

fn settings() -> Result<DatadogMetricSinkSettings, crate::DatadogMetricSinkSettingsError> {
    DatadogMetricSinkSettings::with_endpoint(
        "secret",
        "http://datadog.test/api/v2/series".to_owned(),
        "prod",
        "node-one",
        vec!["env:test".to_owned()],
    )
}

fn points() -> Result<Vec<SequencedMetricPoint>, kernel_api::InvalidIdentifier> {
    let mut first = crate::conformance::metric_point("workload-1", 1_750_000_000_000, 100_000)?;
    first.memory_current_bytes = 1_024;
    first.network_receive_bytes = Some(100);
    first.network_transmit_bytes = Some(200);
    first
        .metadata
        .labels
        .insert("maestro.replica-index".to_owned(), "2".to_owned());
    let mut second = first.clone();
    second.id.collected_at = Timestamp(1_750_000_001_000);
    second.cpu_usage_usec = 600_000;
    second.memory_current_bytes = 2_048;
    second.network_receive_bytes = Some(300);
    second.network_transmit_bytes = Some(500);
    Ok(vec![
        SequencedMetricPoint {
            sequence: MetricSequence(1),
            point: first.clone(),
            previous: None,
        },
        SequencedMetricPoint {
            sequence: MetricSequence(2),
            point: second,
            previous: Some(first),
        },
    ])
}

fn host_points() -> Result<Vec<SequencedHostMetricPoint>, kernel_api::InvalidIdentifier> {
    let mut previous = crate::conformance::host_metric_point("node-one", 1_750_000_000_000, 1_024)?;
    let previous_resources = previous.resources.as_mut().expect("test host resources");
    previous_resources.memory_total_bytes = 4_096;
    let mut current = previous.clone();
    current.id.collected_at = Timestamp(1_750_000_001_000);
    let current_resources = current.resources.as_mut().expect("test host resources");
    current_resources.cpu_total_ticks = 300;
    current_resources.cpu_idle_ticks = 100;
    current_resources.memory_used_bytes = 2_048;
    current_resources.network_receive_bytes = 300;
    current_resources.network_transmit_bytes = 500;
    Ok(vec![SequencedHostMetricPoint {
        sequence: HostMetricSequence(2),
        point: current,
        previous_resources: Some(previous),
    }])
}

#[derive(Clone, PartialEq, Eq)]
struct RecordedRequest {
    url: String,
    headers: BTreeMap<String, String>,
    body: Vec<u8>,
}

struct RecordingTransport {
    responses: Mutex<VecDeque<Result<MetricHttpResponse, MetricHttpTransportError>>>,
    requests: Mutex<Vec<RecordedRequest>>,
}

impl RecordingTransport {
    fn new(
        responses: impl IntoIterator<Item = Result<MetricHttpResponse, MetricHttpTransportError>>,
    ) -> Self {
        Self {
            responses: Mutex::new(responses.into_iter().collect()),
            requests: Mutex::new(Vec::new()),
        }
    }

    fn requests(&self) -> Result<Vec<RecordedRequest>, MetricSinkError> {
        self.requests
            .lock()
            .map(|requests| requests.clone())
            .map_err(|_| MetricSinkError::Unavailable {
                message: "recording metric HTTP request lock was poisoned".to_owned(),
            })
    }
}

#[async_trait]
impl MetricHttpTransport for RecordingTransport {
    async fn send(
        &self,
        request: MetricHttpRequest,
    ) -> Result<MetricHttpResponse, MetricHttpTransportError> {
        self.requests
            .lock()
            .map_err(|_| MetricHttpTransportError::Unavailable {
                message: "recording metric HTTP request lock was poisoned".to_owned(),
            })?
            .push(RecordedRequest {
                url: request.url,
                headers: request.headers,
                body: request.body,
            });
        self.responses
            .lock()
            .map_err(|_| MetricHttpTransportError::Unavailable {
                message: "recording metric HTTP response lock was poisoned".to_owned(),
            })?
            .pop_front()
            .unwrap_or(Ok(MetricHttpResponse {
                status: 202,
                body: String::new(),
            }))
    }
}
