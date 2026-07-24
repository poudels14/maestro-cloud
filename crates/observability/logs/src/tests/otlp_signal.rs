use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use kernel_api::{AssignmentId, ClusterId, DeploymentId, NodeId, ServiceId, Timestamp, WorkloadId};
use node_agent::{NodeMetricHandler, NodeTraceHandler, StatusClock};
use node_fabric::WorkloadClaims;
use node_fabric::otlp::{metrics, traces};
use prost::Message;
use tonic::Code;

use crate::{InMemoryLogStore, OtlpSignal, OtlpSignalHandler};

#[tokio::test]
async fn signal_handler_durably_spools_owned_metrics_and_traces_and_replays_retries()
-> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(InMemoryLogStore::new());
    let handler = OtlpSignalHandler::new(
        ClusterId::new("cluster-1")?,
        store.clone(),
        Arc::new(AdvancingClock::new(100)),
    );
    let metric_request = metrics::ExportMetricsServiceRequest {
        resource_metrics: vec![Default::default()],
    };
    let trace_request = traces::ExportTraceServiceRequest {
        resource_spans: vec![Default::default()],
    };

    handler
        .export_metrics(claims("api")?, metric_request.clone())
        .await?;
    handler
        .export_metrics(claims("api")?, metric_request.clone())
        .await?;
    handler
        .export_traces(claims("api")?, trace_request.clone())
        .await?;

    let envelopes = store.otlp_envelopes()?;
    assert_eq!(envelopes.len(), 2);
    let metric = envelopes
        .iter()
        .find(|envelope| envelope.id.signal == OtlpSignal::Metrics)
        .ok_or("metric envelope missing")?;
    assert_eq!(metric.observed_at, Timestamp(100));
    assert_eq!(metric.metadata.service_id, ServiceId::new("api")?);
    assert_eq!(
        metrics::ExportMetricsServiceRequest::decode(metric.payload.as_slice())?,
        metric_request
    );
    let trace = envelopes
        .iter()
        .find(|envelope| envelope.id.signal == OtlpSignal::Traces)
        .ok_or("trace envelope missing")?;
    assert_eq!(
        traces::ExportTraceServiceRequest::decode(trace.payload.as_slice())?,
        trace_request
    );
    Ok(())
}

#[tokio::test]
async fn signal_handler_rejects_reused_identity_with_different_authenticated_ownership()
-> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(InMemoryLogStore::new());
    let handler = OtlpSignalHandler::new(
        ClusterId::new("cluster-1")?,
        store,
        Arc::new(AdvancingClock::new(100)),
    );
    let request = metrics::ExportMetricsServiceRequest {
        resource_metrics: vec![Default::default()],
    };
    handler
        .export_metrics(claims("api")?, request.clone())
        .await?;

    let collision = handler
        .export_metrics(claims("worker")?, request)
        .await
        .expect_err("ownership collision must fail");
    assert_eq!(collision.code(), Code::InvalidArgument);
    Ok(())
}

fn claims(service_id: &str) -> Result<WorkloadClaims, kernel_api::InvalidIdentifier> {
    Ok(WorkloadClaims {
        workload_id: WorkloadId::new("workload-1")?,
        assignment_id: AssignmentId::new("assignment-1")?,
        node_id: NodeId::new("node-1")?,
        service_id: ServiceId::new(service_id)?,
        deployment_id: DeploymentId::new("deployment-1")?,
        labels: BTreeMap::from([("environment".to_owned(), "test".to_owned())]),
    })
}

struct AdvancingClock {
    next: AtomicI64,
}

impl AdvancingClock {
    fn new(first: i64) -> Self {
        Self {
            next: AtomicI64::new(first),
        }
    }
}

impl StatusClock for AdvancingClock {
    fn now(&self) -> Timestamp {
        Timestamp(self.next.fetch_add(1, Ordering::SeqCst))
    }
}
