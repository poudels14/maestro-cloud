use std::collections::BTreeMap;
use std::sync::Arc;

use kernel_api::{
    AssignmentId, ClusterId, DeploymentId, NodeId, ServiceId, TAILSCALE_GATEWAY_SERVICE_ID,
    Timestamp, WorkloadId,
};
use node_agent::{WorkloadLogEntry, WorkloadLogSink};
use runtime::{LogCursor, LogSource, WorkloadMetadata};

use crate::{InMemoryLogStore, LogBody, LogOrigin, LogStream, RuntimeLogPipeline};

#[tokio::test]
async fn runtime_pipeline_normalizes_identity_json_and_exact_replays()
-> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(InMemoryLogStore::new());
    let pipeline = RuntimeLogPipeline::standard(store.clone());
    let input = workload_entry();

    pipeline.ingest(input.clone()).await?;
    pipeline.ingest(input).await?;

    let entries = store.entries()?;
    let [entry] = entries.as_slice() else {
        return Err("expected one deduplicated entry".into());
    };
    assert_eq!(entry.event_at, Timestamp(1_750_000_000_123));
    assert_eq!(entry.severity, "warn");
    assert_eq!(entry.stream, LogStream::Stderr);
    assert_eq!(entry.body, LogBody::Text("slow".to_owned()));
    assert_eq!(
        entry.attributes.get("attempt").map(String::as_str),
        Some("2")
    );
    let LogOrigin::Workload { metadata } = &entry.origin else {
        return Err("expected workload origin".into());
    };
    assert_eq!(metadata.service_id, ServiceId::new("api")?);
    assert_eq!(metadata.deployment_id, DeploymentId::new("deployment-1")?);
    Ok(())
}

#[tokio::test]
async fn standard_pipeline_drops_tailscale_noise_before_persistence()
-> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(InMemoryLogStore::new());
    let pipeline = RuntimeLogPipeline::standard(store.clone());
    let mut noisy = workload_entry();
    noisy.metadata.service_id = ServiceId::new(TAILSCALE_GATEWAY_SERVICE_ID)?;
    noisy.payload = b"magicsock: disco key changed".to_vec();

    pipeline.ingest(noisy).await?;
    assert!(store.entries()?.is_empty());

    let mut useful = workload_entry();
    useful.cursor = LogCursor::new("cursor-2");
    useful.metadata.service_id = ServiceId::new(TAILSCALE_GATEWAY_SERVICE_ID)?;
    useful.payload = b"listening on 100.64.0.1".to_vec();
    pipeline.ingest(useful).await?;
    assert_eq!(store.entries()?.len(), 1);
    Ok(())
}

fn workload_entry() -> WorkloadLogEntry {
    WorkloadLogEntry {
        metadata: WorkloadMetadata {
            cluster_id: ClusterId::new("cluster-1").unwrap(),
            node_id: NodeId::new("node-1").unwrap(),
            service_id: ServiceId::new("api").unwrap(),
            deployment_id: DeploymentId::new("deployment-1").unwrap(),
            assignment_id: AssignmentId::new("assignment-1").unwrap(),
            workload_id: WorkloadId::new("workload-1").unwrap(),
            labels: BTreeMap::new(),
        },
        received_at: Timestamp(1_750_000_000_999),
        cursor: LogCursor::new("cursor-1"),
        source: LogSource::Stderr,
        payload: br#"{"ts":1750000000123,"level":"warn","msg":"slow","attempt":2}"#.to_vec(),
    }
}
