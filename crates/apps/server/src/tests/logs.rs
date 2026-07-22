use std::collections::BTreeMap;
use std::sync::Arc;

use axum::http::StatusCode;
use kernel_api::{AssignmentId, ClusterId, DeploymentId, NodeId, ServiceId, Timestamp, WorkloadId};
use logs::{
    InMemoryLogStore, IngestLogEntry, LogBody, LogHistogramBucket, LogOrigin, LogProducer,
    LogRecordId, LogSequence, LogStore, LogStream, OriginCursor, SequencedLogEntry,
};
use runtime::WorkloadMetadata;

use crate::{ApiServer, ServerSettings};

use super::deployments::deployment;
use super::{decode, put, request, seeded_store};

#[tokio::test]
async fn service_deployment_system_and_histogram_routes_share_typed_queries()
-> Result<(), Box<dyn std::error::Error>> {
    let (store, cluster_id) = seeded_store().await?;
    let deployment = deployment("api-deployment", "api")?;
    put(
        &store,
        &cluster_id,
        "Deployment",
        deployment.meta.id.as_str(),
        &deployment,
    )
    .await?;
    let logs = Arc::new(InMemoryLogStore::new());
    let mut failed = workload_entry(1, "api", "api-deployment", 100, "error", "failed")?;
    failed
        .attributes
        .insert("DownstreamStatus".to_owned(), "503".to_owned());
    logs.append(&[
        failed,
        workload_entry(2, "worker", "worker-deployment", 150, "info", "other")?,
        system_entry(3, 175)?,
    ])
    .await?;
    let server = ApiServer::new(
        store,
        cluster_id,
        ServerSettings::new("127.0.0.1:3000".parse()?, None),
    )?
    .with_log_query_store(logs);

    let response = request(
        &server,
        "/api/services/api/logs?query=%40http.status_code%3A%5B500%20TO%20599%5D&tail=10",
        None,
    )
    .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let service_logs: Vec<SequencedLogEntry> = decode(response).await?;
    assert_eq!(service_logs.len(), 1);
    assert_eq!(
        service_logs.first().ok_or("service log missing")?.sequence,
        LogSequence(1)
    );

    let response = request(
        &server,
        "/api/services/api/deployments/api-deployment/logs?tail=10",
        None,
    )
    .await?;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(decode::<Vec<SequencedLogEntry>>(response).await?.len(), 1);

    let response = request(&server, "/api/system/logs?component=daemon", None).await?;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(decode::<Vec<SequencedLogEntry>>(response).await?.len(), 1);

    let response = request(
        &server,
        "/api/services/api/logs/histogram?from=0&to=1000&bucketMs=100&groupBy=status",
        None,
    )
    .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let histogram: Vec<LogHistogramBucket> = decode(response).await?;
    assert_eq!(histogram.len(), 1);
    assert_eq!(
        histogram
            .first()
            .and_then(|bucket| bucket.groups.get("5xx")),
        Some(&1)
    );
    Ok(())
}

#[tokio::test]
async fn log_routes_reject_ambiguous_cursors_and_missing_store()
-> Result<(), Box<dyn std::error::Error>> {
    let (store, cluster_id) = seeded_store().await?;
    let server = ApiServer::new(
        store,
        cluster_id,
        ServerSettings::new("127.0.0.1:3000".parse()?, None),
    )?;
    assert_eq!(
        request(&server, "/api/logs", None).await?.status(),
        StatusCode::SERVICE_UNAVAILABLE
    );

    let (store, cluster_id) = seeded_store().await?;
    let server = ApiServer::new(
        store,
        cluster_id,
        ServerSettings::new("127.0.0.1:3000".parse()?, None),
    )?
    .with_log_query_store(Arc::new(InMemoryLogStore::new()));
    assert_eq!(
        request(&server, "/api/logs?after=1&before=2", None)
            .await?
            .status(),
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        request(&server, "/api/logs?query=unknown%3Avalue", None)
            .await?
            .status(),
        StatusCode::BAD_REQUEST
    );
    Ok(())
}

fn workload_entry(
    index: u64,
    service_id: &str,
    deployment_id: &str,
    event_at: i64,
    severity: &str,
    body: &str,
) -> Result<IngestLogEntry, kernel_api::InvalidIdentifier> {
    let node_id = NodeId::new("node-one")?;
    let workload_id = WorkloadId::new(format!("workload-{index}"))?;
    Ok(IngestLogEntry {
        id: LogRecordId {
            node_id: node_id.clone(),
            producer: LogProducer::Workload(workload_id.clone()),
            cursor: OriginCursor::new(index.to_string()),
        },
        observed_at: Timestamp(event_at),
        event_at: Timestamp(event_at),
        severity: severity.to_owned(),
        stream: LogStream::Stdout,
        origin: LogOrigin::Workload {
            metadata: WorkloadMetadata {
                cluster_id: ClusterId::new("server-test")?,
                node_id,
                service_id: ServiceId::new(service_id)?,
                deployment_id: DeploymentId::new(deployment_id)?,
                assignment_id: AssignmentId::new(format!("assignment-{index}"))?,
                workload_id,
                labels: BTreeMap::new(),
            },
        },
        body: LogBody::Text(body.to_owned()),
        attributes: BTreeMap::new(),
    })
}

fn system_entry(
    index: u64,
    event_at: i64,
) -> Result<IngestLogEntry, kernel_api::InvalidIdentifier> {
    let node_id = NodeId::new("node-one")?;
    Ok(IngestLogEntry {
        id: LogRecordId {
            node_id: node_id.clone(),
            producer: LogProducer::System("daemon".to_owned()),
            cursor: OriginCursor::new(index.to_string()),
        },
        observed_at: Timestamp(event_at),
        event_at: Timestamp(event_at),
        severity: "info".to_owned(),
        stream: LogStream::System,
        origin: LogOrigin::System {
            cluster_id: ClusterId::new("server-test")?,
            node_id: Some(node_id),
            component: "daemon".to_owned(),
        },
        body: LogBody::Text("system".to_owned()),
        attributes: BTreeMap::new(),
    })
}
