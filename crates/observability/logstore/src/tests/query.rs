use std::collections::BTreeMap;

use kernel_api::{AssignmentId, ClusterId, DeploymentId, NodeId, ServiceId, Timestamp, WorkloadId};
use logs::{
    IngestLogEntry, LogBody, LogHistogramGroupBy, LogHistogramQuery, LogOrigin, LogProducer,
    LogQueryScope, LogQueryStore, LogReadCursor, LogReadOrder, LogReadQuery, LogRecordId,
    LogSequence, LogStore, LogStream, OriginCursor,
};
use runtime::WorkloadMetadata;

use crate::{DuckLogStoreRuntime, DuckStoreSettings};

const TEN: i64 = 1_784_628_000_000;
const ELEVEN: i64 = 1_784_631_600_000;
const NOON: i64 = 1_784_635_200_000;

#[tokio::test]
async fn queries_hot_and_cold_logs_with_status_aliases_and_typed_scope()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let runtime = DuckLogStoreRuntime::open(DuckStoreSettings::new(
        temporary.path().join("logs.duckdb"),
        8,
    )?)
    .await?;
    let store = runtime.store();
    let mut failed = workload_entry(1, TEN + 60_000, "error", "upstream failed")?;
    failed
        .attributes
        .insert("DownstreamStatus".to_owned(), "503".to_owned());
    store.append(&[failed]).await?;
    assert_eq!(store.rollover_before(Timestamp(ELEVEN)).await?.rows, 1);

    let mut success = workload_entry(2, ELEVEN + 60_000, "info", "request complete")?;
    success
        .attributes
        .insert("http.response.status_code".to_owned(), "200".to_owned());
    store
        .append(&[success, system_entry(3, ELEVEN + 120_000)?])
        .await?;

    let query = LogReadQuery::new(
        LogQueryScope::Service(ServiceId::new("api")?),
        LogReadOrder::NewestFirst,
        10,
    )?
    .with_search("failed @http.status_code:[500 TO 599]".parse()?);
    let entries = store.query_logs(&query).await?;
    assert_eq!(entries.len(), 1);
    let entry = entries.first().ok_or("filtered log missing")?;
    assert_eq!(entry.sequence, LogSequence(1));
    assert_eq!(
        entry.entry.body,
        LogBody::Text("upstream failed".to_owned())
    );

    let system = store
        .query_logs(&LogReadQuery::new(
            LogQueryScope::SystemComponent("daemon".to_owned()),
            LogReadOrder::OldestFirst,
            10,
        )?)
        .await?;
    assert_eq!(system.len(), 1);
    runtime.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn histogram_and_cursors_preserve_order_across_hot_and_cold_tiers()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let runtime = DuckLogStoreRuntime::open(DuckStoreSettings::new(
        temporary.path().join("logs.duckdb"),
        8,
    )?)
    .await?;
    let store = runtime.store();
    let mut failed = workload_entry(1, TEN + 60_000, "error", "failed")?;
    failed
        .attributes
        .insert("statusCode".to_owned(), "503".to_owned());
    store.append(&[failed]).await?;
    store.rollover_before(Timestamp(ELEVEN)).await?;
    let mut success = workload_entry(2, ELEVEN + 60_000, "info", "complete")?;
    success
        .attributes
        .insert("http.status_code".to_owned(), "204".to_owned());
    store.append(&[success]).await?;

    let histogram = store
        .query_log_histogram(&LogHistogramQuery::new(
            LogQueryScope::Service(ServiceId::new("api")?),
            Timestamp(TEN),
            Timestamp(NOON),
            3_600_000,
            LogHistogramGroupBy::HttpStatusClass,
        )?)
        .await?;
    assert_eq!(histogram.len(), 2);
    let [failed_bucket, success_bucket] = histogram.as_slice() else {
        return Err("expected two histogram buckets".into());
    };
    assert_eq!(failed_bucket.groups.get("5xx"), Some(&1));
    assert_eq!(success_bucket.groups.get("2xx"), Some(&1));

    let page = store
        .query_logs(
            &LogReadQuery::new(LogQueryScope::All, LogReadOrder::NewestFirst, 1)?
                .with_cursor(LogReadCursor::Before(LogSequence(3))),
        )
        .await?;
    assert_eq!(
        page.first().ok_or("cursor page missing")?.sequence,
        LogSequence(2)
    );
    runtime.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn query_values_cannot_change_the_generated_statement()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let runtime = DuckLogStoreRuntime::open(DuckStoreSettings::new(
        temporary.path().join("logs.duckdb"),
        8,
    )?)
    .await?;
    let store = runtime.store();
    store
        .append(&[workload_entry(1, ELEVEN, "info", "ordinary")?])
        .await?;
    let query = LogReadQuery::new(LogQueryScope::All, LogReadOrder::OldestFirst, 10)?
        .with_search(r#"message:"x' OR true --""#.parse()?);
    assert!(store.query_logs(&query).await?.is_empty());
    runtime.shutdown().await?;
    Ok(())
}

fn workload_entry(
    index: u64,
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
                cluster_id: ClusterId::new("cluster-one")?,
                node_id,
                service_id: ServiceId::new("api")?,
                deployment_id: DeploymentId::new("api-v1")?,
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
            cluster_id: ClusterId::new("cluster-one")?,
            node_id: Some(node_id),
            component: "daemon".to_owned(),
        },
        body: LogBody::Text("system".to_owned()),
        attributes: BTreeMap::new(),
    })
}
