use std::collections::BTreeMap;
use std::time::Duration;

use async_trait::async_trait;
use kernel_api::{ClusterId, DeploymentId, NodeId, Timestamp};
use logs::{
    IngestLogEntry, LogBody, LogHistogramBucket, LogHistogramQuery, LogOrigin, LogProducer,
    LogQuery, LogQueryScope, LogQueryStoreError, LogReadQuery, LogRecordId, LogSequence, LogStream,
    NodeLogQueryStore, OriginCursor, SequencedLogEntry,
};

use crate::LocalLogError;
use crate::local_logs::{LocalLogOptions, local_log_target, stream_from};

struct FixedQueries {
    entries: Vec<SequencedLogEntry>,
}

#[async_trait]
impl NodeLogQueryStore for FixedQueries {
    async fn query_node_logs(
        &self,
        _node_id: &NodeId,
        _query: &LogReadQuery,
    ) -> Result<Vec<SequencedLogEntry>, LogQueryStoreError> {
        Ok(self.entries.clone())
    }

    async fn query_node_histogram(
        &self,
        _node_id: &NodeId,
        _query: &LogHistogramQuery,
    ) -> Result<Vec<LogHistogramBucket>, LogQueryStoreError> {
        Ok(Vec::new())
    }
}

#[test]
fn source_target_preserves_system_and_workload_compatibility_shapes()
-> Result<(), Box<dyn std::error::Error>> {
    let all = local_log_target(None)?;
    assert_eq!(all.scope, LogQueryScope::All);
    assert!(all.search.is_none());

    let system = local_log_target(Some("daemon"))?;
    assert_eq!(
        system.scope,
        LogQueryScope::SystemComponent("daemon".to_owned())
    );
    assert!(system.search.is_none());

    let workload = local_log_target(Some("api/deployment-1/workload-1"))?;
    assert_eq!(
        workload.scope,
        LogQueryScope::Deployment(DeploymentId::new("deployment-1")?)
    );
    assert_eq!(
        workload.search.as_ref().map(LogQuery::as_str),
        Some(r#"service:"api" AND source:"workload-1""#)
    );
    assert!(matches!(
        local_log_target(Some("api/deployment")),
        Err(LocalLogError::InvalidSource { .. })
    ));
    Ok(())
}

#[tokio::test]
async fn initial_local_page_is_rendered_oldest_first_with_complete_source()
-> Result<(), Box<dyn std::error::Error>> {
    let node_id = NodeId::new("node-a")?;
    let queries = FixedQueries {
        entries: vec![
            system_entry(&node_id, 2, 200, "second")?,
            system_entry(&node_id, 1, 100, "first")?,
        ],
    };
    let mut output = Vec::new();

    stream_from(
        &queries,
        &node_id,
        &LocalLogOptions {
            source: Some("daemon".to_owned()),
            tail: 10,
            follow: false,
        },
        &mut output,
        Duration::ZERO,
    )
    .await?;

    let output = String::from_utf8(output)?;
    let mut lines = output.lines();
    let first = lines.next().ok_or("missing first log line")?;
    let second = lines.next().ok_or("missing second log line")?;
    assert!(lines.next().is_none());
    assert!(first.contains("100 info  node-a/daemon"));
    assert!(first.ends_with("first"));
    assert!(second.contains("200 info  node-a/daemon"));
    assert!(second.ends_with("second"));
    Ok(())
}

fn system_entry(
    node_id: &NodeId,
    sequence: u64,
    event_at: i64,
    body: &str,
) -> Result<SequencedLogEntry, kernel_api::InvalidIdentifier> {
    Ok(SequencedLogEntry {
        sequence: LogSequence(sequence),
        entry: IngestLogEntry {
            id: LogRecordId {
                node_id: node_id.clone(),
                producer: LogProducer::System("daemon".to_owned()),
                cursor: OriginCursor::new(sequence.to_string()),
            },
            observed_at: Timestamp(event_at),
            event_at: Timestamp(event_at),
            severity: "info".to_owned(),
            stream: LogStream::System,
            origin: LogOrigin::System {
                cluster_id: ClusterId::new("cluster-a")?,
                node_id: Some(node_id.clone()),
                component: "daemon".to_owned(),
            },
            body: LogBody::Text(body.to_owned()),
            attributes: BTreeMap::new(),
        },
    })
}
