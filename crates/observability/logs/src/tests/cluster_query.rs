use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use kernel_api::{ClusterId, NodeId, Timestamp};

use crate::{
    ClusterLogCursor, ClusterLogQueryCoordinator, InMemoryLogStore, IngestLogEntry, LogBody,
    LogHistogramBucket, LogHistogramGroupBy, LogHistogramQuery, LogOrigin, LogProducer,
    LogQueryScope, LogQueryStore, LogQueryStoreError, LogReadOrder, LogReadQuery, LogRecordId,
    LogStore, LogStream, NodeLogQueryStore, OriginCursor, SequencedLogEntry,
};

#[derive(Default)]
struct NodeStores {
    stores: BTreeMap<NodeId, Arc<InMemoryLogStore>>,
}

impl NodeStores {
    fn with_nodes(node_ids: &[NodeId]) -> Self {
        Self {
            stores: node_ids
                .iter()
                .cloned()
                .map(|node_id| (node_id, Arc::new(InMemoryLogStore::new())))
                .collect(),
        }
    }

    fn store(&self, node_id: &NodeId) -> Result<&Arc<InMemoryLogStore>, LogQueryStoreError> {
        self.stores
            .get(node_id)
            .ok_or_else(|| LogQueryStoreError::Unavailable {
                message: format!("missing fixture store for node `{node_id}`"),
            })
    }
}

#[async_trait]
impl NodeLogQueryStore for NodeStores {
    async fn query_node_logs(
        &self,
        node_id: &NodeId,
        query: &LogReadQuery,
    ) -> Result<Vec<SequencedLogEntry>, LogQueryStoreError> {
        self.store(node_id)?.query_logs(query).await
    }

    async fn query_node_histogram(
        &self,
        node_id: &NodeId,
        query: &LogHistogramQuery,
    ) -> Result<Vec<LogHistogramBucket>, LogQueryStoreError> {
        self.store(node_id)?.query_log_histogram(query).await
    }
}

#[tokio::test]
async fn newest_tail_merges_by_event_time_and_captures_node_high_watermarks()
-> Result<(), Box<dyn std::error::Error>> {
    let node_a = node("node-a");
    let node_b = node("node-b");
    let stores = Arc::new(NodeStores::with_nodes(&[node_a.clone(), node_b.clone()]));
    append(&stores, &node_a, &[(100, "a-1"), (300, "a-2")]).await?;
    append(&stores, &node_b, &[(200, "b-1"), (400, "b-2")]).await?;

    let coordinator = ClusterLogQueryCoordinator::new(stores);
    let query = LogReadQuery::new(LogQueryScope::All, LogReadOrder::NewestFirst, 3)?;
    let page = coordinator
        .query_logs(
            &[node_b.clone(), node_a.clone(), node_a.clone()],
            &query,
            None,
            None,
        )
        .await?;

    assert_eq!(event_times(&page.entries), vec![400, 300, 200]);
    assert_eq!(page.cursor.get(&node_a).map(|sequence| sequence.0), Some(2));
    assert_eq!(page.cursor.get(&node_b).map(|sequence| sequence.0), Some(2));
    Ok(())
}

#[tokio::test]
async fn follow_pages_advance_only_complete_node_sequence_prefixes()
-> Result<(), Box<dyn std::error::Error>> {
    let node_a = node("node-a");
    let node_b = node("node-b");
    let stores = Arc::new(NodeStores::with_nodes(&[node_a.clone(), node_b.clone()]));
    append(&stores, &node_a, &[(100, "a-1"), (300, "a-2")]).await?;
    append(&stores, &node_b, &[(200, "b-1"), (400, "b-2")]).await?;
    let coordinator = ClusterLogQueryCoordinator::new(stores.clone());
    let initial = LogReadQuery::new(LogQueryScope::All, LogReadOrder::NewestFirst, 4)?;
    let initial = coordinator
        .query_logs(&[node_a.clone(), node_b.clone()], &initial, None, None)
        .await?;

    append(&stores, &node_a, &[(500, "a-3"), (425, "a-4")]).await?;
    append(&stores, &node_b, &[(450, "b-3")]).await?;
    let follow = LogReadQuery::new(LogQueryScope::All, LogReadOrder::NewestFirst, 2)?;
    let first = coordinator
        .query_logs(
            &[node_a.clone(), node_b.clone()],
            &follow,
            Some(&initial.cursor),
            None,
        )
        .await?;

    assert_eq!(event_times(&first.entries), vec![450, 500]);
    assert_eq!(
        first.cursor.get(&node_a).map(|sequence| sequence.0),
        Some(3)
    );
    assert_eq!(
        first.cursor.get(&node_b).map(|sequence| sequence.0),
        Some(3)
    );
    let second = coordinator
        .query_logs(
            &[node_a.clone(), node_b.clone()],
            &follow,
            Some(&first.cursor),
            None,
        )
        .await?;
    assert_eq!(event_times(&second.entries), vec![425]);
    assert_eq!(
        second.cursor.get(&node_a).map(|sequence| sequence.0),
        Some(4)
    );
    Ok(())
}

#[tokio::test]
async fn preceding_pages_walk_each_node_cursor_without_skips_or_duplicates()
-> Result<(), Box<dyn std::error::Error>> {
    let node_a = node("node-a");
    let node_b = node("node-b");
    let stores = Arc::new(NodeStores::with_nodes(&[node_a.clone(), node_b.clone()]));
    append(
        &stores,
        &node_a,
        &[(100, "a-1"), (400, "a-2"), (600, "a-3")],
    )
    .await?;
    append(
        &stores,
        &node_b,
        &[(200, "b-1"), (300, "b-2"), (500, "b-3")],
    )
    .await?;
    let coordinator = ClusterLogQueryCoordinator::new(stores);
    let query = LogReadQuery::new(LogQueryScope::All, LogReadOrder::NewestFirst, 2)?;

    let first = coordinator
        .query_logs(&[node_a.clone(), node_b.clone()], &query, None, None)
        .await?;
    assert_eq!(event_times(&first.entries), vec![600, 500]);
    assert!(first.has_previous);

    let second = coordinator
        .query_logs(
            &[node_a.clone(), node_b.clone()],
            &query,
            Some(&first.cursor),
            first.previous_cursor.as_ref(),
        )
        .await?;
    assert_eq!(event_times(&second.entries), vec![400, 300]);
    assert_eq!(second.cursor, first.cursor);
    assert!(second.has_previous);

    let third = coordinator
        .query_logs(
            &[node_a, node_b],
            &query,
            Some(&second.cursor),
            second.previous_cursor.as_ref(),
        )
        .await?;
    assert_eq!(event_times(&third.entries), vec![200, 100]);
    assert!(!third.has_previous);
    Ok(())
}

#[tokio::test]
async fn preceding_pages_preserve_node_sequence_prefixes_when_timestamps_are_out_of_order()
-> Result<(), Box<dyn std::error::Error>> {
    let node_a = node("node-a");
    let node_b = node("node-b");
    let stores = Arc::new(NodeStores::with_nodes(&[node_a.clone(), node_b.clone()]));
    append(
        &stores,
        &node_a,
        &[(100, "a-1"), (600, "a-2"), (300, "a-3")],
    )
    .await?;
    append(
        &stores,
        &node_b,
        &[(200, "b-1"), (500, "b-2"), (400, "b-3")],
    )
    .await?;
    let coordinator = ClusterLogQueryCoordinator::new(stores);
    let query = LogReadQuery::new(LogQueryScope::All, LogReadOrder::NewestFirst, 2)?;

    let mut cursor = None;
    let mut previous_cursor = None;
    let mut seen = Vec::new();
    loop {
        let page = coordinator
            .query_logs(
                &[node_a.clone(), node_b.clone()],
                &query,
                cursor.as_ref(),
                previous_cursor.as_ref(),
            )
            .await?;
        seen.extend(
            page.entries
                .iter()
                .map(|entry| (entry.node_id.clone(), entry.sequence)),
        );
        cursor = Some(page.cursor);
        previous_cursor = page.previous_cursor;
        if !page.has_previous {
            break;
        }
    }

    seen.sort();
    assert_eq!(
        seen,
        vec![
            (node_a.clone(), crate::LogSequence(1)),
            (node_a.clone(), crate::LogSequence(2)),
            (node_a, crate::LogSequence(3)),
            (node_b.clone(), crate::LogSequence(1)),
            (node_b.clone(), crate::LogSequence(2)),
            (node_b, crate::LogSequence(3)),
        ]
    );
    Ok(())
}

#[tokio::test]
async fn preceding_cursor_keeps_the_initial_snapshot_when_an_unseen_node_gets_new_logs()
-> Result<(), Box<dyn std::error::Error>> {
    let node_a = node("node-a");
    let node_b = node("node-b");
    let stores = Arc::new(NodeStores::with_nodes(&[node_a.clone(), node_b.clone()]));
    append(&stores, &node_a, &[(400, "a-1"), (500, "a-2")]).await?;
    append(&stores, &node_b, &[(100, "b-1")]).await?;
    let coordinator = ClusterLogQueryCoordinator::new(stores.clone());
    let query = LogReadQuery::new(LogQueryScope::All, LogReadOrder::NewestFirst, 2)?;

    let first = coordinator
        .query_logs(&[node_a.clone(), node_b.clone()], &query, None, None)
        .await?;
    assert_eq!(event_times(&first.entries), vec![500, 400]);
    append(&stores, &node_b, &[(600, "b-2")]).await?;
    let followed = coordinator
        .query_logs(
            &[node_a.clone(), node_b.clone()],
            &query,
            Some(&first.cursor),
            None,
        )
        .await?;
    assert_eq!(event_times(&followed.entries), vec![600]);

    let preceding = coordinator
        .query_logs(
            &[node_a, node_b],
            &query,
            Some(&followed.cursor),
            first.previous_cursor.as_ref(),
        )
        .await?;
    assert_eq!(event_times(&preceding.entries), vec![100]);
    assert!(!preceding.has_previous);
    Ok(())
}

#[tokio::test]
async fn oldest_pages_do_not_skip_node_records_hidden_by_the_global_limit()
-> Result<(), Box<dyn std::error::Error>> {
    let node_a = node("node-a");
    let node_b = node("node-b");
    let stores = Arc::new(NodeStores::with_nodes(&[node_a.clone(), node_b.clone()]));
    append(&stores, &node_a, &[(100, "a-1"), (500, "a-2")]).await?;
    append(&stores, &node_b, &[(200, "b-1"), (300, "b-2")]).await?;
    let coordinator = ClusterLogQueryCoordinator::new(stores);
    let query = LogReadQuery::new(LogQueryScope::All, LogReadOrder::OldestFirst, 2)?;

    let first = coordinator
        .query_logs(&[node_a.clone(), node_b.clone()], &query, None, None)
        .await?;
    assert_eq!(event_times(&first.entries), vec![100, 200]);
    assert_eq!(
        first.cursor.get(&node_a).map(|sequence| sequence.0),
        Some(1)
    );
    assert_eq!(
        first.cursor.get(&node_b).map(|sequence| sequence.0),
        Some(1)
    );

    let second = coordinator
        .query_logs(
            &[node_a.clone(), node_b.clone()],
            &query,
            Some(&first.cursor),
            None,
        )
        .await?;
    assert_eq!(event_times(&second.entries), vec![300, 500]);
    Ok(())
}

#[tokio::test]
async fn histograms_saturating_sum_matching_buckets_and_groups()
-> Result<(), Box<dyn std::error::Error>> {
    let node_a = node("node-a");
    let node_b = node("node-b");
    let stores = Arc::new(NodeStores::with_nodes(&[node_a.clone(), node_b.clone()]));
    append(&stores, &node_a, &[(100, "a-1"), (1_100, "a-2")]).await?;
    append(&stores, &node_b, &[(200, "b-1"), (1_200, "b-2")]).await?;
    let coordinator = ClusterLogQueryCoordinator::new(stores);
    let query = LogHistogramQuery::new(
        LogQueryScope::All,
        Timestamp(0),
        Timestamp(2_000),
        1_000,
        LogHistogramGroupBy::Level,
    )?;

    let buckets = coordinator
        .query_histogram(&[node_b.clone(), node_a, node_b], &query)
        .await?;

    assert_eq!(
        buckets,
        vec![
            LogHistogramBucket {
                bucket_at: Timestamp(0),
                count: 2,
                groups: BTreeMap::from([("info".to_owned(), 2)]),
            },
            LogHistogramBucket {
                bucket_at: Timestamp(1_000),
                count: 2,
                groups: BTreeMap::from([("info".to_owned(), 2)]),
            },
        ]
    );
    Ok(())
}

#[test]
fn cluster_cursor_has_a_stable_transport_representation() -> Result<(), Box<dyn std::error::Error>>
{
    let cursor = serde_json::from_str::<ClusterLogCursor>(r#"{"node-a":7,"node-b":11}"#)?;

    assert_eq!(
        serde_json::to_string(&cursor)?,
        r#"{"node-a":7,"node-b":11}"#
    );
    Ok(())
}

async fn append(
    stores: &NodeStores,
    node_id: &NodeId,
    entries: &[(i64, &str)],
) -> Result<(), Box<dyn std::error::Error>> {
    let offset = stores.store(node_id)?.entries()?.len();
    let entries = entries
        .iter()
        .enumerate()
        .map(|(index, (event_at, body))| {
            system_entry(
                node_id,
                offset.saturating_add(index).saturating_add(1),
                *event_at,
                body,
            )
        })
        .collect::<Vec<_>>();
    stores.store(node_id)?.append(&entries).await?;
    Ok(())
}

fn system_entry(node_id: &NodeId, cursor: usize, event_at: i64, body: &str) -> IngestLogEntry {
    IngestLogEntry {
        id: LogRecordId {
            node_id: node_id.clone(),
            producer: LogProducer::System("daemon".to_owned()),
            cursor: OriginCursor::new(cursor.to_string()),
        },
        observed_at: Timestamp(event_at),
        event_at: Timestamp(event_at),
        severity: "info".to_owned(),
        stream: LogStream::System,
        origin: LogOrigin::System {
            cluster_id: ClusterId::new("cluster-1").expect("fixture cluster id is valid"),
            node_id: Some(node_id.clone()),
            component: "daemon".to_owned(),
        },
        body: LogBody::Text(body.to_owned()),
        attributes: BTreeMap::new(),
    }
}

fn node(value: &str) -> NodeId {
    NodeId::new(value).expect("fixture node id is valid")
}

fn event_times(entries: &[crate::ClusterLogEntry]) -> Vec<i64> {
    entries.iter().map(|entry| entry.entry.event_at.0).collect()
}
