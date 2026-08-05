use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;

use async_trait::async_trait;
use futures_util::future::try_join_all;
use kernel_api::NodeId;
use serde::{Deserialize, Serialize};

use crate::{
    IngestLogEntry, LogHistogramBucket, LogHistogramQuery, LogQueryStoreError, LogReadCursor,
    LogReadOrder, LogReadQuery, LogSequence, MAXIMUM_LOG_QUERY_LIMIT, SequencedLogEntry,
};

/// Independent node-local cursors carried across one cluster log stream.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ClusterLogCursor(BTreeMap<NodeId, LogSequence>);

impl ClusterLogCursor {
    /// Creates an empty cursor that begins after sequence zero on every node.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the last emitted or observed sequence for one node.
    pub fn get(&self, node_id: &NodeId) -> Option<LogSequence> {
        self.0.get(node_id).copied()
    }

    /// Returns stable node-to-sequence progress for transport encoding.
    pub fn positions(&self) -> &BTreeMap<NodeId, LogSequence> {
        &self.0
    }

    fn advance(&mut self, node_id: NodeId, sequence: LogSequence) {
        self.0
            .entry(node_id)
            .and_modify(|current| *current = (*current).max(sequence))
            .or_insert(sequence);
    }

    fn retreat(&mut self, node_id: NodeId, sequence: LogSequence) {
        self.0
            .entry(node_id)
            .and_modify(|current| *current = (*current).min(sequence))
            .or_insert(sequence);
    }
}

/// One normalized node-local record after deterministic cluster merge.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterLogEntry {
    /// Node whose store assigned the sequence.
    pub node_id: NodeId,
    /// Monotonic sequence meaningful only within `node_id`.
    pub sequence: LogSequence,
    /// Complete normalized record.
    pub entry: IngestLogEntry,
}

/// One globally bounded cluster page and its independent continuation positions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterLogPage {
    /// Deterministically merged records.
    pub entries: Vec<ClusterLogEntry>,
    /// Cursor safe to use when polling for newer records.
    pub cursor: ClusterLogCursor,
    /// Exclusive per-node boundary for loading the preceding page.
    pub previous_cursor: Option<ClusterLogCursor>,
    /// Whether another preceding page may exist.
    pub has_previous: bool,
}

/// Transport-neutral ability to run an already-validated query on one selected node.
#[async_trait]
pub trait NodeLogQueryStore: Send + Sync {
    /// Executes one node-local page.
    async fn query_node_logs(
        &self,
        node_id: &NodeId,
        query: &LogReadQuery,
    ) -> Result<Vec<SequencedLogEntry>, LogQueryStoreError>;

    /// Executes one node-local histogram.
    async fn query_node_histogram(
        &self,
        node_id: &NodeId,
        query: &LogHistogramQuery,
    ) -> Result<Vec<LogHistogramBucket>, LogQueryStoreError>;
}

/// Concurrent fan-out and deterministic merge over independent node-local stores.
pub struct ClusterLogQueryCoordinator {
    nodes: Arc<dyn NodeLogQueryStore>,
}

impl ClusterLogQueryCoordinator {
    /// Wraps a node query transport without taking ownership of its lifecycle.
    pub fn new(nodes: Arc<dyn NodeLogQueryStore>) -> Self {
        Self { nodes }
    }

    /// Queries every distinct node and returns one globally bounded page.
    pub async fn query_logs(
        &self,
        node_ids: &[NodeId],
        query: &LogReadQuery,
        cursor: Option<&ClusterLogCursor>,
        before: Option<&ClusterLogCursor>,
    ) -> Result<ClusterLogPage, LogQueryStoreError> {
        let node_ids = distinct_nodes(node_ids);
        let follow = cursor.is_some() && before.is_none();
        let preceding =
            before.is_some() || (cursor.is_none() && query.order() == LogReadOrder::NewestFirst);
        let probe_limit = query.limit().saturating_add(1).min(MAXIMUM_LOG_QUERY_LIMIT);
        let pages = try_join_all(node_ids.iter().cloned().map(|node_id| {
            let mut node_query = query.clone();
            if preceding {
                node_query = node_query.with_limit(probe_limit);
            }
            if let Some(before) = before {
                node_query = node_query.with_order(LogReadOrder::NewestFirst);
                if let Some(sequence) = before.get(&node_id) {
                    node_query = node_query.with_cursor(LogReadCursor::Before(sequence));
                }
            } else if let Some(cursor) = cursor {
                node_query = node_query
                    .with_order(LogReadOrder::OldestFirst)
                    .with_cursor(LogReadCursor::After(
                        cursor.get(&node_id).unwrap_or(LogSequence(0)),
                    ));
            }
            async move {
                match self.nodes.query_node_logs(&node_id, &node_query).await {
                    Ok(entries) => Ok((node_id, entries)),
                    Err(error) => Err(node_error(error, &node_id)),
                }
            }
        }))
        .await?;

        let has_previous = preceding
            && (pages
                .iter()
                .map(|(_, entries)| entries.len())
                .sum::<usize>()
                > query.limit()
                || (probe_limit == query.limit()
                    && pages
                        .iter()
                        .any(|(_, entries)| entries.len() == query.limit())));
        let mut previous_cursor = preceding.then(|| {
            before.cloned().unwrap_or_else(|| {
                let mut initial = ClusterLogCursor::new();
                for (node_id, entries) in &pages {
                    let high_watermark = entries
                        .iter()
                        .map(|entry| entry.sequence.0)
                        .max()
                        .unwrap_or(0);
                    initial.advance(
                        node_id.clone(),
                        LogSequence(high_watermark.saturating_add(1)),
                    );
                }
                initial
            })
        });
        let sequence_prefix = follow || preceding || query.order() == LogReadOrder::OldestFirst;
        let capture_high_watermarks =
            cursor.is_none() && before.is_none() && query.order() == LogReadOrder::NewestFirst;
        let mut next_cursor = cursor.cloned().unwrap_or_default();
        if capture_high_watermarks {
            for (node_id, entries) in &pages {
                next_cursor.advance(
                    node_id.clone(),
                    entries
                        .iter()
                        .map(|entry| entry.sequence)
                        .max()
                        .unwrap_or(LogSequence(0)),
                );
            }
        }
        let entries = if sequence_prefix {
            let order = if preceding {
                LogReadOrder::NewestFirst
            } else {
                LogReadOrder::OldestFirst
            };
            merge_sequence_prefixes(pages, query.limit(), order)
        } else {
            let mut entries = flatten_pages(pages);
            sort_entries(&mut entries, query.order());
            entries.truncate(query.limit());
            entries
        };
        if follow || (!preceding && query.order() == LogReadOrder::OldestFirst) {
            for entry in &entries {
                next_cursor.advance(entry.node_id.clone(), entry.sequence);
            }
        }
        if let Some(previous) = &mut previous_cursor {
            for entry in &entries {
                previous.retreat(entry.node_id.clone(), entry.sequence);
            }
        }
        Ok(ClusterLogPage {
            entries,
            cursor: next_cursor,
            previous_cursor,
            has_previous,
        })
    }

    /// Queries every distinct node and saturating-sums equal histogram groups.
    pub async fn query_histogram(
        &self,
        node_ids: &[NodeId],
        query: &LogHistogramQuery,
    ) -> Result<Vec<LogHistogramBucket>, LogQueryStoreError> {
        let node_ids = distinct_nodes(node_ids);
        let results = try_join_all(node_ids.iter().cloned().map(|node_id| async move {
            self.nodes
                .query_node_histogram(&node_id, query)
                .await
                .map_err(|error| node_error(error, &node_id))
        }))
        .await?;
        let mut buckets = BTreeMap::<i64, LogHistogramBucket>::new();
        for bucket in results.into_iter().flatten() {
            let merged = buckets
                .entry(bucket.bucket_at.0)
                .or_insert_with(|| LogHistogramBucket {
                    bucket_at: bucket.bucket_at,
                    count: 0,
                    groups: BTreeMap::new(),
                });
            merged.count = merged.count.saturating_add(bucket.count);
            for (group, count) in bucket.groups {
                let total = merged.groups.entry(group).or_default();
                *total = total.saturating_add(count);
            }
        }
        Ok(buckets.into_values().collect())
    }
}

fn distinct_nodes(node_ids: &[NodeId]) -> Vec<NodeId> {
    node_ids
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn flatten_pages(pages: Vec<(NodeId, Vec<SequencedLogEntry>)>) -> Vec<ClusterLogEntry> {
    pages
        .into_iter()
        .flat_map(|(node_id, entries)| {
            entries.into_iter().map(move |stored| ClusterLogEntry {
                node_id: node_id.clone(),
                sequence: stored.sequence,
                entry: stored.entry,
            })
        })
        .collect()
}

fn merge_sequence_prefixes(
    pages: Vec<(NodeId, Vec<SequencedLogEntry>)>,
    limit: usize,
    order: LogReadOrder,
) -> Vec<ClusterLogEntry> {
    let mut pages = pages
        .into_iter()
        .map(|(node_id, entries)| (node_id, VecDeque::from(entries)))
        .collect::<BTreeMap<_, _>>();
    let mut merged = Vec::new();
    while merged.len() < limit {
        let Some(node_id) = pages
            .iter()
            .filter_map(|(node_id, entries)| entries.front().map(|entry| (node_id, entry)))
            .min_by(|(left_node, left), (right_node, right)| {
                let ordering = left
                    .entry
                    .event_at
                    .cmp(&right.entry.event_at)
                    .then_with(|| left_node.cmp(right_node))
                    .then_with(|| left.sequence.cmp(&right.sequence));
                match order {
                    LogReadOrder::OldestFirst => ordering,
                    LogReadOrder::NewestFirst => ordering.reverse(),
                }
            })
            .map(|(node_id, _)| node_id.clone())
        else {
            break;
        };
        let Some(stored) = pages.get_mut(&node_id).and_then(VecDeque::pop_front) else {
            break;
        };
        merged.push(ClusterLogEntry {
            node_id,
            sequence: stored.sequence,
            entry: stored.entry,
        });
    }
    merged
}

fn sort_entries(entries: &mut [ClusterLogEntry], order: LogReadOrder) {
    entries.sort_by(|left, right| {
        let ordering = left
            .entry
            .event_at
            .cmp(&right.entry.event_at)
            .then_with(|| left.node_id.cmp(&right.node_id))
            .then_with(|| left.sequence.cmp(&right.sequence));
        match order {
            LogReadOrder::OldestFirst => ordering,
            LogReadOrder::NewestFirst => ordering.reverse(),
        }
    });
}

fn node_error(error: LogQueryStoreError, node_id: &NodeId) -> LogQueryStoreError {
    match error {
        LogQueryStoreError::Rejected { message } => LogQueryStoreError::Rejected {
            message: format!("node `{node_id}` rejected log query: {message}"),
        },
        LogQueryStoreError::Unavailable { message } => LogQueryStoreError::Unavailable {
            message: format!("node `{node_id}` log query is unavailable: {message}"),
        },
    }
}
