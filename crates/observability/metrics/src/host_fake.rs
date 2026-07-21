use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, MutexGuard};

use async_trait::async_trait;
use kernel_api::{ClusterId, NodeId};

use crate::{
    HostMetricDeliveryStore, HostMetricDeliveryStoreError, HostMetricHistoryPoint, HostMetricPoint,
    HostMetricQuery, HostMetricQueryStore, HostMetricQueryStoreError, HostMetricRecordId,
    HostMetricSequence, HostMetricSink, HostMetricStore, LatestHostMetricQuery, MetricAppendReport,
    MetricSinkError, MetricSinkId, MetricStoreError, SequencedHostMetricPoint,
};

/// Deterministic idempotent host metric store for pipeline and composition tests.
#[derive(Default)]
pub struct InMemoryHostMetricStore {
    state: Mutex<HostMetricState>,
    fail_next_cursor_commit: AtomicBool,
}

#[derive(Default)]
struct HostMetricState {
    points: BTreeMap<HostMetricRecordId, StoredHostMetricPoint>,
    sequences: BTreeMap<HostMetricSequence, HostMetricRecordId>,
    last_resource_by_node: BTreeMap<(ClusterId, NodeId), HostMetricSequence>,
    cursors: BTreeMap<MetricSinkId, HostMetricSequence>,
    last_sequence: u64,
}

struct StoredHostMetricPoint {
    previous_resources: Option<HostMetricSequence>,
    point: HostMetricPoint,
}

impl InMemoryHostMetricStore {
    /// Creates an empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns all committed points in stable record-id order.
    pub fn points(&self) -> Result<Vec<HostMetricPoint>, MetricStoreError> {
        Ok(lock_store(&self.state)?
            .points
            .values()
            .map(|stored| stored.point.clone())
            .collect())
    }

    /// Injects one host cursor commit failure without mutating progress.
    pub fn fail_next_cursor_commit(&self) {
        self.fail_next_cursor_commit.store(true, Ordering::SeqCst);
    }
}

#[async_trait]
impl HostMetricStore for InMemoryHostMetricStore {
    async fn append_host_metrics(
        &self,
        points: &[HostMetricPoint],
    ) -> Result<MetricAppendReport, MetricStoreError> {
        let mut state = lock_store(&self.state)?;
        let mut pending = BTreeMap::<HostMetricRecordId, HostMetricPoint>::new();
        let mut pending_order = Vec::new();
        let mut deduplicated = 0_usize;
        for point in points {
            point
                .validate()
                .map_err(|error| MetricStoreError::Rejected {
                    message: error.to_string(),
                })?;
            let committed = state.points.get(&point.id).map(|stored| &stored.point);
            let existing = pending.get(&point.id).or(committed);
            match existing {
                Some(existing) if existing == point => {
                    deduplicated = deduplicated.saturating_add(1);
                }
                Some(_) => return Err(identity_collision(point)),
                None => {
                    pending.insert(point.id.clone(), point.clone());
                    pending_order.push(point.id.clone());
                }
            }
        }
        let committed_count = pending.len();
        let increment = u64::try_from(committed_count).map_err(|_| sequence_exhausted())?;
        state
            .last_sequence
            .checked_add(increment)
            .ok_or_else(sequence_exhausted)?;
        for id in pending_order {
            let point = pending
                .remove(&id)
                .ok_or_else(|| MetricStoreError::Unavailable {
                    message: "validated host append lost a pending point".to_owned(),
                })?;
            state.last_sequence = state.last_sequence.saturating_add(1);
            let sequence = HostMetricSequence(state.last_sequence);
            let owner = (point.id.cluster_id.clone(), point.id.node_id.clone());
            let previous_resources = point
                .resources
                .as_ref()
                .and_then(|_| state.last_resource_by_node.insert(owner, sequence));
            state.sequences.insert(sequence, id.clone());
            state.points.insert(
                id,
                StoredHostMetricPoint {
                    previous_resources,
                    point,
                },
            );
        }
        Ok(MetricAppendReport {
            committed: committed_count,
            deduplicated,
        })
    }
}

#[async_trait]
impl HostMetricDeliveryStore for InMemoryHostMetricStore {
    async fn read_host_metrics_after(
        &self,
        cursor: Option<HostMetricSequence>,
        limit: usize,
    ) -> Result<Vec<SequencedHostMetricPoint>, HostMetricDeliveryStoreError> {
        if limit == 0 {
            return Err(delivery_rejected(
                "host metric delivery read limit must be non-zero",
            ));
        }
        let state = lock_delivery(&self.state)?;
        state
            .sequences
            .iter()
            .filter(|(sequence, _)| cursor.is_none_or(|cursor| **sequence > cursor))
            .take(limit)
            .map(|(sequence, id)| sequenced(&state, *sequence, id))
            .collect()
    }

    async fn load_host_metric_sink_cursor(
        &self,
        sink_id: &MetricSinkId,
    ) -> Result<Option<HostMetricSequence>, HostMetricDeliveryStoreError> {
        Ok(lock_delivery(&self.state)?.cursors.get(sink_id).copied())
    }

    async fn commit_host_metric_sink_cursor(
        &self,
        sink_id: &MetricSinkId,
        sequence: HostMetricSequence,
    ) -> Result<(), HostMetricDeliveryStoreError> {
        if self.fail_next_cursor_commit.swap(false, Ordering::SeqCst) {
            return Err(delivery_unavailable(
                "injected host metric cursor commit failure",
            ));
        }
        let mut state = lock_delivery(&self.state)?;
        if !state.sequences.contains_key(&sequence) {
            return Err(delivery_rejected(
                "host metric sink cursor does not identify a stored point",
            ));
        }
        if state
            .cursors
            .get(sink_id)
            .is_some_and(|cursor| sequence < *cursor)
        {
            return Err(delivery_rejected("host metric sink cursor cannot regress"));
        }
        state.cursors.insert(sink_id.clone(), sequence);
        Ok(())
    }
}

#[async_trait]
impl HostMetricQueryStore for InMemoryHostMetricStore {
    async fn query_host_metrics(
        &self,
        query: &HostMetricQuery,
    ) -> Result<Vec<HostMetricHistoryPoint>, HostMetricQueryStoreError> {
        let state = lock_query(&self.state)?;
        let mut previous = BTreeMap::new();
        let mut history = Vec::new();
        for point in state
            .points
            .values()
            .map(|stored| &stored.point)
            .filter(|point| {
                point.id.cluster_id == *query.cluster_id()
                    && query.node_id().is_none_or(|node| point.id.node_id == *node)
                    && query.component().matches(point)
            })
        {
            if point.id.collected_at.0 > query.to().0 {
                continue;
            }
            let baseline = previous.insert(point.id.node_id.clone(), point.clone());
            if point.id.collected_at.0 >= query.from().0 {
                history.push(HostMetricHistoryPoint {
                    point: point.clone(),
                    previous: baseline,
                });
                if history.len() == query.limit() {
                    break;
                }
            }
        }
        Ok(history)
    }

    async fn latest_host_metrics(
        &self,
        query: &LatestHostMetricQuery,
    ) -> Result<Vec<HostMetricPoint>, HostMetricQueryStoreError> {
        let mut latest = BTreeMap::new();
        for point in lock_query(&self.state)?
            .points
            .values()
            .map(|stored| &stored.point)
        {
            if point.id.cluster_id == *query.cluster_id() && query.component().matches(point) {
                latest.insert(point.id.node_id.clone(), point.clone());
            }
        }
        Ok(latest.into_values().take(query.limit()).collect())
    }
}

/// Scriptable host-metric sink recording every attempted sequence batch.
pub struct RecordingHostMetricSink {
    id: MetricSinkId,
    responses: Mutex<VecDeque<Result<(), MetricSinkError>>>,
    attempts: Mutex<Vec<Vec<HostMetricSequence>>>,
}

impl RecordingHostMetricSink {
    /// Creates a host sink that consumes scripted responses then succeeds by default.
    pub fn new(
        id: MetricSinkId,
        responses: impl IntoIterator<Item = Result<(), MetricSinkError>>,
    ) -> Self {
        Self {
            id,
            responses: Mutex::new(responses.into_iter().collect()),
            attempts: Mutex::new(Vec::new()),
        }
    }

    /// Returns every attempted host sequence batch, including retries.
    pub fn attempts(&self) -> Result<Vec<Vec<HostMetricSequence>>, MetricSinkError> {
        self.attempts
            .lock()
            .map(|attempts| attempts.clone())
            .map_err(|_| MetricSinkError::Unavailable {
                message: "recording host metric sink attempt lock was poisoned".to_owned(),
            })
    }
}

#[async_trait]
impl HostMetricSink for RecordingHostMetricSink {
    fn id(&self) -> &MetricSinkId {
        &self.id
    }

    async fn send_host_metrics(
        &self,
        points: &[SequencedHostMetricPoint],
    ) -> Result<(), MetricSinkError> {
        self.attempts
            .lock()
            .map_err(|_| MetricSinkError::Unavailable {
                message: "recording host metric sink attempt lock was poisoned".to_owned(),
            })?
            .push(points.iter().map(|point| point.sequence).collect());
        self.responses
            .lock()
            .map_err(|_| MetricSinkError::Unavailable {
                message: "recording host metric sink response lock was poisoned".to_owned(),
            })?
            .pop_front()
            .unwrap_or(Ok(()))
    }
}

fn sequenced(
    state: &HostMetricState,
    sequence: HostMetricSequence,
    id: &HostMetricRecordId,
) -> Result<SequencedHostMetricPoint, HostMetricDeliveryStoreError> {
    let stored = state
        .points
        .get(id)
        .ok_or_else(|| delivery_unavailable("host metric sequence lost its point"))?;
    let previous_resources = stored
        .previous_resources
        .map(|previous| {
            let previous_id = state.sequences.get(&previous).ok_or_else(|| {
                delivery_unavailable("host resource baseline sequence is missing")
            })?;
            state
                .points
                .get(previous_id)
                .map(|stored| stored.point.clone())
                .ok_or_else(|| delivery_unavailable("host resource baseline point is missing"))
        })
        .transpose()?;
    Ok(SequencedHostMetricPoint {
        sequence,
        point: stored.point.clone(),
        previous_resources,
    })
}

fn identity_collision(point: &HostMetricPoint) -> MetricStoreError {
    MetricStoreError::Rejected {
        message: format!(
            "host sample identity `{}/{}/{}` was reused with different content",
            point.id.cluster_id, point.id.node_id, point.id.collected_at.0
        ),
    }
}

fn sequence_exhausted() -> MetricStoreError {
    MetricStoreError::Rejected {
        message: "host metric delivery sequence space is exhausted".to_owned(),
    }
}

fn delivery_rejected(message: &str) -> HostMetricDeliveryStoreError {
    HostMetricDeliveryStoreError::Rejected {
        message: message.to_owned(),
    }
}

fn delivery_unavailable(message: &str) -> HostMetricDeliveryStoreError {
    HostMetricDeliveryStoreError::Unavailable {
        message: message.to_owned(),
    }
}

fn lock_store(
    state: &Mutex<HostMetricState>,
) -> Result<MutexGuard<'_, HostMetricState>, MetricStoreError> {
    state.lock().map_err(|_| MetricStoreError::Unavailable {
        message: "in-memory host metric store lock was poisoned".to_owned(),
    })
}

fn lock_delivery(
    state: &Mutex<HostMetricState>,
) -> Result<MutexGuard<'_, HostMetricState>, HostMetricDeliveryStoreError> {
    state
        .lock()
        .map_err(|_| delivery_unavailable("in-memory host metric delivery lock was poisoned"))
}

fn lock_query(
    state: &Mutex<HostMetricState>,
) -> Result<MutexGuard<'_, HostMetricState>, HostMetricQueryStoreError> {
    state
        .lock()
        .map_err(|_| HostMetricQueryStoreError::Unavailable {
            message: "in-memory host metric query lock was poisoned".to_owned(),
        })
}
