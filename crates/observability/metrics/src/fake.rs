use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use async_trait::async_trait;
use kernel_api::{NodeId, WorkloadId};

use crate::{
    MetricAppendReport, MetricDeliveryStore, MetricDeliveryStoreError, MetricRecordId,
    MetricSequence, MetricSink, MetricSinkError, MetricSinkId, MetricStore, MetricStoreError,
    MetricStoreRuntime, MetricStoreRuntimeError, SequencedMetricPoint, WorkloadMetricPoint,
};

/// Deterministic idempotent metric store for pipeline and composition tests.
#[derive(Default)]
pub struct InMemoryMetricStore {
    state: Mutex<MetricState>,
    fail_next_cursor_commit: AtomicBool,
}

#[derive(Default)]
struct MetricState {
    points: BTreeMap<MetricRecordId, StoredMetricPoint>,
    sequences: BTreeMap<MetricSequence, MetricRecordId>,
    last_by_workload: BTreeMap<(NodeId, WorkloadId), MetricSequence>,
    cursors: BTreeMap<MetricSinkId, MetricSequence>,
    last_sequence: u64,
}

struct StoredMetricPoint {
    sequence: MetricSequence,
    previous: Option<MetricSequence>,
    point: WorkloadMetricPoint,
}

impl InMemoryMetricStore {
    /// Creates an empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns all committed points in stable record-id order.
    pub fn points(&self) -> Result<Vec<WorkloadMetricPoint>, MetricStoreError> {
        Ok(lock_store(&self.state)?
            .points
            .values()
            .map(|stored| stored.point.clone())
            .collect())
    }

    /// Injects one cursor commit failure without mutating durable progress.
    pub fn fail_next_cursor_commit(&self) {
        self.fail_next_cursor_commit.store(true, Ordering::SeqCst);
    }
}

#[async_trait]
impl MetricStore for InMemoryMetricStore {
    async fn append(
        &self,
        points: &[WorkloadMetricPoint],
    ) -> Result<MetricAppendReport, MetricStoreError> {
        let mut state = lock_store(&self.state)?;
        let mut pending = BTreeMap::<MetricRecordId, WorkloadMetricPoint>::new();
        let mut pending_order = Vec::new();
        let mut deduplicated = 0_usize;
        for point in points {
            let committed = state.points.get(&point.id).map(|stored| &stored.point);
            let existing = pending.get(&point.id).or(committed);
            match existing {
                Some(existing) if existing == point => {
                    deduplicated = deduplicated.saturating_add(1);
                }
                Some(_) => {
                    return Err(MetricStoreError::Rejected {
                        message: format!(
                            "sample identity `{}/{}/{}` was reused with different content",
                            point.id.node_id, point.id.workload_id, point.id.collected_at.0
                        ),
                    });
                }
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
                .ok_or_else(|| MetricStoreError::Rejected {
                    message: "validated metric append lost a pending point".to_owned(),
                })?;
            state.last_sequence = state.last_sequence.saturating_add(1);
            let sequence = MetricSequence(state.last_sequence);
            let owner = (point.id.node_id.clone(), point.id.workload_id.clone());
            let previous = state.last_by_workload.insert(owner, sequence);
            state.sequences.insert(sequence, id.clone());
            state.points.insert(
                id,
                StoredMetricPoint {
                    sequence,
                    previous,
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
impl MetricDeliveryStore for InMemoryMetricStore {
    async fn read_after(
        &self,
        cursor: Option<MetricSequence>,
        limit: usize,
    ) -> Result<Vec<SequencedMetricPoint>, MetricDeliveryStoreError> {
        if limit == 0 {
            return Err(MetricDeliveryStoreError::Rejected {
                message: "metric delivery read limit must be non-zero".to_owned(),
            });
        }
        let state = lock_delivery(&self.state)?;
        state
            .sequences
            .iter()
            .filter(|(sequence, _)| cursor.is_none_or(|cursor| **sequence > cursor))
            .take(limit)
            .map(|(_sequence, id)| sequenced(&state, id))
            .collect()
    }

    async fn load_sink_cursor(
        &self,
        sink_id: &MetricSinkId,
    ) -> Result<Option<MetricSequence>, MetricDeliveryStoreError> {
        Ok(lock_delivery(&self.state)?.cursors.get(sink_id).copied())
    }

    async fn commit_sink_cursor(
        &self,
        sink_id: &MetricSinkId,
        sequence: MetricSequence,
    ) -> Result<(), MetricDeliveryStoreError> {
        if self.fail_next_cursor_commit.swap(false, Ordering::SeqCst) {
            return Err(MetricDeliveryStoreError::Unavailable {
                message: "injected metric cursor commit failure".to_owned(),
            });
        }
        let mut state = lock_delivery(&self.state)?;
        if !state.sequences.contains_key(&sequence) {
            return Err(MetricDeliveryStoreError::Rejected {
                message: "metric sink cursor does not identify a stored point".to_owned(),
            });
        }
        if state
            .cursors
            .get(sink_id)
            .is_some_and(|cursor| sequence < *cursor)
        {
            return Err(MetricDeliveryStoreError::Rejected {
                message: "metric sink cursor cannot regress".to_owned(),
            });
        }
        state.cursors.insert(sink_id.clone(), sequence);
        Ok(())
    }
}

/// No-op lifecycle owner for an in-memory metric store used by composition tests.
pub struct InMemoryMetricStoreRuntime {
    store: Arc<InMemoryMetricStore>,
}

impl InMemoryMetricStoreRuntime {
    /// Creates an empty in-memory runtime.
    pub fn new() -> Self {
        Self {
            store: Arc::new(InMemoryMetricStore::new()),
        }
    }

    /// Returns a typed handle for inspecting committed points.
    pub fn store_handle(&self) -> Arc<InMemoryMetricStore> {
        self.store.clone()
    }
}

impl Default for InMemoryMetricStoreRuntime {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl MetricStoreRuntime for InMemoryMetricStoreRuntime {
    fn store(&self) -> Arc<dyn MetricStore> {
        self.store.clone()
    }

    fn delivery_store(&self) -> Arc<dyn MetricDeliveryStore> {
        self.store.clone()
    }

    async fn shutdown(self: Box<Self>) -> Result<(), MetricStoreRuntimeError> {
        Ok(())
    }
}

/// Scriptable metric sink that records every attempted sequence batch.
pub struct RecordingMetricSink {
    id: MetricSinkId,
    responses: Mutex<VecDeque<Result<(), MetricSinkError>>>,
    attempts: Mutex<Vec<Vec<MetricSequence>>>,
}

impl RecordingMetricSink {
    /// Creates a sink that consumes scripted responses then succeeds by default.
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

    /// Returns every attempted sequence batch, including retries.
    pub fn attempts(&self) -> Result<Vec<Vec<MetricSequence>>, MetricSinkError> {
        self.attempts
            .lock()
            .map(|attempts| attempts.clone())
            .map_err(|_| MetricSinkError::Unavailable {
                message: "recording metric sink attempt lock was poisoned".to_owned(),
            })
    }
}

#[async_trait]
impl MetricSink for RecordingMetricSink {
    fn id(&self) -> &MetricSinkId {
        &self.id
    }

    async fn send(&self, points: &[SequencedMetricPoint]) -> Result<(), MetricSinkError> {
        self.attempts
            .lock()
            .map_err(|_| MetricSinkError::Unavailable {
                message: "recording metric sink attempt lock was poisoned".to_owned(),
            })?
            .push(points.iter().map(|point| point.sequence).collect());
        self.responses
            .lock()
            .map_err(|_| MetricSinkError::Unavailable {
                message: "recording metric sink response lock was poisoned".to_owned(),
            })?
            .pop_front()
            .unwrap_or(Ok(()))
    }
}

fn sequenced(
    state: &MetricState,
    id: &MetricRecordId,
) -> Result<SequencedMetricPoint, MetricDeliveryStoreError> {
    let stored = state
        .points
        .get(id)
        .ok_or_else(|| delivery_inconsistent("metric sequence lost its point"))?;
    let previous = stored
        .previous
        .map(|sequence| {
            let previous_id = state
                .sequences
                .get(&sequence)
                .ok_or_else(|| delivery_inconsistent("metric baseline sequence is missing"))?;
            state
                .points
                .get(previous_id)
                .map(|previous| previous.point.clone())
                .ok_or_else(|| delivery_inconsistent("metric baseline point is missing"))
        })
        .transpose()?;
    Ok(SequencedMetricPoint {
        sequence: stored.sequence,
        point: stored.point.clone(),
        previous,
    })
}

fn lock_store(state: &Mutex<MetricState>) -> Result<MutexGuard<'_, MetricState>, MetricStoreError> {
    state.lock().map_err(|_| MetricStoreError::Unavailable {
        message: "in-memory metric store lock was poisoned".to_owned(),
    })
}

fn lock_delivery(
    state: &Mutex<MetricState>,
) -> Result<MutexGuard<'_, MetricState>, MetricDeliveryStoreError> {
    state
        .lock()
        .map_err(|_| MetricDeliveryStoreError::Unavailable {
            message: "in-memory metric delivery store lock was poisoned".to_owned(),
        })
}

fn sequence_exhausted() -> MetricStoreError {
    MetricStoreError::Rejected {
        message: "metric delivery sequence space is exhausted".to_owned(),
    }
}

fn delivery_inconsistent(message: impl Into<String>) -> MetricDeliveryStoreError {
    MetricDeliveryStoreError::Unavailable {
        message: message.into(),
    }
}
