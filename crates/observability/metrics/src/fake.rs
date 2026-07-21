use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use crate::{
    MetricAppendReport, MetricRecordId, MetricStore, MetricStoreError, MetricStoreRuntime,
    MetricStoreRuntimeError, WorkloadMetricPoint,
};

/// Deterministic idempotent metric store for pipeline and composition tests.
#[derive(Default)]
pub struct InMemoryMetricStore {
    points: Mutex<BTreeMap<MetricRecordId, WorkloadMetricPoint>>,
}

impl InMemoryMetricStore {
    /// Creates an empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns all committed points in stable record-id order.
    pub fn points(&self) -> Result<Vec<WorkloadMetricPoint>, MetricStoreError> {
        self.points
            .lock()
            .map(|points| points.values().cloned().collect())
            .map_err(|_| MetricStoreError::Unavailable {
                message: "in-memory metric store lock was poisoned".to_owned(),
            })
    }
}

#[async_trait]
impl MetricStore for InMemoryMetricStore {
    async fn append(
        &self,
        points: &[WorkloadMetricPoint],
    ) -> Result<MetricAppendReport, MetricStoreError> {
        let mut committed = self
            .points
            .lock()
            .map_err(|_| MetricStoreError::Unavailable {
                message: "in-memory metric store lock was poisoned".to_owned(),
            })?;
        let mut pending = BTreeMap::<MetricRecordId, WorkloadMetricPoint>::new();
        let mut deduplicated = 0_usize;
        for point in points {
            let existing = pending.get(&point.id).or_else(|| committed.get(&point.id));
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
                }
            }
        }
        let committed_count = pending.len();
        committed.extend(pending);
        Ok(MetricAppendReport {
            committed: committed_count,
            deduplicated,
        })
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

    async fn shutdown(self: Box<Self>) -> Result<(), MetricStoreRuntimeError> {
        Ok(())
    }
}
