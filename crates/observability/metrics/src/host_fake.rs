use std::collections::BTreeMap;
use std::sync::{Mutex, MutexGuard};

use async_trait::async_trait;

use crate::{
    HostMetricPoint, HostMetricRecordId, HostMetricStore, MetricAppendReport, MetricStoreError,
};

/// Deterministic idempotent host metric store for pipeline and composition tests.
#[derive(Default)]
pub struct InMemoryHostMetricStore {
    points: Mutex<BTreeMap<HostMetricRecordId, HostMetricPoint>>,
}

impl InMemoryHostMetricStore {
    /// Creates an empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns all committed points in stable record-id order.
    pub fn points(&self) -> Result<Vec<HostMetricPoint>, MetricStoreError> {
        Ok(lock(&self.points)?.values().cloned().collect())
    }
}

#[async_trait]
impl HostMetricStore for InMemoryHostMetricStore {
    async fn append_host_metrics(
        &self,
        points: &[HostMetricPoint],
    ) -> Result<MetricAppendReport, MetricStoreError> {
        let mut committed = lock(&self.points)?;
        let mut pending = BTreeMap::<HostMetricRecordId, HostMetricPoint>::new();
        let mut deduplicated = 0_usize;
        for point in points {
            validate(point)?;
            let existing = pending.get(&point.id).or_else(|| committed.get(&point.id));
            match existing {
                Some(existing) if existing == point => {
                    deduplicated = deduplicated.saturating_add(1);
                }
                Some(_) => {
                    return Err(MetricStoreError::Rejected {
                        message: format!(
                            "host sample identity `{}/{}/{}` was reused with different content",
                            point.id.cluster_id, point.id.node_id, point.id.collected_at.0
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

fn validate(point: &HostMetricPoint) -> Result<(), MetricStoreError> {
    if point.resources.is_none() && point.disks.is_none() {
        return Err(rejected("host metric point has no collected telemetry"));
    }
    if point.resources.is_some_and(|resources| {
        resources.cpu_idle_ticks > resources.cpu_total_ticks
            || resources.memory_used_bytes > resources.memory_total_bytes
    }) {
        return Err(rejected(
            "host resource values exceed their corresponding totals",
        ));
    }
    let mut mounts = BTreeMap::new();
    if point.disks.as_ref().is_some_and(|disks| {
        disks.iter().any(|disk| {
            disk.mount_point.is_empty()
                || !disk.mount_point.starts_with('/')
                || disk.name.is_empty()
                || disk.file_system.is_empty()
                || disk.available_bytes > disk.total_bytes
                || mounts.insert(&disk.mount_point, ()).is_some()
        })
    }) {
        return Err(rejected("host disk inventory contains invalid values"));
    }
    Ok(())
}

fn lock(
    points: &Mutex<BTreeMap<HostMetricRecordId, HostMetricPoint>>,
) -> Result<MutexGuard<'_, BTreeMap<HostMetricRecordId, HostMetricPoint>>, MetricStoreError> {
    points.lock().map_err(|_| MetricStoreError::Unavailable {
        message: "in-memory host metric store lock was poisoned".to_owned(),
    })
}

fn rejected(message: impl Into<String>) -> MetricStoreError {
    MetricStoreError::Rejected {
        message: message.into(),
    }
}
