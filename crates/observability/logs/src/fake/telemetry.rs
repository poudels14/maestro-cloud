use std::collections::BTreeMap;
use std::sync::{Mutex, MutexGuard};

use async_trait::async_trait;

use super::{InMemoryLogState, InMemoryLogStore};
use crate::{
    IngressTrafficBreakdown, IngressTrafficQuery, ServiceTrafficQuery, StatsMetricAppendReport,
    StatsMetricPoint, StatsMetricQuery, StatsMetricStore, StatsMetricStoreError,
    TrafficMetricPoint, TrafficQueryError, TrafficQueryStore, project_ingress_traffic,
    project_service_traffic, validate_stats_metric_point,
};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct StatsMetricIdentity {
    ts: i64,
    name: String,
    labels: BTreeMap<String, String>,
}

impl From<&StatsMetricPoint> for StatsMetricIdentity {
    fn from(point: &StatsMetricPoint) -> Self {
        Self {
            ts: point.ts,
            name: point.name.clone(),
            labels: point.labels.clone(),
        }
    }
}

#[async_trait]
impl StatsMetricStore for InMemoryLogStore {
    async fn append_stats_metrics(
        &self,
        points: &[StatsMetricPoint],
    ) -> Result<StatsMetricAppendReport, StatsMetricStoreError> {
        for point in points {
            validate_stats_metric_point(point)?;
        }
        let mut state = lock_state_for_stats_metrics(&self.state)?;
        let mut pending = BTreeMap::<StatsMetricIdentity, StatsMetricPoint>::new();
        let mut deduplicated = 0_usize;
        for point in points {
            let identity = StatsMetricIdentity::from(point);
            let existing = pending
                .get(&identity)
                .or_else(|| state.stats_metrics.get(&identity));
            match existing {
                Some(existing) if existing.value.to_bits() == point.value.to_bits() => {
                    deduplicated = deduplicated.saturating_add(1);
                }
                Some(_) => {
                    return Err(StatsMetricStoreError::Rejected {
                        message: "stats metric identity was reused with a different value"
                            .to_owned(),
                    });
                }
                None => {
                    pending.insert(identity, point.clone());
                }
            }
        }
        let committed = pending.len();
        state.stats_metrics.extend(pending);
        Ok(StatsMetricAppendReport {
            committed,
            deduplicated,
        })
    }

    async fn query_stats_metrics(
        &self,
        query: &StatsMetricQuery,
    ) -> Result<Vec<StatsMetricPoint>, StatsMetricStoreError> {
        Ok(lock_state_for_stats_metrics(&self.state)?
            .stats_metrics
            .values()
            .filter(|point| {
                point.ts >= query.from()
                    && point.ts <= query.to()
                    && query.name().is_none_or(|name| point.name == name)
            })
            .take(query.limit())
            .cloned()
            .collect())
    }
}

#[async_trait]
impl TrafficQueryStore for InMemoryLogStore {
    async fn query_ingress_traffic(
        &self,
        query: &IngressTrafficQuery,
    ) -> Result<IngressTrafficBreakdown, TrafficQueryError> {
        let state = lock_state_for_traffic(&self.state)?;
        Ok(project_ingress_traffic(
            state.entries.values().map(|entry| &entry.entry),
            query,
        ))
    }

    async fn query_service_traffic(
        &self,
        query: &ServiceTrafficQuery,
    ) -> Result<Vec<TrafficMetricPoint>, TrafficQueryError> {
        let state = lock_state_for_traffic(&self.state)?;
        Ok(project_service_traffic(
            state.entries.values().map(|entry| &entry.entry),
            query,
        ))
    }
}

fn lock_state_for_stats_metrics(
    state: &Mutex<InMemoryLogState>,
) -> Result<MutexGuard<'_, InMemoryLogState>, StatsMetricStoreError> {
    state
        .lock()
        .map_err(|_| StatsMetricStoreError::Unavailable {
            message: "in-memory stats metric lock was poisoned".to_owned(),
        })
}

fn lock_state_for_traffic(
    state: &Mutex<InMemoryLogState>,
) -> Result<MutexGuard<'_, InMemoryLogState>, TrafficQueryError> {
    state.lock().map_err(|_| TrafficQueryError::Unavailable {
        message: "in-memory traffic query lock was poisoned".to_owned(),
    })
}
