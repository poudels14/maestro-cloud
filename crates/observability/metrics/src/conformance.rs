use std::collections::BTreeMap;

use kernel_api::{AssignmentId, ClusterId, DeploymentId, NodeId, ServiceId, Timestamp, WorkloadId};
use runtime::WorkloadMetadata;

use crate::{
    MetricAppendReport, MetricDeliveryStore, MetricDeliveryStoreError, MetricRecordId,
    MetricSequence, MetricSinkId, MetricStore, MetricStoreError, WorkloadMetricPoint,
};

/// Runs the reusable append, replay, collision, and atomicity battery on a fresh store.
pub async fn check_metric_store(
    store: &dyn MetricStore,
) -> Result<(), MetricStoreConformanceError> {
    let first = metric_point("workload-1", 1, 10)?;
    require_report(
        "initial append",
        store.append(std::slice::from_ref(&first)).await?,
        MetricAppendReport {
            committed: 1,
            deduplicated: 0,
        },
    )?;
    require_report(
        "exact replay",
        store.append(std::slice::from_ref(&first)).await?,
        MetricAppendReport {
            committed: 0,
            deduplicated: 1,
        },
    )?;

    let mut collision = first;
    collision.cpu_usage_usec = 11;
    if !matches!(
        store.append(std::slice::from_ref(&collision)).await,
        Err(MetricStoreError::Rejected { .. })
    ) {
        return Err(MetricStoreConformanceError::CollisionAccepted);
    }

    let second = metric_point("workload-2", 1, 20)?;
    if !matches!(
        store.append(&[second.clone(), collision]).await,
        Err(MetricStoreError::Rejected { .. })
    ) {
        return Err(MetricStoreConformanceError::CollisionAccepted);
    }
    require_report(
        "append after rejected batch",
        store.append(&[second]).await?,
        MetricAppendReport {
            committed: 1,
            deduplicated: 0,
        },
    )
}

/// Runs ordered-read, stable-baseline, and isolated-cursor checks on a fresh store.
pub async fn check_metric_delivery_store(
    store: &dyn MetricStore,
    delivery: &dyn MetricDeliveryStore,
) -> Result<(), MetricStoreConformanceError> {
    let first = metric_point("workload-1", 1, 10)?;
    let other = metric_point("workload-2", 2, 20)?;
    let latest = metric_point("workload-1", 3, 30)?;
    store
        .append(&[first.clone(), other.clone(), latest.clone()])
        .await?;

    if !matches!(
        delivery.read_after(None, 0).await,
        Err(MetricDeliveryStoreError::Rejected { .. })
    ) {
        return Err(MetricStoreConformanceError::InvalidDeliveryAccepted);
    }
    let first_page = delivery.read_after(None, 2).await?;
    let second_page = delivery.read_after(Some(MetricSequence(2)), 2).await?;
    if first_page.len() != 2
        || first_page.first().map(|point| point.sequence) != Some(MetricSequence(1))
        || first_page.get(1).map(|point| point.sequence) != Some(MetricSequence(2))
        || second_page.len() != 1
        || second_page.first().map(|point| point.sequence) != Some(MetricSequence(3))
        || second_page
            .first()
            .and_then(|point| point.previous.as_ref())
            != Some(&first)
        || second_page.first().map(|point| &point.point) != Some(&latest)
    {
        return Err(MetricStoreConformanceError::UnexpectedDelivery);
    }

    let primary = MetricSinkId::new("datadog")?;
    let secondary = MetricSinkId::new("archive")?;
    delivery
        .commit_sink_cursor(&primary, MetricSequence(2))
        .await?;
    if delivery.load_sink_cursor(&primary).await? != Some(MetricSequence(2))
        || delivery.load_sink_cursor(&secondary).await?.is_some()
        || !matches!(
            delivery
                .commit_sink_cursor(&primary, MetricSequence(1))
                .await,
            Err(MetricDeliveryStoreError::Rejected { .. })
        )
        || !matches!(
            delivery
                .commit_sink_cursor(&secondary, MetricSequence(99))
                .await,
            Err(MetricDeliveryStoreError::Rejected { .. })
        )
    {
        return Err(MetricStoreConformanceError::InvalidDeliveryAccepted);
    }
    Ok(())
}

fn require_report(
    stage: &'static str,
    actual: MetricAppendReport,
    expected: MetricAppendReport,
) -> Result<(), MetricStoreConformanceError> {
    if actual == expected {
        Ok(())
    } else {
        Err(MetricStoreConformanceError::UnexpectedReport {
            stage,
            expected,
            actual,
        })
    }
}

/// Builds one normalized point for cross-backend conformance and sink tests.
pub fn metric_point(
    workload: &str,
    timestamp: i64,
    cpu_usage_usec: u64,
) -> Result<WorkloadMetricPoint, kernel_api::InvalidIdentifier> {
    let node_id = NodeId::new("node-1")?;
    let workload_id = WorkloadId::new(workload)?;
    Ok(WorkloadMetricPoint {
        id: MetricRecordId {
            node_id: node_id.clone(),
            workload_id: workload_id.clone(),
            collected_at: Timestamp(timestamp),
        },
        metadata: WorkloadMetadata {
            cluster_id: ClusterId::new("metric-store-conformance")?,
            node_id,
            service_id: ServiceId::new("api")?,
            deployment_id: DeploymentId::new("deployment-1")?,
            assignment_id: AssignmentId::new(workload)?,
            workload_id,
            labels: BTreeMap::new(),
        },
        cpu_usage_usec,
        cpu_user_usec: cpu_usage_usec,
        cpu_system_usec: 0,
        cpu_periods: 1,
        cpu_throttled_periods: 0,
        cpu_throttled_usec: 0,
        memory_current_bytes: 1_024,
        memory_maximum_bytes: Some(2_048),
        memory_out_of_memory_kills: 0,
        memory_low_events: 0,
        memory_high_events: 0,
        memory_maximum_events: 0,
        memory_out_of_memory_events: 0,
        memory_out_of_memory_group_kills: 0,
        io_read_bytes: 10,
        io_write_bytes: 20,
        io_read_operations: 1,
        io_write_operations: 2,
        io_discarded_bytes: 0,
        io_discard_operations: 0,
        network_receive_bytes: Some(100),
        network_transmit_bytes: Some(200),
        processes_current: 1,
        processes_maximum: Some(32),
    })
}

/// A store violated behavior required by the normalized-metric contract.
#[derive(Debug, thiserror::Error)]
pub enum MetricStoreConformanceError {
    /// The store itself failed while processing valid conformance input.
    #[error(transparent)]
    Store(#[from] MetricStoreError),
    /// The delivery view failed while processing valid conformance input.
    #[error(transparent)]
    Delivery(#[from] MetricDeliveryStoreError),
    /// A sink identifier in the conformance fixture was unexpectedly invalid.
    #[error(transparent)]
    SinkId(#[from] crate::MetricSinkIdError),
    /// Test identifiers unexpectedly violated kernel identifier rules.
    #[error(transparent)]
    InvalidIdentifier(#[from] kernel_api::InvalidIdentifier),
    /// A replay identity was accepted with different immutable content.
    #[error("metric store accepted a replay identity with different content")]
    CollisionAccepted,
    /// Delivery ordering, pagination, or a stable baseline differed from the contract.
    #[error("metric delivery store returned an unexpected ordered view")]
    UnexpectedDelivery,
    /// A zero bound, unknown cursor, or cursor regression was accepted.
    #[error("metric delivery store accepted an invalid operation")]
    InvalidDeliveryAccepted,
    /// Append accounting did not describe the tested operation.
    #[error("{stage} returned {actual:?}, expected {expected:?}")]
    UnexpectedReport {
        /// Conformance stage that produced the mismatch.
        stage: &'static str,
        /// Required accounting.
        expected: MetricAppendReport,
        /// Store-provided accounting.
        actual: MetricAppendReport,
    },
}
