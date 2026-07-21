use std::collections::BTreeMap;

use kernel_api::{AssignmentId, ClusterId, DeploymentId, NodeId, ServiceId, Timestamp, WorkloadId};
use runtime::WorkloadMetadata;

use crate::{
    HostDiskMetricPoint, HostMetricComponent, HostMetricPoint, HostMetricQuery,
    HostMetricQueryError, HostMetricQueryStore, HostMetricQueryStoreError, HostMetricRecordId,
    HostMetricStore, HostResourceMetricPoint, LatestHostMetricQuery, MetricAppendReport,
    MetricDeliveryStore, MetricDeliveryStoreError, MetricRecordId, MetricSequence, MetricSinkId,
    MetricStore, MetricStoreError, WorkloadMetricPoint,
};

/// Runs bounded history, component filtering, and latest-per-node checks on a host query store.
pub async fn check_host_metric_query_store(
    store: &dyn HostMetricStore,
    queries: &dyn HostMetricQueryStore,
) -> Result<(), MetricStoreConformanceError> {
    let cluster_id = ClusterId::new("metric-query-conformance")?;
    let mut node_one_resources = host_metric_point("node-1", 2, 200)?;
    node_one_resources.id.cluster_id = cluster_id.clone();
    node_one_resources.disks = None;
    let mut node_one_disks = host_metric_point("node-1", 3, 300)?;
    node_one_disks.id.cluster_id = cluster_id.clone();
    node_one_disks.resources = None;
    let mut node_two_both = host_metric_point("node-2", 1, 100)?;
    node_two_both.id.cluster_id = cluster_id.clone();
    let mut node_two_resources = host_metric_point("node-2", 4, 400)?;
    node_two_resources.id.cluster_id = cluster_id.clone();
    node_two_resources.disks = None;
    store
        .append_host_metrics(&[
            node_two_both.clone(),
            node_one_disks.clone(),
            node_one_resources.clone(),
            node_two_resources.clone(),
        ])
        .await?;

    let history = queries
        .query_host_metrics(&HostMetricQuery::new(
            cluster_id.clone(),
            None,
            Timestamp(1),
            Timestamp(4),
            HostMetricComponent::Resources,
            2,
        )?)
        .await?;
    let latest_resources = queries
        .latest_host_metrics(&LatestHostMetricQuery::new(
            cluster_id.clone(),
            HostMetricComponent::Resources,
            8,
        )?)
        .await?;
    let latest_disks = queries
        .latest_host_metrics(&LatestHostMetricQuery::new(
            cluster_id,
            HostMetricComponent::Disks,
            8,
        )?)
        .await?;
    if history != [node_one_resources.clone(), node_two_both.clone()]
        || latest_resources != [node_one_resources, node_two_resources]
        || latest_disks != [node_one_disks, node_two_both]
    {
        return Err(MetricStoreConformanceError::UnexpectedHostQuery);
    }
    if !matches!(
        HostMetricQuery::new(
            ClusterId::new("metric-query-conformance")?,
            None,
            Timestamp(2),
            Timestamp(1),
            HostMetricComponent::Any,
            1,
        ),
        Err(HostMetricQueryError::InvertedTimeRange)
    ) || !matches!(
        LatestHostMetricQuery::new(
            ClusterId::new("metric-query-conformance")?,
            HostMetricComponent::Any,
            0,
        ),
        Err(HostMetricQueryError::InvalidLimit { .. })
    ) {
        return Err(MetricStoreConformanceError::InvalidHostQueryAccepted);
    }
    Ok(())
}

/// Runs the reusable append, replay, collision, validation, and atomicity battery on a host store.
pub async fn check_host_metric_store(
    store: &dyn HostMetricStore,
) -> Result<(), MetricStoreConformanceError> {
    let first = host_metric_point("node-1", 1, 10)?;
    require_report(
        "initial host append",
        store
            .append_host_metrics(std::slice::from_ref(&first))
            .await?,
        MetricAppendReport {
            committed: 1,
            deduplicated: 0,
        },
    )?;
    require_report(
        "exact host replay",
        store
            .append_host_metrics(std::slice::from_ref(&first))
            .await?,
        MetricAppendReport {
            committed: 0,
            deduplicated: 1,
        },
    )?;

    let mut collision = first;
    collision
        .resources
        .as_mut()
        .ok_or(MetricStoreConformanceError::UnexpectedHostPoint)?
        .memory_used_bytes = 11;
    if !matches!(
        store
            .append_host_metrics(std::slice::from_ref(&collision))
            .await,
        Err(MetricStoreError::Rejected { .. })
    ) {
        return Err(MetricStoreConformanceError::CollisionAccepted);
    }

    let second = host_metric_point("node-2", 1, 20)?;
    if !matches!(
        store
            .append_host_metrics(&[second.clone(), collision])
            .await,
        Err(MetricStoreError::Rejected { .. })
    ) {
        return Err(MetricStoreConformanceError::CollisionAccepted);
    }
    require_report(
        "host append after rejected batch",
        store.append_host_metrics(&[second]).await?,
        MetricAppendReport {
            committed: 1,
            deduplicated: 0,
        },
    )?;

    let mut empty = host_metric_point("node-3", 1, 30)?;
    empty.resources = None;
    empty.disks = None;
    if !matches!(
        store.append_host_metrics(&[empty]).await,
        Err(MetricStoreError::Rejected { .. })
    ) {
        return Err(MetricStoreConformanceError::InvalidHostPointAccepted);
    }
    Ok(())
}

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

/// Builds one normalized host point for cross-backend conformance tests.
pub fn host_metric_point(
    node: &str,
    timestamp: i64,
    memory_used_bytes: u64,
) -> Result<HostMetricPoint, kernel_api::InvalidIdentifier> {
    Ok(HostMetricPoint {
        id: HostMetricRecordId {
            cluster_id: ClusterId::new("metric-store-conformance")?,
            node_id: NodeId::new(node)?,
            collected_at: Timestamp(timestamp),
        },
        resources: Some(HostResourceMetricPoint {
            cpu_total_ticks: 100,
            cpu_idle_ticks: 40,
            memory_used_bytes,
            memory_total_bytes: 1_024,
            network_receive_bytes: 100,
            network_transmit_bytes: 200,
        }),
        disks: Some(vec![HostDiskMetricPoint {
            name: "/dev/vda".to_owned(),
            mount_point: "/".to_owned(),
            total_bytes: 10_000,
            available_bytes: 4_000,
            file_system: "ext4".to_owned(),
        }]),
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
    /// The host query view failed while processing valid conformance input.
    #[error(transparent)]
    HostQueryStore(#[from] HostMetricQueryStoreError),
    /// A host query fixture unexpectedly violated the public bounds.
    #[error(transparent)]
    HostQuery(#[from] HostMetricQueryError),
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
    /// A valid host fixture unexpectedly lacked its resource values.
    #[error("host metric conformance fixture was unexpectedly incomplete")]
    UnexpectedHostPoint,
    /// An empty or internally inconsistent host point was accepted.
    #[error("host metric store accepted an invalid point")]
    InvalidHostPointAccepted,
    /// Host history ordering, filtering, or latest selection differed from the contract.
    #[error("host metric query store returned an unexpected view")]
    UnexpectedHostQuery,
    /// An inverted or unbounded host query was accepted.
    #[error("host metric query accepted invalid bounds")]
    InvalidHostQueryAccepted,
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
