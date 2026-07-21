use crate::{
    HostMetricDeliveryStore, HostMetricDeliveryStoreError, HostMetricSequence, HostMetricStore,
    MetricSinkId, MetricStoreError,
};

/// Runs ordered reads, resource baselines, and isolated host cursor checks on a fresh store.
pub async fn check_host_metric_delivery_store(
    store: &dyn HostMetricStore,
    delivery: &dyn HostMetricDeliveryStore,
) -> Result<(), HostMetricDeliveryConformanceError> {
    let first = super::host_metric_point("node-1", 1, 10)?;
    let mut disk_only = super::host_metric_point("node-1", 2, 20)?;
    disk_only.resources = None;
    let other = super::host_metric_point("node-2", 3, 30)?;
    let latest = super::host_metric_point("node-1", 4, 40)?;
    store
        .append_host_metrics(&[
            first.clone(),
            disk_only.clone(),
            other.clone(),
            latest.clone(),
        ])
        .await?;

    if !matches!(
        delivery.read_host_metrics_after(None, 0).await,
        Err(HostMetricDeliveryStoreError::Rejected { .. })
    ) {
        return Err(HostMetricDeliveryConformanceError::InvalidOperationAccepted);
    }
    let first_page = delivery.read_host_metrics_after(None, 2).await?;
    let second_page = delivery
        .read_host_metrics_after(Some(HostMetricSequence(2)), 2)
        .await?;
    if first_page.len() != 2
        || first_page.first().map(|point| point.sequence) != Some(HostMetricSequence(1))
        || first_page.first().map(|point| &point.point) != Some(&first)
        || first_page.get(1).map(|point| &point.point) != Some(&disk_only)
        || second_page.len() != 2
        || second_page.first().map(|point| &point.point) != Some(&other)
        || second_page.get(1).map(|point| &point.point) != Some(&latest)
        || second_page
            .get(1)
            .and_then(|point| point.previous_resources.as_ref())
            != Some(&first)
    {
        return Err(HostMetricDeliveryConformanceError::UnexpectedDelivery);
    }

    let primary = MetricSinkId::new("datadog")?;
    let secondary = MetricSinkId::new("archive")?;
    delivery
        .commit_host_metric_sink_cursor(&primary, HostMetricSequence(2))
        .await?;
    if delivery.load_host_metric_sink_cursor(&primary).await? != Some(HostMetricSequence(2))
        || delivery
            .load_host_metric_sink_cursor(&secondary)
            .await?
            .is_some()
        || !matches!(
            delivery
                .commit_host_metric_sink_cursor(&primary, HostMetricSequence(1))
                .await,
            Err(HostMetricDeliveryStoreError::Rejected { .. })
        )
        || !matches!(
            delivery
                .commit_host_metric_sink_cursor(&secondary, HostMetricSequence(99))
                .await,
            Err(HostMetricDeliveryStoreError::Rejected { .. })
        )
    {
        return Err(HostMetricDeliveryConformanceError::InvalidOperationAccepted);
    }
    Ok(())
}

/// A store violated required host-metric delivery behavior.
#[derive(Debug, thiserror::Error)]
pub enum HostMetricDeliveryConformanceError {
    /// Host append storage failed valid fixture input.
    #[error(transparent)]
    Store(#[from] MetricStoreError),
    /// Host delivery storage failed valid fixture input.
    #[error(transparent)]
    Delivery(#[from] HostMetricDeliveryStoreError),
    /// A fixture sink identifier unexpectedly failed validation.
    #[error(transparent)]
    SinkId(#[from] crate::MetricSinkIdError),
    /// A fixture kernel identifier unexpectedly failed validation.
    #[error(transparent)]
    InvalidIdentifier(#[from] kernel_api::InvalidIdentifier),
    /// Ordering, pagination, or the stable resource baseline was incorrect.
    #[error("host metric delivery store returned an unexpected ordered view")]
    UnexpectedDelivery,
    /// A zero bound, unknown cursor, or cursor regression was accepted.
    #[error("host metric delivery store accepted an invalid operation")]
    InvalidOperationAccepted,
}
