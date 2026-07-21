use kernel_api::{ClusterId, DeploymentId, ServiceId, Timestamp};

use crate::{MetricStore, WorkloadMetricQuery, WorkloadMetricQueryError, WorkloadMetricQueryStore};

use super::{MetricStoreConformanceError, metric_point};

/// Runs ownership filtering, bounded ordering, and pre-range baseline checks on workload reads.
pub async fn check_workload_metric_query_store(
    store: &dyn MetricStore,
    queries: &dyn WorkloadMetricQueryStore,
) -> Result<(), MetricStoreConformanceError> {
    let cluster_id = ClusterId::new("metric-query-conformance")?;
    let mut first = metric_point("query-workload-1", 1, 10)?;
    first.metadata.cluster_id = cluster_id.clone();
    let mut latest = metric_point("query-workload-1", 3, 30)?;
    latest.metadata.cluster_id = cluster_id.clone();
    let mut other_service = metric_point("query-workload-2", 2, 20)?;
    other_service.metadata.cluster_id = cluster_id.clone();
    other_service.metadata.service_id = ServiceId::new("web")?;
    other_service.metadata.deployment_id = DeploymentId::new("web-deployment")?;
    let mut foreign = metric_point("query-workload-3", 2, 20)?;
    foreign.metadata.cluster_id = ClusterId::new("other-query-cluster")?;
    store
        .append(&[
            first.clone(),
            other_service.clone(),
            latest.clone(),
            foreign,
        ])
        .await?;

    let service_history = queries
        .query_workload_metrics(
            &WorkloadMetricQuery::new(cluster_id.clone(), Timestamp(3), Timestamp(3), 8)?
                .with_service(ServiceId::new("api")?),
        )
        .await?;
    let bounded_history = queries
        .query_workload_metrics(&WorkloadMetricQuery::new(
            cluster_id.clone(),
            Timestamp(1),
            Timestamp(3),
            2,
        )?)
        .await?;
    let deployment_history = queries
        .query_workload_metrics(
            &WorkloadMetricQuery::new(cluster_id.clone(), Timestamp(1), Timestamp(3), 8)?
                .with_deployment(DeploymentId::new("web-deployment")?),
        )
        .await?;
    if service_history.len() != 1
        || service_history.first().map(|history| &history.point) != Some(&latest)
        || service_history
            .first()
            .and_then(|history| history.previous.as_ref())
            != Some(&first)
        || bounded_history.len() != 2
        || bounded_history.first().map(|history| &history.point) != Some(&first)
        || bounded_history.get(1).map(|history| &history.point) != Some(&latest)
        || deployment_history.len() != 1
        || deployment_history.first().map(|history| &history.point) != Some(&other_service)
    {
        return Err(MetricStoreConformanceError::UnexpectedWorkloadQuery);
    }
    if !matches!(
        WorkloadMetricQuery::new(cluster_id.clone(), Timestamp(2), Timestamp(1), 1),
        Err(WorkloadMetricQueryError::InvertedTimeRange)
    ) || !matches!(
        WorkloadMetricQuery::new(cluster_id, Timestamp(1), Timestamp(2), 0),
        Err(WorkloadMetricQueryError::InvalidLimit { .. })
    ) {
        return Err(MetricStoreConformanceError::InvalidWorkloadQueryAccepted);
    }
    Ok(())
}
