use async_trait::async_trait;
use metrics::{
    HostMetricDeliveryStore, HostMetricDeliveryStoreError, HostMetricSequence, MetricSinkId,
    SequencedHostMetricPoint,
};
use tokio::sync::oneshot;

use crate::metric_duck::{Command, DuckMetricStore};

#[async_trait]
impl HostMetricDeliveryStore for DuckMetricStore {
    async fn read_host_metrics_after(
        &self,
        cursor: Option<HostMetricSequence>,
        limit: usize,
    ) -> Result<Vec<SequencedHostMetricPoint>, HostMetricDeliveryStoreError> {
        let (response, result) = oneshot::channel();
        self.commands
            .send(Command::ReadHostAfter {
                cursor,
                limit,
                response,
            })
            .await
            .map_err(|_| worker_stopped("accepting ordered host metric read"))?;
        result
            .await
            .map_err(|_| worker_stopped("completing ordered host metric read"))?
    }

    async fn load_host_metric_sink_cursor(
        &self,
        sink_id: &MetricSinkId,
    ) -> Result<Option<HostMetricSequence>, HostMetricDeliveryStoreError> {
        let (response, result) = oneshot::channel();
        self.commands
            .send(Command::LoadHostCursor {
                sink_id: sink_id.clone(),
                response,
            })
            .await
            .map_err(|_| worker_stopped("accepting host metric cursor read"))?;
        result
            .await
            .map_err(|_| worker_stopped("completing host metric cursor read"))?
    }

    async fn commit_host_metric_sink_cursor(
        &self,
        sink_id: &MetricSinkId,
        sequence: HostMetricSequence,
    ) -> Result<(), HostMetricDeliveryStoreError> {
        let (response, result) = oneshot::channel();
        self.commands
            .send(Command::CommitHostCursor {
                sink_id: sink_id.clone(),
                sequence,
                response,
            })
            .await
            .map_err(|_| worker_stopped("accepting host metric cursor commit"))?;
        result
            .await
            .map_err(|_| worker_stopped("completing host metric cursor commit"))?
    }
}

fn worker_stopped(action: &'static str) -> HostMetricDeliveryStoreError {
    HostMetricDeliveryStoreError::Unavailable {
        message: format!("DuckDB metric writer stopped before {action}"),
    }
}
