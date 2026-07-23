use std::collections::BTreeSet;
use std::sync::Arc;

use metrics::{
    HostMetricDeliveryStore, HostMetricSink, HostMetricSinkWorker, MetricDeliveryStore, MetricSink,
    MetricSinkWorker, MetricSinkWorkerSettings, TokioMetricSinkSleeper,
};

use crate::RoleError;

pub(crate) fn build_metric_sink_workers(
    sinks: &[Arc<dyn MetricSink>],
    store: Arc<dyn MetricDeliveryStore>,
    settings: MetricSinkWorkerSettings,
) -> Result<Vec<MetricSinkWorker>, RoleError> {
    let mut sink_ids = BTreeSet::new();
    let mut workers = Vec::with_capacity(sinks.len());
    for sink in sinks {
        if !sink_ids.insert(sink.id().clone()) {
            return Err(RoleError::new(format!(
                "duplicate metric sink identifier `{}`",
                sink.id().as_str()
            )));
        }
        workers.push(
            MetricSinkWorker::new(
                store.clone(),
                sink.clone(),
                Arc::new(TokioMetricSinkSleeper),
                settings,
            )
            .map_err(|error| RoleError::new(error.to_string()))?,
        );
    }
    Ok(workers)
}

pub(crate) fn build_host_metric_sink_workers(
    sinks: &[Arc<dyn HostMetricSink>],
    store: Arc<dyn HostMetricDeliveryStore>,
    settings: MetricSinkWorkerSettings,
) -> Result<Vec<HostMetricSinkWorker>, RoleError> {
    let mut sink_ids = BTreeSet::new();
    let mut workers = Vec::with_capacity(sinks.len());
    for sink in sinks {
        if !sink_ids.insert(sink.id().clone()) {
            return Err(RoleError::new(format!(
                "duplicate host metric sink identifier `{}`",
                sink.id().as_str()
            )));
        }
        workers.push(
            HostMetricSinkWorker::new(
                store.clone(),
                sink.clone(),
                Arc::new(TokioMetricSinkSleeper),
                settings,
            )
            .map_err(|error| RoleError::new(error.to_string()))?,
        );
    }
    Ok(workers)
}
