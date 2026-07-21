use std::collections::BTreeSet;
use std::sync::Arc;

use metrics::{
    MetricDeliveryStore, MetricSink, MetricSinkWorker, MetricSinkWorkerSettings,
    TokioMetricSinkSleeper,
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

#[cfg(test)]
mod tests {
    use super::*;
    use metrics::{InMemoryMetricStore, MetricSinkId, RecordingMetricSink};

    #[test]
    fn duplicate_metric_sink_cursor_namespaces_fail_closed()
    -> Result<(), Box<dyn std::error::Error>> {
        let sink = Arc::new(RecordingMetricSink::new(
            MetricSinkId::new("duplicate")?,
            [],
        ));
        let sinks: Vec<Arc<dyn MetricSink>> = vec![sink.clone(), sink];
        let store = Arc::new(InMemoryMetricStore::new());

        let error =
            match build_metric_sink_workers(&sinks, store, MetricSinkWorkerSettings::default()) {
                Ok(_) => return Err("duplicate metric sink identifiers were accepted".into()),
                Err(error) => error,
            };
        assert!(error.detail().contains("duplicate"));
        Ok(())
    }
}
