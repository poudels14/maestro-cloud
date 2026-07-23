use std::sync::Arc;

use metrics::{
    HostMetricSink, InMemoryHostMetricStore, InMemoryMetricStore, MetricSink, MetricSinkId,
    MetricSinkWorkerSettings, RecordingHostMetricSink, RecordingMetricSink,
};

use crate::metric_delivery::{build_host_metric_sink_workers, build_metric_sink_workers};

#[test]
fn duplicate_metric_sink_cursor_namespaces_fail_closed() -> Result<(), Box<dyn std::error::Error>> {
    let sink = Arc::new(RecordingMetricSink::new(
        MetricSinkId::new("duplicate")?,
        [],
    ));
    let sinks: Vec<Arc<dyn MetricSink>> = vec![sink.clone(), sink];
    let store = Arc::new(InMemoryMetricStore::new());

    let error = match build_metric_sink_workers(&sinks, store, MetricSinkWorkerSettings::default())
    {
        Ok(_) => return Err("duplicate metric sink identifiers were accepted".into()),
        Err(error) => error,
    };
    assert!(error.detail().contains("duplicate"));
    Ok(())
}

#[test]
fn duplicate_host_metric_sink_cursor_namespaces_fail_closed()
-> Result<(), Box<dyn std::error::Error>> {
    let sink = Arc::new(RecordingHostMetricSink::new(
        MetricSinkId::new("duplicate")?,
        [],
    ));
    let sinks: Vec<Arc<dyn HostMetricSink>> = vec![sink.clone(), sink];
    let store = Arc::new(InMemoryHostMetricStore::new());

    let error =
        match build_host_metric_sink_workers(&sinks, store, MetricSinkWorkerSettings::default()) {
            Ok(_) => return Err("duplicate host metric sink identifiers were accepted".into()),
            Err(error) => error,
        };
    assert!(error.detail().contains("duplicate"));
    Ok(())
}
