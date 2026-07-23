use std::sync::Arc;

use logs::{
    InMemoryLogDeliveryStore, LogSink, LogSinkId, RecordingLogSink, SinkRuntimeRegistry,
    SinkWorkerSettings,
};

use crate::log_delivery::build_sink_workers;

#[test]
fn duplicate_sink_cursor_namespaces_fail_closed() -> Result<(), Box<dyn std::error::Error>> {
    let sink = Arc::new(RecordingLogSink::new(LogSinkId::new("duplicate")?, []));
    let sinks: Vec<Arc<dyn LogSink>> = vec![sink.clone(), sink];
    let store = Arc::new(InMemoryLogDeliveryStore::new(Vec::new())?);

    let error = match build_sink_workers(
        &sinks,
        store,
        SinkWorkerSettings::default(),
        SinkRuntimeRegistry::default(),
    ) {
        Ok(_) => return Err("duplicate sink identifiers were accepted".into()),
        Err(error) => error,
    };
    assert!(error.detail().contains("duplicate"));
    Ok(())
}
