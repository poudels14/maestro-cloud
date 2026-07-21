use std::collections::BTreeSet;
use std::sync::Arc;

use logs::{LogDeliveryStore, LogSink, SinkWorker, SinkWorkerSettings, TokioSinkSleeper};

use crate::RoleError;

pub(crate) fn build_sink_workers(
    sinks: &[Arc<dyn LogSink>],
    store: Arc<dyn LogDeliveryStore>,
    settings: SinkWorkerSettings,
) -> Result<Vec<SinkWorker>, RoleError> {
    let mut sink_ids = BTreeSet::new();
    let mut workers = Vec::with_capacity(sinks.len());
    for sink in sinks {
        if !sink_ids.insert(sink.id().clone()) {
            return Err(RoleError::new(format!(
                "duplicate log sink identifier `{}`",
                sink.id().as_str()
            )));
        }
        workers.push(
            SinkWorker::new(
                store.clone(),
                sink.clone(),
                Arc::new(TokioSinkSleeper),
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
    use logs::{InMemoryLogDeliveryStore, LogSinkId, RecordingLogSink};

    #[test]
    fn duplicate_sink_cursor_namespaces_fail_closed() -> Result<(), Box<dyn std::error::Error>> {
        let sink = Arc::new(RecordingLogSink::new(LogSinkId::new("duplicate")?, []));
        let sinks: Vec<Arc<dyn LogSink>> = vec![sink.clone(), sink];
        let store = Arc::new(InMemoryLogDeliveryStore::new(Vec::new())?);

        let error = match build_sink_workers(&sinks, store, SinkWorkerSettings::default()) {
            Ok(_) => return Err("duplicate sink identifiers were accepted".into()),
            Err(error) => error,
        };
        assert!(error.detail().contains("duplicate"));
        Ok(())
    }
}
