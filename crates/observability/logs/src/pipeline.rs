use std::sync::Arc;

use async_trait::async_trait;
use node_agent::{WorkloadLogEntry, WorkloadLogSink, WorkloadLogSinkError};
use runtime::LogSource;

use crate::{
    IngestLogEntry, LogOrigin, LogParser, LogProducer, LogRecordId, LogStore, LogStoreError,
    LogStream, OriginCursor, standard_parsers,
};

/// Normalizes runtime-native frames and commits one replay-safe representation.
pub struct RuntimeLogPipeline {
    store: Arc<dyn LogStore>,
    parsers: Vec<Arc<dyn LogParser>>,
}

impl RuntimeLogPipeline {
    /// Builds the standard first-match parser chain ending in a byte-preserving fallback.
    pub fn standard(store: Arc<dyn LogStore>) -> Self {
        Self {
            store,
            parsers: standard_parsers(),
        }
    }

    /// Builds an explicit parser chain; an unmatched payload is permanently rejected.
    pub fn with_parsers(store: Arc<dyn LogStore>, parsers: Vec<Arc<dyn LogParser>>) -> Self {
        Self { store, parsers }
    }
}

#[async_trait]
impl WorkloadLogSink for RuntimeLogPipeline {
    async fn ingest(&self, entry: WorkloadLogEntry) -> Result<(), WorkloadLogSinkError> {
        let parsed = self
            .parsers
            .iter()
            .find_map(|parser| parser.parse(&entry.payload))
            .ok_or_else(|| WorkloadLogSinkError::Rejected {
                message: "no configured parser accepted the runtime payload".to_owned(),
            })?;
        let normalized = IngestLogEntry {
            id: LogRecordId {
                node_id: entry.metadata.node_id.clone(),
                producer: LogProducer::Workload(entry.metadata.workload_id.clone()),
                cursor: OriginCursor::new(entry.cursor.as_str()),
            },
            observed_at: entry.received_at,
            event_at: parsed.event_at.unwrap_or(entry.received_at),
            severity: parsed.severity.unwrap_or_else(|| "info".to_owned()),
            stream: match entry.source {
                LogSource::Stdout => LogStream::Stdout,
                LogSource::Stderr => LogStream::Stderr,
            },
            origin: LogOrigin::Workload {
                metadata: entry.metadata,
            },
            body: parsed.body,
            attributes: parsed.attributes,
        };
        self.store
            .append(&[normalized])
            .await
            .map(|_report| ())
            .map_err(map_store_error)
    }
}

fn map_store_error(error: LogStoreError) -> WorkloadLogSinkError {
    match error {
        LogStoreError::Rejected { message } => WorkloadLogSinkError::Rejected { message },
        LogStoreError::Unavailable { message } => WorkloadLogSinkError::Unavailable { message },
    }
}
