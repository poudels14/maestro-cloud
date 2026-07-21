//! Normalized log ingestion and delivery contracts for Maestro observability.
//!
//! This crate may depend on runtime and node-agent contracts, but must not depend on cluster
//! formation, operators, application composition roots, or a particular storage backend.

#[cfg(any(test, feature = "test-util"))]
pub mod conformance;
mod dead_letter;
mod delivery;
#[cfg(any(test, feature = "test-util"))]
mod fake;
#[cfg(any(test, feature = "test-util"))]
mod fake_delivery;
mod filter;
mod model;
#[cfg(unix)]
mod otlp;
mod parser;
#[cfg(unix)]
mod pipeline;
mod sink_worker;
mod store;

pub use dead_letter::{DeadLetterStore, DeadLetterStoreError, SinkDeadLetter, SinkDeadLetterStats};
pub use delivery::{
    LogDeliveryStore, LogDeliveryStoreError, LogSequence, LogSink, LogSinkError, LogSinkId,
    LogSinkIdError, LogSinkOutcome, SequencedLogEntry,
};
#[cfg(any(test, feature = "test-util"))]
pub use fake::{InMemoryLogStore, InMemoryLogStoreRuntime};
#[cfg(any(test, feature = "test-util"))]
pub use fake_delivery::{InMemoryDeadLetterStore, InMemoryLogDeliveryStore, RecordingLogSink};
pub use filter::{
    LogFilter, LogFilterChain, LogFilterKind, SuccessfulHealthcheckFilter, TailscaleNoiseFilter,
    standard_ingest_filters,
};
pub use model::{
    IngestLogEntry, LogBody, LogOrigin, LogProducer, LogRecordId, LogStream, OriginCursor,
};
#[cfg(unix)]
pub use otlp::OtlpLogHandler;
pub use parser::{
    DatePrefixedLogParser, JsonLogParser, LogParser, LogrusLogParser, ParsedLog,
    PlainTextLogParser, Rfc3339PrefixedLogParser, standard_parsers,
};
#[cfg(unix)]
pub use pipeline::RuntimeLogPipeline;
pub use sink_worker::{
    SinkSleeper, SinkWorker, SinkWorkerError, SinkWorkerReport, SinkWorkerSettings,
    SinkWorkerSettingsError, TokioSinkSleeper,
};
pub use store::{LogAppendReport, LogStore, LogStoreError, LogStoreRuntime, LogStoreRuntimeError};

#[cfg(test)]
mod tests;
