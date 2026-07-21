//! Normalized log ingestion and delivery contracts for Maestro observability.
//!
//! This crate may depend on runtime and node-agent contracts, but must not depend on cluster
//! formation, operators, application composition roots, or a particular storage backend.

#[cfg(any(test, feature = "test-util"))]
pub mod conformance;
#[cfg(any(test, feature = "test-util"))]
mod fake;
mod filter;
mod model;
#[cfg(unix)]
mod otlp;
mod parser;
#[cfg(unix)]
mod pipeline;
mod store;

#[cfg(any(test, feature = "test-util"))]
pub use fake::{InMemoryLogStore, InMemoryLogStoreRuntime};
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
pub use store::{LogAppendReport, LogStore, LogStoreError, LogStoreRuntime, LogStoreRuntimeError};

#[cfg(test)]
mod tests;
