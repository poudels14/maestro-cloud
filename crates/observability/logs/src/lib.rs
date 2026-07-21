//! Normalized log ingestion and delivery contracts for Maestro observability.
//!
//! This crate may depend on runtime and node-agent contracts, but must not depend on cluster
//! formation, operators, application composition roots, or a particular storage backend.

#[cfg(any(test, feature = "test-util"))]
pub mod conformance;
#[cfg(any(test, feature = "test-util"))]
mod fake;
mod model;
mod parser;
#[cfg(unix)]
mod pipeline;
mod store;

#[cfg(any(test, feature = "test-util"))]
pub use fake::InMemoryLogStore;
pub use model::{
    IngestLogEntry, LogBody, LogOrigin, LogProducer, LogRecordId, LogStream, OriginCursor,
};
pub use parser::{JsonLogParser, LogParser, ParsedLog, PlainTextLogParser, standard_parsers};
#[cfg(unix)]
pub use pipeline::RuntimeLogPipeline;
pub use store::{LogAppendReport, LogStore, LogStoreError};

#[cfg(test)]
mod tests;
