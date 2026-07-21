//! Normalized metric ingestion and delivery contracts for Maestro observability.
//!
//! This crate may depend on node-agent and runtime contracts, but must not depend on cluster
//! formation, operators, application composition roots, or a particular storage backend.

#[cfg(any(test, feature = "test-util"))]
pub mod conformance;
#[cfg(any(test, feature = "test-util"))]
mod fake;
mod model;
mod pipeline;
mod store;

#[cfg(any(test, feature = "test-util"))]
pub use fake::{InMemoryMetricStore, InMemoryMetricStoreRuntime};
pub use model::{MetricRecordId, WorkloadMetricPoint};
pub use pipeline::WorkloadMetricPipeline;
pub use store::{
    MetricAppendReport, MetricStore, MetricStoreError, MetricStoreRuntime, MetricStoreRuntimeError,
};

#[cfg(test)]
mod tests;
