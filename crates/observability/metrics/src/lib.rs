//! Normalized metric ingestion and delivery contracts for Maestro observability.
//!
//! This crate may depend on node-agent and runtime contracts, but must not depend on cluster
//! formation, operators, application composition roots, or a particular storage backend.

#[cfg(any(test, feature = "test-util"))]
pub mod conformance;
mod delivery;
#[cfg(any(test, feature = "test-util"))]
mod fake;
mod model;
mod pipeline;
mod sink_worker;
mod store;

pub use delivery::{
    MetricDeliveryStore, MetricDeliveryStoreError, MetricSequence, MetricSink, MetricSinkError,
    MetricSinkId, MetricSinkIdError, SequencedMetricPoint,
};
#[cfg(any(test, feature = "test-util"))]
pub use fake::{InMemoryMetricStore, InMemoryMetricStoreRuntime, RecordingMetricSink};
pub use model::{MetricRecordId, WorkloadMetricPoint};
pub use pipeline::WorkloadMetricPipeline;
pub use sink_worker::{
    MetricSinkSleeper, MetricSinkWorker, MetricSinkWorkerError, MetricSinkWorkerReport,
    MetricSinkWorkerSettings, MetricSinkWorkerSettingsError, TokioMetricSinkSleeper,
};
pub use store::{
    MetricAppendReport, MetricStore, MetricStoreError, MetricStoreRuntime, MetricStoreRuntimeError,
};

#[cfg(test)]
mod tests;
