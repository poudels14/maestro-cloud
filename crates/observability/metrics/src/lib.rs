//! Normalized metric ingestion and delivery contracts for Maestro observability.
//!
//! This crate may depend on node-agent and runtime contracts, but must not depend on cluster
//! formation, operators, application composition roots, or a particular storage backend.

#[cfg(any(test, feature = "test-util"))]
pub mod conformance;
mod datadog;
mod delivery;
#[cfg(any(test, feature = "test-util"))]
mod fake;
#[cfg(any(test, feature = "test-util"))]
mod host_fake;
mod host_model;
mod host_pipeline;
mod host_query;
mod host_store;
mod http;
mod model;
mod pipeline;
mod query;
mod resource_view;
mod sink_worker;
mod store;

pub use datadog::{DatadogMetricSink, DatadogMetricSinkSettings, DatadogMetricSinkSettingsError};
pub use delivery::{
    MetricDeliveryStore, MetricDeliveryStoreError, MetricSequence, MetricSink, MetricSinkError,
    MetricSinkId, MetricSinkIdError, SequencedMetricPoint,
};
#[cfg(any(test, feature = "test-util"))]
pub use fake::{InMemoryMetricStore, InMemoryMetricStoreRuntime, RecordingMetricSink};
#[cfg(any(test, feature = "test-util"))]
pub use host_fake::InMemoryHostMetricStore;
pub use host_model::{
    HostDiskMetricPoint, HostMetricPoint, HostMetricRecordId, HostMetricValidationError,
    HostResourceMetricPoint,
};
pub use host_pipeline::HostMetricPipeline;
pub use host_query::{
    HostMetricComponent, HostMetricHistoryPoint, HostMetricQuery, HostMetricQueryError,
    HostMetricQueryStore, HostMetricQueryStoreError, LatestHostMetricQuery,
};
pub use host_store::HostMetricStore;
pub use http::{
    MetricHttpRequest, MetricHttpResponse, MetricHttpTransport, MetricHttpTransportError,
    ReqwestMetricHttpTransport, ReqwestMetricHttpTransportError,
};
pub use model::{MetricRecordId, WorkloadMetricPoint, WorkloadMetricValidationError};
pub use pipeline::WorkloadMetricPipeline;
pub use query::{
    WorkloadMetricHistoryPoint, WorkloadMetricQuery, WorkloadMetricQueryError,
    WorkloadMetricQueryStore, WorkloadMetricQueryStoreError,
};
pub use resource_view::{
    DiskInfo, ResourceMetricPoint, ResourceMetricSource, aggregate_workload_resource_metrics,
    project_host_resource_metrics, project_latest_disks, project_workload_resource_metrics,
};
pub use sink_worker::{
    MetricSinkSleeper, MetricSinkWorker, MetricSinkWorkerError, MetricSinkWorkerReport,
    MetricSinkWorkerSettings, MetricSinkWorkerSettingsError, TokioMetricSinkSleeper,
};
pub use store::{
    MetricAppendReport, MetricStore, MetricStoreError, MetricStoreRuntime, MetricStoreRuntimeError,
};

#[cfg(test)]
mod tests;
