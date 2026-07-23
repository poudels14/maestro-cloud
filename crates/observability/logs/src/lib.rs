//! Normalized log ingestion and delivery contracts for Maestro observability.
//!
//! This crate may depend on runtime and node-agent contracts, but must not depend on cluster
//! formation, operators, application composition roots, or a particular storage backend.

mod cluster_query;
mod cluster_stats;
#[cfg(any(test, feature = "test-util"))]
pub mod conformance;
mod controller_stats_provider;
mod datadog;
mod dead_letter;
mod delivery;
#[cfg(any(test, feature = "test-util"))]
mod fake;
#[cfg(any(test, feature = "test-util"))]
mod fake_delivery;
#[cfg(any(test, feature = "test-util"))]
mod fake_query;
mod filter;
mod http;
mod model;
mod operational_metrics;
#[cfg(unix)]
mod otlp;
mod parser;
#[cfg(unix)]
mod pipeline;
mod query;
mod sink_runtime;
mod sink_worker;
mod stats;
mod stats_warnings;
mod store;
mod traffic;
mod traffic_cluster;
mod uptime;

pub use cluster_query::{
    ClusterLogCursor, ClusterLogEntry, ClusterLogPage, ClusterLogQueryCoordinator,
    NodeLogQueryStore,
};
pub use cluster_stats::{
    BackupStatsSnapshot, ClusterStatsResponse, ControllerStatsSnapshot, DeadLetterStatsSnapshot,
    ProbeStatsSnapshot, SinkStatsSnapshot, SpoolStatsSnapshot, StatsMetricPoint, StatsWarning,
    collect_controller_stats,
};
pub use controller_stats_provider::{
    BackupStatsProvider, BackupStatsProviderError, ControllerStatsProvider, LiveControllerStats,
};
pub use datadog::{DatadogLogSink, DatadogLogSinkSettings, DatadogLogSinkSettingsError};
pub use dead_letter::{
    DeadLetterStore, DeadLetterStoreError, SinkDeadLetter, SinkDeadLetterMetadata,
    SinkDeadLetterStats,
};
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
pub use http::{
    HttpRequest, HttpResponse, HttpTransport, HttpTransportError, ReqwestHttpTransport,
    ReqwestHttpTransportError,
};
pub use logql::{LogQuery, LogQueryParseError};
pub use model::{
    IngestLogEntry, LogBody, LogOrigin, LogProducer, LogRecordId, LogStream, OriginCursor,
};
pub use operational_metrics::{
    MAXIMUM_STATS_METRIC_QUERY_LIMIT, StatsMetricAppendReport, StatsMetricQuery, StatsMetricStore,
    StatsMetricStoreError, validate_stats_metric_point,
};
#[cfg(unix)]
pub use otlp::OtlpLogHandler;
pub use parser::{
    DatePrefixedLogParser, JsonLogParser, LogParser, LogrusLogParser, ParsedLog,
    PlainTextLogParser, Rfc3339PrefixedLogParser, standard_parsers,
};
#[cfg(unix)]
pub use pipeline::RuntimeLogPipeline;
pub use query::{
    LogHistogramBucket, LogHistogramGroupBy, LogHistogramQuery, LogQueryError, LogQueryScope,
    LogQueryStore, LogQueryStoreError, LogReadCursor, LogReadOrder, LogReadQuery,
    MAXIMUM_LOG_QUERY_LIMIT,
};
pub use sink_runtime::{
    SinkRuntimeClock, SinkRuntimeRegistry, SinkRuntimeSnapshot, SystemSinkRuntimeClock,
};
pub use sink_worker::{
    SinkSleeper, SinkWorker, SinkWorkerError, SinkWorkerReport, SinkWorkerSettings,
    SinkWorkerSettingsError, TokioSinkSleeper,
};
pub use stats::{
    LogSinkCursorStats, LogSpoolStats, LogStatsStore, LogStatsStoreError,
    MAX_RETAINED_DEAD_LETTERS, SinkDeadLetterSnapshot,
};
pub use stats_warnings::derive_stats_warnings;
pub use store::{LogAppendReport, LogStore, LogStoreError, LogStoreRuntime, LogStoreRuntimeError};
pub use traffic::{
    IngressTrafficBreakdown, IngressTrafficQuery, IngressTrafficScope,
    MAXIMUM_TRAFFIC_BREAKDOWN_LIMIT, MAXIMUM_TRAFFIC_METRIC_LIMIT, ServiceTrafficQuery,
    TRAFFIC_BUCKET_MS, TrafficBreakdownEntry, TrafficMetricPoint, TrafficQueryError,
    TrafficQueryStore, merge_ingress_traffic, merge_service_traffic, project_ingress_traffic,
    project_service_traffic,
};
pub use traffic_cluster::{ClusterTrafficQueryCoordinator, NodeTrafficQueryStore};
pub use uptime::{SystemUptimeClock, UptimeClock};

#[cfg(test)]
mod tests;
