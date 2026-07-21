/// Maestro-specific Identity and privileged Control gRPC contracts.
pub mod proto {
    tonic::include_proto!("maestro.node.v1");
}

/// Standard OTLP collector contracts accepted by the node socket.
pub mod otlp {
    /// OTLP log export messages, client, and server.
    pub use opentelemetry_proto::tonic::collector::logs::v1 as logs;
    /// OTLP metric export messages, client, and server.
    pub use opentelemetry_proto::tonic::collector::metrics::v1 as metrics;
    /// OTLP trace export messages, client, and server.
    pub use opentelemetry_proto::tonic::collector::trace::v1 as traces;
    /// OTLP common values, key/value attributes, and instrumentation scopes.
    pub use opentelemetry_proto::tonic::common::v1 as common;
    /// OTLP log data records nested inside collector requests.
    pub use opentelemetry_proto::tonic::logs::v1 as log_data;
    /// OTLP resource attributes shared by every signal.
    pub use opentelemetry_proto::tonic::resource::v1 as resource;
}
