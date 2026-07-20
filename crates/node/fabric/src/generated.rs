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
}
