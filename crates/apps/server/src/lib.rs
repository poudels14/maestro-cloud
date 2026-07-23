//! Authenticated HTTP API over canonical Maestro resources.
//!
//! Domain routers stay thin: stores own persistence, operators own behavior,
//! and this application owns transport validation, authentication, masking,
//! and OpenAPI composition.

mod api_server;
mod auth;
mod error;
mod exec_service;
mod listener;
mod mask;
mod mutation;
mod node_artifact_client;
mod node_http_client;
mod node_log_client;
mod node_metric_client;
mod node_stats_client;
mod openapi;
mod openapi_admission;
mod openapi_commands;
mod openapi_logs;
mod openapi_metrics;
mod openapi_stats;
mod openapi_tailscale;
mod openapi_traffic;
mod resource;
mod routes;
mod settings;
mod state;
mod system_resources;

pub use api_server::ApiServer;
pub use auth::OperatorIdentity;
pub use error::{ApiError, ApiErrorBody, ServerError};
pub use exec_service::{
    ClusterExecSessions, ExecSessionOpenError, HttpClusterExecSessions, HttpExecClientError,
};
pub use listener::BoundApiServer;
pub use node_artifact_client::{HttpNodeArtifactClient, NodeArtifactTransferError};
pub use node_http_client::NodeHttpClientError;
pub use node_log_client::{HttpNodeLogClient, HttpNodeLogQueryStore};
pub use node_metric_client::{
    HttpNodeMetricQueryStore, NodeMetricQueryError, NodeMetricQueryStore,
};
pub use node_stats_client::{HttpNodeStatsQueryStore, NodeStatsQueryError, NodeStatsQueryStore};
pub use openapi::openapi_document;
pub use settings::{ServerSettings, ServerSettingsError, TlsIdentity};

pub type NodeLogClientError = NodeHttpClientError;
pub type NodeMetricClientError = NodeHttpClientError;

pub(crate) use state::{AppState, VerifiedNodeCertificate};

#[cfg(test)]
mod tests;
