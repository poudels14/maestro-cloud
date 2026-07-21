//! Host-to-workload wire and authorization contracts for Maestro nodes.
//!
//! This crate contains generated protobuf messages and authentication types
//! only. It may depend on `kernel-api`, but must not depend on agents,
//! runtimes, operators, observability implementations, cluster provisioning,
//! or applications.

mod auth;
mod contract;
mod generated;

pub use auth::{
    AuthorizationError, SocketPeer, WorkloadAuthorization, WorkloadClaims, WorkloadToken,
};
pub use contract::{
    WORKLOAD_NODE_DIRECTORY, WORKLOAD_NODE_SOCKET_FILE, WORKLOAD_NODE_SOCKET_PATH,
    WORKLOAD_NODE_TOKEN_FILE, WORKLOAD_NODE_TOKEN_PATH, WORKLOAD_TOKEN_HEADER,
};
pub use generated::{otlp, proto};

#[cfg(test)]
mod tests;
