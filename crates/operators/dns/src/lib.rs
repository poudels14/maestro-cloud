//! Deterministic service DNS projection for Maestro.
//!
//! This operator depends only on kernel contracts and later the controller/store
//! kernels. It must not depend on node, runtime, cluster, or sibling operators.

mod controller;
mod model;
mod plan;
mod reconciler;
mod resource;
mod snapshot;
mod target;
mod validation;
mod writer;

pub use controller::{DnsController, DnsError, DnsReport};
pub use model::{DnsInput, DnsPlan, DnsSettings};
pub use plan::{DnsPlanError, plan};
pub use reconciler::DnsReconciler;

/// Comma-separated short DNS names published for an annotated Service.
pub const DNS_ALIASES_ANNOTATION: &str = "dns.maestro.dev/aliases";

#[cfg(test)]
mod tests;
