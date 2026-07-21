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

#[cfg(test)]
mod tests;
