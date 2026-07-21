//! Deterministic service DNS projection for Maestro.
//!
//! This operator depends only on kernel contracts and later the controller/store
//! kernels. It must not depend on node, runtime, cluster, or sibling operators.

mod model;
mod plan;
mod resource;
mod target;
mod validation;

pub use model::{DnsInput, DnsPlan, DnsSettings};
pub use plan::{DnsPlanError, plan};

#[cfg(test)]
mod tests;
