//! Deterministic nftables policy compilation for Maestro.
//!
//! This operator depends only on kernel contracts and later controller/store
//! kernels. It must not depend on node, runtime, cluster, or sibling operators.

mod cidr;
mod controller;
mod model;
mod plan;
mod reconciler;
mod render;
mod snapshot;
mod validation;
mod writer;

pub use controller::{FirewallController, FirewallError, FirewallReport};
pub use model::{
    FirewallBundle, FirewallInput, FirewallPlan, FirewallPolicyStatusUpdate, FirewallRuleset,
    FirewallSettings, HostPortProtocol, HostPortRoute, SystemHostAccess, SystemHostEndpoint,
};
pub use plan::{FirewallPlanError, plan};
pub use reconciler::{FirewallBaselineReconciler, FirewallPolicyReconciler};

#[cfg(test)]
mod tests;
