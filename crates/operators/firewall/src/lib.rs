//! Deterministic nftables policy compilation for Maestro.
//!
//! This operator depends only on kernel contracts and later controller/store
//! kernels. It must not depend on node, runtime, cluster, or sibling operators.

mod cidr;
mod model;
mod plan;
mod render;
mod validation;

pub use model::{
    FirewallInput, FirewallPlan, FirewallPolicyStatusUpdate, FirewallRuleset, FirewallSettings,
};
pub use plan::{FirewallPlanError, plan};

#[cfg(test)]
mod tests;
