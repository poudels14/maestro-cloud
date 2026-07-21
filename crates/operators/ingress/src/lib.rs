//! Deterministic ingress traffic-generation planning for Maestro services.
//!
//! The pure planner captures immutable route and ready-target snapshots, then
//! describes blue/green publication separately from resource persistence.

mod lifecycle;
mod model;
mod plan;
mod resource;
mod target;
mod validation;

pub use model::{
    BackendChange, IngressInput, IngressPlan, IngressSettings, PublishedTraffic,
    ResourceStatusUpdate,
};
pub use plan::{IngressPlanError, plan};

#[cfg(test)]
mod tests;
