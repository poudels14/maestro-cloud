//! Deployment lifecycle planning for Maestro services.
//!
//! The pure planner in this crate owns rollout state transitions. Store
//! projection, atomic writes, and the controller-runtime adapter are kept at
//! separate boundaries so lifecycle behavior remains exhaustively testable.

mod model;
mod plan;
mod readiness;
mod resource;

pub use model::{DeploymentInput, DeploymentPlan, LifecycleSettings, ResourceStatusUpdate};
pub use plan::{DeploymentPlanError, plan};

#[cfg(test)]
mod tests;
