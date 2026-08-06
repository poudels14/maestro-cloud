//! Deployment lifecycle planning for Maestro services.
//!
//! The pure planner in this crate owns rollout state transitions. Store
//! projection, atomic writes, and the controller-runtime adapter are kept at
//! separate boundaries so lifecycle behavior remains exhaustively testable.

mod controller;
mod environment;
mod goal;
mod model;
mod plan;
mod readiness;
mod reconciler;
mod resource;
mod snapshot;
mod writer;

pub use controller::{DeploymentController, DeploymentError, DeploymentReport};
pub use model::{
    DeploymentInput, DeploymentPlan, LifecycleSettings, ResourceStatusUpdate, ServiceUpdate,
};
pub use plan::{DeploymentPlanError, plan};
pub use reconciler::DeploymentReconciler;

#[cfg(test)]
mod tests;
