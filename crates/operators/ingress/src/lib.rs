//! Deterministic ingress traffic-generation planning for Maestro services.
//!
//! The pure planner captures immutable route and ready-target snapshots, then
//! describes blue/green publication separately from resource persistence.

mod backend;
mod controller;
mod lifecycle;
mod model;
mod plan;
mod reconciler;
mod resource;
mod snapshot;
mod target;
mod traefik;
mod validation;
mod writer;

pub use backend::{IngressBackend, IngressBackendError};
pub use controller::{IngressController, IngressError, IngressReport};
pub use model::{
    BackendChange, IngressInput, IngressPlan, IngressSettings, PublishedTraffic,
    ResourceStatusUpdate,
};
pub use plan::{IngressPlanError, plan};
pub use reconciler::IngressReconciler;
pub use traefik::{TraefikBackend, TraefikCutover, TraefikProvider, TraefikStage};

#[cfg(test)]
mod tests;
