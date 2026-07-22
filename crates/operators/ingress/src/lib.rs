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
mod traefik_store;
mod validation;
mod writer;

pub use backend::{IngressBackend, IngressBackendError};
pub use controller::{IngressController, IngressError, IngressReport};
pub use model::{
    BackendChange, IngressBlocklistChange, IngressInput, IngressPlan, IngressSettings,
    PublishedTraffic, ResourceStatusUpdate,
};
pub use plan::{IngressPlanError, plan};
pub use reconciler::{IngressBlocklistReconciler, IngressReconciler};
pub use traefik::{
    TRAEFIK_BLOCKED_ROUTER_PREFIX, TraefikBackend, TraefikBlocklistConfig, TraefikCutover,
    TraefikProvider, TraefikStage, traefik_service_router_prefix,
};
pub use traefik_store::StoreTraefikProvider;
pub use validation::validate_route_spec;

#[cfg(test)]
mod tests;
