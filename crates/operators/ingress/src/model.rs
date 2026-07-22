use std::time::Duration;

use kernel_api::{
    Assignment, Deployment, Generation, IngressBlocklist, IngressBlocklistId,
    IngressBlocklistStatus, IngressRoute, IngressRouteId, IngressRouteStatus, ReplicaState,
    ResourceRevision, Service, ServiceId, Timestamp, TrafficGeneration, TrafficGenerationId,
    TrafficGenerationSpec, TrafficGenerationStatus,
};

/// Timing policy applied to retired ingress generations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IngressSettings {
    /// Minimum time old configuration remains addressable after a cutover.
    pub retirement_grace: Duration,
}

/// Complete typed snapshot consumed by one deterministic ingress pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngressInput {
    /// Cluster identity included in stable generation identities.
    pub cluster_id: kernel_api::ClusterId,
    /// Current UTC time used only for persisted transition timestamps.
    pub now: Timestamp,
    /// Ingress lifecycle timing policy.
    pub settings: IngressSettings,
    /// Desired services and their active deployment selections.
    pub services: Vec<Service>,
    /// Immutable deployments referenced by services and generations.
    pub deployments: Vec<Deployment>,
    /// Desired external routes.
    pub routes: Vec<IngressRoute>,
    /// Current scheduler placements used to address ready targets.
    pub assignments: Vec<Assignment>,
    /// Exact assignment readiness observations.
    pub replicas: Vec<ReplicaState>,
    /// Existing immutable traffic generations.
    pub traffic_generations: Vec<TrafficGeneration>,
    /// Desired singleton client-address blocklist.
    pub blocklists: Vec<IngressBlocklist>,
}

/// Optimistic status replacement for one existing resource.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceStatusUpdate<Id, Status> {
    /// Stable resource identity.
    pub id: Id,
    /// Store revision from which this update was planned.
    pub observed_revision: ResourceRevision,
    /// Complete desired status.
    pub status: Status,
}

/// Immutable configuration handed to the ingress publication backend.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishedTraffic {
    /// Traffic generation being published.
    pub generation_id: TrafficGenerationId,
    /// Complete immutable route and target snapshot.
    pub spec: TrafficGenerationSpec,
}

/// One idempotent per-service backend convergence request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackendChange {
    /// Service whose router and backend configuration must converge.
    pub service_id: ServiceId,
    /// Generation that should receive new traffic, or no generation while deleting.
    pub active: Option<PublishedTraffic>,
    /// Obsolete staged or retired generations safe to remove.
    pub remove: Vec<TrafficGenerationId>,
}

/// One idempotent publication of the cluster ingress blocklist.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngressBlocklistChange {
    /// Desired resource generation acknowledged after publication.
    pub generation: Generation,
    /// Canonical unique addresses denied before service routing.
    pub addresses: Vec<std::net::IpAddr>,
    /// Stable digest of the exact desired address set.
    pub configuration_digest: String,
}

/// Desired resource and publication mutations from one ingress pass.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IngressPlan {
    /// Missing immutable generations to persist in Staged phase.
    pub create_generations: Vec<TrafficGeneration>,
    /// Obsolete staged or grace-expired retired generations to delete.
    pub delete_generations: Vec<TrafficGenerationId>,
    /// Existing generation status replacements.
    pub generation_updates: Vec<ResourceStatusUpdate<TrafficGenerationId, TrafficGenerationStatus>>,
    /// Route publication acknowledgements.
    pub route_updates: Vec<ResourceStatusUpdate<IngressRouteId, IngressRouteStatus>>,
    /// Idempotent side effects which must precede the matching status commit.
    pub backend_changes: Vec<BackendChange>,
    /// Cluster blocklist publication required before acknowledgement.
    pub blocklist_change: Option<IngressBlocklistChange>,
    /// Singleton blocklist publication acknowledgement.
    pub blocklist_updates: Vec<ResourceStatusUpdate<IngressBlocklistId, IngressBlocklistStatus>>,
    /// Earliest persisted retirement deadline requiring another lifecycle pass.
    pub requeue_at: Option<Timestamp>,
}
