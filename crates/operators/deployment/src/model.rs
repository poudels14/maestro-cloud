use std::time::Duration;

use kernel_api::{
    Assignment, Build, BuildId, Deployment, DeploymentId, DeploymentStatus, ReplicaStateId,
    ResourceRevision, Service, ServiceId, ServiceStatus, Timestamp, TrafficGeneration,
};

/// Timing policy applied by the pure deployment lifecycle planner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LifecycleSettings {
    /// Minimum time a superseded deployment remains available while traffic drains.
    pub drain_grace: Duration,
}

/// Complete typed snapshot consumed by one deterministic lifecycle pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeploymentInput {
    /// Cluster identity included in deterministic child resource identities.
    pub cluster_id: kernel_api::ClusterId,
    /// Current UTC time used only for persisted lifecycle timestamps.
    pub now: Timestamp,
    /// Deployment lifecycle timing policy.
    pub settings: LifecycleSettings,
    /// Desired services.
    pub services: Vec<Service>,
    /// Existing immutable deployments.
    pub deployments: Vec<Deployment>,
    /// Existing artifact builds.
    pub builds: Vec<Build>,
    /// Current scheduler placements.
    pub assignments: Vec<Assignment>,
    /// Agent-observed replica states.
    pub replicas: Vec<kernel_api::ReplicaState>,
    /// Ingress-observed traffic generations used as cutover acknowledgements.
    pub traffic_generations: Vec<TrafficGeneration>,
}

/// Optimistic status replacement for one existing resource.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceStatusUpdate<Id, Status> {
    /// Stable resource identity.
    pub id: Id,
    /// Store revision from which this update was planned.
    pub observed_revision: ResourceRevision,
    /// Complete desired status preserving unrelated status fields.
    pub status: Status,
}

/// Desired resource mutations from one lifecycle pass.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DeploymentPlan {
    /// Deterministically identified deployments missing from the snapshot.
    pub create_deployments: Vec<Deployment>,
    /// Deterministically identified builds required by active queued deployments.
    pub create_builds: Vec<Build>,
    /// Removed deployments whose owning Service is finalizing.
    pub delete_deployments: Vec<DeploymentId>,
    /// Builds owned by deployments being deleted.
    pub delete_builds: Vec<BuildId>,
    /// Stale replica observations owned by deployments being deleted.
    pub delete_replicas: Vec<ReplicaStateId>,
    /// Existing deployment status replacements.
    pub deployment_updates: Vec<ResourceStatusUpdate<DeploymentId, DeploymentStatus>>,
    /// Existing service status replacements.
    pub service_updates: Vec<ResourceStatusUpdate<ServiceId, ServiceStatus>>,
}
