pub mod assignment_store;
pub mod auth;
pub mod bootstrap;
pub mod control;
pub mod data_plane;
pub mod elector;
pub mod executor;
pub mod identity;
pub mod join;
pub mod leader_loop;
pub mod migration;
pub mod network;
pub mod provision;
pub mod reconciler;
pub mod registry;
pub mod scheduler;
pub mod telemetry;
pub mod traefik;
pub mod types;
pub mod upgrade;

#[cfg(test)]
mod integration_tests;

pub use types::{
    Assignment, AssignmentManifest, ClusterMaintenanceKind, ClusterMeta, ClusterNodeEndpoint,
    ClusterRuntime, ImageAssignment, LeaderInfo, NodeAffinity, NodeDiskInfo, NodeGatewayEndpoint,
    NodeInfo, NodeRecord, NodeRole, NodeState, PlacementHistory, ReplicaEndpoint,
    SystemUpgradeStage, TrafficGeneration, UnschedulableReplica, UpgradeBatch, UpgradeEvent,
    UpgradePhase, UpgradeRun,
};

pub(crate) fn retain_peer_image(
    status: &crate::deployment::types::DeploymentStatus,
    deployment_id: &str,
    latest_deployment_id: Option<&str>,
) -> bool {
    use crate::deployment::types::DeploymentStatus;

    matches!(
        status,
        DeploymentStatus::Building
            | DeploymentStatus::PendingReady
            | DeploymentStatus::Ready
            | DeploymentStatus::Draining
    ) || latest_deployment_id == Some(deployment_id)
}
