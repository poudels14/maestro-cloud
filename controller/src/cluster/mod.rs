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
pub mod recovery;
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
    ClusterRuntime, LeaderInfo, NodeAffinity, NodeDiskInfo, NodeGatewayEndpoint, NodeInfo,
    NodeRecord, NodeRole, NodeState, PlacementHistory, ReplicaEndpoint, TrafficGeneration,
    UnschedulableReplica, UpgradeEvent, UpgradePhase, UpgradeRun,
};
