//! Node-local reconciliation for Maestro workloads and system networking.
//!
//! This crate implements agent behavior over `kernel-api` contracts. It must
//! not depend on cluster provisioning, operators, observability pipelines, or
//! application composition roots.

mod assignment;
mod assignment_error;
mod assignment_plan;
mod assignment_resource;
mod assignment_restart;
mod assignment_status;
mod health;
mod health_probe;
mod health_status;
#[cfg(target_os = "linux")]
mod linux_mesh;
mod mesh;
mod mesh_identity;
mod mesh_resource;

#[cfg(target_os = "linux")]
pub use linux_mesh::LinuxMeshBackend;

pub use assignment::{AssignmentAgent, AssignmentAgentSettings, AssignmentReconcileReport};
pub use assignment_error::AssignmentAgentError;
pub use health::{HealthAgent, HealthAgentError, HealthAgentSettings, HealthReconcileReport};
pub use health_probe::{HealthProbeError, HealthProbeTarget, HealthProber, NetworkHealthProber};
pub use mesh::{
    MESH_INTERFACE_NAME, MESH_MTU_BYTES, MeshBackend, MeshBackendError, MeshConfiguration,
    MeshError, MeshInterface, MeshPeer, MeshPlanner, MeshReconciler, MeshRoute, MeshSubnet,
};
pub use mesh_identity::{MeshIdentity, MeshIdentityError, WireGuardPrivateKey, WireGuardPublicKey};
pub use mesh_resource::{MeshResourceAgent, MeshResourceError, StatusClock, SystemStatusClock};

#[cfg(test)]
mod tests;
