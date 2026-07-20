//! Node-local reconciliation for Maestro workloads and system networking.
//!
//! This crate implements agent behavior over `kernel-api` contracts. It must
//! not depend on cluster provisioning, operators, observability pipelines, or
//! application composition roots.

mod mesh;
mod mesh_identity;

pub use mesh::{
    MESH_INTERFACE_NAME, MESH_MTU_BYTES, MeshBackend, MeshBackendError, MeshConfiguration,
    MeshError, MeshInterface, MeshPeer, MeshPlanner, MeshReconciler, MeshRoute, MeshSubnet,
};
pub use mesh_identity::{MeshIdentity, MeshIdentityError, WireGuardPrivateKey, WireGuardPublicKey};

#[cfg(test)]
mod tests;
