mod build;
mod dns;
mod error;
mod health;
mod ingress;
mod node;
mod reconciler;
mod source;
mod status;
mod system;
mod wireguard;

pub use build::{BuildKitBuilder, BuildKitSettings, BuildRequest, image_name};
pub use dns::{DnsRecords, DnsServer, internal_names, upstream_resolvers};
pub use error::AgentError;
pub use health::{HealthMonitor, probe};
pub use ingress::IngressWriter;
pub use node::{apply_workload_slice_limits, detect_capacity, system_reserve};
pub use reconciler::{Reconciler, ReconcilerSettings};
pub use source::{LocalPlanSource, PlanSource};
pub use status::StatusBoard;
pub use system::{ADMIN_CONTAINER_ID, SystemSupervisor, admin_container, traefik_container};
pub use wireguard::{
    CommandOutput, CommandRunner, DEFAULT_INTERFACE as WIREGUARD_INTERFACE,
    DEFAULT_LISTEN_PORT as WIREGUARD_PORT, HostCommandRunner, MeshConfig, MeshPeer, NodeKey,
    WireGuardMesh, load_or_generate_key, public_key,
};
