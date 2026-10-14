mod attach;
mod build;
mod disks;
mod dns;
mod error;
mod health;
mod ingress;
mod node;
mod reconciler;
mod secret_files;
mod source;
mod status;
mod system;
mod wireguard;

pub use attach::{Attachment, ControlLink, StreamPlanSource};
pub use build::{
    BuildKitBuilder, BuildKitSettings, BuildOutput, BuildRequest, image_name, remote_head,
};
pub use disks::{Disk, disks};
pub use dns::{DnsRecords, DnsServer, upstream_resolvers};
pub use error::AgentError;
pub use health::{HealthMonitor, probe};
pub use ingress::{IngressWriter, serve_denied};
pub use node::{apply_workload_slice_limits, detect_capacity, system_reserve};
pub use reconciler::{Reconciler, ReconcilerSettings};
pub use source::{LocalPlanSource, PlanSource};
pub use status::StatusBoard;
pub use system::{ADMIN_CONTAINER_ID, SystemSupervisor, admin_container, traefik_container};
pub use wireguard::{
    CommandOutput, CommandRunner, DEFAULT_INTERFACE as WIREGUARD_INTERFACE,
    DEFAULT_LISTEN_PORT as WIREGUARD_PORT, HostCommandRunner, MeshConfig, NodeKey, WireGuardMesh,
    load_or_generate_key, public_key,
};
