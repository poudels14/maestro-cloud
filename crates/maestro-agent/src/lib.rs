mod attach;
mod build;
mod cloudflare;
mod dns;
mod egress;
mod error;
mod health;
mod ingress;
mod node;
mod reconciler;
mod replica_manifest;
mod secret_files;
mod source;
mod status;
mod system;
mod tailscale;
mod wireguard;

pub use attach::{Attachment, ControlLink, StreamPlanSource};
pub use build::{
    BuildKitBuilder, BuildKitSettings, BuildOutput, BuildRequest, DepotBuild, image_name,
    remote_head,
};
pub use cloudflare::TunnelConnectors;
pub use dns::{DnsRecords, DnsServer, PeerClusters, upstream_resolvers};
pub use egress::{EgressFirewall, EgressPolicy};
pub use error::AgentError;
pub use health::{HealthMonitor, probe};
pub use ingress::{IngressWriter, serve_denied};
pub use node::{apply_workload_slice_limits, detect_capacity, reset_stale_bridge, system_reserve};
pub use reconciler::{ImageFetcher, Reconciler, ReconcilerSettings};
pub use replica_manifest::{LocalReplica, publish_replica_manifest};
pub use source::{LocalPlanSource, PlanSource};
pub use status::StatusBoard;
pub use system::{
    ADMIN_CONTAINER_ID, ADMIN_PORT, SystemSupervisor, admin_container, traefik_container,
};
pub use tailscale::{
    PANEL_SOCKET_FILE, TAILSCALE_CONTAINER_ID, TailscaleNode, discover_peer_clusters,
    tailscale_container, write_panel_serve_config,
};
pub use wireguard::{
    CommandOutput, CommandRunner, DEFAULT_INTERFACE as WIREGUARD_INTERFACE,
    DEFAULT_LISTEN_PORT as WIREGUARD_PORT, HostCommandRunner, MeshConfig, NodeKey, WireGuardMesh,
    load_or_generate_key, public_key,
};
