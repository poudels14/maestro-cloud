//! Node-local reconciliation for Maestro workloads and system networking.
//!
//! This crate implements agent behavior over `kernel-api` contracts. It must
//! not depend on cluster provisioning, operators, observability pipelines, or
//! application composition roots.

mod assignment;
mod assignment_error;
#[cfg(unix)]
mod assignment_node_api;
mod assignment_plan;
mod assignment_replica;
mod assignment_resource;
mod assignment_restart;
mod assignment_status;
mod assignment_types;
#[cfg(target_os = "linux")]
mod cgroup_stats;
mod dns;
mod dns_resource;
mod dns_server;
mod exec;
mod firewall;
mod health;
mod health_probe;
mod health_status;
#[cfg(target_os = "linux")]
mod linux_firewall;
#[cfg(target_os = "linux")]
mod linux_mesh;
#[cfg(unix)]
mod log_agent;
#[cfg(unix)]
mod log_checkpoint;
mod mesh;
mod mesh_identity;
mod mesh_resource;
#[cfg(unix)]
mod node_api;
#[cfg(unix)]
mod node_api_files;
#[cfg(unix)]
mod node_api_mount;
mod secret_mount;
#[cfg(target_os = "linux")]
mod stats;

#[cfg(target_os = "linux")]
pub use linux_firewall::NftablesFirewallBackend;
#[cfg(target_os = "linux")]
pub use linux_mesh::LinuxMeshBackend;
#[cfg(unix)]
pub use log_agent::{
    RuntimeLogAgent, RuntimeLogAgentError, RuntimeLogAgentSettings, RuntimeLogFailure,
    RuntimeLogFailureStage, RuntimeLogReport, WorkloadLogEntry, WorkloadLogSink,
    WorkloadLogSinkError,
};
#[cfg(unix)]
pub use log_checkpoint::{FileLogCheckpointStore, LogCheckpointError, LogCheckpointStore};

pub use assignment::AssignmentAgent;
pub use assignment_error::AssignmentAgentError;
pub use assignment_types::{AssignmentAgentSettings, AssignmentReconcileReport};
#[cfg(target_os = "linux")]
pub use cgroup_stats::{
    CgroupCpuStats, CgroupIoStats, CgroupMemoryEvents, CgroupMemoryStats, CgroupProcessStats,
    CgroupStats, CgroupStatsError, CgroupStatsReader, CgroupV2StatsReader,
};
pub use dns::{
    AuthoritativeDnsResolver, DnsAnswer, DnsLookup, DnsQueryType, DnsResolverError,
    DnsResponseCode, DnsZoneSummary, MAESTRO_DNS_ZONE,
};
pub use dns_resource::{DnsReconcileReport, DnsResourceAgent, DnsResourceError};
pub use dns_server::{BoundDnsServer, DnsServerError, DnsServerSettings};
pub use exec::{NodeExecError, NodeExecService, NodeExecSettings};
pub use firewall::{
    FirewallAgentError, FirewallBackend, FirewallBackendError, FirewallReconcileReport,
    NodeFirewallAgent,
};
pub use health::{HealthAgent, HealthAgentError, HealthAgentSettings, HealthReconcileReport};
pub use health_probe::{HealthProbeError, HealthProbeTarget, HealthProber, NetworkHealthProber};
pub use mesh::{
    MESH_INTERFACE_NAME, MESH_MTU_BYTES, MeshBackend, MeshBackendError, MeshConfiguration,
    MeshError, MeshInterface, MeshPeer, MeshPlanner, MeshReconciler, MeshRoute, MeshSubnet,
};
pub use mesh_identity::{MeshIdentity, MeshIdentityError, WireGuardPrivateKey, WireGuardPublicKey};
pub use mesh_resource::{MeshResourceAgent, MeshResourceError, StatusClock, SystemStatusClock};
#[cfg(unix)]
pub use node_api::{
    BoundWorkloadNodeApi, NodeApiServerError, NodeApiServices, NodeApiSocketOwner,
    NodeControlHandler, NodeTelemetryHandler,
};
#[cfg(unix)]
pub use node_api_files::NodeApiMountError;
pub use secret_mount::SecretMountError;
#[cfg(target_os = "linux")]
pub use stats::{
    WorkloadStatsAgent, WorkloadStatsAgentError, WorkloadStatsFailure, WorkloadStatsFailureStage,
    WorkloadStatsReport, WorkloadStatsSample, WorkloadStatsSettings,
};

#[cfg(test)]
mod tests;
