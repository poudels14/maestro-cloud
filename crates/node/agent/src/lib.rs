//! Node-local reconciliation for Maestro workloads and system networking.
//!
//! This crate implements agent behavior over `kernel-api` contracts. It must
//! not depend on cluster provisioning, operators, observability pipelines, or
//! application composition roots.

mod artifact_drain;
mod artifact_holder;
mod artifact_replication;
mod artifact_retention;
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
mod cgroup_stats;
mod dns;
mod dns_plugin;
mod dns_resource;
mod dns_server;
mod dns_socks;
mod exec;
mod firewall;
mod health;
mod health_probe;
mod health_status;
mod host_disks;
mod host_stats;
mod host_telemetry;
#[cfg(target_os = "linux")]
mod linux_bridge;
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
mod network_stats;
#[cfg(unix)]
mod node_api;
#[cfg(unix)]
mod node_api_files;
#[cfg(unix)]
mod node_api_mount;
#[cfg(unix)]
mod node_control;
mod node_registry;
mod secret_mount;
mod secret_mount_files;
mod stats;
mod system_host_ports;
mod workload_bridge;

#[cfg(target_os = "linux")]
pub use linux_bridge::LinuxWorkloadBridgeBackend;
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

pub use artifact_holder::{ArtifactHolder, ArtifactHolderRegistry, ArtifactHolderRegistryError};
pub use artifact_replication::{
    ArtifactPeerSource, ArtifactPeerSourceError, ArtifactReplicationAgent,
    ArtifactReplicationError, ArtifactReplicationFailure, ArtifactReplicationOutcome,
    ArtifactReplicationReport, ArtifactReplicationSettings,
};
pub use assignment::AssignmentAgent;
pub use assignment_error::AssignmentAgentError;
pub use assignment_types::{AssignmentAgentSettings, AssignmentReconcileReport, WorkloadDns};
pub use cgroup_stats::{
    CgroupCpuStats, CgroupIoStats, CgroupMemoryEvents, CgroupMemoryStats, CgroupProcessStats,
    CgroupStats, CgroupStatsError, CgroupStatsReader, CgroupV2StatsReader,
};
pub use dns::{
    AuthoritativeDnsResolver, DnsAnswer, DnsLookup, DnsQueryType, DnsResolverError,
    DnsResponseCode, DnsZoneReader, DnsZoneSummary, MAESTRO_DNS_ZONE,
};
pub use dns_plugin::{
    DnsResolverPlugin, DnsResolverPluginError, TailscaleDnsPluginSettings, TailscaleDnsRoute,
};
pub use dns_resource::{DnsReconcileReport, DnsResourceAgent, DnsResourceError};
pub use dns_server::{
    AUTHORITATIVE_DNS_PORT, BoundDnsServer, DnsServerBinder, DnsServerError, DnsServerRuntime,
    DnsServerSettings, HickoryDnsServerBinder,
};
pub use exec::{NodeExecError, NodeExecService, NodeExecSettings};
pub use firewall::{
    FirewallAgentError, FirewallBackend, FirewallBackendError, FirewallReconcileReport,
    NodeFirewallAgent,
};
pub use health::{HealthAgent, HealthAgentError, HealthAgentSettings, HealthReconcileReport};
pub use health_probe::{HealthProbeError, HealthProbeTarget, HealthProber, NetworkHealthProber};
pub use host_disks::{
    HostDiskError, HostDiskFailure, HostDiskReader, HostDiskReport, HostDiskStats,
    LinuxHostDiskReader,
};
pub use host_stats::{
    HostCpuStats, HostMemoryStats, HostNetworkStats, HostResourceStats, HostStatsError,
    HostStatsReader, LinuxHostStatsReader,
};
pub use host_telemetry::{
    HostTelemetryAgent, HostTelemetryAgentError, HostTelemetryFailure, HostTelemetryFailureStage,
    HostTelemetryReport, HostTelemetrySample, HostTelemetrySettings, HostTelemetrySink,
    HostTelemetrySinkError,
};
pub use mesh::{
    MESH_INTERFACE_NAME, MESH_MTU_BYTES, MeshBackend, MeshBackendError, MeshConfiguration,
    MeshError, MeshInterface, MeshPeer, MeshPlanner, MeshReconciler, MeshRoute, MeshSubnet,
};
pub use mesh_identity::{MeshIdentity, MeshIdentityError, WireGuardPrivateKey, WireGuardPublicKey};
pub use mesh_resource::{MeshResourceAgent, MeshResourceError, StatusClock, SystemStatusClock};
pub use network_stats::{
    HostNetworkStatsReader, WorkloadNetworkStats, WorkloadNetworkStatsError,
    WorkloadNetworkStatsReader,
};
#[cfg(unix)]
pub use node_api::{
    BoundWorkloadNodeApi, NodeApiServerError, NodeApiServices, NodeApiSocketOwner,
    NodeControlHandler, NodeLogHandler, NodeMetricHandler, NodeTraceHandler, WorkloadControlAccess,
};
#[cfg(unix)]
pub use node_api_files::NodeApiMountError;
#[cfg(unix)]
pub use node_control::StoreNodeControlHandler;
pub use node_registry::{
    NodeRegistration, NodeRegistryAction, NodeRegistryAgent, NodeRegistryError,
    NodeRegistrySettings, NodeRegistrySettingsError,
};
pub use secret_mount::SecretMountError;
pub use stats::{
    WorkloadStatsAgent, WorkloadStatsAgentError, WorkloadStatsFailure, WorkloadStatsFailureStage,
    WorkloadStatsReport, WorkloadStatsSample, WorkloadStatsSettings, WorkloadStatsSink,
    WorkloadStatsSinkError,
};
pub use workload_bridge::{
    WORKLOAD_BRIDGE_NAME, WorkloadBridge, WorkloadBridgeAgent, WorkloadBridgeBackend,
    WorkloadBridgeBackendError, WorkloadBridgeError,
};

#[cfg(test)]
mod tests;
