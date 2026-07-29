//! Maestro daemon composition root and role lifetime ownership.
//!
//! This crate selects controller and agent roles from validated cluster
//! topology. Production adapters implement the role factory; the composition
//! runtime guarantees ordered startup, reverse shutdown, and rollback.

mod admission;
mod agent_api;
mod agent_lifecycle;
mod agent_network;
mod agent_role;
mod agent_tasks;
mod artifact_replication;
mod cloudflare_resources;
mod cluster_query_clients;
mod config_view;
mod control_plane;
mod datadog;
mod dead_letter_admin;
mod depot_config;
mod dns_launch;
mod dns_reconciler;
mod dns_resources;
mod error;
mod join_activation;
mod launch;
mod launch_config_admin;
mod launch_error;
mod leadership;
mod local_logs;
mod log_backup_config;
mod log_delivery;
mod log_maintenance;
mod metric_delivery;
mod operator_error;
mod operator_leader;
mod operator_settings;
mod operators;
mod plan;
#[cfg(any(target_os = "macos", feature = "macos-platform"))]
mod platform;
mod preview_config;
mod role_tasks;
mod runtime;
mod s3_backup;
mod stats_metric_sampler;
mod system_service_reconciler;
mod tailscale_reconciler;
mod tailscale_resources;
mod traefik_resources;
mod upgrade_config;
mod workload_agents;

pub use admission::AdmissionDependencies;
pub use cluster::{
    DatadogLaunchConfig, DatadogLogsLaunchConfig, DatadogMetricsLaunchConfig, DepotLaunchConfig,
    LogBackupLaunchConfig, NixosUpgradeLaunchConfig, PreviewLaunchConfig,
};
pub use control_plane::{
    AgentStore, DaemonRoleDependencies, DaemonRoleFactory, DaemonRoleSettings,
    HostTelemetryDependencies, LeaderWorkload, NodeUpgradeDependencies,
};
pub use dead_letter_admin::{
    DeadLetterAdminCommand, DeadLetterAdminError, DeadLetterAdminOutput, administer_dead_letters,
};
pub use depot_config::DepotLaunchError;
pub use dns_launch::{
    DEFAULT_DNS_RESOLVER_PORT, DnsResolverLaunchConfig, DnsResolverLaunchError, run_dns_resolver,
};
pub use error::{DaemonError, RoleError, RoleFailure};
pub use launch::{
    DaemonLaunchConfig, DaemonLaunchDocument, StoreLaunchMode, launch_daemon,
    launch_daemon_with_document, load_launch_config, load_launch_document,
};
pub use launch_error::DaemonLaunchError;
pub use local_logs::{LocalLogError, LocalLogOptions, stream_local_logs};
pub use log_maintenance::{
    LogBackupTarget, LogMaintenanceError, LogMaintenanceSettings, LogMaintenanceWorker,
};
pub use operator_error::OperatorSuiteError;
pub use operator_leader::{BuildOperatorBackends, OperatorBackends, OperatorLeaderWorkload};
pub use operator_settings::{OperatorSettings, PreviewOperatorSettings};
pub use operators::{OperatorInvocationReport, OperatorSuite};
pub use plan::{DaemonPlan, DaemonRole, RoleSpec};
pub use preview_config::PreviewLaunchError;
pub use runtime::{Daemon, RoleFactory, RoleRuntime, RunningDaemon};
pub use s3_backup::{S3BackupObjectStore, S3BackupObjectStoreError};
pub use upgrade_config::NixosUpgradeLaunchError;

#[cfg(test)]
mod tests;
