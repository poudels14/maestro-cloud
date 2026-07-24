//! Maestro daemon composition root and role lifetime ownership.
//!
//! This crate selects controller and agent roles from validated cluster
//! topology. Production adapters implement the role factory; the composition
//! runtime guarantees ordered startup, reverse shutdown, and rollback.

mod admission;
mod agent_lifecycle;
mod agent_network;
mod agent_role;
mod agent_tasks;
mod artifact_replication;
mod cluster_query_clients;
mod config_view;
mod control_plane;
mod datadog;
mod dead_letter_admin;
mod error;
mod join_activation;
mod launch;
mod launch_error;
mod leadership;
mod local_logs;
mod log_backup_config;
mod log_delivery;
mod log_maintenance;
mod metric_delivery;
mod operator_error;
mod operator_settings;
mod operators;
mod plan;
mod preview_config;
mod runtime;
mod s3_backup;
mod stats_metric_sampler;
mod tailscale_reconciler;
mod tailscale_resources;
mod upgrade_config;
mod workload_agents;

pub use admission::AdmissionDependencies;
pub use control_plane::{
    AgentStore, DaemonRoleDependencies, DaemonRoleFactory, DaemonRoleSettings, LeaderWorkload,
    NodeUpgradeDependencies,
};
pub use datadog::{DatadogLaunchConfig, DatadogLogsLaunchConfig, DatadogMetricsLaunchConfig};
pub use dead_letter_admin::{
    DeadLetterAdminCommand, DeadLetterAdminError, DeadLetterAdminOutput, administer_dead_letters,
};
pub use error::{DaemonError, RoleError, RoleFailure};
pub use launch::{DaemonLaunchConfig, StoreLaunchMode, launch_daemon, load_launch_config};
pub use launch_error::DaemonLaunchError;
pub use local_logs::{LocalLogError, LocalLogOptions, stream_local_logs};
pub use log_backup_config::LogBackupLaunchConfig;
pub use log_maintenance::{
    LogBackupTarget, LogMaintenanceError, LogMaintenanceSettings, LogMaintenanceWorker,
};
pub use operator_error::OperatorSuiteError;
pub use operator_settings::{OperatorSettings, PreviewOperatorSettings};
pub use operators::{
    BuildOperatorBackends, OperatorBackends, OperatorInvocationReport, OperatorLeaderWorkload,
    OperatorSuite,
};
pub use plan::{DaemonPlan, DaemonRole, RoleSpec};
pub use preview_config::{PreviewLaunchConfig, PreviewLaunchError};
pub use runtime::{Daemon, RoleFactory, RoleRuntime, RunningDaemon};
pub use s3_backup::{S3BackupObjectStore, S3BackupObjectStoreError};
pub use upgrade_config::{NixosUpgradeLaunchConfig, NixosUpgradeLaunchError};

#[cfg(test)]
mod tests;
