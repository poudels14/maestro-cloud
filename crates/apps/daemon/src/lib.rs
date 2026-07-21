//! Maestro daemon composition root and role lifetime ownership.
//!
//! This crate selects controller and agent roles from validated cluster
//! topology. Production adapters implement the role factory; the composition
//! runtime guarantees ordered startup, reverse shutdown, and rollback.

mod agent_lifecycle;
mod agent_role;
mod control_plane;
mod datadog;
mod dead_letter_admin;
mod error;
mod launch;
mod leadership;
mod log_delivery;
mod metric_delivery;
mod operators;
mod plan;
mod runtime;
mod s3_backup;
mod workload_agents;

pub use control_plane::{
    AgentStore, DaemonRoleDependencies, DaemonRoleFactory, DaemonRoleSettings, LeaderWorkload,
};
pub use datadog::{DatadogLaunchConfig, DatadogLogsLaunchConfig, DatadogMetricsLaunchConfig};
pub use dead_letter_admin::{
    DeadLetterAdminCommand, DeadLetterAdminError, DeadLetterAdminOutput, administer_dead_letters,
};
pub use error::{DaemonError, RoleError, RoleFailure};
pub use launch::{
    DaemonLaunchConfig, DaemonLaunchError, StoreLaunchMode, launch_daemon, load_launch_config,
};
pub use operators::{
    OperatorBackends, OperatorInvocationReport, OperatorLeaderWorkload, OperatorSettings,
    OperatorSuite, OperatorSuiteError,
};
pub use plan::{DaemonPlan, DaemonRole, RoleSpec};
pub use runtime::{Daemon, RoleFactory, RoleRuntime, RunningDaemon};
pub use s3_backup::{S3BackupObjectStore, S3BackupObjectStoreError};

#[cfg(test)]
mod tests;
