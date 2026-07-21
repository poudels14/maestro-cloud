//! Maestro daemon composition root and role lifetime ownership.
//!
//! This crate selects controller and agent roles from validated cluster
//! topology. Production adapters implement the role factory; the composition
//! runtime guarantees ordered startup, reverse shutdown, and rollback.

mod agent_lifecycle;
mod agent_role;
mod control_plane;
mod error;
mod launch;
mod leadership;
mod operators;
mod plan;
mod runtime;
mod workload_agents;

pub use control_plane::{
    AgentStore, DaemonRoleDependencies, DaemonRoleFactory, DaemonRoleSettings, LeaderWorkload,
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

#[cfg(test)]
mod tests;
