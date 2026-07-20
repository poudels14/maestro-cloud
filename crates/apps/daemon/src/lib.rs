//! Maestro daemon composition root and role lifetime ownership.
//!
//! This crate selects controller and agent roles from validated cluster
//! topology. Production adapters implement the role factory; the composition
//! runtime guarantees ordered startup, reverse shutdown, and rollback.

mod error;
mod plan;
mod runtime;

pub use error::{DaemonError, RoleError, RoleFailure};
pub use plan::{DaemonPlan, DaemonRole, RoleSpec};
pub use runtime::{Daemon, RoleFactory, RoleRuntime, RunningDaemon};

#[cfg(test)]
mod tests;
