//! Detached host-process supervision primitives.
//!
//! This kernel crate owns process-group spawning, durable PID identity, inspection, reaping, and
//! signaling. It must not depend on runtimes, node agents, operators, cluster formation, or apps.

mod error;
#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "linux")]
mod manager;
mod spec;

pub use error::SupervisorError;
#[cfg(target_os = "linux")]
pub use manager::ProcessSupervisor;
pub use spec::{
    EnvironmentInheritance, ProcessCommand, ProcessEnvironment, ProcessExit, ProcessHandle,
    ProcessLogFiles, ProcessSignal, ProcessSpec, ProcessStatus, ProcessUser,
};

#[cfg(test)]
mod tests;
