//! Deterministic workload placement for Maestro resources.
//!
//! This crate turns scheduler-specific resource views into assignments. It must
//! not depend on another operator, a runtime backend, or a store implementation.

mod address;
mod assignment;
mod clock;
mod model;
mod plan;
mod projection;
mod reconciler;
mod resource;
mod scheduler;

pub use assignment::AssignmentWriteError;
pub use clock::{SystemTimestampClock, TimestampClock};
pub use model::{
    DeploymentGroup, NodeSchedulingState, ScheduleInput, ScheduleNode, SchedulePlan,
    ServiceSchedule, UnhealthySlot, UnschedulableReason, UnschedulableReplica,
};
pub use plan::plan;
pub use reconciler::SchedulerReconciler;
pub use scheduler::{Scheduler, SchedulerError, SchedulerReport, SchedulerSettings};

#[cfg(test)]
mod tests;
