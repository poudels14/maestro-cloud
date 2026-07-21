//! Deterministic workload placement for Maestro resources.
//!
//! This crate turns scheduler-specific resource views into assignments. It must
//! not depend on another operator, a runtime backend, or a store implementation.

mod address;
mod model;
mod plan;

pub use model::{
    DeploymentGroup, NodeSchedulingState, ScheduleInput, ScheduleNode, SchedulePlan,
    ServiceSchedule, UnhealthySlot, UnschedulableReason, UnschedulableReplica,
};
pub use plan::plan;

#[cfg(test)]
mod tests;
