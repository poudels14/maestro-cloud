//! One-shot, resumable migration from legacy Maestro state into typed resources.

mod plan;
mod runner;
mod snapshot;

pub use plan::{MigrationPlan, MigrationWrite, PlanError};
pub use runner::{CutoverMigration, MigrationError, MigrationOutcome};
pub use snapshot::{LegacyEntry, LegacySnapshot, SnapshotError};

#[cfg(test)]
mod tests;
