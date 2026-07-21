//! Artifact build reconciliation for Maestro Build resources.
//!
//! The operator persists source resolution before invoking a potentially long
//! artifact build. Both external boundaries are injected so controller state
//! transitions remain deterministic and independently testable.

mod reconciler;
mod source;
mod writer;

pub use reconciler::BuildReconciler;
pub use source::{BuildSourceError, BuildSourceProvider, PreparedBuildSource};

#[cfg(test)]
mod tests;
