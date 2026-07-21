//! Reconciliation, leadership, and write-safety kernel for Maestro operators.
//!
//! This crate depends only on kernel API and store contracts. It must not
//! depend on nodes, runtimes, operators, observability, cluster provisioning,
//! or applications.

mod backoff;
mod dedup;
mod elector;
mod error;
mod fencing;
#[cfg(feature = "test-util")]
mod journal;
mod leadership;
mod queue;
mod reconciler;
mod runtime;
mod timestamp;

pub use backoff::{Backoff, BackoffError};
pub use dedup::{DedupOutcome, RequestFingerprint};
pub use elector::StoreLeaderElector;
pub use error::ControllerError;
pub use fencing::FencedStore;
#[cfg(feature = "test-util")]
pub use journal::{JournalAction, JournalEntry, ReconcileJournal};
pub use leadership::{
    LeaderElector, LeaderIdentity, LeadershipLease, LeadershipObservation, LeadershipToken,
};
pub use reconciler::{Action, ReconcileContext, ReconcileError, Reconciler};
pub use runtime::{ControllerRuntime, RuntimeConfig, RuntimeConfigError};
pub use timestamp::{SystemTimestampClock, TimestampClock};

#[cfg(test)]
mod tests;
