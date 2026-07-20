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
mod leadership;
mod reconciler;
mod runtime;

pub use backoff::{Backoff, BackoffError};
pub use dedup::{DedupOutcome, RequestFingerprint};
pub use elector::StoreLeaderElector;
pub use error::ControllerError;
pub use fencing::FencedStore;
pub use leadership::{
    LeaderElector, LeaderIdentity, LeadershipLease, LeadershipObservation, LeadershipToken,
};
pub use reconciler::{Action, ReconcileContext, ReconcileError, Reconciler};
pub use runtime::{ControllerRuntime, RuntimeConfig, RuntimeConfigError};

#[cfg(test)]
mod tests;
