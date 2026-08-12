//! Backend-neutral persistence contracts for Maestro cluster state.
//!
//! This crate owns all `/maestro/` key construction and storage semantics. It
//! may depend on `kernel-api`, but must not depend on controllers, nodes,
//! operators, runtimes, observability, cluster provisioning, or applications.

mod clock;
mod encryption;
mod error;
mod etcd;
mod etcd_session;
mod etcd_support;
mod etcd_value;
mod etcd_watch;
mod key;
mod memory;
mod model;
#[cfg(feature = "test-util")]
mod snapshot;
#[cfg(feature = "test-util")]
mod snapshot_normalize;
mod store;

pub use clock::{Clock, MonotonicTime, TokioClock};
pub use encryption::{
    EncryptedValue, EncryptionError, EncryptionKey, derive_key, derive_key_with_context, open,
    open_with_context, seal, seal_with_context,
};
pub use error::StoreError;
pub use etcd::{EtcdStore, EtcdTlsConfig};
pub use key::{Keyspace, StoreKey, StorePrefix};
pub use memory::InMemoryStore;
pub use model::{
    CasOutcome, Compare, DeleteRequest, ExpectedVersion, ListResult, Mutation, MutationResult,
    PutRequest, SessionBinding, SessionId, StoredValue, Transaction, TransactionOutcome, Version,
    WatchCursor, WatchEvent, WatchEventKind, WatchStart,
};
#[cfg(feature = "test-util")]
pub use snapshot::{ClusterSnapshot, SnapshotError, StoreSnapshotExt, UnregisteredResource};
#[cfg(feature = "test-util")]
pub use snapshot_normalize::{NormalizedClusterSnapshot, NormalizedUnregisteredResource};
pub use store::{Session, Store, StoreWatch};

/// Maximum number of compares and mutations accepted in one store transaction.
///
/// This matches the etcd cluster contract and is also enforced by the in-memory
/// implementation so tests exercise production transaction sizing.
pub const TRANSACTION_OPERATION_LIMIT: usize = 128;

#[cfg(feature = "test-util")]
pub mod conformance;

#[cfg(test)]
mod tests;
