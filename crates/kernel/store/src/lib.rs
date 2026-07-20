//! Backend-neutral persistence contracts for Maestro cluster state.
//!
//! This crate owns all `/maestro/` key construction and storage semantics. It
//! may depend on `kernel-api`, but must not depend on controllers, nodes,
//! operators, runtimes, observability, cluster provisioning, or applications.

mod clock;
mod encryption;
mod error;
mod etcd;
mod key;
mod memory;
mod model;
mod store;

pub use clock::{Clock, MonotonicTime, TokioClock};
pub use encryption::{EncryptedValue, EncryptionError, EncryptionKey, derive_key, open, seal};
pub use error::StoreError;
pub use etcd::{EtcdStore, EtcdTlsConfig};
pub use key::{Keyspace, StoreKey, StorePrefix};
pub use memory::InMemoryStore;
pub use model::{
    CasOutcome, Compare, DeleteRequest, ExpectedVersion, ListResult, Mutation, MutationResult,
    PutRequest, SessionBinding, SessionId, StoredValue, Transaction, TransactionOutcome, Version,
    WatchCursor, WatchEvent, WatchEventKind, WatchStart,
};
pub use store::{Session, Store, StoreWatch};

#[cfg(feature = "test-util")]
pub mod conformance;

#[cfg(test)]
mod tests;
