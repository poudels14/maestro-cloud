use crate::{StoreKey, StorePrefix};

/// Opaque version of one key used for compare-and-swap.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Version(pub(crate) u64);

/// Opaque ordered watch position used to resume an event stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WatchCursor(pub(crate) u64);

/// Opaque identity of one TTL-bound backend session.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SessionId(pub(crate) u64);

/// Version condition required by a CAS mutation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpectedVersion {
    /// The key must not exist.
    Missing,
    /// The key must exist at exactly this observed version.
    Exact(Version),
}

/// Optional TTL session attached to a written key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionBinding {
    /// Active session that owns the key lifetime.
    pub session_id: SessionId,
}

/// Linearizable value and version read from the backend.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredValue {
    /// Exact canonical key.
    pub key: StoreKey,
    /// Opaque application bytes.
    pub value: Vec<u8>,
    /// Version used by future CAS operations.
    pub version: Version,
}

/// Result of a linearizable prefix list and its watch resume point.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListResult {
    /// Values sorted lexicographically by key.
    pub values: Vec<StoredValue>,
    /// Cursor immediately after the returned snapshot.
    pub cursor: WatchCursor,
}

/// One conditional put operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PutRequest {
    /// Exact key to create or replace.
    pub key: StoreKey,
    /// Opaque application bytes.
    pub value: Vec<u8>,
    /// Required current key version.
    pub expected: ExpectedVersion,
    /// Optional session that deletes the key automatically on expiry.
    pub session: Option<SessionBinding>,
}

/// One conditional delete operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteRequest {
    /// Exact key to remove.
    pub key: StoreKey,
    /// Required current key version.
    pub expected: Version,
}

/// Applied value or observed conflict from a CAS operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CasOutcome<Value> {
    /// The mutation applied atomically.
    Applied(Value),
    /// The key did not match the expected version.
    Conflict {
        /// Current version, or `None` when the key does not exist.
        actual: Option<Version>,
    },
}

/// One version predicate in a multi-key transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Compare {
    /// Key whose version is compared.
    pub key: StoreKey,
    /// Required current version.
    pub expected: ExpectedVersion,
}

/// One mutation in an atomic multi-key transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Mutation {
    /// Create or replace a value after every compare succeeds.
    Put {
        /// Exact key to write.
        key: StoreKey,
        /// Opaque application bytes.
        value: Vec<u8>,
        /// Optional session that owns the key lifetime.
        session: Option<SessionBinding>,
    },
    /// Delete a value after every compare succeeds.
    Delete {
        /// Exact key to remove.
        key: StoreKey,
    },
}

/// Atomic conditional batch evaluated at one linearizable revision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Transaction {
    /// Predicates that must all match before any mutation applies.
    pub compares: Vec<Compare>,
    /// Ordered mutations committed atomically.
    pub mutations: Vec<Mutation>,
}

/// Per-mutation evidence returned by an applied transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MutationResult {
    /// Value written with its new key version.
    Put(StoredValue),
    /// Key removed at its previous version.
    Deleted {
        /// Removed key.
        key: StoreKey,
        /// Version removed by the transaction.
        previous_version: Version,
    },
    /// A requested delete found no key and made no change.
    DeleteMissing {
        /// Exact key that was already absent.
        key: StoreKey,
    },
}

/// Atomic transaction result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransactionOutcome {
    /// Every compare matched and all mutations committed.
    Applied {
        /// Results in mutation order.
        results: Vec<MutationResult>,
        /// Cursor immediately after the atomic batch.
        cursor: WatchCursor,
    },
    /// At least one compare failed and no mutation applied.
    Conflict,
}

/// Starting position for a new prefix watch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WatchStart {
    /// Observe changes committed after watch creation.
    Current,
    /// Resume strictly after an earlier list or watch cursor.
    After(WatchCursor),
}

/// Kind of one ordered watch event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WatchEventKind {
    /// A key was created or replaced.
    Put(StoredValue),
    /// A key was deleted.
    Delete {
        /// Exact key removed from the backend.
        key: StoreKey,
        /// Version that existed immediately before deletion.
        previous_version: Version,
    },
}

/// One at-least-once event ordered with respect to the changed key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WatchEvent {
    /// Prefix originally requested by the watcher.
    pub prefix: StorePrefix,
    /// Resumable backend position after this event.
    pub cursor: WatchCursor,
    /// Created, updated, or deleted value evidence.
    pub kind: WatchEventKind,
}
