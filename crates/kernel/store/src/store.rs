use std::time::Duration;

use async_trait::async_trait;

use crate::{
    CasOutcome, DeleteRequest, ListResult, PutRequest, SessionId, StoreError, StoreKey,
    StorePrefix, StoredValue, Transaction, TransactionOutcome, Version, WatchEvent, WatchStart,
};

/// Ordered, resumable, at-least-once change stream for one prefix.
#[async_trait]
pub trait StoreWatch: Send {
    /// Waits for the next ordered event.
    ///
    /// Canceling this future does not advance the watch cursor. A
    /// [`StoreError::CursorExpired`] result requires a full list and new watch.
    async fn next(&mut self) -> Result<WatchEvent, StoreError>;
}

/// TTL-bound liveness handle whose attached keys disappear after expiry.
#[async_trait]
pub trait Session: Send + Sync {
    /// Returns the backend-opaque session identity used for key bindings.
    fn id(&self) -> SessionId;

    /// Returns the requested session lifetime after the last successful keepalive.
    fn ttl(&self) -> Duration;

    /// Extends the session lifetime by its configured TTL.
    ///
    /// Canceling the future leaves the previously acknowledged expiry intact.
    async fn keep_alive(&self) -> Result<(), StoreError>;

    /// Closes the session and removes attached keys.
    ///
    /// Canceling close may leave the session alive until its TTL expires; the
    /// operation is safe to retry with another handle to the same session.
    async fn close(&self) -> Result<(), StoreError>;
}

/// Backend-pluggable linearizable store contract used by every controller.
#[async_trait]
pub trait Store: Send + Sync {
    /// Reads one key at a linearizable backend revision.
    async fn get(&self, key: &StoreKey) -> Result<Option<StoredValue>, StoreError>;

    /// Lists a prefix at one linearizable revision in key order.
    async fn list(&self, prefix: &StorePrefix) -> Result<ListResult, StoreError>;

    /// Conditionally creates or replaces one key.
    ///
    /// The mutation is atomic. Canceling before a result may still leave it
    /// committed, so callers converge by reading and comparing desired state.
    async fn put_cas(&self, request: PutRequest) -> Result<CasOutcome<StoredValue>, StoreError>;

    /// Conditionally deletes one key and returns its removed version.
    ///
    /// The mutation is atomic and has the same cancellation semantics as put.
    async fn delete_cas(&self, request: DeleteRequest) -> Result<CasOutcome<Version>, StoreError>;

    /// Applies a multi-key conditional batch atomically.
    ///
    /// Canceling before a result may still leave the whole batch committed;
    /// partial mutation is never observable.
    async fn txn(&self, transaction: Transaction) -> Result<TransactionOutcome, StoreError>;

    /// Creates a resumable, at-least-once prefix watch.
    fn watch(
        &self,
        prefix: StorePrefix,
        start: WatchStart,
    ) -> Result<Box<dyn StoreWatch>, StoreError>;

    /// Creates a TTL-bound session.
    ///
    /// The returned handle owns no detached tasks; backend keepalive work is
    /// driven by explicit calls and canceled when the backend connection ends.
    async fn session(&self, ttl: Duration) -> Result<Box<dyn Session>, StoreError>;
}
