use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use kernel_store::{
    CasOutcome, DeleteRequest, ListResult, PutRequest, Session, Store, StoreError, StoreKey,
    StorePrefix, StoreWatch, StoredValue, Transaction, TransactionOutcome, Version, WatchStart,
};

pub(super) struct FailFirstListStore {
    inner: Arc<dyn Store>,
    fail_next_list: AtomicBool,
    list_calls: AtomicUsize,
}

impl FailFirstListStore {
    pub(super) fn new(inner: Arc<dyn Store>) -> Self {
        Self {
            inner,
            fail_next_list: AtomicBool::new(true),
            list_calls: AtomicUsize::new(0),
        }
    }

    pub(super) fn list_calls(&self) -> usize {
        self.list_calls.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl Store for FailFirstListStore {
    async fn get(&self, key: &StoreKey) -> Result<Option<StoredValue>, StoreError> {
        self.inner.get(key).await
    }

    async fn list(&self, prefix: &StorePrefix) -> Result<ListResult, StoreError> {
        self.list_calls.fetch_add(1, Ordering::SeqCst);
        if self.fail_next_list.swap(false, Ordering::SeqCst) {
            return Err(StoreError::Unavailable {
                message: "injected controller list outage".to_owned(),
            });
        }
        self.inner.list(prefix).await
    }

    async fn put_cas(&self, request: PutRequest) -> Result<CasOutcome<StoredValue>, StoreError> {
        self.inner.put_cas(request).await
    }

    async fn delete_cas(&self, request: DeleteRequest) -> Result<CasOutcome<Version>, StoreError> {
        self.inner.delete_cas(request).await
    }

    async fn txn(&self, transaction: Transaction) -> Result<TransactionOutcome, StoreError> {
        self.inner.txn(transaction).await
    }

    fn watch(
        &self,
        prefix: StorePrefix,
        start: WatchStart,
    ) -> Result<Box<dyn StoreWatch>, StoreError> {
        self.inner.watch(prefix, start)
    }

    async fn session(&self, ttl: Duration) -> Result<Box<dyn Session>, StoreError> {
        self.inner.session(ttl).await
    }
}
