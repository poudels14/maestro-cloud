use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use cluster::{
    MemberActivation, StoreJoinTicket, StoreMember, StoreProvider, StoreProviderError,
    StoreRecovery, StoreRecoveryPermit, StoreRuntime, StoreShutdown, StoreStartMode,
};
use kernel_api::NodeId;
use kernel_store::{InMemoryStore, Store};

pub(super) struct FakeProvider {
    store: Arc<InMemoryStore>,
    shutdowns: Arc<Mutex<u32>>,
}

impl FakeProvider {
    pub(super) fn new(store: Arc<InMemoryStore>, shutdowns: Arc<Mutex<u32>>) -> Self {
        Self { store, shutdowns }
    }
}

#[async_trait]
impl StoreProvider for FakeProvider {
    async fn start(
        &self,
        _mode: StoreStartMode,
    ) -> Result<Box<dyn StoreRuntime>, StoreProviderError> {
        Ok(Box::new(FakeStoreRuntime {
            store: self.store.clone(),
            shutdowns: self.shutdowns.clone(),
        }))
    }

    async fn stage_member(
        &self,
        _member: StoreMember,
    ) -> Result<(StoreJoinTicket, MemberActivation), StoreProviderError> {
        Err(unsupported())
    }

    async fn activate_member(
        &self,
        _ticket: &StoreJoinTicket,
    ) -> Result<MemberActivation, StoreProviderError> {
        Err(unsupported())
    }

    async fn remove_member(&self, _node_id: &NodeId) -> Result<(), StoreProviderError> {
        Err(unsupported())
    }

    async fn recover(
        &self,
        _permit: StoreRecoveryPermit,
    ) -> Result<StoreRecovery, StoreProviderError> {
        Err(unsupported())
    }
}

struct FakeStoreRuntime {
    store: Arc<InMemoryStore>,
    shutdowns: Arc<Mutex<u32>>,
}

#[async_trait]
impl StoreRuntime for FakeStoreRuntime {
    fn store(&self) -> Arc<dyn Store> {
        self.store.clone()
    }

    async fn shutdown(self: Box<Self>, _request: StoreShutdown) -> Result<(), StoreProviderError> {
        let mut shutdowns = self
            .shutdowns
            .lock()
            .map_err(|_| StoreProviderError::Lifecycle {
                reason: "shutdown count lock poisoned".to_owned(),
            })?;
        *shutdowns = shutdowns.saturating_add(1);
        Ok(())
    }
}

fn unsupported() -> StoreProviderError {
    StoreProviderError::InvalidConfiguration {
        reason: "operation is not used by the daemon role test".to_owned(),
    }
}
