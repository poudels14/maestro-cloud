use std::sync::Arc;

use kernel_store::{
    Compare, ExpectedVersion, Mutation, Store, StoreKey, Transaction, TransactionOutcome,
};

use crate::{ControllerError, LeadershipToken};

/// Store mutation facade that injects and verifies the active leadership fence.
#[derive(Clone)]
pub struct FencedStore {
    store: Arc<dyn Store>,
    leader_key: StoreKey,
    token: LeadershipToken,
}

impl FencedStore {
    /// Binds a store facade to one observed leader-key version.
    pub fn new(store: Arc<dyn Store>, leader_key: StoreKey, token: LeadershipToken) -> Self {
        Self {
            store,
            leader_key,
            token,
        }
    }

    /// Returns the leadership token enforced by every mutation.
    pub fn token(&self) -> &LeadershipToken {
        &self.token
    }

    pub(crate) fn raw_store(&self) -> &dyn Store {
        self.store.as_ref()
    }

    /// Verifies that this facade still owns the active leader key.
    pub async fn verify_leadership(&self) -> Result<(), ControllerError> {
        let active_version = self
            .store
            .get(&self.leader_key)
            .await?
            .map(|leader| leader.version);
        if active_version == Some(self.token.leader_version()) {
            Ok(())
        } else {
            Err(ControllerError::LeadershipLost)
        }
    }

    /// Applies an atomic transaction only while this token still owns leadership.
    ///
    /// Cancellation may leave the full transaction committed, never partially
    /// applied. A stale token is distinguishable from an application compare
    /// conflict so runtimes stop reconciling immediately after leadership loss.
    pub async fn txn(
        &self,
        mut transaction: Transaction,
    ) -> Result<TransactionOutcome, ControllerError> {
        let mutates_leader = transaction.mutations.iter().any(|mutation| match mutation {
            Mutation::Put { key, .. } | Mutation::Delete { key } => key == &self.leader_key,
        });
        if mutates_leader {
            Err(ControllerError::Contract {
                message: "fenced application transactions cannot mutate the leader key".to_string(),
            })
        } else {
            transaction.compares.insert(
                0,
                Compare {
                    key: self.leader_key.clone(),
                    expected: ExpectedVersion::Exact(self.token.leader_version()),
                },
            );
            let outcome = self.store.txn(transaction).await?;
            if outcome == TransactionOutcome::Conflict {
                let active_version = self
                    .store
                    .get(&self.leader_key)
                    .await?
                    .map(|leader| leader.version);
                if active_version == Some(self.token.leader_version()) {
                    Ok(outcome)
                } else {
                    Err(ControllerError::LeadershipLost)
                }
            } else {
                Ok(outcome)
            }
        }
    }
}
