use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use kernel_store::{
    CasOutcome, DeleteRequest, ExpectedVersion, PutRequest, Session, SessionBinding, Store,
    StoreError, StoreKey,
};

use crate::{
    ControllerError, LeaderElector, LeaderIdentity, LeadershipLease, LeadershipObservation,
    LeadershipToken,
};

/// Leader election built entirely on CAS and TTL session store primitives.
pub struct StoreLeaderElector {
    store: Arc<dyn Store>,
    leader_key: StoreKey,
}

impl StoreLeaderElector {
    /// Creates an elector for one cluster-scoped leader key.
    pub fn new(store: Arc<dyn Store>, leader_key: StoreKey) -> Self {
        Self { store, leader_key }
    }
}

#[async_trait]
impl LeaderElector for StoreLeaderElector {
    async fn campaign(
        &self,
        identity: LeaderIdentity,
        ttl: Duration,
    ) -> Result<Option<Box<dyn LeadershipLease>>, ControllerError> {
        let session = self.store.session(ttl).await?;
        let value = serde_json::to_vec(&identity).map_err(|error| ControllerError::Contract {
            message: format!("leader identity could not be encoded: {error}"),
        })?;
        let outcome = self
            .store
            .put_cas(PutRequest {
                key: self.leader_key.clone(),
                value,
                expected: ExpectedVersion::Missing,
                session: Some(SessionBinding {
                    session_id: session.id(),
                }),
            })
            .await?;
        match outcome {
            CasOutcome::Applied(stored) => {
                let token = LeadershipToken::from_campaign(identity, session.id(), stored.version);
                Ok(Some(Box::new(StoreLeadershipLease {
                    store: self.store.clone(),
                    leader_key: self.leader_key.clone(),
                    session,
                    token,
                })))
            }
            CasOutcome::Conflict { .. } => {
                session.close().await?;
                Ok(None)
            }
        }
    }

    async fn observe(&self) -> Result<LeadershipObservation, ControllerError> {
        if let Some(stored) = self.store.get(&self.leader_key).await? {
            let identity = serde_json::from_slice(&stored.value).map_err(|error| {
                ControllerError::MalformedLeader {
                    message: error.to_string(),
                }
            })?;
            Ok(LeadershipObservation::Leader(identity))
        } else {
            Ok(LeadershipObservation::Vacant)
        }
    }
}

struct StoreLeadershipLease {
    store: Arc<dyn Store>,
    leader_key: StoreKey,
    session: Box<dyn Session>,
    token: LeadershipToken,
}

#[async_trait]
impl LeadershipLease for StoreLeadershipLease {
    fn token(&self) -> &LeadershipToken {
        &self.token
    }

    async fn keep_alive(&self) -> Result<(), ControllerError> {
        self.session.keep_alive().await?;
        Ok(())
    }

    async fn resign(&self) -> Result<(), ControllerError> {
        let deleted = self
            .store
            .delete_cas(DeleteRequest {
                key: self.leader_key.clone(),
                expected: self.token.leader_version(),
            })
            .await?;
        match deleted {
            CasOutcome::Applied(_) => match self.session.close().await {
                Ok(()) | Err(StoreError::SessionExpired { .. }) => Ok(()),
                Err(error) => Err(error.into()),
            },
            CasOutcome::Conflict { .. } => Err(ControllerError::LeadershipLost),
        }
    }
}
