use std::time::Duration;

use async_trait::async_trait;
use kernel_api::{NodeId, NodeInstanceId};
use kernel_store::{SessionId, Version};
use serde::{Deserialize, Serialize};

use crate::ControllerError;

/// Stable node and process identity participating in leader election.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LeaderIdentity {
    /// Node running the controller candidate.
    pub node_id: NodeId,
    /// Exact daemon process participating in the campaign.
    pub instance_id: NodeInstanceId,
}

/// Unforgeable write fence established by a successful leader-key CAS.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeadershipToken {
    identity: LeaderIdentity,
    session_id: SessionId,
    leader_version: Version,
}

impl LeadershipToken {
    /// Creates a token from the exact session and key version returned by a successful campaign.
    pub fn from_campaign(
        identity: LeaderIdentity,
        session_id: SessionId,
        leader_version: Version,
    ) -> Self {
        Self {
            identity,
            session_id,
            leader_version,
        }
    }

    /// Returns the candidate identity that owns this fence.
    pub fn identity(&self) -> &LeaderIdentity {
        &self.identity
    }

    /// Returns the TTL session that bounds leadership lifetime.
    pub fn session_id(&self) -> SessionId {
        self.session_id
    }

    pub(crate) fn leader_version(&self) -> Version {
        self.leader_version
    }
}

/// Current result of observing one leader campaign key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeadershipObservation {
    /// A live candidate owns leadership.
    Leader(LeaderIdentity),
    /// No live candidate currently owns the leader key.
    Vacant,
}

/// Active leadership lease retained until resignation or session expiry.
#[async_trait]
pub trait LeadershipLease: Send + Sync {
    /// Returns the fencing token produced by the successful campaign.
    fn token(&self) -> &LeadershipToken;

    /// Renews the TTL session that owns the leader key.
    ///
    /// Cancellation leaves the prior acknowledged session expiry intact.
    async fn keep_alive(&self) -> Result<(), ControllerError>;

    /// Releases leadership and removes the leader key when still fenced.
    ///
    /// Cancellation may leave leadership active until session expiry; stale
    /// mutations remain rejected by the key-version fence.
    async fn resign(&self) -> Result<(), ControllerError>;
}

/// Backend-neutral leader campaign and observation port.
#[async_trait]
pub trait LeaderElector: Send + Sync {
    /// Attempts to own the leader key under a new TTL session.
    ///
    /// `None` means another live candidate already owns the key.
    async fn campaign(
        &self,
        identity: LeaderIdentity,
        ttl: Duration,
    ) -> Result<Option<Box<dyn LeadershipLease>>, ControllerError>;

    /// Reads the current live leader without campaigning.
    async fn observe(&self) -> Result<LeadershipObservation, ControllerError>;
}
