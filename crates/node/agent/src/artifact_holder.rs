use std::sync::Arc;

use kernel_api::{ClusterId, InvalidIdentifier, NodeId, ResourceName};
use kernel_store::{
    Keyspace, Mutation, SessionBinding, SessionId, Store, StoreError, StoredValue, Transaction,
};
use runtime::ArtifactDigest;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const MAX_HOLDER_BYTES: usize = 4 * 1_024;

/// One live node that can export an immutable runtime artifact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ArtifactHolder {
    /// Immutable runtime digest advertised by the node.
    pub digest: ArtifactDigest,
    /// Live node owning the local artifact copy.
    pub node_id: NodeId,
}

/// Session-bound forward and reverse artifact-holder index.
pub struct ArtifactHolderRegistry {
    store: Arc<dyn Store>,
    keyspace: Keyspace,
    node_id: NodeId,
    session_id: SessionId,
}

impl ArtifactHolderRegistry {
    /// Binds artifact advertisements to the caller's active node-liveness session.
    pub fn new(
        store: Arc<dyn Store>,
        cluster_id: &ClusterId,
        node_id: NodeId,
        session_id: SessionId,
    ) -> Self {
        Self {
            store,
            keyspace: Keyspace::new(cluster_id),
            node_id,
            session_id,
        }
    }

    /// Atomically publishes both digest and node lookup directions.
    pub async fn publish(
        &self,
        digest: &ArtifactDigest,
    ) -> Result<(), ArtifactHolderRegistryError> {
        let artifact_id = artifact_id(digest)?;
        let value = encode(&ArtifactHolder {
            digest: digest.clone(),
            node_id: self.node_id.clone(),
        })?;
        self.store
            .txn(Transaction {
                compares: Vec::new(),
                mutations: vec![
                    Mutation::Put {
                        key: self.keyspace.artifact_holder(&artifact_id, &self.node_id),
                        value: value.clone(),
                        session: Some(SessionBinding {
                            session_id: self.session_id,
                        }),
                    },
                    Mutation::Put {
                        key: self
                            .keyspace
                            .node_artifact_holder(&self.node_id, &artifact_id),
                        value,
                        session: Some(SessionBinding {
                            session_id: self.session_id,
                        }),
                    },
                ],
            })
            .await?;
        Ok(())
    }

    /// Atomically removes this node's forward and reverse advertisements.
    pub async fn remove(&self, digest: &ArtifactDigest) -> Result<(), ArtifactHolderRegistryError> {
        let artifact_id = artifact_id(digest)?;
        self.store
            .txn(Transaction {
                compares: Vec::new(),
                mutations: vec![
                    Mutation::Delete {
                        key: self.keyspace.artifact_holder(&artifact_id, &self.node_id),
                    },
                    Mutation::Delete {
                        key: self
                            .keyspace
                            .node_artifact_holder(&self.node_id, &artifact_id),
                    },
                ],
            })
            .await?;
        Ok(())
    }

    /// Lists live holders for one exact digest in stable node order.
    pub async fn holders(
        &self,
        digest: &ArtifactDigest,
    ) -> Result<Vec<ArtifactHolder>, ArtifactHolderRegistryError> {
        let artifact_id = artifact_id(digest)?;
        let values = self
            .store
            .list(&self.keyspace.artifact_holders(&artifact_id))
            .await?
            .values;
        let mut holders = values
            .iter()
            .map(|stored| self.decode_digest_holder(stored, digest, &artifact_id))
            .collect::<Result<Vec<_>, _>>()?;
        holders.sort_by(|left, right| left.node_id.cmp(&right.node_id));
        Ok(holders)
    }

    /// Lists this node's live advertisements in stable digest order.
    pub async fn local_holders(&self) -> Result<Vec<ArtifactHolder>, ArtifactHolderRegistryError> {
        let values = self
            .store
            .list(&self.keyspace.node_artifact_holders(&self.node_id))
            .await?
            .values;
        let mut holders = values
            .iter()
            .map(|stored| self.decode_node_holder(stored))
            .collect::<Result<Vec<_>, _>>()?;
        holders.sort_by(|left, right| left.digest.cmp(&right.digest));
        Ok(holders)
    }

    fn decode_digest_holder(
        &self,
        stored: &StoredValue,
        digest: &ArtifactDigest,
        artifact_id: &ResourceName,
    ) -> Result<ArtifactHolder, ArtifactHolderRegistryError> {
        let holder = decode(stored)?;
        let expected = self.keyspace.artifact_holder(artifact_id, &holder.node_id);
        if &holder.digest != digest || stored.key != expected {
            return Err(ArtifactHolderRegistryError::IndexMismatch {
                key: stored.key.to_string(),
            });
        }
        Ok(holder)
    }

    fn decode_node_holder(
        &self,
        stored: &StoredValue,
    ) -> Result<ArtifactHolder, ArtifactHolderRegistryError> {
        let holder = decode(stored)?;
        let artifact_id = artifact_id(&holder.digest)?;
        let expected = self
            .keyspace
            .node_artifact_holder(&self.node_id, &artifact_id);
        if holder.node_id != self.node_id || stored.key != expected {
            return Err(ArtifactHolderRegistryError::IndexMismatch {
                key: stored.key.to_string(),
            });
        }
        Ok(holder)
    }
}

fn artifact_id(digest: &ArtifactDigest) -> Result<ResourceName, InvalidIdentifier> {
    ResourceName::new(format!("{:x}", Sha256::digest(digest.as_str().as_bytes())))
}

fn encode(holder: &ArtifactHolder) -> Result<Vec<u8>, ArtifactHolderRegistryError> {
    let value = serde_json::to_vec(holder).map_err(|error| {
        ArtifactHolderRegistryError::MalformedHolder {
            message: error.to_string(),
        }
    })?;
    if value.len() > MAX_HOLDER_BYTES {
        Err(ArtifactHolderRegistryError::MalformedHolder {
            message: format!("document exceeds {MAX_HOLDER_BYTES} bytes"),
        })
    } else {
        Ok(value)
    }
}

fn decode(stored: &StoredValue) -> Result<ArtifactHolder, ArtifactHolderRegistryError> {
    if stored.value.len() > MAX_HOLDER_BYTES {
        return Err(ArtifactHolderRegistryError::MalformedHolder {
            message: format!("document exceeds {MAX_HOLDER_BYTES} bytes"),
        });
    }
    serde_json::from_slice(&stored.value).map_err(|error| {
        ArtifactHolderRegistryError::MalformedHolder {
            message: error.to_string(),
        }
    })
}

/// Artifact-holder publication, validation, or store failure.
#[derive(Debug, thiserror::Error)]
pub enum ArtifactHolderRegistryError {
    /// Digest hashing did not produce a valid internal identifier.
    #[error(transparent)]
    InvalidIdentifier(#[from] InvalidIdentifier),
    /// The durable holder document was malformed or exceeded its size bound.
    #[error("malformed artifact holder: {message}")]
    MalformedHolder { message: String },
    /// A holder payload disagreed with the exact forward or reverse index key.
    #[error("artifact holder at `{key}` disagrees with its index key")]
    IndexMismatch { key: String },
    /// Store access or active session validation failed.
    #[error(transparent)]
    Store(#[from] StoreError),
}

impl ArtifactHolderRegistryError {
    pub(crate) fn is_malformed_store_data(&self) -> bool {
        matches!(
            self,
            Self::MalformedHolder { .. } | Self::IndexMismatch { .. }
        )
    }
}
