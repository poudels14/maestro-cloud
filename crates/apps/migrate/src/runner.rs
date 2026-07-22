use std::sync::Arc;

use kernel_api::{ResourceKind, ResourceName};
use kernel_store::{
    CasOutcome, ExpectedVersion, Keyspace, PutRequest, Store, StoreError, StoreKey,
};
use serde::{Deserialize, Serialize};

use crate::MigrationPlan;

const MARKER_SCHEMA_VERSION: u32 = 1;

/// Resumable application of one snapshot-bound cutover plan.
pub struct CutoverMigration {
    store: Arc<dyn Store>,
    keyspace: Keyspace,
    migration_id: ResourceName,
}

impl CutoverMigration {
    /// Binds a migration identity to one cluster's canonical keyspace.
    pub fn new(store: Arc<dyn Store>, keyspace: Keyspace, migration_id: ResourceName) -> Self {
        Self {
            store,
            keyspace,
            migration_id,
        }
    }

    /// Converges every resource, then commits the completion marker last.
    pub async fn apply(&self, plan: &MigrationPlan) -> Result<MigrationOutcome, MigrationError> {
        let marker = MigrationMarker::new(&self.migration_id, plan);
        let marker_key = self.keyspace.migration_marker(&self.migration_id);
        if let Some(stored) = self.store.get(&marker_key).await? {
            let existing = decode_marker(&marker_key, &stored.value)?;
            if existing == marker {
                return Ok(MigrationOutcome::AlreadyComplete {
                    resources: plan.writes().len(),
                });
            }
            return Err(MigrationError::MarkerMismatch {
                key: marker_key.to_string(),
                expected_digest: marker.source_sha256,
                actual_digest: existing.source_sha256,
                expected_resources: marker.resources,
                actual_resources: existing.resources,
            });
        }

        let mut written = 0;
        let mut reused = 0;
        for write in plan.writes() {
            let kind = ResourceKind::new(write.kind().as_str()).map_err(|error| {
                MigrationError::InvalidDestinationKind {
                    kind: write.kind().to_string(),
                    message: error.to_string(),
                }
            })?;
            let key = self.keyspace.resource(&kind, write.id());
            match self.write_exact(key, write.value().to_vec()).await? {
                WriteDisposition::Written => written += 1,
                WriteDisposition::Reused => reused += 1,
            }
        }

        let marker_bytes =
            serde_json::to_vec(&marker).map_err(|error| MigrationError::MarkerEncode {
                message: error.to_string(),
            })?;
        self.write_exact(marker_key, marker_bytes).await?;

        Ok(MigrationOutcome::Applied {
            resources: plan.writes().len(),
            written,
            reused,
        })
    }

    async fn write_exact(
        &self,
        key: StoreKey,
        value: Vec<u8>,
    ) -> Result<WriteDisposition, MigrationError> {
        let outcome = self
            .store
            .put_cas(PutRequest {
                key: key.clone(),
                value: value.clone(),
                expected: ExpectedVersion::Missing,
                session: None,
            })
            .await?;
        match outcome {
            CasOutcome::Applied(_) => Ok(WriteDisposition::Written),
            CasOutcome::Conflict { .. } => match self.store.get(&key).await? {
                Some(stored) if stored.value == value => Ok(WriteDisposition::Reused),
                Some(_) => Err(MigrationError::DestinationCollision {
                    key: key.to_string(),
                }),
                None => Err(MigrationError::DestinationChanged {
                    key: key.to_string(),
                }),
            },
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WriteDisposition {
    Written,
    Reused,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct MigrationMarker {
    schema_version: u32,
    migration_id: ResourceName,
    source_sha256: String,
    resources: usize,
}

impl MigrationMarker {
    fn new(migration_id: &ResourceName, plan: &MigrationPlan) -> Self {
        Self {
            schema_version: MARKER_SCHEMA_VERSION,
            migration_id: migration_id.clone(),
            source_sha256: hex::encode(plan.source_digest()),
            resources: plan.writes().len(),
        }
    }
}

fn decode_marker(key: &StoreKey, value: &[u8]) -> Result<MigrationMarker, MigrationError> {
    serde_json::from_slice(value).map_err(|error| MigrationError::MalformedMarker {
        key: key.to_string(),
        message: error.to_string(),
    })
}

/// Observable result of applying a cutover plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MigrationOutcome {
    /// Every destination converged and a completion marker was committed.
    Applied {
        /// Total resource count in the plan.
        resources: usize,
        /// Resources newly created by this invocation.
        written: usize,
        /// Exact resource bytes reused after an earlier partial invocation.
        reused: usize,
    },
    /// An identical snapshot-bound plan had already completed.
    AlreadyComplete {
        /// Total resource count confirmed by the marker.
        resources: usize,
    },
}

/// A cutover plan could not converge without overwriting state.
#[derive(Debug, thiserror::Error)]
pub enum MigrationError {
    /// The destination store operation failed.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// A built-in kind could not be represented by the open key type.
    #[error("invalid destination kind `{kind}`: {message}")]
    InvalidDestinationKind {
        /// Rejected kind.
        kind: String,
        /// Validation detail.
        message: String,
    },
    /// A destination exists with bytes not produced by this plan.
    #[error("destination `{key}` already exists with different bytes")]
    DestinationCollision {
        /// Colliding canonical key.
        key: String,
    },
    /// A conflicting destination disappeared before it could be verified.
    #[error("destination `{key}` changed while verifying a CAS conflict")]
    DestinationChanged {
        /// Unstable canonical key.
        key: String,
    },
    /// A completion marker could not be serialized.
    #[error("could not encode migration completion marker: {message}")]
    MarkerEncode {
        /// Serialization detail.
        message: String,
    },
    /// An existing completion marker is not valid migration metadata.
    #[error("migration marker `{key}` is malformed: {message}")]
    MalformedMarker {
        /// Marker key.
        key: String,
        /// Decoding detail.
        message: String,
    },
    /// The migration identity was already used for a different input plan.
    #[error(
        "migration marker `{key}` binds {actual_resources} resources from {actual_digest}, \
         not {expected_resources} resources from {expected_digest}"
    )]
    MarkerMismatch {
        /// Marker key.
        key: String,
        /// Digest requested by this run.
        expected_digest: String,
        /// Digest stored by the prior run.
        actual_digest: String,
        /// Resource count requested by this run.
        expected_resources: usize,
        /// Resource count stored by the prior run.
        actual_resources: usize,
    },
}
