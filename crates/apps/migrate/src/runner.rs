use std::collections::BTreeMap;
use std::fmt::Display;
use std::future::Future;
use std::sync::Arc;

use kernel_api::{ClusterId, ResourceKind, ResourceName};
use kernel_store::{
    CasOutcome, ExpectedVersion, Keyspace, PutRequest, Store, StoreError, StoreKey,
};
use serde::{Deserialize, Serialize};

use crate::MigrationPlan;

const MARKER_SCHEMA_VERSION: u32 = 2;

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
        self.apply_guarded(plan, || async { Ok::<(), std::convert::Infallible>(()) })
            .await
    }

    /// Verifies the completion marker and every destination against one reviewed plan.
    pub async fn verify(
        &self,
        plan: &MigrationPlan,
    ) -> Result<MigrationVerification, MigrationError> {
        self.validate_destination(plan)?;
        let expected_marker = MigrationMarker::new(&self.migration_id, plan);
        let marker_key = self.keyspace.migration_marker(&self.migration_id);
        let mut stored = self.destination_snapshot().await?;
        let marker_value =
            stored
                .remove(&marker_key)
                .ok_or_else(|| MigrationError::MissingMarker {
                    key: marker_key.to_string(),
                })?;
        let actual_marker = decode_marker(&marker_key, &marker_value)?;
        if actual_marker != expected_marker {
            return Err(marker_mismatch(
                marker_key.to_string(),
                &expected_marker,
                &actual_marker,
            ));
        }

        for (key, expected) in self.plan_destinations(plan)? {
            match stored.remove(&key) {
                Some(actual) if actual == expected => {}
                Some(_) => {
                    return Err(MigrationError::DestinationMismatch {
                        key: key.to_string(),
                    });
                }
                None => {
                    return Err(MigrationError::MissingDestination {
                        key: key.to_string(),
                    });
                }
            }
        }
        if let Some(key) = stored.keys().next() {
            return Err(MigrationError::UnexpectedDestination {
                key: key.to_string(),
            });
        }

        Ok(MigrationVerification::Verified {
            resources: plan.writes().len(),
            request_claims: plan.request_claims().len(),
            source_sha256: hex::encode(plan.source_digest()),
        })
    }

    /// Converges every destination and rechecks the source immediately before completion.
    pub async fn apply_guarded<Check, CheckFuture, CheckError>(
        &self,
        plan: &MigrationPlan,
        source_check: Check,
    ) -> Result<MigrationOutcome, MigrationError>
    where
        Check: FnOnce() -> CheckFuture,
        CheckFuture: Future<Output = Result<(), CheckError>>,
        CheckError: Display,
    {
        self.validate_destination(plan)?;
        let marker = MigrationMarker::new(&self.migration_id, plan);
        let marker_key = self.keyspace.migration_marker(&self.migration_id);
        if self.store.get(&marker_key).await?.is_some() {
            self.verify(plan).await?;
            source_check()
                .await
                .map_err(|error| MigrationError::SourceFence {
                    message: error.to_string(),
                })?;
            return Ok(MigrationOutcome::AlreadyComplete {
                resources: plan.writes().len(),
                request_claims: plan.request_claims().len(),
            });
        }
        self.verify_partial_destination(plan).await?;

        let mut written = 0;
        let mut reused = 0;
        for claim in plan.request_claims() {
            let key = self.keyspace.request_claim(claim.request_id());
            match self.write_exact(key, claim.value().to_vec()).await? {
                WriteDisposition::Written => written += 1,
                WriteDisposition::Reused => reused += 1,
            }
        }
        for write in plan.writes() {
            let kind = destination_kind(write.kind().as_str())?;
            let key = self.keyspace.resource(&kind, write.id());
            match self.write_exact(key, write.value().to_vec()).await? {
                WriteDisposition::Written => written += 1,
                WriteDisposition::Reused => reused += 1,
            }
        }
        source_check()
            .await
            .map_err(|error| MigrationError::SourceFence {
                message: error.to_string(),
            })?;

        let marker_bytes =
            serde_json::to_vec(&marker).map_err(|error| MigrationError::MarkerEncode {
                message: error.to_string(),
            })?;
        self.write_exact(marker_key, marker_bytes).await?;
        self.verify(plan).await?;

        Ok(MigrationOutcome::Applied {
            resources: plan.writes().len(),
            request_claims: plan.request_claims().len(),
            written,
            reused,
        })
    }

    fn validate_destination(&self, plan: &MigrationPlan) -> Result<(), MigrationError> {
        let destination_prefix = self.keyspace.cluster().to_string();
        let expected_prefix = format!("/maestro/clusters/{}/", plan.cluster_id());
        if destination_prefix == expected_prefix {
            Ok(())
        } else {
            Err(MigrationError::DestinationClusterMismatch {
                plan_cluster_id: plan.cluster_id().clone(),
                destination_prefix,
            })
        }
    }

    fn plan_destinations<'a>(
        &self,
        plan: &'a MigrationPlan,
    ) -> Result<BTreeMap<StoreKey, &'a [u8]>, MigrationError> {
        let mut destinations = BTreeMap::new();
        for claim in plan.request_claims() {
            destinations.insert(
                self.keyspace.request_claim(claim.request_id()),
                claim.value(),
            );
        }
        for write in plan.writes() {
            let kind = destination_kind(write.kind().as_str())?;
            destinations.insert(self.keyspace.resource(&kind, write.id()), write.value());
        }
        Ok(destinations)
    }

    async fn destination_snapshot(&self) -> Result<BTreeMap<StoreKey, Vec<u8>>, MigrationError> {
        Ok(self
            .store
            .list(&self.keyspace.cluster())
            .await?
            .values
            .into_iter()
            .map(|stored| (stored.key, stored.value))
            .collect())
    }

    async fn verify_partial_destination(&self, plan: &MigrationPlan) -> Result<(), MigrationError> {
        let expected = self.plan_destinations(plan)?;
        for (key, actual) in self.destination_snapshot().await? {
            match expected.get(&key) {
                Some(value) if actual == *value => {}
                Some(_) => {
                    return Err(MigrationError::DestinationCollision {
                        key: key.to_string(),
                    });
                }
                None => {
                    return Err(MigrationError::UnexpectedDestination {
                        key: key.to_string(),
                    });
                }
            }
        }
        Ok(())
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

fn destination_kind(kind: &str) -> Result<ResourceKind, MigrationError> {
    ResourceKind::new(kind).map_err(|error| MigrationError::InvalidDestinationKind {
        kind: kind.to_owned(),
        message: error.to_string(),
    })
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
    cluster_id: ClusterId,
    source_sha256: String,
    resources: usize,
    request_claims: usize,
}

impl MigrationMarker {
    fn new(migration_id: &ResourceName, plan: &MigrationPlan) -> Self {
        Self {
            schema_version: MARKER_SCHEMA_VERSION,
            migration_id: migration_id.clone(),
            cluster_id: plan.cluster_id().clone(),
            source_sha256: hex::encode(plan.source_digest()),
            resources: plan.writes().len(),
            request_claims: plan.request_claims().len(),
        }
    }
}

fn decode_marker(key: &StoreKey, value: &[u8]) -> Result<MigrationMarker, MigrationError> {
    serde_json::from_slice(value).map_err(|error| MigrationError::MalformedMarker {
        key: key.to_string(),
        message: error.to_string(),
    })
}

fn marker_mismatch(
    key: String,
    expected: &MigrationMarker,
    actual: &MigrationMarker,
) -> MigrationError {
    MigrationError::MarkerMismatch {
        key,
        message: format!(
            "expected schema {}, migration {}, cluster {}, digest {}, {} resources, and {} request \
             claims; found schema {}, migration {}, cluster {}, digest {}, {} resources, and {} \
             request claims",
            expected.schema_version,
            expected.migration_id,
            expected.cluster_id,
            expected.source_sha256,
            expected.resources,
            expected.request_claims,
            actual.schema_version,
            actual.migration_id,
            actual.cluster_id,
            actual.source_sha256,
            actual.resources,
            actual.request_claims,
        ),
    }
}

/// Observable result of applying a cutover plan.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase", tag = "status")]
pub enum MigrationOutcome {
    /// Every destination converged and a completion marker was committed.
    Applied {
        /// Total resource count in the plan.
        resources: usize,
        /// Total legacy request collision barriers in the plan.
        request_claims: usize,
        /// Destinations newly created by this invocation.
        written: usize,
        /// Exact destination bytes reused after an earlier partial invocation.
        reused: usize,
    },
    /// An identical snapshot-bound plan had already completed.
    AlreadyComplete {
        /// Total resource count confirmed by the marker.
        resources: usize,
        /// Total request collision barriers confirmed by the marker.
        request_claims: usize,
    },
}

/// Read-only proof that one completion marker and all planned destinations agree.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase", tag = "status")]
pub enum MigrationVerification {
    /// Every planned destination exists with its exact logical value.
    Verified {
        /// Total resource count confirmed against the store.
        resources: usize,
        /// Total request collision barriers confirmed against the store.
        request_claims: usize,
        /// Canonical legacy snapshot digest bound by the completion marker.
        source_sha256: String,
    },
}

/// A cutover plan could not converge without overwriting state.
#[derive(Debug, thiserror::Error)]
pub enum MigrationError {
    /// The destination store operation failed.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// The selected destination keyspace belongs to a different cluster.
    #[error(
        "migration plan for cluster `{plan_cluster_id}` cannot write to `{destination_prefix}`"
    )]
    DestinationClusterMismatch {
        /// Identity authenticated by the legacy cluster metadata.
        plan_cluster_id: ClusterId,
        /// Canonical destination prefix selected by the caller.
        destination_prefix: String,
    },
    /// A built-in kind could not be represented by the open key type.
    #[error("invalid destination kind `{kind}`: {message}")]
    InvalidDestinationKind {
        /// Rejected kind.
        kind: String,
        /// Validation detail.
        message: String,
    },
    /// The stopped legacy source could not be revalidated before completion.
    #[error("legacy source fence failed before migration completion: {message}")]
    SourceFence {
        /// Source comparison or read failure detail.
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
    /// A planned destination is absent after a completion marker was committed.
    #[error("verified migration destination `{key}` is missing")]
    MissingDestination {
        /// Missing canonical key.
        key: String,
    },
    /// A planned destination changed after its completion marker was committed.
    #[error("verified migration destination `{key}` does not match the reviewed plan")]
    DestinationMismatch {
        /// Changed canonical key.
        key: String,
    },
    /// State outside the reviewed plan exists in the destination namespace.
    #[error("unexpected destination `{key}` exists outside the reviewed migration plan")]
    UnexpectedDestination {
        /// Unexpected canonical key.
        key: String,
    },
    /// The selected migration has not committed its completion marker.
    #[error("migration marker `{key}` is missing")]
    MissingMarker {
        /// Expected canonical marker key.
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
    #[error("migration marker `{key}` does not match the reviewed plan: {message}")]
    MarkerMismatch {
        /// Marker key.
        key: String,
        /// Complete secret-free comparison detail.
        message: String,
    },
}
