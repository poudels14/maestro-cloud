use std::sync::Arc;

use kernel_store::{
    Compare, ExpectedVersion, Mutation, MutationResult, Store, StoreKey, Transaction,
    TransactionOutcome, WatchCursor,
};
use serde::{Deserialize, Serialize};

use crate::{ControllerError, FencedStore};

const MAXIMUM_CLAIM_RESPONSE_BYTES: usize = 64 * 1024;

/// Stable digest of the authenticated route, identity, and request body.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequestFingerprint([u8; 32]);

impl RequestFingerprint {
    /// Constructs a fingerprint from a caller-selected cryptographic digest.
    pub fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }
}

/// Result of atomically claiming and applying one external write request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DedupOutcome {
    /// This request claimed its identity and committed every mutation.
    Committed {
        /// Application mutation results, excluding the internal claim write.
        results: Vec<MutationResult>,
        /// Cursor immediately after the application and claim transaction.
        cursor: WatchCursor,
    },
    /// The same request already committed and its original response is replayed.
    Duplicate {
        /// Opaque response bytes stored by the first successful request.
        response: Vec<u8>,
    },
    /// An application compare failed and no request claim was created.
    MutationConflict,
}

/// Any-node request writer that commits a mutation and its replay claim atomically.
#[derive(Clone)]
pub struct RequestDeduplicator {
    store: Arc<dyn Store>,
}

impl RequestDeduplicator {
    /// Binds request deduplication to the cluster's canonical store.
    pub fn new(store: Arc<dyn Store>) -> Self {
        Self { store }
    }

    /// Applies a resource write without requiring this API node to be leader.
    pub async fn deduplicate(
        &self,
        claim_key: StoreKey,
        fingerprint: RequestFingerprint,
        response: Vec<u8>,
        transaction: Transaction,
    ) -> Result<DedupOutcome, ControllerError> {
        let transaction = append_claim(claim_key.clone(), fingerprint, response, transaction)?;
        let outcome = self.store.txn(transaction).await?;
        resolve_outcome(self.store.as_ref(), claim_key, fingerprint, outcome).await
    }
}

impl FencedStore {
    /// Claims and applies an idempotent request in one leadership-fenced transaction.
    ///
    /// Cancellation may leave the entire application-and-claim transaction
    /// committed. A retry with the same fingerprint then returns `Duplicate`
    /// and the original response without reapplying side effects.
    pub async fn deduplicate(
        &self,
        claim_key: StoreKey,
        fingerprint: RequestFingerprint,
        response: Vec<u8>,
        transaction: Transaction,
    ) -> Result<DedupOutcome, ControllerError> {
        let transaction = append_claim(claim_key.clone(), fingerprint, response, transaction)?;
        let outcome = self.txn(transaction).await?;
        resolve_outcome(self.raw_store(), claim_key, fingerprint, outcome).await
    }
}

fn append_claim(
    claim_key: StoreKey,
    fingerprint: RequestFingerprint,
    response: Vec<u8>,
    mut transaction: Transaction,
) -> Result<Transaction, ControllerError> {
    if response.len() > MAXIMUM_CLAIM_RESPONSE_BYTES {
        return Err(ControllerError::Contract {
            message: format!("request claim response exceeds {MAXIMUM_CLAIM_RESPONSE_BYTES} bytes"),
        });
    }
    let mutates_claim = transaction.mutations.iter().any(|mutation| match mutation {
        Mutation::Put { key, .. } | Mutation::Delete { key } => key == &claim_key,
    });
    if mutates_claim {
        return Err(ControllerError::Contract {
            message: "application transaction cannot mutate its internal request claim key"
                .to_string(),
        });
    }
    let encoded_claim = serde_json::to_vec(&RequestClaim {
        fingerprint: fingerprint.0,
        response,
    })
    .map_err(|error| ControllerError::Contract {
        message: format!("request claim could not be encoded: {error}"),
    })?;
    transaction.compares.push(Compare {
        key: claim_key.clone(),
        expected: ExpectedVersion::Missing,
    });
    transaction.mutations.push(Mutation::Put {
        key: claim_key,
        value: encoded_claim,
        session: None,
    });
    Ok(transaction)
}

async fn resolve_outcome(
    store: &dyn Store,
    claim_key: StoreKey,
    fingerprint: RequestFingerprint,
    outcome: TransactionOutcome,
) -> Result<DedupOutcome, ControllerError> {
    match outcome {
        TransactionOutcome::Applied {
            mut results,
            cursor,
        } => {
            let claim_result = results.pop();
            if matches!(claim_result, Some(MutationResult::Put(_))) {
                Ok(DedupOutcome::Committed { results, cursor })
            } else {
                Err(ControllerError::Contract {
                    message: "dedup transaction did not return its appended claim write"
                        .to_string(),
                })
            }
        }
        TransactionOutcome::Conflict => match store.get(&claim_key).await? {
            Some(stored) => {
                let existing: RequestClaim =
                    serde_json::from_slice(&stored.value).map_err(|error| {
                        ControllerError::MalformedRequestClaim {
                            message: error.to_string(),
                        }
                    })?;
                if existing.fingerprint == fingerprint.0 {
                    Ok(DedupOutcome::Duplicate {
                        response: existing.response,
                    })
                } else {
                    Err(ControllerError::RequestCollision)
                }
            }
            None => Ok(DedupOutcome::MutationConflict),
        },
    }
}

#[derive(Serialize, Deserialize)]
struct RequestClaim {
    fingerprint: [u8; 32],
    response: Vec<u8>,
}
