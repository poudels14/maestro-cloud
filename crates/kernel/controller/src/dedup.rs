use kernel_store::{
    Compare, ExpectedVersion, Mutation, MutationResult, StoreKey, Transaction, TransactionOutcome,
    WatchCursor,
};
use serde::{Deserialize, Serialize};

use crate::{ControllerError, FencedStore};

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
        mut transaction: Transaction,
    ) -> Result<DedupOutcome, ControllerError> {
        let mutates_claim = transaction.mutations.iter().any(|mutation| match mutation {
            Mutation::Put { key, .. } | Mutation::Delete { key } => key == &claim_key,
        });
        if mutates_claim {
            Err(ControllerError::Contract {
                message: "application transaction cannot mutate its internal request claim key"
                    .to_string(),
            })
        } else {
            let claim = RequestClaim {
                fingerprint: fingerprint.0,
                response,
            };
            let encoded_claim =
                serde_json::to_vec(&claim).map_err(|error| ControllerError::Contract {
                    message: format!("request claim could not be encoded: {error}"),
                })?;
            transaction.compares.push(Compare {
                key: claim_key.clone(),
                expected: ExpectedVersion::Missing,
            });
            transaction.mutations.push(Mutation::Put {
                key: claim_key.clone(),
                value: encoded_claim,
                session: None,
            });
            self.apply_deduplicated(claim_key, fingerprint, transaction)
                .await
        }
    }

    async fn apply_deduplicated(
        &self,
        claim_key: StoreKey,
        fingerprint: RequestFingerprint,
        transaction: Transaction,
    ) -> Result<DedupOutcome, ControllerError> {
        match self.txn(transaction).await? {
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
            TransactionOutcome::Conflict => {
                if let Some(stored) = self.raw_store().get(&claim_key).await? {
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
                } else {
                    Ok(DedupOutcome::MutationConflict)
                }
            }
        }
    }
}

#[derive(Serialize, Deserialize)]
struct RequestClaim {
    fingerprint: [u8; 32],
    response: Vec<u8>,
}
