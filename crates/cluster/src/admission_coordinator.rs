use std::net::Ipv4Addr;
use std::sync::Arc;

use kernel_api::{NodeId, NodeRole};
use kernel_store::{
    Compare, ExpectedVersion, Keyspace, Mutation, Store, Transaction, TransactionOutcome,
};

use crate::admission::admit_replayed_join_request;
use crate::{
    AdmissionError, CertificateError, CertificateValidity, ClusterCertificateAuthority,
    ClusterConfig, ClusterPreflightError, EncryptedJoinResponse, JoinPayload, JoinProtocolError,
    JoinRequest, JoinResponseStatus, RequestSignature, StoreMember, StoreProvider,
    StoreProviderError, admit_join_request, create_ca_discovery_response, encrypt_join_response,
};

mod record;

use record::{
    AcceptedJoin, NodeJoinRecord, NodeJoinRecordState, StoredJoin, decode_record, encode_record,
};

const MAXIMUM_ADMISSION_CAS_ATTEMPTS: usize = 8;

/// Store-backed coordinator for signed configured-node admission and encrypted grants.
pub struct AdmissionCoordinator {
    config: ClusterConfig,
    authority: ClusterCertificateAuthority,
    provider: Arc<dyn StoreProvider>,
    store: Arc<dyn Store>,
    keys: Keyspace,
}

impl AdmissionCoordinator {
    /// Validates static formation dependencies without mutating membership.
    pub fn new(
        config: ClusterConfig,
        authority: ClusterCertificateAuthority,
        provider: Arc<dyn StoreProvider>,
        store: Arc<dyn Store>,
    ) -> Result<Self, AdmissionCoordinatorError> {
        config.preflight()?;
        let keys = Keyspace::new(&config.cluster_id);
        Ok(Self {
            config,
            authority,
            provider,
            store,
            keys,
        })
    }

    /// Returns an HMAC-authenticated trust root for a not-yet-trusted joiner.
    pub fn discover(
        &self,
        request: &crate::CaDiscoveryRequest,
    ) -> Result<crate::CaDiscoveryResponse, AdmissionCoordinatorError> {
        create_ca_discovery_response(
            &self.config.join_secret,
            &self.config.name,
            &self.config.cluster_id,
            &self.authority.certificate_pem,
            request,
        )
        .map_err(Into::into)
    }

    /// Authenticates and durably binds one configured node to its first exact request.
    pub async fn admit(
        &self,
        request: &JoinRequest,
        signature: &RequestSignature,
        source_address: Ipv4Addr,
        now_unix_ms: i64,
        certificate_validity: CertificateValidity,
    ) -> Result<EncryptedJoinResponse, AdmissionCoordinatorError> {
        let evidence = match admit_join_request(
            &self.config,
            request,
            signature,
            source_address,
            now_unix_ms,
        ) {
            Ok(evidence) => evidence,
            Err(
                error @ AdmissionError::Protocol(JoinProtocolError::TimestampOutsideWindow {
                    ..
                }),
            ) => {
                return self
                    .replay_expired_request(request, signature, source_address, now_unix_ms, error)
                    .await;
            }
            Err(error) => return Err(error.into()),
        };
        if evidence.role == NodeRole::Master {
            return Err(AdmissionCoordinatorError::MasterCannotJoin);
        }
        let key = self.keys.join_record(&evidence.node_id);
        for _attempt in 0..MAXIMUM_ADMISSION_CAS_ATTEMPTS {
            self.ensure_join_allowed(&evidence.node_id).await?;
            let stored = self.store.get(&key).await?;
            if let Some(stored) = &stored {
                let record = decode_record(stored)?;
                if record.view.public_key_sha256 != evidence.public_key_sha256 {
                    return Err(AdmissionCoordinatorError::JoinKeyConflict {
                        node_id: evidence.node_id,
                    });
                }
                if let Some(accepted) = record.accepted {
                    if accepted.request_sha256 == evidence.request_sha256 {
                        return self.encrypt(request, &accepted.payload);
                    }
                    return Err(AdmissionCoordinatorError::AdmissionConflict {
                        node_id: evidence.node_id,
                    });
                }
            }

            let payload = self.issue_payload(request, certificate_validity).await?;
            let record = StoredJoin {
                view: NodeJoinRecord {
                    node_id: evidence.node_id.clone(),
                    public_key_sha256: evidence.public_key_sha256.clone(),
                    approved_at_unix_ms: now_unix_ms,
                    state: NodeJoinRecordState::Admitted,
                    admitted_at_unix_ms: Some(now_unix_ms),
                },
                accepted: Some(AcceptedJoin {
                    request_sha256: evidence.request_sha256.clone(),
                    payload: payload.clone(),
                }),
            };
            let outcome = self
                .store
                .txn(Transaction {
                    compares: vec![
                        Compare {
                            key: key.clone(),
                            expected: stored.map_or(ExpectedVersion::Missing, |value| {
                                ExpectedVersion::Exact(value.version)
                            }),
                        },
                        Compare {
                            key: self.keys.node_removal(&evidence.node_id),
                            expected: ExpectedVersion::Missing,
                        },
                        Compare {
                            key: self.keys.node_tombstone(&evidence.node_id),
                            expected: ExpectedVersion::Missing,
                        },
                    ],
                    mutations: vec![Mutation::Put {
                        key: key.clone(),
                        value: encode_record(&record)?,
                        session: None,
                    }],
                })
                .await?;
            if matches!(outcome, TransactionOutcome::Applied { .. }) {
                return self.encrypt(request, &payload);
            }
        }
        Err(AdmissionCoordinatorError::ConcurrentAdmission {
            node_id: evidence.node_id,
        })
    }

    async fn replay_expired_request(
        &self,
        request: &JoinRequest,
        signature: &RequestSignature,
        source_address: Ipv4Addr,
        now_unix_ms: i64,
        freshness_error: AdmissionError,
    ) -> Result<EncryptedJoinResponse, AdmissionCoordinatorError> {
        let evidence = admit_replayed_join_request(
            &self.config,
            request,
            signature,
            source_address,
            now_unix_ms,
        )?;
        self.ensure_join_allowed(&evidence.node_id).await?;
        let stored = self
            .store
            .get(&self.keys.join_record(&evidence.node_id))
            .await?;
        let Some(stored) = stored else {
            return Err(freshness_error.into());
        };
        let record = decode_record(&stored)?;
        if record.view.public_key_sha256 != evidence.public_key_sha256 {
            return Err(AdmissionCoordinatorError::JoinKeyConflict {
                node_id: evidence.node_id,
            });
        }
        match record.accepted {
            Some(accepted) if accepted.request_sha256 == evidence.request_sha256 => {
                self.encrypt(request, &accepted.payload)
            }
            Some(_) => Err(AdmissionCoordinatorError::AdmissionConflict {
                node_id: evidence.node_id,
            }),
            None => Err(freshness_error.into()),
        }
    }

    async fn issue_payload(
        &self,
        request: &JoinRequest,
        certificate_validity: CertificateValidity,
    ) -> Result<JoinPayload, AdmissionCoordinatorError> {
        let node = crate::NodeDefinition {
            hostname: request.hostname.clone(),
            endpoint: request.endpoint,
            workload_subnet: request.workload_subnet,
            role: request.role,
        };
        let certificates = self.authority.issue_node_certificate_for_definition(
            &request.node_id,
            &node,
            certificate_validity,
        )?;
        let store_join_ticket = if request.role.is_control_plane() {
            Some(
                self.provider
                    .stage_member(StoreMember {
                        node_id: request.node_id.clone(),
                        host_address: request.endpoint.host_address,
                    })
                    .await?
                    .0,
            )
        } else {
            None
        };
        Ok(JoinPayload {
            cluster_id: self.config.cluster_id.clone(),
            cluster_name: self.config.name.clone(),
            nodes: self.config.nodes.clone(),
            ports: self.config.ports,
            certificates,
            store_join_ticket,
        })
    }

    fn encrypt(
        &self,
        request: &JoinRequest,
        payload: &JoinPayload,
    ) -> Result<EncryptedJoinResponse, AdmissionCoordinatorError> {
        encrypt_join_response(
            &self.config.join_secret,
            request,
            payload,
            JoinResponseStatus::ACCEPTED,
        )
        .map_err(Into::into)
    }

    async fn ensure_join_allowed(&self, node_id: &NodeId) -> Result<(), AdmissionCoordinatorError> {
        if self
            .store
            .get(&self.keys.node_tombstone(node_id))
            .await?
            .is_some()
        {
            return Err(AdmissionCoordinatorError::NodeRemoved {
                node_id: node_id.clone(),
            });
        }
        if self
            .store
            .get(&self.keys.node_removal(node_id))
            .await?
            .is_some()
        {
            return Err(AdmissionCoordinatorError::NodeRemovalInProgress {
                node_id: node_id.clone(),
            });
        }
        Ok(())
    }
}

/// Why a configured-node admission did not converge safely.
#[derive(Debug, thiserror::Error)]
pub enum AdmissionCoordinatorError {
    /// Static cluster topology was invalid.
    #[error(transparent)]
    InvalidTopology(#[from] ClusterPreflightError),
    /// The designated seed is initialized by bootstrap and never joins.
    #[error("the designated master cannot join its own cluster")]
    MasterCannotJoin,
    /// A pending permanent removal blocks new trust or membership grants.
    #[error("node `{node_id}` is being permanently removed")]
    NodeRemovalInProgress { node_id: NodeId },
    /// A tombstoned node identity can never receive another trust or membership grant.
    #[error("node `{node_id}` was permanently removed")]
    NodeRemoved { node_id: NodeId },
    /// A configured identity was already bound to another join key.
    #[error("node `{node_id}` is already bound to another join key")]
    JoinKeyConflict { node_id: NodeId },
    /// A configured identity already admitted another exact request.
    #[error("node `{node_id}` already admitted another join request")]
    AdmissionConflict { node_id: NodeId },
    /// Admission writes changed too frequently to converge.
    #[error("join admission for node `{node_id}` kept changing")]
    ConcurrentAdmission { node_id: NodeId },
    /// Signed request authentication or topology matching failed.
    #[error(transparent)]
    Admission(#[from] AdmissionError),
    /// Certificate issuance failed.
    #[error(transparent)]
    Certificate(#[from] CertificateError),
    /// Store membership staging failed.
    #[error(transparent)]
    Provider(#[from] StoreProviderError),
    /// Durable join-record storage failed.
    #[error(transparent)]
    Store(#[from] kernel_store::StoreError),
    /// Join-record persistence could not be encoded or decoded.
    #[error("invalid persisted join record: {0}")]
    Serialization(#[from] serde_json::Error),
    /// Join discovery or response encryption failed.
    #[error(transparent)]
    Protocol(#[from] JoinProtocolError),
}
