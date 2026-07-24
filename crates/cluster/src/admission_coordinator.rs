use std::net::Ipv4Addr;
use std::sync::Arc;

use kernel_api::{NodeId, NodeRole, SecretValue};
use kernel_store::{
    Compare, ExpectedVersion, Keyspace, Mutation, Store, Transaction, TransactionOutcome,
};

use crate::admission::admit_replayed_join_request;
use crate::{
    AdmissionError, CertificateError, CertificateValidity, ClusterCertificateAuthority,
    ClusterConfig, ClusterLaunchPolicy, ClusterPreflightError, EncryptedJoinResponse, JoinPayload,
    JoinProtocolError, JoinRequest, JoinResponseStatus, RequestSignature, StoreMember,
    StoreProvider, StoreProviderError, admit_join_request, create_ca_discovery_response,
    encrypt_join_response,
};

mod approval;

use approval::{AcceptedJoin, StoredApproval, decode_record, encode_record, validate_fingerprint};
pub use approval::{NodeJoinApproval, NodeJoinApprovalRequest, NodeJoinApprovalState};

const MAXIMUM_APPROVAL_CAS_ATTEMPTS: usize = 8;

/// Store-backed coordinator for approval, signed admission, and encrypted grants.
pub struct AdmissionCoordinator {
    config: ClusterConfig,
    authority: ClusterCertificateAuthority,
    operator_jwt_secret: SecretValue,
    store_encryption_secret: SecretValue,
    launch_policy: ClusterLaunchPolicy,
    provider: Arc<dyn StoreProvider>,
    store: Arc<dyn Store>,
    keys: Keyspace,
}

impl AdmissionCoordinator {
    /// Validates static formation dependencies without mutating membership.
    pub fn new(
        config: ClusterConfig,
        authority: ClusterCertificateAuthority,
        operator_jwt_secret: SecretValue,
        store_encryption_secret: SecretValue,
        launch_policy: ClusterLaunchPolicy,
        provider: Arc<dyn StoreProvider>,
        store: Arc<dyn Store>,
    ) -> Result<Self, AdmissionCoordinatorError> {
        config.preflight()?;
        if operator_jwt_secret.expose().len() < 32 {
            return Err(AdmissionCoordinatorError::WeakOperatorSecret);
        }
        if store_encryption_secret.expose().chars().count() < 32 {
            return Err(AdmissionCoordinatorError::WeakStoreSecret);
        }
        let keys = Keyspace::new(&config.cluster_id);
        Ok(Self {
            config,
            authority,
            operator_jwt_secret,
            store_encryption_secret,
            launch_policy,
            provider,
            store,
            keys,
        })
    }

    /// Creates or replays one approval without replacing another key identity.
    pub async fn approve(
        &self,
        node_id: NodeId,
        public_key_sha256: String,
        now_unix_ms: i64,
    ) -> Result<NodeJoinApproval, AdmissionCoordinatorError> {
        validate_fingerprint(&public_key_sha256)?;
        let node = self.config.nodes.get(&node_id).ok_or_else(|| {
            AdmissionCoordinatorError::UnknownNode {
                node_id: node_id.clone(),
            }
        })?;
        if node.role == NodeRole::Master {
            return Err(AdmissionCoordinatorError::MasterCannotJoin);
        }
        let key = self.keys.join_approval(&node_id);
        for _attempt in 0..MAXIMUM_APPROVAL_CAS_ATTEMPTS {
            self.ensure_join_allowed(&node_id).await?;
            let stored = self.store.get(&key).await?;
            if let Some(stored) = stored {
                let existing = decode_record(&stored)?;
                if existing.view.public_key_sha256 == public_key_sha256 {
                    return Ok(existing.view);
                }
                return Err(AdmissionCoordinatorError::ApprovalConflict { node_id });
            }
            let record = StoredApproval {
                view: NodeJoinApproval {
                    node_id: node_id.clone(),
                    public_key_sha256: public_key_sha256.clone(),
                    approved_at_unix_ms: now_unix_ms,
                    state: NodeJoinApprovalState::Approved,
                    admitted_at_unix_ms: None,
                },
                accepted: None,
            };
            let outcome = self
                .store
                .txn(Transaction {
                    compares: vec![
                        Compare {
                            key: key.clone(),
                            expected: ExpectedVersion::Missing,
                        },
                        Compare {
                            key: self.keys.node_removal(&node_id),
                            expected: ExpectedVersion::Missing,
                        },
                        Compare {
                            key: self.keys.node_tombstone(&node_id),
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
                return Ok(record.view);
            }
        }
        Err(AdmissionCoordinatorError::ConcurrentApproval { node_id })
    }

    /// Lists secret-free approvals in stable node identity order.
    pub async fn list(&self) -> Result<Vec<NodeJoinApproval>, AdmissionCoordinatorError> {
        let listed = self.store.list(&self.keys.join_approvals()).await?;
        listed
            .values
            .iter()
            .map(decode_record)
            .map(|record| record.map(|record| record.view))
            .collect()
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

    /// Authenticates, stages, and durably commits one exact approved request.
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
        let key = self.keys.join_approval(&evidence.node_id);
        for _attempt in 0..MAXIMUM_APPROVAL_CAS_ATTEMPTS {
            self.ensure_join_allowed(&evidence.node_id).await?;
            let stored = self.store.get(&key).await?.ok_or_else(|| {
                AdmissionCoordinatorError::ApprovalRequired {
                    node_id: evidence.node_id.clone(),
                }
            })?;
            let mut record = decode_record(&stored)?;
            if record.view.public_key_sha256 != evidence.public_key_sha256 {
                return Err(AdmissionCoordinatorError::ApprovalKeyMismatch {
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

            let payload = self.issue_payload(request, certificate_validity).await?;
            record.view.state = NodeJoinApprovalState::Admitted;
            record.view.admitted_at_unix_ms = Some(now_unix_ms);
            record.accepted = Some(AcceptedJoin {
                request_sha256: evidence.request_sha256.clone(),
                payload: payload.clone(),
            });
            let outcome = self
                .store
                .txn(Transaction {
                    compares: vec![
                        Compare {
                            key: key.clone(),
                            expected: ExpectedVersion::Exact(stored.version),
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
            .get(&self.keys.join_approval(&evidence.node_id))
            .await?;
        let Some(stored) = stored else {
            return Err(freshness_error.into());
        };
        let record = decode_record(&stored)?;
        if record.view.public_key_sha256 != evidence.public_key_sha256 {
            return Err(AdmissionCoordinatorError::ApprovalKeyMismatch {
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
        let certificates = self.authority.issue_node_certificate(
            &request.node_id,
            &request.hostname,
            request.endpoint.host_address,
            request.role,
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
            cluster_cidr: self.config.cluster_cidr,
            node_limit: self.config.node_limit,
            node_prefix: self.config.node_prefix,
            nodes: self.config.nodes.clone(),
            control_allow_cidrs: self.config.control_allow_cidrs.clone(),
            ports: self.config.ports,
            tailscale: self.config.tailscale.clone(),
            cloudflare: self.config.cloudflare.clone(),
            launch_policy: self.launch_policy.clone(),
            certificates,
            operator_jwt_secret: self.operator_jwt_secret.clone(),
            store_encryption_secret: self.store_encryption_secret.clone(),
            store_join_ticket,
            certificate_issuer: request
                .role
                .is_control_plane()
                .then(|| self.authority.clone()),
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

/// Why approval or admission did not converge safely.
#[derive(Debug, thiserror::Error)]
pub enum AdmissionCoordinatorError {
    /// Static cluster topology was invalid.
    #[error(transparent)]
    InvalidTopology(#[from] ClusterPreflightError),
    /// The API authentication secret cannot safely start an admitted node.
    #[error("operator JWT secret must contain at least 32 bytes")]
    WeakOperatorSecret,
    /// The value-encryption secret cannot safely start an admitted node.
    #[error("store encryption secret must contain at least 32 characters")]
    WeakStoreSecret,
    /// An approval named no declared topology node.
    #[error("node `{node_id}` is absent from the cluster topology")]
    UnknownNode { node_id: NodeId },
    /// The designated seed never joins its own already-bootstrapped cluster.
    #[error("the designated master cannot receive a join approval")]
    MasterCannotJoin,
    /// A pending permanent removal blocks new trust or membership grants.
    #[error("node `{node_id}` is being permanently removed")]
    NodeRemovalInProgress { node_id: NodeId },
    /// A tombstoned node identity can never receive another trust or membership grant.
    #[error("node `{node_id}` was permanently removed")]
    NodeRemoved { node_id: NodeId },
    /// A public-key fingerprint was not canonical SHA-256 hexadecimal.
    #[error("join public key fingerprint must be 64 lowercase hexadecimal characters")]
    InvalidFingerprint,
    /// A node already has an approval for another key.
    #[error("node `{node_id}` already has an approval for another key")]
    ApprovalConflict { node_id: NodeId },
    /// Approval writes changed too frequently to converge.
    #[error("join approval for node `{node_id}` kept changing")]
    ConcurrentApproval { node_id: NodeId },
    /// A signed request had no operator approval.
    #[error("node `{node_id}` requires operator approval before joining")]
    ApprovalRequired { node_id: NodeId },
    /// A signed request used a key other than the approved key.
    #[error("node `{node_id}` join key does not match its approval")]
    ApprovalKeyMismatch { node_id: NodeId },
    /// A one-time approval already admitted another request.
    #[error("node `{node_id}` approval already admitted another request")]
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
    /// Durable approval storage failed.
    #[error(transparent)]
    Store(#[from] kernel_store::StoreError),
    /// Approval persistence could not be encoded or decoded.
    #[error("invalid persisted join approval: {0}")]
    Serialization(#[from] serde_json::Error),
    /// Join discovery or response encryption failed.
    #[error(transparent)]
    Protocol(#[from] JoinProtocolError),
}
