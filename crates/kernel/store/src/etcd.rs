use std::fmt::{Debug, Formatter};
use std::time::Duration;

use async_trait::async_trait;
use etcd_client::{
    Certificate, Client, ConnectOptions, GetOptions, Identity, TlsOptions, Txn, TxnOpResponse,
};
use zeroize::Zeroizing;

use crate::etcd_session::EtcdSession;
use crate::etcd_support::{
    duration_to_ttl, etcd_compare, etcd_operation, operation_error, response_revision, session_id,
    stored_value, unavailable, validate_transaction, version,
};
use crate::etcd_value::ValueProtector;
use crate::etcd_watch::EtcdWatch;
use crate::{
    CasOutcome, Compare, DeleteRequest, EncryptionKey, ExpectedVersion, ListResult, Mutation,
    MutationResult, PutRequest, Session, Store, StoreError, StoreKey, StorePrefix, StoreWatch,
    StoredValue, Transaction, TransactionOutcome, Version, WatchCursor, WatchStart,
};

/// Production linearizable store backed by an etcd v3 cluster.
#[derive(Clone)]
pub struct EtcdStore {
    client: Client,
    values: ValueProtector,
}

/// Mutual-TLS material used to authenticate one etcd client connection.
#[derive(Clone)]
pub struct EtcdTlsConfig {
    server_name: Option<String>,
    certificate_authority: Vec<u8>,
    client_certificate: Vec<u8>,
    client_private_key: Zeroizing<Vec<u8>>,
}

impl EtcdTlsConfig {
    /// Wraps PEM-encoded trust roots and client identity material.
    pub fn new(
        server_name: impl Into<String>,
        certificate_authority: Vec<u8>,
        client_certificate: Vec<u8>,
        client_private_key: Vec<u8>,
    ) -> Self {
        Self {
            server_name: Some(server_name.into()),
            certificate_authority,
            client_certificate,
            client_private_key: Zeroizing::new(client_private_key),
        }
    }

    /// Uses each endpoint host as its own TLS server name for a heterogeneous member set.
    pub fn for_endpoints(
        certificate_authority: Vec<u8>,
        client_certificate: Vec<u8>,
        client_private_key: Vec<u8>,
    ) -> Self {
        Self {
            server_name: None,
            certificate_authority,
            client_certificate,
            client_private_key: Zeroizing::new(client_private_key),
        }
    }
}

impl Debug for EtcdTlsConfig {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("EtcdTlsConfig")
            .field("server_name", &self.server_name)
            .field("certificate_authority", &"[PEM]")
            .field("client_certificate", &"[PEM]")
            .field("client_private_key", &"[REDACTED]")
            .finish()
    }
}

impl EtcdStore {
    /// Connects without value encryption for conformance tests and migration reads.
    ///
    /// Endpoint balancing and reconnect behavior are owned by `etcd-client`.
    /// Cluster provisioning supplies HTTPS endpoints once mutual TLS is active.
    pub async fn connect<E, S>(endpoints: E) -> Result<Self, StoreError>
    where
        E: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self::connect_with_options(endpoints, None, None).await
    }

    /// Connects and protects internal values with one derived cluster key.
    ///
    /// The explicitly external Traefik provider subtree remains directly
    /// readable and must never contain secret-bearing values.
    pub async fn connect_encrypted<E, S>(
        endpoints: E,
        encryption_key: EncryptionKey,
    ) -> Result<Self, StoreError>
    where
        E: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self::connect_with_options(endpoints, None, Some(encryption_key)).await
    }

    /// Connects with mutual TLS but without application-level value encryption.
    pub async fn connect_with_tls<E, S>(
        endpoints: E,
        tls: EtcdTlsConfig,
    ) -> Result<Self, StoreError>
    where
        E: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let identity =
            Identity::from_pem(tls.client_certificate, tls.client_private_key.as_slice());
        let mut tls_options = TlsOptions::new()
            .ca_certificate(Certificate::from_pem(tls.certificate_authority))
            .identity(identity);
        if let Some(server_name) = tls.server_name {
            tls_options = tls_options.domain_name(server_name);
        }
        Self::connect_with_options(
            endpoints,
            Some(ConnectOptions::new().with_tls(tls_options)),
            None,
        )
        .await
    }

    /// Connects with mutual TLS and application-level value encryption.
    pub async fn connect_with_tls_and_encryption<E, S>(
        endpoints: E,
        tls: EtcdTlsConfig,
        encryption_key: EncryptionKey,
    ) -> Result<Self, StoreError>
    where
        E: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let identity =
            Identity::from_pem(tls.client_certificate, tls.client_private_key.as_slice());
        let mut tls_options = TlsOptions::new()
            .ca_certificate(Certificate::from_pem(tls.certificate_authority))
            .identity(identity);
        if let Some(server_name) = tls.server_name {
            tls_options = tls_options.domain_name(server_name);
        }
        Self::connect_with_options(
            endpoints,
            Some(ConnectOptions::new().with_tls(tls_options)),
            Some(encryption_key),
        )
        .await
    }

    async fn connect_with_options<E, S>(
        endpoints: E,
        options: Option<ConnectOptions>,
        encryption_key: Option<EncryptionKey>,
    ) -> Result<Self, StoreError>
    where
        E: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let endpoints = endpoints.into_iter().map(Into::into).collect::<Vec<_>>();
        let client = Client::connect(&endpoints, options)
            .await
            .map_err(unavailable)?;
        Ok(Self {
            client,
            values: ValueProtector::new(encryption_key),
        })
    }

    async fn execute_transaction(
        &self,
        transaction: Transaction,
    ) -> Result<TransactionOutcome, StoreError> {
        validate_transaction(&transaction)?;
        let session = transaction
            .mutations
            .iter()
            .find_map(|mutation| match mutation {
                Mutation::Put { session, .. } => *session,
                Mutation::Delete { .. } => None,
            });
        let compares = transaction
            .compares
            .into_iter()
            .map(etcd_compare)
            .collect::<Result<Vec<_>, _>>()?;
        let mutations = transaction.mutations;
        let operations = mutations
            .iter()
            .map(|mutation| etcd_operation(mutation, &self.values))
            .collect::<Result<Vec<_>, _>>()?;
        let response = self
            .client
            .clone()
            .txn(Txn::new().when(compares).and_then(operations))
            .await
            .map_err(|error| operation_error(error, session))?;
        if !response.succeeded() {
            return Ok(TransactionOutcome::Conflict);
        }
        let revision = response_revision(response.header())?;
        let op_responses = response.op_responses();
        if op_responses.len() != mutations.len() {
            return Err(StoreError::Contract {
                message: format!(
                    "etcd returned {} transaction responses for {} mutations",
                    op_responses.len(),
                    mutations.len()
                ),
            });
        }

        let mut results = Vec::with_capacity(mutations.len());
        let mut event_index = 0_u32;
        let mut last_cursor = WatchCursor::snapshot(revision);
        for (mutation, response) in mutations.into_iter().zip(op_responses) {
            match (mutation, response) {
                (Mutation::Put { key, value, .. }, TxnOpResponse::Put(_)) => {
                    let stored = StoredValue {
                        key,
                        value,
                        version: Version(revision),
                    };
                    results.push(MutationResult::Put(stored));
                    last_cursor = WatchCursor::event(revision, event_index);
                    event_index = event_index.saturating_add(1);
                }
                (Mutation::Delete { key }, TxnOpResponse::Delete(response)) => {
                    if response.deleted() == 0 {
                        results.push(MutationResult::DeleteMissing { key });
                    } else {
                        let previous =
                            response
                                .prev_kvs()
                                .first()
                                .ok_or_else(|| StoreError::Contract {
                                    message:
                                        "etcd omitted the previous value for a transaction delete"
                                            .to_string(),
                                })?;
                        results.push(MutationResult::Deleted {
                            key,
                            previous_version: version(previous.mod_revision())?,
                        });
                        last_cursor = WatchCursor::event(revision, event_index);
                        event_index = event_index.saturating_add(1);
                    }
                }
                _ => {
                    return Err(StoreError::Contract {
                        message: "etcd transaction response kind did not match its mutation"
                            .to_string(),
                    });
                }
            }
        }
        Ok(TransactionOutcome::Applied {
            results,
            cursor: last_cursor,
        })
    }
}

#[async_trait]
impl Store for EtcdStore {
    async fn get(&self, key: &StoreKey) -> Result<Option<StoredValue>, StoreError> {
        let response = self
            .client
            .clone()
            .get(key.as_str(), None)
            .await
            .map_err(unavailable)?;
        match response.kvs() {
            [] => Ok(None),
            [value] => Ok(Some(stored_value(value, &self.values)?)),
            values => Err(StoreError::Contract {
                message: format!("etcd returned {} values for one exact key", values.len()),
            }),
        }
    }

    async fn list(&self, prefix: &StorePrefix) -> Result<ListResult, StoreError> {
        let response = self
            .client
            .clone()
            .get(prefix.as_str(), Some(GetOptions::new().with_prefix()))
            .await
            .map_err(unavailable)?;
        let revision = response_revision(response.header())?;
        let values = response
            .kvs()
            .iter()
            .map(|value| stored_value(value, &self.values))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(ListResult {
            values,
            cursor: WatchCursor::snapshot(revision),
        })
    }

    async fn put_cas(&self, request: PutRequest) -> Result<CasOutcome<StoredValue>, StoreError> {
        let key = request.key.clone();
        let outcome = self
            .execute_transaction(Transaction {
                compares: vec![Compare {
                    key: key.clone(),
                    expected: request.expected,
                }],
                mutations: vec![Mutation::Put {
                    key: request.key,
                    value: request.value,
                    session: request.session,
                }],
            })
            .await?;
        match outcome {
            TransactionOutcome::Applied { mut results, .. } => match results.pop() {
                Some(MutationResult::Put(value)) => Ok(CasOutcome::Applied(value)),
                _ => Err(StoreError::Contract {
                    message: "etcd put transaction omitted its result".to_string(),
                }),
            },
            TransactionOutcome::Conflict => Ok(CasOutcome::Conflict {
                actual: self.get(&key).await?.map(|value| value.version),
            }),
        }
    }

    async fn delete_cas(&self, request: DeleteRequest) -> Result<CasOutcome<Version>, StoreError> {
        let key = request.key.clone();
        let outcome = self
            .execute_transaction(Transaction {
                compares: vec![Compare {
                    key: key.clone(),
                    expected: ExpectedVersion::Exact(request.expected),
                }],
                mutations: vec![Mutation::Delete { key: request.key }],
            })
            .await?;
        match outcome {
            TransactionOutcome::Applied { mut results, .. } => match results.pop() {
                Some(MutationResult::Deleted {
                    previous_version, ..
                }) => Ok(CasOutcome::Applied(previous_version)),
                _ => Err(StoreError::Contract {
                    message: "etcd delete transaction omitted its removed value".to_string(),
                }),
            },
            TransactionOutcome::Conflict => Ok(CasOutcome::Conflict {
                actual: self.get(&key).await?.map(|value| value.version),
            }),
        }
    }

    async fn txn(&self, transaction: Transaction) -> Result<TransactionOutcome, StoreError> {
        self.execute_transaction(transaction).await
    }

    fn watch(
        &self,
        prefix: StorePrefix,
        start: WatchStart,
    ) -> Result<Box<dyn StoreWatch>, StoreError> {
        let resume_after = match start {
            WatchStart::Current => None,
            WatchStart::After(cursor) => Some(cursor),
        };
        Ok(Box::new(EtcdWatch::new(
            self.client.clone(),
            self.values.clone(),
            prefix,
            resume_after,
        )))
    }

    async fn session(&self, ttl: Duration) -> Result<Box<dyn Session>, StoreError> {
        let ttl_seconds = duration_to_ttl(ttl)?;
        let response = self
            .client
            .clone()
            .lease_grant(ttl_seconds, None)
            .await
            .map_err(unavailable)?;
        let id = session_id(response.id())?;
        Ok(Box::new(EtcdSession::new(self.client.clone(), id, ttl)))
    }
}
