use std::collections::{BTreeSet, VecDeque};
use std::fmt::{Debug, Formatter};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use etcd_client::{
    Certificate, Client, Compare as EtcdCompare, CompareOp, ConnectOptions, DeleteOptions,
    EventType, GetOptions, Identity, KeyValue, LeaseKeepAliveStream, LeaseKeeper, PutOptions,
    TlsOptions, Txn, TxnOp, TxnOpResponse, WatchOptions, WatchStream,
};
use tokio::sync::Mutex;
use zeroize::Zeroizing;

use crate::etcd_value::ValueProtector;
use crate::{
    CasOutcome, Compare, DeleteRequest, EncryptionKey, ExpectedVersion, ListResult, Mutation,
    MutationResult, PutRequest, Session, SessionBinding, SessionId, Store, StoreError, StoreKey,
    StorePrefix, StoreWatch, StoredValue, Transaction, TransactionOutcome, Version, WatchCursor,
    WatchEvent, WatchEventKind, WatchStart,
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
        Ok(Box::new(EtcdWatch {
            client: self.client.clone(),
            values: self.values.clone(),
            prefix,
            resume_after,
            stream: None,
            pending: VecDeque::new(),
            active_revision: None,
            next_event_index: 0,
        }))
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
        Ok(Box::new(EtcdSession {
            client: self.client.clone(),
            id,
            ttl,
            closed: AtomicBool::new(false),
            keepalive: Mutex::new(None),
        }))
    }
}

struct EtcdWatch {
    client: Client,
    values: ValueProtector,
    prefix: StorePrefix,
    resume_after: Option<WatchCursor>,
    stream: Option<WatchStream>,
    pending: VecDeque<WatchEvent>,
    active_revision: Option<u64>,
    next_event_index: u32,
}

#[async_trait]
impl StoreWatch for EtcdWatch {
    async fn next(&mut self) -> Result<WatchEvent, StoreError> {
        loop {
            if let Some(event) = self.pending.pop_front() {
                return Ok(event);
            }
            if self.stream.is_none() {
                let mut options = WatchOptions::new().with_prefix().with_prev_key();
                if let Some(cursor) = self.resume_after {
                    options = options.with_start_revision(revision_i64(cursor.revision)?);
                }
                let stream = self
                    .client
                    .watch(self.prefix.as_str(), Some(options))
                    .await
                    .map_err(unavailable)?;
                self.stream = Some(stream);
            }
            let response = self
                .stream
                .as_mut()
                .ok_or_else(|| StoreError::Contract {
                    message: "etcd watch stream was not initialized".to_string(),
                })?
                .message()
                .await
                .map_err(unavailable)?
                .ok_or_else(|| StoreError::Unavailable {
                    message: "etcd closed the watch stream".to_string(),
                })?;
            if response.compact_revision() > 0 {
                return Err(StoreError::CursorExpired {
                    cursor: self.resume_after.unwrap_or_default(),
                });
            }
            if response.canceled() {
                return Err(StoreError::Unavailable {
                    message: format!("etcd canceled the watch: {}", response.cancel_reason()),
                });
            }
            for event in response.events() {
                let kv = event.kv().ok_or_else(|| StoreError::Contract {
                    message: "etcd watch event omitted its key/value metadata".to_string(),
                })?;
                let revision = revision(kv.mod_revision())?;
                if self.active_revision != Some(revision) {
                    self.active_revision = Some(revision);
                    self.next_event_index = 0;
                }
                let cursor = WatchCursor::event(revision, self.next_event_index);
                self.next_event_index = self.next_event_index.saturating_add(1);
                if self.resume_after.is_some_and(|resume| cursor <= resume) {
                    continue;
                }
                let kind = match event.event_type() {
                    EventType::Put => WatchEventKind::Put(stored_value(kv, &self.values)?),
                    EventType::Delete => {
                        let previous = event.prev_kv().ok_or_else(|| StoreError::Contract {
                            message: "etcd delete watch event omitted its previous value"
                                .to_string(),
                        })?;
                        WatchEventKind::Delete {
                            key: StoreKey::from_backend(kv.key())?,
                            previous_version: version(previous.mod_revision())?,
                        }
                    }
                };
                self.pending.push_back(WatchEvent {
                    prefix: self.prefix.clone(),
                    cursor,
                    kind,
                });
            }
        }
    }
}

struct EtcdSession {
    client: Client,
    id: SessionId,
    ttl: Duration,
    closed: AtomicBool,
    keepalive: Mutex<Option<(LeaseKeeper, LeaseKeepAliveStream)>>,
}

#[async_trait]
impl Session for EtcdSession {
    fn id(&self) -> SessionId {
        self.id
    }

    fn ttl(&self) -> Duration {
        self.ttl
    }

    async fn keep_alive(&self) -> Result<(), StoreError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(StoreError::SessionExpired {
                session_id: self.id,
            });
        }
        let mut keepalive = self.keepalive.lock().await;
        if keepalive.is_none() {
            let pair = self
                .client
                .clone()
                .lease_keep_alive(session_i64(self.id)?)
                .await
                .map_err(|error| {
                    operation_error(
                        error,
                        Some(SessionBinding {
                            session_id: self.id,
                        }),
                    )
                })?;
            *keepalive = Some(pair);
        }
        let (keeper, responses) = keepalive.as_mut().ok_or_else(|| StoreError::Contract {
            message: "etcd keepalive stream was not initialized".to_string(),
        })?;
        keeper.keep_alive().await.map_err(|error| {
            operation_error(
                error,
                Some(SessionBinding {
                    session_id: self.id,
                }),
            )
        })?;
        let response = responses
            .message()
            .await
            .map_err(|error| {
                operation_error(
                    error,
                    Some(SessionBinding {
                        session_id: self.id,
                    }),
                )
            })?
            .ok_or(StoreError::SessionExpired {
                session_id: self.id,
            })?;
        if response.ttl() <= 0 {
            self.closed.store(true, Ordering::Release);
            Err(StoreError::SessionExpired {
                session_id: self.id,
            })
        } else {
            Ok(())
        }
    }

    async fn close(&self) -> Result<(), StoreError> {
        if self.closed.swap(true, Ordering::AcqRel) {
            return Err(StoreError::SessionExpired {
                session_id: self.id,
            });
        }
        self.client
            .clone()
            .lease_revoke(session_i64(self.id)?)
            .await
            .map_err(|error| {
                operation_error(
                    error,
                    Some(SessionBinding {
                        session_id: self.id,
                    }),
                )
            })?;
        Ok(())
    }
}

fn etcd_compare(compare: Compare) -> Result<EtcdCompare, StoreError> {
    let key = compare.key.as_str();
    match compare.expected {
        ExpectedVersion::Missing => Ok(EtcdCompare::version(key, CompareOp::Equal, 0)),
        ExpectedVersion::Exact(version) => Ok(EtcdCompare::mod_revision(
            key,
            CompareOp::Equal,
            revision_i64(version.0)?,
        )),
    }
}

fn etcd_operation(mutation: &Mutation, values: &ValueProtector) -> Result<TxnOp, StoreError> {
    match mutation {
        Mutation::Put {
            key,
            value,
            session,
        } => {
            let options = session
                .map(|binding| session_i64(binding.session_id))
                .transpose()?
                .map(|lease| PutOptions::new().with_lease(lease));
            let value = values.protect(key, value)?;
            Ok(TxnOp::put(key.as_str(), value, options))
        }
        Mutation::Delete { key } => Ok(TxnOp::delete(
            key.as_str(),
            Some(DeleteOptions::new().with_prev_key()),
        )),
    }
}

fn stored_value(value: &KeyValue, values: &ValueProtector) -> Result<StoredValue, StoreError> {
    let key = StoreKey::from_backend(value.key())?;
    Ok(StoredValue {
        value: values.unprotect(&key, value.value())?,
        key,
        version: version(value.mod_revision())?,
    })
}

fn validate_transaction(transaction: &Transaction) -> Result<(), StoreError> {
    let unique_keys = transaction
        .mutations
        .iter()
        .map(|mutation| match mutation {
            Mutation::Put { key, .. } | Mutation::Delete { key } => key,
        })
        .collect::<BTreeSet<_>>();
    if unique_keys.len() == transaction.mutations.len() {
        Ok(())
    } else {
        Err(StoreError::Contract {
            message: "a transaction cannot mutate the same key more than once".to_string(),
        })
    }
}

fn response_revision(header: Option<&etcd_client::ResponseHeader>) -> Result<u64, StoreError> {
    let header = header.ok_or_else(|| StoreError::Contract {
        message: "etcd response omitted its linearizable revision header".to_string(),
    })?;
    revision(header.revision())
}

fn revision(value: i64) -> Result<u64, StoreError> {
    u64::try_from(value).map_err(|_| StoreError::Contract {
        message: format!("etcd returned an invalid negative revision: {value}"),
    })
}

fn version(value: i64) -> Result<Version, StoreError> {
    revision(value).map(Version)
}

fn revision_i64(value: u64) -> Result<i64, StoreError> {
    i64::try_from(value).map_err(|_| StoreError::Contract {
        message: format!("store revision does not fit etcd's signed revision space: {value}"),
    })
}

fn session_id(value: i64) -> Result<SessionId, StoreError> {
    u64::try_from(value)
        .map(SessionId)
        .map_err(|_| StoreError::Contract {
            message: format!("etcd returned an invalid negative lease identity: {value}"),
        })
}

fn session_i64(value: SessionId) -> Result<i64, StoreError> {
    i64::try_from(value.0).map_err(|_| StoreError::Contract {
        message: "store session identity does not fit etcd's lease space".to_string(),
    })
}

fn duration_to_ttl(ttl: Duration) -> Result<i64, StoreError> {
    if ttl.is_zero() {
        return Err(StoreError::Contract {
            message: "store session TTL must be greater than zero".to_string(),
        });
    }
    let seconds = ttl
        .as_secs()
        .saturating_add(u64::from(ttl.subsec_nanos() > 0));
    i64::try_from(seconds).map_err(|_| StoreError::Contract {
        message: "store session TTL exceeds etcd's supported range".to_string(),
    })
}

fn operation_error(error: etcd_client::Error, session: Option<SessionBinding>) -> StoreError {
    let message = error.to_string();
    if message.contains("requested lease not found")
        && let Some(binding) = session
    {
        StoreError::SessionExpired {
            session_id: binding.session_id,
        }
    } else {
        StoreError::Unavailable { message }
    }
}

fn unavailable(error: etcd_client::Error) -> StoreError {
    StoreError::Unavailable {
        message: error.to_string(),
    }
}
