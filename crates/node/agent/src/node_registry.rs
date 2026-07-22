use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use kernel_api::{
    ClusterId, Generation, InvalidIdentifier, Node, NodeId, NodeInstanceId, NodeSpec, NodeStatus,
    ObjectMeta, ResourceKind, ResourceName, ResourceRevision,
};
use kernel_store::{
    Clock, Compare, ExpectedVersion, Keyspace, Mutation, Session, SessionBinding, Store,
    StoreError, StoredValue, Transaction, TransactionOutcome,
};
use semver::Version;
use tokio::sync::watch;

use crate::StatusClock;

const NODE_KIND: &str = "Node";
const MAX_NODE_BYTES: usize = 256 * 1_024;
const MAX_CAS_ATTEMPTS: usize = 16;

/// Static identity and lease policy for one node daemon.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeRegistrySettings {
    /// Cluster receiving this node's durable resource and volatile lease.
    pub cluster_id: ClusterId,
    /// Stable node identity selected from the protected topology.
    pub node_id: NodeId,
    /// Immutable host definition advertised by this node.
    pub node_spec: NodeSpec,
    /// Unique identity of this daemon process.
    pub instance_id: NodeInstanceId,
    /// Semantic version of this daemon process.
    pub running_version: Version,
    /// Lifetime of the store session after its last acknowledged keepalive.
    pub session_ttl: Duration,
    /// Interval between acknowledged session renewals.
    pub keepalive_interval: Duration,
}

impl NodeRegistrySettings {
    /// Rejects lease policy that could expire before a scheduled renewal.
    pub fn validate(self) -> Result<Self, NodeRegistrySettingsError> {
        if self.session_ttl.is_zero() {
            return Err(NodeRegistrySettingsError::ZeroSessionTtl);
        }
        if self.keepalive_interval.is_zero() {
            return Err(NodeRegistrySettingsError::ZeroKeepaliveInterval);
        }
        if self.keepalive_interval >= self.session_ttl {
            return Err(NodeRegistrySettingsError::KeepaliveNotBeforeTtl);
        }
        Ok(self)
    }
}

/// Invalid node registry lease policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum NodeRegistrySettingsError {
    /// A zero TTL cannot represent live process ownership.
    #[error("node registry session TTL must be greater than zero")]
    ZeroSessionTtl,
    /// A zero keepalive interval creates a hot loop.
    #[error("node registry keepalive interval must be greater than zero")]
    ZeroKeepaliveInterval,
    /// Renewal must be attempted before the session expires.
    #[error("node registry keepalive interval must be less than the session TTL")]
    KeepaliveNotBeforeTtl,
}

/// Result of one atomic node resource and liveness publication.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeRegistryAction {
    /// No liveness key existed before this publication.
    Registered,
    /// This exact daemon identity renewed its existing publication.
    Renewed,
}

/// Active session ownership returned after the initial node publication.
pub struct NodeRegistration {
    session: Box<dyn Session>,
}

impl NodeRegistration {
    /// Returns the active node-liveness session used by related ephemeral advertisements.
    pub fn session_id(&self) -> kernel_store::SessionId {
        self.session.id()
    }

    /// Explicitly removes this process's liveness key before its TTL expires.
    pub async fn close(self) -> Result<(), NodeRegistryError> {
        self.session.close().await.map_err(NodeRegistryError::Store)
    }
}

/// Node-local publisher for durable node status and session-bound liveness.
pub struct NodeRegistryAgent {
    store: Arc<dyn Store>,
    node_key: kernel_store::StoreKey,
    liveness_key: kernel_store::StoreKey,
    settings: NodeRegistrySettings,
    monotonic_clock: Arc<dyn Clock>,
    status_clock: Arc<dyn StatusClock>,
}

impl NodeRegistryAgent {
    /// Binds one validated node identity to its store and injected clocks.
    pub fn new(
        store: Arc<dyn Store>,
        settings: NodeRegistrySettings,
        monotonic_clock: Arc<dyn Clock>,
        status_clock: Arc<dyn StatusClock>,
    ) -> Result<Self, NodeRegistryError> {
        let settings = settings.validate()?;
        let keyspace = Keyspace::new(&settings.cluster_id);
        let kind = ResourceKind::new(NODE_KIND)?;
        let resource_name = ResourceName::new(settings.node_id.as_str())?;
        Ok(Self {
            node_key: keyspace.resource(&kind, &resource_name),
            liveness_key: keyspace.node_liveness(&settings.node_id),
            store,
            settings,
            monotonic_clock,
            status_clock,
        })
    }

    /// Publishes one heartbeat using a caller-owned active store session.
    pub async fn reconcile_once(
        &self,
        session: &dyn Session,
    ) -> Result<NodeRegistryAction, NodeRegistryError> {
        for _attempt in 0..MAX_CAS_ATTEMPTS {
            let current_node = self.store.get(&self.node_key).await?;
            let current_liveness = self.store.get(&self.liveness_key).await?;
            self.validate_liveness(current_liveness.as_ref())?;
            let node = self.desired_node(current_node.as_ref())?;
            let action = if current_liveness.is_some() {
                NodeRegistryAction::Renewed
            } else {
                NodeRegistryAction::Registered
            };
            let transaction = Transaction {
                compares: vec![
                    Compare {
                        key: self.node_key.clone(),
                        expected: expected(current_node.as_ref()),
                    },
                    Compare {
                        key: self.liveness_key.clone(),
                        expected: expected(current_liveness.as_ref()),
                    },
                ],
                mutations: vec![
                    Mutation::Put {
                        key: self.node_key.clone(),
                        value: encode_node(&node)?,
                        session: None,
                    },
                    Mutation::Put {
                        key: self.liveness_key.clone(),
                        value: self.settings.instance_id.as_str().as_bytes().to_vec(),
                        session: Some(SessionBinding {
                            session_id: session.id(),
                        }),
                    },
                ],
            };
            if matches!(
                self.store.txn(transaction).await?,
                TransactionOutcome::Applied { .. }
            ) {
                return Ok(action);
            }
        }
        Err(NodeRegistryError::Contention)
    }

    /// Creates a session and completes the first atomic publication.
    pub async fn register(&self) -> Result<NodeRegistration, NodeRegistryError> {
        let session = self.store.session(self.settings.session_ttl).await?;
        if let Err(error) = self.reconcile_once(session.as_ref()).await {
            return close_after_error(session, error).await;
        }
        Ok(NodeRegistration { session })
    }

    /// Owns registration, renewal, and explicit lease cleanup until shutdown.
    pub async fn run(&self, shutdown: watch::Receiver<bool>) -> Result<(), NodeRegistryError> {
        if *shutdown.borrow() {
            return Ok(());
        }
        let registration = self.register().await?;
        self.run_registered(registration, shutdown).await
    }

    /// Renews an already-published registration until shutdown.
    pub async fn run_registered(
        &self,
        registration: NodeRegistration,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<(), NodeRegistryError> {
        let session = registration.session;
        loop {
            if *shutdown.borrow() {
                return session.close().await.map_err(NodeRegistryError::Store);
            }
            let deadline = self
                .monotonic_clock
                .now()
                .saturating_add(self.settings.keepalive_interval);
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return session.close().await.map_err(NodeRegistryError::Store);
                    }
                }
                () = self.monotonic_clock.sleep_until(deadline) => {
                    if let Err(error) = session.keep_alive().await {
                        return close_after_error(session, error.into()).await;
                    }
                    if let Err(error) = self.reconcile_once(session.as_ref()).await {
                        return close_after_error(session, error).await;
                    }
                }
            }
        }
    }

    fn validate_liveness(&self, stored: Option<&StoredValue>) -> Result<(), NodeRegistryError> {
        let Some(stored) = stored else {
            return Ok(());
        };
        let value = std::str::from_utf8(&stored.value).map_err(|error| {
            NodeRegistryError::MalformedLiveness {
                message: error.to_string(),
            }
        })?;
        let instance_id =
            NodeInstanceId::new(value).map_err(|error| NodeRegistryError::MalformedLiveness {
                message: error.to_string(),
            })?;
        if instance_id == self.settings.instance_id {
            Ok(())
        } else {
            Err(NodeRegistryError::DuplicateLiveInstance { instance_id })
        }
    }

    fn desired_node(&self, stored: Option<&StoredValue>) -> Result<Node, NodeRegistryError> {
        let now = self.status_clock.now();
        let mut node = match stored {
            Some(stored) => decode_node(stored, &self.settings.node_id)?,
            None => Node {
                meta: ObjectMeta {
                    id: self.settings.node_id.clone(),
                    labels: BTreeMap::new(),
                    annotations: BTreeMap::new(),
                    revision: ResourceRevision::default(),
                    generation: Generation(1),
                    owner_refs: Vec::new(),
                    finalizers: BTreeSet::new(),
                    deletion_timestamp: None,
                },
                spec: self.settings.node_spec.clone(),
                status: NodeStatus {
                    instance_id: self.settings.instance_id.clone(),
                    version: self.settings.running_version.to_string(),
                    last_seen: now,
                    conditions: Vec::new(),
                },
            },
        };
        if node.meta.deletion_timestamp.is_some() {
            return Err(NodeRegistryError::NodeDeleting);
        }
        if !same_definition(&node.spec, &self.settings.node_spec) {
            return Err(NodeRegistryError::DefinitionConflict);
        }
        if let Some(stored) = stored {
            node.meta.revision = stored.version.resource_revision();
        }
        node.status.instance_id = self.settings.instance_id.clone();
        node.status.version = self.settings.running_version.to_string();
        node.status.last_seen = now;
        Ok(node)
    }
}

fn same_definition(current: &NodeSpec, desired: &NodeSpec) -> bool {
    current.hostname == desired.hostname
        && current.host_address == desired.host_address
        && current.role == desired.role
}

fn expected(stored: Option<&StoredValue>) -> ExpectedVersion {
    stored.map_or(ExpectedVersion::Missing, |stored| {
        ExpectedVersion::Exact(stored.version)
    })
}

fn decode_node(stored: &StoredValue, node_id: &NodeId) -> Result<Node, NodeRegistryError> {
    if stored.value.len() > MAX_NODE_BYTES {
        return Err(NodeRegistryError::MalformedNode {
            message: format!("document exceeds {MAX_NODE_BYTES} bytes"),
        });
    }
    let node: Node = serde_json::from_slice(&stored.value).map_err(|error| {
        NodeRegistryError::MalformedNode {
            message: error.to_string(),
        }
    })?;
    if &node.meta.id == node_id {
        Ok(node)
    } else {
        Err(NodeRegistryError::NodeIdentityMismatch {
            actual: node.meta.id,
        })
    }
}

fn encode_node(node: &Node) -> Result<Vec<u8>, NodeRegistryError> {
    let encoded = serde_json::to_vec(node).map_err(|error| NodeRegistryError::SerializeNode {
        message: error.to_string(),
    })?;
    if encoded.len() > MAX_NODE_BYTES {
        Err(NodeRegistryError::SerializeNode {
            message: format!("document exceeds {MAX_NODE_BYTES} bytes"),
        })
    } else {
        Ok(encoded)
    }
}

async fn close_after_error<T>(
    session: Box<dyn Session>,
    error: NodeRegistryError,
) -> Result<T, NodeRegistryError> {
    match session.close().await {
        Ok(()) => Err(error),
        Err(close_error) => Err(NodeRegistryError::SessionRollback {
            operation: error.to_string(),
            rollback: close_error.to_string(),
        }),
    }
}

/// Node resource, liveness ownership, or session lifecycle failure.
#[derive(Debug, thiserror::Error)]
pub enum NodeRegistryError {
    /// Lease timing settings were unsafe.
    #[error(transparent)]
    Settings(#[from] NodeRegistrySettingsError),
    /// The fixed Node resource kind or name was invalid.
    #[error(transparent)]
    InvalidIdentifier(#[from] InvalidIdentifier),
    /// Store access or session maintenance failed.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// The durable Node document could not be decoded safely.
    #[error("malformed local Node resource: {message}")]
    MalformedNode { message: String },
    /// The stored typed identity did not match the local node key.
    #[error("stored Node identity `{actual}` does not match the local node")]
    NodeIdentityMismatch { actual: NodeId },
    /// Protected topology disagreed with an existing durable node definition.
    #[error("stored Node definition conflicts with the protected cluster topology")]
    DefinitionConflict,
    /// A deleting node must not be silently re-registered.
    #[error("local Node resource is being deleted")]
    NodeDeleting,
    /// The session-bound liveness value was not a valid daemon identity.
    #[error("malformed local node liveness: {message}")]
    MalformedLiveness { message: String },
    /// Another daemon still owns the node's live lease.
    #[error("node is already live under daemon instance `{instance_id}`")]
    DuplicateLiveInstance { instance_id: NodeInstanceId },
    /// Repeated concurrent writes prevented an atomic heartbeat.
    #[error("node registry publication remained contended after bounded retries")]
    Contention,
    /// The durable Node document could not be encoded within its bound.
    #[error("failed to serialize local Node resource: {message}")]
    SerializeNode { message: String },
    /// Registration or renewal failed and the newly owned session also failed to close.
    #[error("node registry failed: {operation}; session rollback failed: {rollback}")]
    SessionRollback { operation: String, rollback: String },
}
