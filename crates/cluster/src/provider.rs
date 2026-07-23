use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{Debug, Formatter};
use std::net::Ipv4Addr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use kernel_api::{ClusterId, NodeId, SecretValue};
use kernel_store::{MonotonicTime, Store, StoreError};
use serde::{Deserialize, Serialize};

use crate::{ClusterPorts, NodeCertificateBundle};

/// Provider-neutral address of one control-plane store member.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct StoreMember {
    /// Stable cluster node identity.
    pub node_id: NodeId,
    /// Private host address used for client and membership traffic.
    pub host_address: Ipv4Addr,
}

/// Narrow configuration view supplied to one store provider instance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreProviderConfig {
    cluster_id: ClusterId,
    local_member: StoreMember,
    known_members: BTreeMap<NodeId, StoreMember>,
    client_port: u16,
    membership_port: u16,
    data_directory: PathBuf,
    store_encryption_secret: SecretValue,
    security: NodeCertificateBundle,
}

impl StoreProviderConfig {
    /// Validates provider inputs without starting or mutating a backend.
    pub fn new(
        cluster_id: ClusterId,
        local_member: StoreMember,
        known_members: BTreeMap<NodeId, StoreMember>,
        ports: ClusterPorts,
        data_directory: PathBuf,
        store_encryption_secret: SecretValue,
        security: NodeCertificateBundle,
    ) -> Result<Self, StoreProviderError> {
        Self::new_with_address_validator(
            cluster_id,
            local_member,
            known_members,
            ports,
            data_directory,
            store_encryption_secret,
            security,
            valid_private_host_address,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_with_address_validator(
        cluster_id: ClusterId,
        local_member: StoreMember,
        known_members: BTreeMap<NodeId, StoreMember>,
        ports: ClusterPorts,
        data_directory: PathBuf,
        store_encryption_secret: SecretValue,
        security: NodeCertificateBundle,
        validate_host_address: fn(Ipv4Addr) -> bool,
    ) -> Result<Self, StoreProviderError> {
        ports
            .validate()
            .map_err(|error| StoreProviderError::InvalidConfiguration {
                reason: error.to_string(),
            })?;
        if !matches!(known_members.len(), 1 | 3) {
            return Err(StoreProviderError::InvalidConfiguration {
                reason: "store provider requires exactly one or three declared members".to_owned(),
            });
        }
        let Some(configured_local) = known_members.get(&local_member.node_id) else {
            return Err(StoreProviderError::InvalidConfiguration {
                reason: "local member is absent from known members".to_owned(),
            });
        };
        if configured_local != &local_member {
            return Err(StoreProviderError::InvalidConfiguration {
                reason: "local member differs from its known-member entry".to_owned(),
            });
        }
        if known_members.iter().any(|(node_id, member)| {
            node_id != &member.node_id || !validate_host_address(member.host_address)
        }) {
            return Err(StoreProviderError::InvalidConfiguration {
                reason: "store member IDs must match their keys and addresses must be private"
                    .to_owned(),
            });
        }
        let unique_addresses = known_members
            .values()
            .map(|member| member.host_address)
            .collect::<BTreeSet<_>>();
        if unique_addresses.len() != known_members.len() {
            return Err(StoreProviderError::InvalidConfiguration {
                reason: "store member addresses must be unique".to_owned(),
            });
        }
        if ports.store_client == ports.store_peer {
            return Err(StoreProviderError::InvalidConfiguration {
                reason: "store client and membership ports must be distinct".to_owned(),
            });
        }
        if data_directory.as_os_str().is_empty() {
            return Err(StoreProviderError::InvalidConfiguration {
                reason: "store data directory cannot be empty".to_owned(),
            });
        }
        if store_encryption_secret.expose().chars().count() < 32 {
            return Err(StoreProviderError::InvalidConfiguration {
                reason: "store encryption secret must contain at least 32 characters".to_owned(),
            });
        }
        Ok(Self {
            cluster_id,
            local_member,
            known_members,
            client_port: ports.store_client,
            membership_port: ports.store_peer,
            data_directory,
            store_encryption_secret,
            security,
        })
    }

    /// Returns the stable cluster identity.
    pub fn cluster_id(&self) -> &ClusterId {
        &self.cluster_id
    }

    /// Returns the member owned by this provider instance.
    pub fn local_member(&self) -> &StoreMember {
        &self.local_member
    }

    /// Returns all declared control-plane members in stable identity order.
    pub fn known_members(&self) -> &BTreeMap<NodeId, StoreMember> {
        &self.known_members
    }

    /// Returns the provider client port fixed during cluster initialization.
    pub fn client_port(&self) -> u16 {
        self.client_port
    }

    /// Returns the provider membership port fixed during cluster initialization.
    pub fn membership_port(&self) -> u16 {
        self.membership_port
    }

    /// Returns the provider-owned local persistence directory.
    pub fn data_directory(&self) -> &Path {
        &self.data_directory
    }

    /// Returns the cluster-wide master secret used only for store value protection.
    pub fn store_encryption_secret(&self) -> &SecretValue {
        &self.store_encryption_secret
    }

    /// Returns the node identity and cluster trust root for mutual TLS.
    pub fn security(&self) -> &NodeCertificateBundle {
        &self.security
    }
}

/// Opaque backend data needed to start a newly staged member.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct StoreJoinTicket {
    node_id: NodeId,
    provider_data: String,
}

impl StoreJoinTicket {
    /// Returns the node identity bound to this one-time membership plan.
    pub fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    /// Encodes provider-owned membership data without exposing its schema.
    pub fn from_provider_data(node_id: NodeId, provider_data: &[u8]) -> Self {
        Self {
            node_id,
            provider_data: BASE64.encode(provider_data),
        }
    }

    /// Decodes the opaque bytes for the provider that issued this ticket.
    pub fn provider_data(&self) -> Result<Vec<u8>, StoreProviderError> {
        BASE64
            .decode(&self.provider_data)
            .map_err(|_| StoreProviderError::InvalidJoinTicket)
    }
}

impl Debug for StoreJoinTicket {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StoreJoinTicket")
            .field("node_id", &self.node_id)
            .field("provider_data", &"[OPAQUE]")
            .finish()
    }
}

/// How the provider should open its local persisted member.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoreStartMode {
    /// Create the first member of a new cluster.
    Bootstrap,
    /// Start a member previously staged by the active provider.
    Join(StoreJoinTicket),
    /// Reopen an already initialized local member without changing membership.
    Restart,
}

/// Provider-neutral membership state visible to the formation coordinator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum MemberState {
    /// Membership is reserved while the new member catches up.
    Staged,
    /// Membership participates fully in cluster progress.
    Active,
}

/// Result of staging or activating one member.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MemberActivation {
    /// Stable node whose membership was observed.
    pub node_id: NodeId,
    /// Current provider-neutral state.
    pub state: MemberState,
}

/// Explicit operator authorization for destructive membership recovery.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StoreRecoveryPermit {
    /// Cluster whose persisted state may be recovered.
    cluster_id: ClusterId,
    /// One surviving member whose local state is retained.
    retained_node: NodeId,
    /// Membership identities expected before the outage.
    expected_members: BTreeSet<NodeId>,
    /// Wall-clock issue time for operator audit logs.
    issued_at_unix_ms: i64,
}

impl StoreRecoveryPermit {
    /// Validates that recovery retains one member from a multi-member cluster.
    pub fn new(
        cluster_id: ClusterId,
        retained_node: NodeId,
        expected_members: BTreeSet<NodeId>,
        issued_at_unix_ms: i64,
    ) -> Result<Self, StoreProviderError> {
        if expected_members.len() != 3 || !expected_members.contains(&retained_node) {
            return Err(StoreProviderError::UnsafeRecovery {
                reason: "recovery requires one retained member from a three-member set".to_owned(),
            });
        }
        Ok(Self {
            cluster_id,
            retained_node,
            expected_members,
            issued_at_unix_ms,
        })
    }

    /// Returns the cluster authorized for recovery.
    pub fn cluster_id(&self) -> &ClusterId {
        &self.cluster_id
    }

    /// Returns the one member whose persisted data must be retained.
    pub fn retained_node(&self) -> &NodeId {
        &self.retained_node
    }

    /// Returns the membership set recorded before the outage.
    pub fn expected_members(&self) -> &BTreeSet<NodeId> {
        &self.expected_members
    }

    /// Returns the operator-audit issue time.
    pub fn issued_at_unix_ms(&self) -> i64 {
        self.issued_at_unix_ms
    }
}

/// Provider-neutral outcome of recovering persisted state on one member.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreRecoveryReport {
    /// Node whose persisted state became the recovered source of truth.
    pub retained_node: NodeId,
    /// Prior member identities that must join again through normal admission.
    pub members_to_rejoin: Vec<NodeId>,
}

/// Running recovered backend together with its membership reconciliation work.
pub struct StoreRecovery {
    /// Owned running store process or in-process backend.
    pub runtime: Box<dyn StoreRuntime>,
    /// Provider-neutral recovery result.
    pub report: StoreRecoveryReport,
}

/// Requested local process shutdown behavior.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoreShutdown {
    /// Ask the backend to exit and force it down at the monotonic deadline.
    Graceful { deadline: MonotonicTime },
    /// Stop immediately without waiting for backend cleanup.
    Immediate,
}

/// Owned lifetime of a started store backend.
#[async_trait]
pub trait StoreRuntime: Send + Sync {
    /// Returns the backend-neutral store used by all higher layers.
    fn store(&self) -> Arc<dyn Store>;

    /// Stops all provider-owned work and waits for process exit.
    ///
    /// Canceling a graceful shutdown can leave the backend running; dropping
    /// the runtime must still prevent an unowned backend process.
    async fn shutdown(self: Box<Self>, request: StoreShutdown) -> Result<(), StoreProviderError>;
}

/// Provisioning and membership seam for a cluster-state backend.
#[async_trait]
pub trait StoreProvider: Send + Sync {
    /// Starts local state in one explicit mode and returns an owned runtime.
    ///
    /// Cancellation before success must not leave an unowned backend process.
    async fn start(
        &self,
        mode: StoreStartMode,
    ) -> Result<Box<dyn StoreRuntime>, StoreProviderError>;

    /// Reserves membership for an admitted control-plane node.
    ///
    /// Repeating an identical request is idempotent. A different identity at
    /// the same address is a conflict.
    async fn stage_member(
        &self,
        member: StoreMember,
    ) -> Result<(StoreJoinTicket, MemberActivation), StoreProviderError>;

    /// Promotes a caught-up staged member to active membership.
    async fn activate_member(
        &self,
        ticket: &StoreJoinTicket,
    ) -> Result<MemberActivation, StoreProviderError>;

    /// Removes one non-local member while refusing unsafe last-member removal.
    async fn remove_member(&self, node_id: &NodeId) -> Result<(), StoreProviderError>;

    /// Recovers persisted state under an explicit operator permit.
    ///
    /// Cancellation may leave recovery incomplete; retrying the same permit
    /// must converge without discarding the retained member's data.
    async fn recover(
        &self,
        permit: StoreRecoveryPermit,
    ) -> Result<StoreRecovery, StoreProviderError>;
}

/// Matchable store provisioning and membership failures.
#[derive(Debug, thiserror::Error)]
pub enum StoreProviderError {
    /// Provider configuration failed before external mutation.
    #[error("invalid store provider configuration: {reason}")]
    InvalidConfiguration { reason: String },
    /// A join ticket was malformed or belonged to another provider.
    #[error("invalid store provider join ticket")]
    InvalidJoinTicket,
    /// A requested membership operation conflicts with live state.
    #[error("store membership conflict: {reason}")]
    MembershipConflict { reason: String },
    /// A staged member has not caught up enough to activate safely.
    #[error("store member `{node_id}` is not ready for activation")]
    MemberNotReady { node_id: NodeId },
    /// An explicit recovery request failed safety validation.
    #[error("unsafe store recovery request: {reason}")]
    UnsafeRecovery { reason: String },
    /// A backend process could not be spawned or controlled.
    #[error("store backend lifecycle failed: {reason}")]
    Lifecycle { reason: String },
    /// The backend did not become ready or lost connectivity.
    #[error("store backend is unavailable: {reason}")]
    Unavailable { reason: String },
    /// The backend-neutral store connection failed.
    #[error("store connection failed: {0}")]
    Store(#[from] StoreError),
}

fn valid_private_host_address(address: Ipv4Addr) -> bool {
    address.is_private() && !address.is_loopback()
}
