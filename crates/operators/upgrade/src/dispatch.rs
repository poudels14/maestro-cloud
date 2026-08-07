use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use kernel_api::{
    ClusterId, NodeId, NodeInstanceId, RESTART_TARGET_VERSION, UpgradeOperation, UpgradeRunId,
};
use kernel_controller::FencedStore;
use kernel_store::{
    Clock, Compare, ExpectedVersion, Keyspace, Mutation, StoreKey, Transaction, TransactionOutcome,
    Version,
};
use semver::Version as SemanticVersion;
use serde::{Deserialize, Serialize};

use crate::{NodeUpgradeBackend, NodeUpgradeBackendError, NodeUpgradeRequest};

const MAX_COMMAND_BYTES: usize = 16 * 1_024;

/// Durable phase of one node's internal two-phase upgrade command.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum NodeUpgradeCommandState {
    /// The node may validate and stage its boot generation but must not reboot.
    Requested,
    /// The node staged successfully and is waiting for the complete batch.
    Staged,
    /// The active leader atomically released every target in the batch.
    Released,
    /// The node accepted its reboot and is expected to return with a new identity.
    Restarting,
    /// Node-local staging or reboot policy failed before restart acceptance.
    Failed,
}

impl NodeUpgradeCommandState {
    /// Whether the internal two-phase protocol permits this durable transition.
    pub fn can_transition_to(self, target: Self) -> bool {
        self == target
            || matches!(
                (self, target),
                (Self::Requested, Self::Staged | Self::Failed)
                    | (Self::Staged, Self::Released | Self::Failed)
                    | (Self::Released, Self::Restarting | Self::Failed)
            )
    }
}

/// Matchable node-local failure retained for the leader's dispatch outcome.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "kind")]
pub enum NodeUpgradeCommandFailure {
    /// A transient host or process failure permits a bounded operator retry.
    Unavailable { message: String },
    /// Host policy or source validation requires desired-state intervention.
    Rejected { message: String },
}

/// Internal per-node command written under the active leadership fence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NodeUpgradeCommand {
    /// Upgrade run owning this command.
    pub run_id: UpgradeRunId,
    /// Stable target node.
    pub node_id: NodeId,
    /// Whether this command stages an upgrade or only reboots the current generation.
    pub operation: UpgradeOperation,
    /// Minimum Maestro version, or the restart sentinel for restart-only commands.
    pub target_version: String,
    /// Daemon identity observed before the operator drained this node.
    pub previous_instance_id: NodeInstanceId,
    /// Planned recovery shared by the complete control-plane reboot batch.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub store_recovery: Option<crate::PlannedStoreRecovery>,
    /// Current stage of the leader-to-agent handshake.
    pub state: NodeUpgradeCommandState,
    /// Failure detail present exactly when `state` is `Failed`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure: Option<NodeUpgradeCommandFailure>,
}

impl NodeUpgradeCommand {
    fn requested(request: &NodeUpgradeRequest, target_index: usize) -> Option<Self> {
        let target = request.targets.get(target_index)?;
        Some(Self {
            run_id: request.run_id.clone(),
            node_id: target.node_id.clone(),
            operation: request.operation,
            target_version: request.target_version.clone(),
            previous_instance_id: target.previous_instance_id.clone(),
            store_recovery: request.store_recovery.clone(),
            state: NodeUpgradeCommandState::Requested,
            failure: None,
        })
    }

    fn matches_request(&self, desired: &Self) -> bool {
        self.run_id == desired.run_id
            && self.node_id == desired.node_id
            && self.operation == desired.operation
            && self.target_version == desired.target_version
            && self.previous_instance_id == desired.previous_instance_id
            && self.store_recovery == desired.store_recovery
    }
}

/// Bounded wait policy for durable node staging and reboot acceptance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StoreNodeUpgradeBackendSettings {
    /// Maximum time one dispatch call waits for the target batch to accept restart.
    pub acceptance_timeout: Duration,
    /// Poll cadence while agents stage or accept their released commands.
    pub poll_interval: Duration,
}

impl StoreNodeUpgradeBackendSettings {
    /// Rejects hot polling and a timeout too short to observe one interval.
    pub fn new(
        acceptance_timeout: Duration,
        poll_interval: Duration,
    ) -> Result<Self, StoreNodeUpgradeBackendSettingsError> {
        if acceptance_timeout.is_zero() || poll_interval.is_zero() {
            return Err(StoreNodeUpgradeBackendSettingsError::ZeroDuration);
        }
        if acceptance_timeout < poll_interval {
            return Err(StoreNodeUpgradeBackendSettingsError::TimeoutBeforePoll);
        }
        Ok(Self {
            acceptance_timeout,
            poll_interval,
        })
    }
}

/// Invalid timing policy for the store-backed upgrade dispatcher.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum StoreNodeUpgradeBackendSettingsError {
    /// Zero durations would disable progress or create a hot loop.
    #[error("node upgrade acceptance timeout and poll interval must be greater than zero")]
    ZeroDuration,
    /// At least one poll must fit inside the acceptance window.
    #[error("node upgrade acceptance timeout must include at least one poll interval")]
    TimeoutBeforePoll,
}

/// Leadership-fenced dispatcher for node-local two-phase upgrade commands.
pub struct StoreNodeUpgradeBackend {
    keys: Keyspace,
    store: Arc<FencedStore>,
    clock: Arc<dyn Clock>,
    settings: StoreNodeUpgradeBackendSettings,
}

impl StoreNodeUpgradeBackend {
    /// Binds command writes and the collective reboot release to one leadership term.
    pub fn new(
        cluster_id: &ClusterId,
        store: Arc<FencedStore>,
        clock: Arc<dyn Clock>,
        settings: StoreNodeUpgradeBackendSettings,
    ) -> Self {
        Self {
            keys: Keyspace::new(cluster_id),
            store,
            clock,
            settings,
        }
    }

    async fn dispatch(&self, request: &NodeUpgradeRequest) -> Result<(), NodeUpgradeBackendError> {
        let desired = validate_request(request)?;
        let deadline = self
            .clock
            .now()
            .saturating_add(self.settings.acceptance_timeout);
        loop {
            self.store
                .verify_leadership()
                .await
                .map_err(controller_unavailable)?;
            let snapshot = self.load(&desired).await?;
            reject_collisions(&snapshot, &desired)?;
            if let Some(failure) = first_failure(&snapshot) {
                self.clear(&snapshot).await?;
                return Err(command_failure(failure));
            }
            if snapshot.iter().all(|entry| {
                entry
                    .command
                    .as_ref()
                    .is_some_and(|command| command.state == NodeUpgradeCommandState::Restarting)
            }) {
                return Ok(());
            }
            let released = snapshot
                .iter()
                .filter(|entry| {
                    entry.command.as_ref().is_some_and(|command| {
                        matches!(
                            command.state,
                            NodeUpgradeCommandState::Released | NodeUpgradeCommandState::Restarting
                        )
                    })
                })
                .count();
            if released > 0 {
                if released != snapshot.len() {
                    return Err(NodeUpgradeBackendError::Rejected {
                        message: "node upgrade batch contains a partial collective release"
                            .to_string(),
                    });
                }
            } else if snapshot.iter().all(|entry| {
                entry
                    .command
                    .as_ref()
                    .is_some_and(|command| command.state == NodeUpgradeCommandState::Staged)
            }) {
                self.release(&snapshot).await?;
            } else {
                self.create_missing(&snapshot, &desired).await?;
            }
            let now = self.clock.now();
            if now >= deadline {
                return Err(NodeUpgradeBackendError::Unavailable {
                    message: format!(
                        "nodes did not stage and accept upgrade run `{}` before the timeout",
                        request.run_id
                    ),
                });
            }
            let poll_at = now
                .saturating_add(self.settings.poll_interval)
                .min(deadline);
            self.clock.sleep_until(poll_at).await;
        }
    }

    async fn load(
        &self,
        desired: &[NodeUpgradeCommand],
    ) -> Result<Vec<CommandEntry>, NodeUpgradeBackendError> {
        let mut entries = Vec::with_capacity(desired.len());
        for command in desired {
            let key = self.keys.node_upgrade_command(&command.node_id);
            let stored = self.store.get(&key).await.map_err(controller_unavailable)?;
            let (decoded, version) = match stored {
                Some(stored) => {
                    if stored.value.len() > MAX_COMMAND_BYTES {
                        return Err(NodeUpgradeBackendError::Rejected {
                            message: format!(
                                "node `{}` upgrade command exceeds {MAX_COMMAND_BYTES} bytes",
                                command.node_id
                            ),
                        });
                    }
                    let decoded = serde_json::from_slice(&stored.value).map_err(|error| {
                        NodeUpgradeBackendError::Rejected {
                            message: format!(
                                "node `{}` upgrade command is malformed: {error}",
                                command.node_id
                            ),
                        }
                    })?;
                    (Some(decoded), Some(stored.version))
                }
                None => (None, None),
            };
            entries.push(CommandEntry {
                key,
                command: decoded,
                version,
            });
        }
        Ok(entries)
    }

    async fn create_missing(
        &self,
        snapshot: &[CommandEntry],
        desired: &[NodeUpgradeCommand],
    ) -> Result<(), NodeUpgradeBackendError> {
        let mut transaction = Transaction {
            compares: Vec::with_capacity(snapshot.len()),
            mutations: Vec::new(),
        };
        for (entry, command) in snapshot.iter().zip(desired) {
            transaction.compares.push(Compare {
                key: entry.key.clone(),
                expected: entry
                    .version
                    .map_or(ExpectedVersion::Missing, ExpectedVersion::Exact),
            });
            if entry.command.is_none() {
                transaction.mutations.push(Mutation::Put {
                    key: entry.key.clone(),
                    value: encode(command)?,
                    session: None,
                });
            }
        }
        apply_transaction(&self.store, transaction, "create node upgrade commands").await
    }

    async fn release(&self, snapshot: &[CommandEntry]) -> Result<(), NodeUpgradeBackendError> {
        let mut transaction = Transaction {
            compares: Vec::with_capacity(snapshot.len()),
            mutations: Vec::with_capacity(snapshot.len()),
        };
        for entry in snapshot {
            let version = entry
                .version
                .ok_or_else(|| NodeUpgradeBackendError::Unavailable {
                    message: "node upgrade command disappeared before collective release"
                        .to_string(),
                })?;
            let mut command =
                entry
                    .command
                    .clone()
                    .ok_or_else(|| NodeUpgradeBackendError::Unavailable {
                        message: "node upgrade command disappeared before collective release"
                            .to_string(),
                    })?;
            command.state = NodeUpgradeCommandState::Released;
            command.failure = None;
            transaction.compares.push(Compare {
                key: entry.key.clone(),
                expected: ExpectedVersion::Exact(version),
            });
            transaction.mutations.push(Mutation::Put {
                key: entry.key.clone(),
                value: encode(&command)?,
                session: None,
            });
        }
        apply_transaction(&self.store, transaction, "release node upgrade batch").await
    }

    async fn clear(&self, snapshot: &[CommandEntry]) -> Result<(), NodeUpgradeBackendError> {
        let mut transaction = Transaction {
            compares: Vec::with_capacity(snapshot.len()),
            mutations: Vec::with_capacity(snapshot.len()),
        };
        for entry in snapshot {
            if let Some(version) = entry.version {
                transaction.compares.push(Compare {
                    key: entry.key.clone(),
                    expected: ExpectedVersion::Exact(version),
                });
                transaction.mutations.push(Mutation::Delete {
                    key: entry.key.clone(),
                });
            }
        }
        apply_transaction(&self.store, transaction, "clear failed node upgrade batch").await
    }
}

#[async_trait]
impl NodeUpgradeBackend for StoreNodeUpgradeBackend {
    async fn apply(&self, request: &NodeUpgradeRequest) -> Result<(), NodeUpgradeBackendError> {
        self.dispatch(request).await
    }
}

#[derive(Clone)]
struct CommandEntry {
    key: StoreKey,
    command: Option<NodeUpgradeCommand>,
    version: Option<Version>,
}

fn validate_request(
    request: &NodeUpgradeRequest,
) -> Result<Vec<NodeUpgradeCommand>, NodeUpgradeBackendError> {
    SemanticVersion::parse(request.target_version.trim()).map_err(|error| {
        NodeUpgradeBackendError::Rejected {
            message: format!(
                "invalid node upgrade target version `{}`: {error}",
                request.target_version
            ),
        }
    })?;
    if request.operation == UpgradeOperation::Restart
        && request.target_version != RESTART_TARGET_VERSION
    {
        return Err(NodeUpgradeBackendError::Rejected {
            message: format!("restart command target version must be `{RESTART_TARGET_VERSION}`"),
        });
    }
    if request.targets.is_empty() {
        return Err(NodeUpgradeBackendError::Rejected {
            message: "node upgrade dispatch has no targets".to_string(),
        });
    }
    let unique = request
        .targets
        .iter()
        .map(|target| &target.node_id)
        .collect::<BTreeSet<_>>();
    if unique.len() != request.targets.len() {
        return Err(NodeUpgradeBackendError::Rejected {
            message: "node upgrade dispatch contains duplicate targets".to_string(),
        });
    }
    (0..request.targets.len())
        .map(|index| {
            NodeUpgradeCommand::requested(request, index).ok_or_else(|| {
                NodeUpgradeBackendError::Rejected {
                    message: "node upgrade target index no longer resolves".to_string(),
                }
            })
        })
        .collect()
}

fn reject_collisions(
    snapshot: &[CommandEntry],
    desired: &[NodeUpgradeCommand],
) -> Result<(), NodeUpgradeBackendError> {
    for (entry, desired) in snapshot.iter().zip(desired) {
        if let Some(existing) = &entry.command
            && !existing.matches_request(desired)
        {
            return Err(NodeUpgradeBackendError::Rejected {
                message: format!(
                    "node `{}` already has an upgrade command owned by run `{}`",
                    existing.node_id, existing.run_id
                ),
            });
        }
        if let Some(existing) = &entry.command {
            let failure_matches_state =
                (existing.state == NodeUpgradeCommandState::Failed) == existing.failure.is_some();
            if !failure_matches_state {
                return Err(NodeUpgradeBackendError::Rejected {
                    message: format!(
                        "node `{}` upgrade command has inconsistent failure state",
                        existing.node_id
                    ),
                });
            }
        }
    }
    Ok(())
}

fn first_failure(snapshot: &[CommandEntry]) -> Option<NodeUpgradeCommandFailure> {
    snapshot.iter().find_map(|entry| {
        entry.command.as_ref().and_then(|command| {
            (command.state == NodeUpgradeCommandState::Failed)
                .then(|| command.failure.clone())
                .flatten()
        })
    })
}

fn command_failure(failure: NodeUpgradeCommandFailure) -> NodeUpgradeBackendError {
    match failure {
        NodeUpgradeCommandFailure::Unavailable { message } => {
            NodeUpgradeBackendError::Unavailable { message }
        }
        NodeUpgradeCommandFailure::Rejected { message } => {
            NodeUpgradeBackendError::Rejected { message }
        }
    }
}

fn encode(command: &NodeUpgradeCommand) -> Result<Vec<u8>, NodeUpgradeBackendError> {
    serde_json::to_vec(command).map_err(|error| NodeUpgradeBackendError::Rejected {
        message: format!("node upgrade command could not be encoded: {error}"),
    })
}

async fn apply_transaction(
    store: &FencedStore,
    transaction: Transaction,
    action: &str,
) -> Result<(), NodeUpgradeBackendError> {
    if transaction.mutations.is_empty() {
        return Ok(());
    }
    match store
        .txn(transaction)
        .await
        .map_err(controller_unavailable)?
    {
        TransactionOutcome::Applied { .. } => Ok(()),
        TransactionOutcome::Conflict => Err(NodeUpgradeBackendError::Unavailable {
            message: format!("concurrent state changed while attempting to {action}"),
        }),
    }
}

fn controller_unavailable(error: impl std::fmt::Display) -> NodeUpgradeBackendError {
    NodeUpgradeBackendError::Unavailable {
        message: error.to_string(),
    }
}
