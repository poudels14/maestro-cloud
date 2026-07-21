use std::sync::Arc;
use std::time::Duration;

use kernel_api::{
    ClusterId, NodeId, NodeInstanceId, ResourceKind, ResourceName, UpgradePhase, UpgradeRun,
};
use kernel_store::{
    CasOutcome, Clock, DeleteRequest, ExpectedVersion, Keyspace, PutRequest, Store, StoredValue,
};
use semver::Version;
use tokio::sync::watch;

use crate::{
    NixosUpgradeStager, NixosUpgradeStagingError, NodeRebooter, NodeUpgradeCommand,
    NodeUpgradeCommandFailure, NodeUpgradeCommandState,
};

const MAX_COMMAND_BYTES: usize = 16 * 1_024;
const MAX_RUN_BYTES: usize = 256 * 1_024;

/// Static identity and polling policy for one node's upgrade command agent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeUpgradeAgentSettings {
    /// Cluster containing the command and owning UpgradeRun.
    pub cluster_id: ClusterId,
    /// Stable node allowed to consume the command key.
    pub node_id: NodeId,
    /// Identity of this exact daemon process.
    pub instance_id: NodeInstanceId,
    /// Semantic version reported by this daemon process.
    pub running_version: Version,
    /// Level-triggered command resync interval.
    pub resync_interval: Duration,
}

impl NodeUpgradeAgentSettings {
    /// Rejects a zero interval that would create a hot command loop.
    pub fn validate(self) -> Result<Self, NodeUpgradeAgentSettingsError> {
        if self.resync_interval.is_zero() {
            Err(NodeUpgradeAgentSettingsError::ZeroResyncInterval)
        } else {
            Ok(self)
        }
    }
}

/// Invalid node upgrade agent timing policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum NodeUpgradeAgentSettingsError {
    /// A zero resync interval would continuously poll the store.
    #[error("node upgrade agent resync interval must be greater than zero")]
    ZeroResyncInterval,
}

/// Result of one bounded node-local command reconciliation pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeUpgradeAgentAction {
    /// This node has no upgrade command.
    Idle,
    /// The current command already agrees with its observed host state.
    Waiting,
    /// A boot generation was staged and acknowledged without releasing reboot.
    Staged,
    /// The released reboot was accepted or already observed under a new identity.
    RestartAccepted,
    /// A stale or terminal command was removed without host mutation.
    Cleared,
    /// A matchable node-local failure was persisted for the active leader.
    Failed,
    /// Concurrent state won; the level-triggered loop will read it again.
    Conflict,
}

/// Store-driven node-local executor for staged NixOS upgrade commands.
pub struct NodeUpgradeAgent {
    store: Arc<dyn Store>,
    keys: Keyspace,
    command_key: kernel_store::StoreKey,
    run_kind: ResourceKind,
    settings: NodeUpgradeAgentSettings,
    stager: Arc<dyn NixosUpgradeStager>,
    rebooter: Arc<dyn NodeRebooter>,
    clock: Arc<dyn Clock>,
}

impl NodeUpgradeAgent {
    /// Binds one node identity to its durable command and host mutation seams.
    pub fn new(
        store: Arc<dyn Store>,
        settings: NodeUpgradeAgentSettings,
        stager: Arc<dyn NixosUpgradeStager>,
        rebooter: Arc<dyn NodeRebooter>,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, NodeUpgradeAgentError> {
        let settings = settings.validate()?;
        let keys = Keyspace::new(&settings.cluster_id);
        Ok(Self {
            command_key: keys.node_upgrade_command(&settings.node_id),
            run_kind: ResourceKind::new("UpgradeRun")?,
            store,
            keys,
            settings,
            stager,
            rebooter,
            clock,
        })
    }

    /// Converges one command without waiting for the next resync interval.
    pub async fn reconcile_once(&self) -> Result<NodeUpgradeAgentAction, NodeUpgradeAgentError> {
        let Some(stored) = self.store.get(&self.command_key).await? else {
            return Ok(NodeUpgradeAgentAction::Idle);
        };
        let command = decode_command(&stored, &self.settings.node_id)?;
        let run = self.load_run(&command).await?;
        if should_clear(&command, run.as_ref()) {
            return self.delete(stored).await;
        }
        if !command_is_active(&command, run.as_ref()) {
            return Ok(NodeUpgradeAgentAction::Waiting);
        }
        match command.state {
            NodeUpgradeCommandState::Requested => self.stage(stored, command).await,
            NodeUpgradeCommandState::Staged | NodeUpgradeCommandState::Restarting => {
                Ok(NodeUpgradeAgentAction::Waiting)
            }
            NodeUpgradeCommandState::Released => self.restart(stored, command).await,
            NodeUpgradeCommandState::Failed => Ok(NodeUpgradeAgentAction::Waiting),
        }
    }

    /// Runs level-triggered command reconciliation until shutdown.
    pub async fn run(
        &self,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<(), NodeUpgradeAgentError> {
        loop {
            if *shutdown.borrow() || shutdown.has_changed().is_err() {
                return Ok(());
            }
            self.reconcile_once().await?;
            let deadline = self
                .clock
                .now()
                .saturating_add(self.settings.resync_interval);
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return Ok(());
                    }
                }
                () = self.clock.sleep_until(deadline) => {}
            }
        }
    }

    async fn stage(
        &self,
        stored: StoredValue,
        command: NodeUpgradeCommand,
    ) -> Result<NodeUpgradeAgentAction, NodeUpgradeAgentError> {
        let target = parse_target(&command)?;
        if self.settings.running_version < target {
            match self.stager.stage(&target).await {
                Ok(source)
                    if source.version() > &self.settings.running_version
                        && source.version() >= &target => {}
                Ok(source) => {
                    return self
                        .persist_staging_failure(
                            stored,
                            command,
                            NixosUpgradeStagingError::Rejected {
                                message: format!(
                                    "stager returned source {} for running {} and requested {target}",
                                    source.version(), self.settings.running_version
                                ),
                            },
                        )
                        .await;
                }
                Err(error) => {
                    return self.persist_staging_failure(stored, command, error).await;
                }
            }
        }
        self.transition(
            stored,
            command,
            NodeUpgradeCommandState::Staged,
            None,
            NodeUpgradeAgentAction::Staged,
        )
        .await
    }

    async fn restart(
        &self,
        stored: StoredValue,
        command: NodeUpgradeCommand,
    ) -> Result<NodeUpgradeAgentAction, NodeUpgradeAgentError> {
        let result = if self.settings.instance_id == command.previous_instance_id {
            self.rebooter.reboot().await
        } else {
            Ok(())
        };
        match result {
            Ok(()) => {
                self.transition(
                    stored,
                    command,
                    NodeUpgradeCommandState::Restarting,
                    None,
                    NodeUpgradeAgentAction::RestartAccepted,
                )
                .await
            }
            Err(error) => {
                self.transition(
                    stored,
                    command,
                    NodeUpgradeCommandState::Failed,
                    Some(NodeUpgradeCommandFailure::Unavailable {
                        message: error.to_string(),
                    }),
                    NodeUpgradeAgentAction::Failed,
                )
                .await
            }
        }
    }

    async fn persist_staging_failure(
        &self,
        stored: StoredValue,
        command: NodeUpgradeCommand,
        error: NixosUpgradeStagingError,
    ) -> Result<NodeUpgradeAgentAction, NodeUpgradeAgentError> {
        let failure = match error {
            NixosUpgradeStagingError::Unavailable { message } => {
                NodeUpgradeCommandFailure::Unavailable { message }
            }
            NixosUpgradeStagingError::Rejected { message } => {
                NodeUpgradeCommandFailure::Rejected { message }
            }
        };
        self.transition(
            stored,
            command,
            NodeUpgradeCommandState::Failed,
            Some(failure),
            NodeUpgradeAgentAction::Failed,
        )
        .await
    }

    async fn transition(
        &self,
        stored: StoredValue,
        mut command: NodeUpgradeCommand,
        target: NodeUpgradeCommandState,
        failure: Option<NodeUpgradeCommandFailure>,
        action: NodeUpgradeAgentAction,
    ) -> Result<NodeUpgradeAgentAction, NodeUpgradeAgentError> {
        if !command.state.can_transition_to(target) {
            return Err(NodeUpgradeAgentError::InvalidTransition {
                from: command.state,
                to: target,
            });
        }
        command.state = target;
        command.failure = failure;
        let outcome = self
            .store
            .put_cas(PutRequest {
                key: self.command_key.clone(),
                value: serde_json::to_vec(&command).map_err(serialize)?,
                expected: ExpectedVersion::Exact(stored.version),
                session: None,
            })
            .await?;
        Ok(if matches!(outcome, CasOutcome::Applied(_)) {
            action
        } else {
            NodeUpgradeAgentAction::Conflict
        })
    }

    async fn delete(
        &self,
        stored: StoredValue,
    ) -> Result<NodeUpgradeAgentAction, NodeUpgradeAgentError> {
        let outcome = self
            .store
            .delete_cas(DeleteRequest {
                key: self.command_key.clone(),
                expected: stored.version,
            })
            .await?;
        Ok(if matches!(outcome, CasOutcome::Applied(_)) {
            NodeUpgradeAgentAction::Cleared
        } else {
            NodeUpgradeAgentAction::Conflict
        })
    }

    async fn load_run(
        &self,
        command: &NodeUpgradeCommand,
    ) -> Result<Option<UpgradeRun>, NodeUpgradeAgentError> {
        let key = self
            .keys
            .resource(&self.run_kind, &ResourceName::from(command.run_id.clone()));
        let Some(stored) = self.store.get(&key).await? else {
            return Ok(None);
        };
        if stored.value.len() > MAX_RUN_BYTES {
            return Err(NodeUpgradeAgentError::MalformedRun {
                run_id: command.run_id.clone(),
                message: format!("resource exceeds {MAX_RUN_BYTES} bytes"),
            });
        }
        serde_json::from_slice(&stored.value)
            .map(Some)
            .map_err(|error| NodeUpgradeAgentError::MalformedRun {
                run_id: command.run_id.clone(),
                message: error.to_string(),
            })
    }
}

fn decode_command(
    stored: &StoredValue,
    node_id: &NodeId,
) -> Result<NodeUpgradeCommand, NodeUpgradeAgentError> {
    if stored.value.len() > MAX_COMMAND_BYTES {
        return Err(NodeUpgradeAgentError::MalformedCommand {
            message: format!("command exceeds {MAX_COMMAND_BYTES} bytes"),
        });
    }
    let command: NodeUpgradeCommand = serde_json::from_slice(&stored.value).map_err(|error| {
        NodeUpgradeAgentError::MalformedCommand {
            message: error.to_string(),
        }
    })?;
    if &command.node_id != node_id {
        return Err(NodeUpgradeAgentError::MalformedCommand {
            message: format!(
                "command for node `{}` was stored under node `{node_id}`",
                command.node_id
            ),
        });
    }
    let failure_matches_state =
        (command.state == NodeUpgradeCommandState::Failed) == command.failure.is_some();
    if !failure_matches_state {
        return Err(NodeUpgradeAgentError::MalformedCommand {
            message: "command has inconsistent failure state".to_string(),
        });
    }
    Ok(command)
}

fn command_is_active(command: &NodeUpgradeCommand, run: Option<&UpgradeRun>) -> bool {
    let Some(run) = run else {
        return false;
    };
    if run.meta.deletion_timestamp.is_some() || run.spec.target_version != command.target_version {
        return false;
    }
    matches!(
        run.status.phase,
        UpgradePhase::Applying | UpgradePhase::Restarting
    ) && run.status.nodes.iter().any(|node| {
        node.node_id == command.node_id
            && node.previous_instance_id.as_ref() == Some(&command.previous_instance_id)
            && matches!(
                node.phase,
                UpgradePhase::Applying | UpgradePhase::Restarting
            )
    })
}

fn should_clear(command: &NodeUpgradeCommand, run: Option<&UpgradeRun>) -> bool {
    let Some(run) = run else {
        return true;
    };
    if run.meta.deletion_timestamp.is_some()
        || run.spec.target_version != command.target_version
        || matches!(
            run.status.phase,
            UpgradePhase::Verifying
                | UpgradePhase::Completed
                | UpgradePhase::Failed
                | UpgradePhase::Canceled
        )
    {
        return true;
    }
    !run.status.nodes.iter().any(|node| {
        node.node_id == command.node_id
            && node.previous_instance_id.as_ref() == Some(&command.previous_instance_id)
            && matches!(
                node.phase,
                UpgradePhase::Applying | UpgradePhase::Restarting
            )
    })
}

fn parse_target(command: &NodeUpgradeCommand) -> Result<Version, NodeUpgradeAgentError> {
    Version::parse(command.target_version.trim()).map_err(|error| {
        NodeUpgradeAgentError::MalformedCommand {
            message: format!(
                "invalid target version `{}`: {error}",
                command.target_version
            ),
        }
    })
}

fn serialize(error: serde_json::Error) -> NodeUpgradeAgentError {
    NodeUpgradeAgentError::SerializeCommand {
        message: error.to_string(),
    }
}

/// Matchable node-local upgrade command failure.
#[derive(Debug, thiserror::Error)]
pub enum NodeUpgradeAgentError {
    /// Static resync policy was invalid.
    #[error(transparent)]
    Settings(#[from] NodeUpgradeAgentSettingsError),
    /// A fixed internal resource identifier was invalid.
    #[error(transparent)]
    Identifier(#[from] kernel_api::InvalidIdentifier),
    /// Cluster persistence was unavailable.
    #[error(transparent)]
    Store(#[from] kernel_store::StoreError),
    /// The node command could not be decoded or did not match its key.
    #[error("node upgrade command is malformed: {message}")]
    MalformedCommand { message: String },
    /// The owning UpgradeRun could not be decoded safely.
    #[error("upgrade run `{run_id}` is malformed: {message}")]
    MalformedRun {
        run_id: kernel_api::UpgradeRunId,
        message: String,
    },
    /// An internal command could not be encoded for CAS persistence.
    #[error("node upgrade command could not be serialized: {message}")]
    SerializeCommand { message: String },
    /// Persisted command state attempted an invalid protocol transition.
    #[error("invalid node upgrade command transition from {from:?} to {to:?}")]
    InvalidTransition {
        from: NodeUpgradeCommandState,
        to: NodeUpgradeCommandState,
    },
}
