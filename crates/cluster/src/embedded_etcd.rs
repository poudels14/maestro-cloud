use std::fmt::{Debug, Formatter};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use kernel_store::Clock;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    MemberActivation, StoreJoinTicket, StoreMember, StoreProvider, StoreProviderConfig,
    StoreProviderError, StoreRecovery, StoreRecoveryPermit, StoreRecoveryReport, StoreRejoin,
    StoreRuntime, StoreShutdown, StoreStartMode,
    embedded_etcd_files::materialize_security,
    embedded_etcd_membership::{
        activate_member, remove_member, stage_member, stage_recovered_local_member,
    },
    embedded_etcd_plan::{EtcdLaunchMode, EtcdStartPlan},
    embedded_etcd_process::{EtcdProcess, ReadinessDeadline, RunningEtcd, connect_store},
};

pub(crate) const LOCAL_STATE_FORMAT_VERSION: u8 = 1;

/// Time bounds used by embedded-etcd lifecycle and membership operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EmbeddedEtcdSettings {
    startup_timeout: Duration,
    operation_timeout: Duration,
    initial_retry_delay: Duration,
    maximum_retry_delay: Duration,
}

impl EmbeddedEtcdSettings {
    /// Creates bounded lifecycle settings.
    pub fn new(
        startup_timeout: Duration,
        operation_timeout: Duration,
        initial_retry_delay: Duration,
        maximum_retry_delay: Duration,
    ) -> Result<Self, StoreProviderError> {
        if startup_timeout.is_zero()
            || operation_timeout.is_zero()
            || initial_retry_delay.is_zero()
            || maximum_retry_delay < initial_retry_delay
        {
            return Err(StoreProviderError::InvalidConfiguration {
                reason: "embedded store timeouts must be non-zero and retry bounds ordered"
                    .to_owned(),
            });
        }
        Ok(Self {
            startup_timeout,
            operation_timeout,
            initial_retry_delay,
            maximum_retry_delay,
        })
    }

    /// Maximum time allowed for a local member to prove mode-specific readiness.
    pub fn startup_timeout(self) -> Duration {
        self.startup_timeout
    }

    /// Per-RPC connect and operation timeout.
    pub fn operation_timeout(self) -> Duration {
        self.operation_timeout
    }

    /// First readiness retry delay.
    pub fn initial_retry_delay(self) -> Duration {
        self.initial_retry_delay
    }

    /// Upper readiness retry delay bound.
    pub fn maximum_retry_delay(self) -> Duration {
        self.maximum_retry_delay
    }
}

impl Default for EmbeddedEtcdSettings {
    fn default() -> Self {
        Self {
            startup_timeout: Duration::from_secs(60),
            operation_timeout: Duration::from_secs(2),
            initial_retry_delay: Duration::from_millis(250),
            maximum_retry_delay: Duration::from_secs(2),
        }
    }
}

/// Production provider that owns an etcd child process and its membership API.
pub struct EmbeddedEtcdProvider {
    config: StoreProviderConfig,
    binary: PathBuf,
    clock: Arc<dyn Clock>,
    settings: EmbeddedEtcdSettings,
}

impl EmbeddedEtcdProvider {
    /// Creates a provider without touching disk or starting a process.
    pub fn new(
        config: StoreProviderConfig,
        binary: PathBuf,
        clock: Arc<dyn Clock>,
        settings: EmbeddedEtcdSettings,
    ) -> Result<Self, StoreProviderError> {
        if binary.as_os_str().is_empty() {
            return Err(StoreProviderError::InvalidConfiguration {
                reason: "embedded store binary path cannot be empty".to_owned(),
            });
        }
        Ok(Self {
            config,
            binary,
            clock,
            settings,
        })
    }

    async fn launch(
        &self,
        mode: EtcdLaunchMode,
    ) -> Result<Box<dyn StoreRuntime>, StoreProviderError> {
        prepare_local_state(&self.config, &mode)?;
        let readiness_deadline = match &mode {
            EtcdLaunchMode::Start(StoreStartMode::Restart) => ReadinessDeadline::Persistent,
            EtcdLaunchMode::Start(StoreStartMode::Bootstrap | StoreStartMode::Join(_))
            | EtcdLaunchMode::Recover => ReadinessDeadline::Bounded,
        };
        let security = materialize_security(&self.config)?;
        let plan = EtcdStartPlan::build(&self.config, mode, &security)?;
        let mut process =
            EtcdProcess::spawn(&self.binary, &plan, &self.config.local_member().node_id)?;
        if let Err(error) = process
            .wait_until_ready(
                &self.config,
                &plan,
                self.settings,
                readiness_deadline,
                self.clock.as_ref(),
            )
            .await
        {
            let _ = process
                .shutdown(StoreShutdown::Immediate, self.clock.as_ref())
                .await;
            return Err(error);
        }
        let store = match connect_store(&self.config, &plan.local_client_url).await {
            Ok(store) => store,
            Err(error) => {
                let _ = process
                    .shutdown(StoreShutdown::Immediate, self.clock.as_ref())
                    .await;
                return Err(error);
            }
        };
        Ok(Box::new(RunningEtcd::new(
            process,
            store,
            self.clock.clone(),
        )))
    }
}

impl Debug for EmbeddedEtcdProvider {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("EmbeddedEtcdProvider")
            .field("config", &self.config)
            .field("binary", &self.binary)
            .field("settings", &self.settings)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl StoreProvider for EmbeddedEtcdProvider {
    async fn start(
        &self,
        mode: StoreStartMode,
    ) -> Result<Box<dyn StoreRuntime>, StoreProviderError> {
        self.launch(EtcdLaunchMode::Start(mode)).await
    }

    async fn stage_member(
        &self,
        member: StoreMember,
    ) -> Result<(StoreJoinTicket, MemberActivation), StoreProviderError> {
        stage_member(&self.config, self.settings, member).await
    }

    async fn activate_member(
        &self,
        ticket: &StoreJoinTicket,
    ) -> Result<MemberActivation, StoreProviderError> {
        activate_member(&self.config, self.settings, ticket).await
    }

    async fn remove_member(&self, node_id: &kernel_api::NodeId) -> Result<(), StoreProviderError> {
        remove_member(&self.config, self.settings, node_id).await
    }

    async fn recover(
        &self,
        permit: StoreRecoveryPermit,
    ) -> Result<StoreRecovery, StoreProviderError> {
        validate_recovery(&self.config, &permit)?;
        let runtime = self.launch(EtcdLaunchMode::Recover).await?;
        let members_to_rejoin = permit
            .expected_members()
            .iter()
            .filter(|node_id| *node_id != permit.retained_node())
            .cloned()
            .collect();
        Ok(StoreRecovery {
            runtime,
            report: StoreRecoveryReport {
                retained_node: permit.retained_node().clone(),
                members_to_rejoin,
            },
        })
    }

    async fn rejoin_recovered(
        &self,
        permit: StoreRecoveryPermit,
        canonical_member: StoreMember,
    ) -> Result<StoreRejoin, StoreProviderError> {
        validate_rejoin(&self.config, &permit, &canonical_member)?;
        let ticket =
            stage_recovered_local_member(&self.config, self.settings, &canonical_member, &permit)
                .await?;
        prepare_recovered_rejoin_state(&self.config, &ticket)?;
        let runtime = self
            .launch(EtcdLaunchMode::Start(StoreStartMode::Join(ticket.clone())))
            .await?;
        Ok(StoreRejoin { runtime, ticket })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct LocalProviderState {
    pub(crate) format_version: u8,
    pub(crate) cluster_id: kernel_api::ClusterId,
    pub(crate) node_id: kernel_api::NodeId,
    pub(crate) initialization: LocalInitialization,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "kind", content = "digest")]
pub(crate) enum LocalInitialization {
    Bootstrap,
    Join(String),
    Restored(String),
}

pub(crate) fn prepare_local_state(
    config: &StoreProviderConfig,
    mode: &EtcdLaunchMode,
) -> Result<(), StoreProviderError> {
    let path = config.data_directory().join("provider-state.json");
    let existing = read_local_state(&path)?;
    let desired = match mode {
        EtcdLaunchMode::Start(StoreStartMode::Bootstrap) => Some(LocalInitialization::Bootstrap),
        EtcdLaunchMode::Start(StoreStartMode::Join(ticket)) => {
            let digest = Sha256::digest(ticket.provider_data()?);
            Some(LocalInitialization::Join(hex::encode(digest)))
        }
        EtcdLaunchMode::Start(StoreStartMode::Restart) | EtcdLaunchMode::Recover => None,
    };
    if let Some(existing) = existing {
        validate_existing_state(config, &existing)?;
        if let Some(desired) = desired
            && existing.initialization != desired
        {
            return Err(StoreProviderError::MembershipConflict {
                reason: "local provider state belongs to a different initialization".to_owned(),
            });
        }
        if matches!(mode, EtcdLaunchMode::Recover)
            && !config.data_directory().join("data/member").exists()
        {
            return Err(StoreProviderError::UnsafeRecovery {
                reason: "retained member data is absent".to_owned(),
            });
        }
        return Ok(());
    }

    let Some(initialization) = desired else {
        return Err(StoreProviderError::MembershipConflict {
            reason: "local provider state is absent; restart or recovery cannot infer membership"
                .to_owned(),
        });
    };
    std::fs::create_dir_all(config.data_directory()).map_err(|error| {
        StoreProviderError::Lifecycle {
            reason: format!(
                "failed to create provider directory `{}`: {error}",
                config.data_directory().display()
            ),
        }
    })?;
    let state = LocalProviderState {
        format_version: LOCAL_STATE_FORMAT_VERSION,
        cluster_id: config.cluster_id().clone(),
        node_id: config.local_member().node_id.clone(),
        initialization,
    };
    write_new_state(&path, &state)
}

fn validate_recovery(
    config: &StoreProviderConfig,
    permit: &StoreRecoveryPermit,
) -> Result<(), StoreProviderError> {
    let configured = config.known_members().keys().cloned().collect();
    if permit.cluster_id() != config.cluster_id()
        || permit.retained_node() != &config.local_member().node_id
        || permit.expected_members() != &configured
    {
        return Err(StoreProviderError::UnsafeRecovery {
            reason: "recovery permit differs from local cluster membership".to_owned(),
        });
    }
    Ok(())
}

fn validate_rejoin(
    config: &StoreProviderConfig,
    permit: &StoreRecoveryPermit,
    canonical_member: &StoreMember,
) -> Result<(), StoreProviderError> {
    let configured = config.known_members().keys().cloned().collect();
    if permit.cluster_id() != config.cluster_id()
        || permit.retained_node() != &canonical_member.node_id
        || permit.expected_members() != &configured
        || canonical_member.node_id == config.local_member().node_id
        || config.known_members().get(&canonical_member.node_id) != Some(canonical_member)
    {
        return Err(StoreProviderError::UnsafeRecovery {
            reason: "rejoin permit differs from local cluster membership".to_owned(),
        });
    }
    Ok(())
}

fn prepare_recovered_rejoin_state(
    config: &StoreProviderConfig,
    ticket: &StoreJoinTicket,
) -> Result<(), StoreProviderError> {
    let path = config.data_directory().join("provider-state.json");
    let desired = LocalProviderState {
        format_version: LOCAL_STATE_FORMAT_VERSION,
        cluster_id: config.cluster_id().clone(),
        node_id: config.local_member().node_id.clone(),
        initialization: LocalInitialization::Join(hex::encode(Sha256::digest(
            ticket.provider_data()?,
        ))),
    };
    if read_local_state(&path)?.as_ref() == Some(&desired) {
        return Ok(());
    }
    let data = config.data_directory().join("data");
    match std::fs::remove_dir_all(&data) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(StoreProviderError::Lifecycle {
                reason: format!(
                    "failed to discard obsolete member data `{}`: {error}",
                    data.display()
                ),
            });
        }
    }
    write_replacement_state(&path, &desired)
}

fn write_replacement_state(
    path: &Path,
    state: &LocalProviderState,
) -> Result<(), StoreProviderError> {
    let parent = path.parent().ok_or_else(|| StoreProviderError::Lifecycle {
        reason: "provider state path has no parent".to_owned(),
    })?;
    std::fs::create_dir_all(parent).map_err(|error| StoreProviderError::Lifecycle {
        reason: format!(
            "failed to create provider directory `{}`: {error}",
            parent.display()
        ),
    })?;
    let temporary = path.with_extension("tmp");
    let encoded =
        serde_json::to_vec_pretty(state).map_err(|error| StoreProviderError::Lifecycle {
            reason: format!("failed to encode local provider state: {error}"),
        })?;
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temporary)
        .map_err(|error| StoreProviderError::Lifecycle {
            reason: format!(
                "failed to create provider state `{}`: {error}",
                temporary.display()
            ),
        })?;
    file.write_all(&encoded)
        .and_then(|()| file.sync_all())
        .map_err(|error| StoreProviderError::Lifecycle {
            reason: format!(
                "failed to persist provider state `{}`: {error}",
                temporary.display()
            ),
        })?;
    std::fs::rename(&temporary, path).map_err(|error| StoreProviderError::Lifecycle {
        reason: format!(
            "failed to replace provider state `{}`: {error}",
            path.display()
        ),
    })?;
    std::fs::File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| StoreProviderError::Lifecycle {
            reason: format!(
                "failed to sync provider directory `{}`: {error}",
                parent.display()
            ),
        })
}

fn read_local_state(path: &Path) -> Result<Option<LocalProviderState>, StoreProviderError> {
    match std::fs::read(path) {
        Ok(bytes) => serde_json::from_slice(&bytes).map(Some).map_err(|error| {
            StoreProviderError::MembershipConflict {
                reason: format!("invalid local provider state `{}`: {error}", path.display()),
            }
        }),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(StoreProviderError::Lifecycle {
            reason: format!(
                "failed to read provider state `{}`: {error}",
                path.display()
            ),
        }),
    }
}

fn validate_existing_state(
    config: &StoreProviderConfig,
    state: &LocalProviderState,
) -> Result<(), StoreProviderError> {
    if state.format_version != LOCAL_STATE_FORMAT_VERSION
        || state.cluster_id != *config.cluster_id()
        || state.node_id != config.local_member().node_id
    {
        return Err(StoreProviderError::MembershipConflict {
            reason: "local provider state belongs to another cluster or node".to_owned(),
        });
    }
    Ok(())
}

pub(crate) fn write_new_state(
    path: &Path,
    state: &LocalProviderState,
) -> Result<(), StoreProviderError> {
    let encoded =
        serde_json::to_vec_pretty(state).map_err(|error| StoreProviderError::Lifecycle {
            reason: format!("failed to encode local provider state: {error}"),
        })?;
    let mut file = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .map_err(|error| StoreProviderError::Lifecycle {
            reason: format!(
                "failed to create provider state `{}`: {error}",
                path.display()
            ),
        })?;
    file.write_all(&encoded)
        .and_then(|()| file.sync_all())
        .map_err(|error| StoreProviderError::Lifecycle {
            reason: format!(
                "failed to persist provider state `{}`: {error}",
                path.display()
            ),
        })
}
