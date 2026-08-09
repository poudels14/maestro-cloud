use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use async_trait::async_trait;
use kernel_api::{
    ClusterId, Generation, NodeId, NodeInstanceId, NodeUpgradeStatus, Object, ObjectMeta,
    RESTART_TARGET_VERSION, ResourceKind, ResourceName, ResourceRevision, UpgradeMode,
    UpgradeOperation, UpgradePhase, UpgradeRun, UpgradeRunId, UpgradeRunSpec, UpgradeRunStatus,
};
use kernel_store::{
    CasOutcome, Clock, ExpectedVersion, InMemoryStore, Keyspace, MonotonicTime, PutRequest, Store,
};
use semver::Version;

use crate::nixos::{
    NixosCommand, NixosCommandError, NixosCommandOutput, NixosCommandRunner, NixosUpgradeSource,
};
use crate::{
    NixosUpgradeStager, NixosUpgradeStagingError, NodeRebootError, NodeRebooter, NodeUpgradeAgent,
    NodeUpgradeAgentAction, NodeUpgradeAgentSettings, NodeUpgradeCommand,
    NodeUpgradeCommandFailure, NodeUpgradeCommandState, ProcessNodeRebooter,
};

#[tokio::test]
async fn agent_stages_then_reboots_only_after_collective_release()
-> Result<(), Box<dyn std::error::Error>> {
    let world = World::new(NodeInstanceId::new("instance-node-1")?).await?;

    assert_eq!(
        world.agent.reconcile_once().await?,
        NodeUpgradeAgentAction::Staged
    );
    assert_eq!(
        world.command().await?.state,
        NodeUpgradeCommandState::Staged
    );
    assert_eq!(world.stager.targets(), vec![Version::new(2, 0, 0)]);
    assert_eq!(world.rebooter.calls(), 0);

    world
        .set_command_state(NodeUpgradeCommandState::Released)
        .await?;
    assert_eq!(
        world.agent.reconcile_once().await?,
        NodeUpgradeAgentAction::RebootRequested
    );
    assert_eq!(
        world.command().await?.state,
        NodeUpgradeCommandState::Released
    );
    assert_eq!(world.rebooter.calls(), 1);
    Ok(())
}

#[tokio::test]
async fn restart_operation_skips_nixos_staging_and_reboots_after_release()
-> Result<(), Box<dyn std::error::Error>> {
    let world = World::new_without_staging(NodeInstanceId::new("instance-node-1")?).await?;
    world.set_operation(UpgradeOperation::Restart).await?;

    assert_eq!(
        world.agent.reconcile_once().await?,
        NodeUpgradeAgentAction::Staged
    );
    assert!(world.stager.targets().is_empty());

    world
        .set_command_state(NodeUpgradeCommandState::Released)
        .await?;
    assert_eq!(
        world.agent.reconcile_once().await?,
        NodeUpgradeAgentAction::RebootRequested
    );
    assert_eq!(
        world.command().await?.state,
        NodeUpgradeCommandState::Released
    );
    assert_eq!(world.rebooter.calls(), 1);
    Ok(())
}

#[tokio::test]
async fn upgrade_without_a_stager_fails_closed_before_reboot()
-> Result<(), Box<dyn std::error::Error>> {
    let world = World::new_without_staging(NodeInstanceId::new("instance-node-1")?).await?;

    assert_eq!(
        world.agent.reconcile_once().await?,
        NodeUpgradeAgentAction::Failed
    );
    let command = world.command().await?;
    assert_eq!(
        command.failure,
        Some(NodeUpgradeCommandFailure::Rejected {
            message: "NixOS upgrade staging is not configured".to_string(),
        })
    );
    assert_eq!(world.rebooter.calls(), 0);
    Ok(())
}

#[tokio::test]
async fn agent_persists_source_rejection_for_the_active_leader()
-> Result<(), Box<dyn std::error::Error>> {
    let world = World::new(NodeInstanceId::new("instance-node-1")?).await?;
    world.stager.fail(NixosUpgradeStagingError::Rejected {
        message: "source is stale".to_string(),
    });

    assert_eq!(
        world.agent.reconcile_once().await?,
        NodeUpgradeAgentAction::Failed
    );

    let command = world.command().await?;
    assert_eq!(command.state, NodeUpgradeCommandState::Failed);
    assert_eq!(
        command.failure,
        Some(NodeUpgradeCommandFailure::Rejected {
            message: "source is stale".to_string(),
        })
    );
    assert_eq!(world.rebooter.calls(), 0);
    Ok(())
}

#[tokio::test]
async fn agent_clears_a_canceled_command_without_host_mutation()
-> Result<(), Box<dyn std::error::Error>> {
    let world = World::new(NodeInstanceId::new("instance-node-1")?).await?;
    world.set_run_phase(UpgradePhase::Canceled).await?;

    assert_eq!(
        world.agent.reconcile_once().await?,
        NodeUpgradeAgentAction::Cleared
    );

    assert!(world.command_optional().await?.is_none());
    assert!(world.stager.targets().is_empty());
    assert_eq!(world.rebooter.calls(), 0);
    Ok(())
}

#[tokio::test]
async fn new_daemon_process_in_the_same_boot_reissues_the_reboot()
-> Result<(), Box<dyn std::error::Error>> {
    let world = World::new(NodeInstanceId::new("instance-node-1-new")?).await?;
    world
        .set_command_state(NodeUpgradeCommandState::Released)
        .await?;

    assert_eq!(
        world.agent.reconcile_once().await?,
        NodeUpgradeAgentAction::RebootRequested
    );

    assert_eq!(
        world.command().await?.state,
        NodeUpgradeCommandState::Released
    );
    assert_eq!(world.rebooter.calls(), 1);
    Ok(())
}

#[tokio::test]
async fn changed_boot_identity_acknowledges_the_reboot_without_repeating_it()
-> Result<(), Box<dyn std::error::Error>> {
    let world =
        World::new_with_boot_id(NodeInstanceId::new("instance-node-1-new")?, "boot-2").await?;
    world
        .set_command_state(NodeUpgradeCommandState::Released)
        .await?;

    assert_eq!(
        world.agent.reconcile_once().await?,
        NodeUpgradeAgentAction::RestartObserved
    );
    assert_eq!(
        world.command().await?.state,
        NodeUpgradeCommandState::Restarting
    );
    assert_eq!(world.rebooter.calls(), 0);
    Ok(())
}

#[tokio::test]
async fn agent_persists_reboot_failure_after_collective_release()
-> Result<(), Box<dyn std::error::Error>> {
    let world = World::new(NodeInstanceId::new("instance-node-1")?).await?;
    world
        .set_command_state(NodeUpgradeCommandState::Released)
        .await?;
    world.rebooter.fail("system manager rejected reboot");

    assert_eq!(
        world.agent.reconcile_once().await?,
        NodeUpgradeAgentAction::Failed
    );

    let command = world.command().await?;
    assert_eq!(command.state, NodeUpgradeCommandState::Failed);
    assert!(matches!(
        command.failure,
        Some(NodeUpgradeCommandFailure::Unavailable { .. })
    ));
    Ok(())
}

#[tokio::test]
async fn agent_collects_restart_command_after_the_run_begins_verification()
-> Result<(), Box<dyn std::error::Error>> {
    let world = World::new(NodeInstanceId::new("instance-node-1-new")?).await?;
    world
        .set_command_state(NodeUpgradeCommandState::Restarting)
        .await?;
    world.set_run_phase(UpgradePhase::Verifying).await?;

    assert_eq!(
        world.agent.reconcile_once().await?,
        NodeUpgradeAgentAction::Cleared
    );
    assert!(world.command_optional().await?.is_none());
    Ok(())
}

#[tokio::test]
async fn process_rebooter_executes_the_exact_bounded_systemctl_request()
-> Result<(), Box<dyn std::error::Error>> {
    let runner = Arc::new(RecordingCommandRunner::default());
    let rebooter = ProcessNodeRebooter::with_runner(
        PathBuf::from("/run/current-system/sw/bin/systemctl"),
        runner.clone(),
    );

    rebooter.reboot().await?;

    assert_eq!(
        runner.commands(),
        vec![NixosCommand {
            executable: PathBuf::from("/run/current-system/sw/bin/systemctl"),
            arguments: vec!["reboot".into()],
        }]
    );
    Ok(())
}

struct World {
    store: Arc<InMemoryStore>,
    keys: Keyspace,
    agent: NodeUpgradeAgent,
    stager: Arc<FakeStager>,
    rebooter: Arc<FakeRebooter>,
}

impl World {
    async fn new(instance_id: NodeInstanceId) -> Result<Self, Box<dyn std::error::Error>> {
        Self::with_staging(instance_id, true).await
    }

    async fn new_without_staging(
        instance_id: NodeInstanceId,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Self::with_staging(instance_id, false).await
    }

    async fn new_with_boot_id(
        instance_id: NodeInstanceId,
        boot_id: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Self::with_staging_and_boot_id(instance_id, true, boot_id).await
    }

    async fn with_staging(
        instance_id: NodeInstanceId,
        staging_enabled: bool,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Self::with_staging_and_boot_id(instance_id, staging_enabled, "boot-1").await
    }

    async fn with_staging_and_boot_id(
        instance_id: NodeInstanceId,
        staging_enabled: bool,
        boot_id: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let cluster_id = ClusterId::new("agent-test")?;
        let node_id = NodeId::new("node-1")?;
        let keys = Keyspace::new(&cluster_id);
        let clock = Arc::new(NoopClock);
        let store = Arc::new(InMemoryStore::new(clock.clone()));
        put_missing(
            &store,
            keys.resource(
                &ResourceKind::new("UpgradeRun")?,
                &ResourceName::new("upgrade-1")?,
            ),
            serde_json::to_vec(&run()?)?,
        )
        .await?;
        put_missing(
            &store,
            keys.node_upgrade_command(&node_id),
            serde_json::to_vec(&command(NodeUpgradeCommandState::Requested)?)?,
        )
        .await?;
        let stager = Arc::new(FakeStager::default());
        let rebooter = Arc::new(FakeRebooter::default());
        let agent = NodeUpgradeAgent::new(
            store.clone(),
            NodeUpgradeAgentSettings {
                cluster_id,
                node_id,
                instance_id,
                boot_id: boot_id.to_owned(),
                running_version: Version::new(1, 0, 0),
                resync_interval: Duration::from_secs(1),
            },
            staging_enabled.then(|| stager.clone() as Arc<dyn NixosUpgradeStager>),
            rebooter.clone(),
            clock,
        )?;
        Ok(Self {
            store,
            keys,
            agent,
            stager,
            rebooter,
        })
    }

    async fn command(&self) -> Result<NodeUpgradeCommand, Box<dyn std::error::Error>> {
        self.command_optional()
            .await?
            .ok_or_else(|| "command missing".into())
    }

    async fn command_optional(
        &self,
    ) -> Result<Option<NodeUpgradeCommand>, Box<dyn std::error::Error>> {
        self.store
            .get(&self.keys.node_upgrade_command(&NodeId::new("node-1")?))
            .await?
            .map(|stored| serde_json::from_slice(&stored.value).map_err(Into::into))
            .transpose()
    }

    async fn set_command_state(
        &self,
        state: NodeUpgradeCommandState,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let key = self.keys.node_upgrade_command(&NodeId::new("node-1")?);
        let stored = self.store.get(&key).await?.ok_or("command missing")?;
        let mut command: NodeUpgradeCommand = serde_json::from_slice(&stored.value)?;
        command.state = state;
        if matches!(
            state,
            NodeUpgradeCommandState::Staged
                | NodeUpgradeCommandState::Released
                | NodeUpgradeCommandState::Restarting
        ) {
            command.staged_boot_id = Some("boot-1".to_owned());
        }
        command.failure = None;
        replace(
            &self.store,
            key,
            stored.version,
            serde_json::to_vec(&command)?,
        )
        .await
    }

    async fn set_run_phase(&self, phase: UpgradePhase) -> Result<(), Box<dyn std::error::Error>> {
        let key = self.keys.resource(
            &ResourceKind::new("UpgradeRun")?,
            &ResourceName::new("upgrade-1")?,
        );
        let stored = self.store.get(&key).await?.ok_or("run missing")?;
        let mut run: UpgradeRun = serde_json::from_slice(&stored.value)?;
        run.status.phase = phase;
        for node in &mut run.status.nodes {
            node.phase = phase;
        }
        replace(&self.store, key, stored.version, serde_json::to_vec(&run)?).await
    }

    async fn set_operation(
        &self,
        operation: UpgradeOperation,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let run_key = self.keys.resource(
            &ResourceKind::new("UpgradeRun")?,
            &ResourceName::new("upgrade-1")?,
        );
        let stored_run = self.store.get(&run_key).await?.ok_or("run missing")?;
        let mut run: UpgradeRun = serde_json::from_slice(&stored_run.value)?;
        run.spec.operation = operation;
        run.spec.target_version = RESTART_TARGET_VERSION.to_string();
        replace(
            &self.store,
            run_key,
            stored_run.version,
            serde_json::to_vec(&run)?,
        )
        .await?;

        let command_key = self.keys.node_upgrade_command(&NodeId::new("node-1")?);
        let stored_command = self
            .store
            .get(&command_key)
            .await?
            .ok_or("command missing")?;
        let mut command: NodeUpgradeCommand = serde_json::from_slice(&stored_command.value)?;
        command.operation = operation;
        command.target_version = RESTART_TARGET_VERSION.to_string();
        replace(
            &self.store,
            command_key,
            stored_command.version,
            serde_json::to_vec(&command)?,
        )
        .await
    }
}

#[derive(Default)]
struct FakeStager {
    targets: Mutex<Vec<Version>>,
    failure: Mutex<Option<NixosUpgradeStagingError>>,
}

impl FakeStager {
    fn targets(&self) -> Vec<Version> {
        lock(&self.targets).clone()
    }

    fn fail(&self, error: NixosUpgradeStagingError) {
        *lock(&self.failure) = Some(error);
    }
}

#[async_trait]
impl NixosUpgradeStager for FakeStager {
    async fn stage(
        &self,
        minimum_version: &Version,
    ) -> Result<NixosUpgradeSource, NixosUpgradeStagingError> {
        lock(&self.targets).push(minimum_version.clone());
        if let Some(error) = lock(&self.failure).take() {
            Err(error)
        } else {
            Ok(NixosUpgradeSource::new(minimum_version.clone()))
        }
    }
}

#[derive(Default)]
struct FakeRebooter {
    calls: Mutex<usize>,
    failure: Mutex<Option<NodeRebootError>>,
}

impl FakeRebooter {
    fn calls(&self) -> usize {
        *lock(&self.calls)
    }

    fn fail(&self, message: &str) {
        *lock(&self.failure) = Some(NodeRebootError::new(message));
    }
}

#[async_trait]
impl NodeRebooter for FakeRebooter {
    async fn reboot(&self) -> Result<(), NodeRebootError> {
        *lock(&self.calls) += 1;
        match lock(&self.failure).take() {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

#[derive(Default)]
struct RecordingCommandRunner {
    commands: Mutex<Vec<NixosCommand>>,
}

impl RecordingCommandRunner {
    fn commands(&self) -> Vec<NixosCommand> {
        lock(&self.commands).clone()
    }
}

#[async_trait]
impl NixosCommandRunner for RecordingCommandRunner {
    async fn run(&self, command: NixosCommand) -> Result<NixosCommandOutput, NixosCommandError> {
        lock(&self.commands).push(command);
        Ok(NixosCommandOutput { stdout: Vec::new() })
    }
}

fn run() -> Result<UpgradeRun, kernel_api::InvalidIdentifier> {
    Ok(Object {
        meta: metadata(UpgradeRunId::new("upgrade-1")?),
        spec: UpgradeRunSpec {
            operation: kernel_api::UpgradeOperation::Upgrade,
            target_version: "2.0.0".to_string(),
            mode: UpgradeMode::Rolling,
            node_ids: Vec::new(),
        },
        status: UpgradeRunStatus {
            phase: UpgradePhase::Applying,
            nodes: vec![NodeUpgradeStatus {
                node_id: NodeId::new("node-1")?,
                phase: UpgradePhase::Applying,
                previous_instance_id: Some(NodeInstanceId::new("instance-node-1")?),
                observed_version: Some("1.0.0".to_string()),
                attempts: 0,
                retry_at: None,
            }],
            conditions: Vec::new(),
        },
    })
}

fn command(
    state: NodeUpgradeCommandState,
) -> Result<NodeUpgradeCommand, kernel_api::InvalidIdentifier> {
    Ok(NodeUpgradeCommand {
        run_id: UpgradeRunId::new("upgrade-1")?,
        node_id: NodeId::new("node-1")?,
        operation: kernel_api::UpgradeOperation::Upgrade,
        target_version: "2.0.0".to_string(),
        previous_instance_id: NodeInstanceId::new("instance-node-1")?,
        staged_boot_id: (state != NodeUpgradeCommandState::Requested).then(|| "boot-1".to_owned()),
        store_recovery: None,
        state,
        failure: None,
    })
}

fn metadata<Id>(id: Id) -> ObjectMeta<Id> {
    ObjectMeta {
        id,
        labels: BTreeMap::new(),
        annotations: BTreeMap::new(),
        revision: ResourceRevision::default(),
        generation: Generation(1),
        owner_refs: Vec::new(),
        finalizers: BTreeSet::new(),
        deletion_timestamp: None,
    }
}

async fn put_missing(
    store: &InMemoryStore,
    key: kernel_store::StoreKey,
    value: Vec<u8>,
) -> Result<(), Box<dyn std::error::Error>> {
    let outcome = store
        .put_cas(PutRequest {
            key,
            value,
            expected: ExpectedVersion::Missing,
            session: None,
        })
        .await?;
    if matches!(outcome, CasOutcome::Applied(_)) {
        Ok(())
    } else {
        Err("seed write conflicted".into())
    }
}

async fn replace(
    store: &InMemoryStore,
    key: kernel_store::StoreKey,
    version: kernel_store::Version,
    value: Vec<u8>,
) -> Result<(), Box<dyn std::error::Error>> {
    let outcome = store
        .put_cas(PutRequest {
            key,
            value,
            expected: ExpectedVersion::Exact(version),
            session: None,
        })
        .await?;
    if matches!(outcome, CasOutcome::Applied(_)) {
        Ok(())
    } else {
        Err("replacement conflicted".into())
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

struct NoopClock;

#[async_trait]
impl Clock for NoopClock {
    fn now(&self) -> MonotonicTime {
        MonotonicTime::default()
    }

    async fn sleep_until(&self, _deadline: MonotonicTime) {
        std::future::pending::<()>().await;
    }
}
