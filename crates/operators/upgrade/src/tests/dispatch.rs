use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use async_trait::async_trait;
use kernel_api::{ClusterId, NodeId, NodeInstanceId, UpgradeRunId};
use kernel_controller::{FencedStore, LeaderIdentity, LeadershipToken};
use kernel_store::{
    CasOutcome, Clock, ExpectedVersion, InMemoryStore, Keyspace, MonotonicTime, PutRequest,
    Session, SessionBinding, Store,
};

use crate::{
    NodeUpgradeBackend, NodeUpgradeBackendError, NodeUpgradeCommand, NodeUpgradeCommandFailure,
    NodeUpgradeCommandState, NodeUpgradeRequest, NodeUpgradeTarget, StoreNodeUpgradeBackend,
    StoreNodeUpgradeBackendSettings,
};

#[tokio::test]
async fn store_backend_releases_the_complete_staged_batch_atomically()
-> Result<(), Box<dyn std::error::Error>> {
    let world = World::new().await?;
    let request = request("upgrade-1", &["node-1", "node-2"])?;
    let backend = world.backend()?;
    let agent = world.drive_agents(&request, AgentOutcome::Restarting);

    let (result, agent_result) = tokio::join!(backend.apply(&request), agent);

    result?;
    agent_result?;
    let commands = world.commands(&request).await?;
    assert_eq!(commands.len(), 2);
    assert!(
        commands
            .values()
            .all(|command| command.state == NodeUpgradeCommandState::Restarting)
    );
    assert!(world.clock.sleeps() > 0);
    Ok(())
}

#[tokio::test]
async fn store_backend_replays_a_released_batch_without_recreating_commands()
-> Result<(), Box<dyn std::error::Error>> {
    let world = World::new().await?;
    let request = request("upgrade-1", &["node-1", "node-2"])?;
    let backend = world.backend()?;
    let (result, agent_result) = tokio::join!(
        backend.apply(&request),
        world.drive_agents(&request, AgentOutcome::Restarting),
    );
    result?;
    agent_result?;
    let before = world.stored_values(&request).await?;
    let sleeps = world.clock.sleeps();

    backend.apply(&request).await?;

    assert_eq!(world.stored_values(&request).await?, before);
    assert_eq!(world.clock.sleeps(), sleeps);
    Ok(())
}

#[tokio::test]
async fn store_backend_surfaces_agent_rejection_and_clears_the_whole_batch()
-> Result<(), Box<dyn std::error::Error>> {
    let world = World::new().await?;
    let request = request("upgrade-1", &["node-1", "node-2"])?;
    let backend = world.backend()?;
    let agent = world.drive_agents(
        &request,
        AgentOutcome::Rejected {
            node_id: NodeId::new("node-2")?,
        },
    );

    let (result, agent_result) = tokio::join!(backend.apply(&request), agent);
    let error = result.expect_err("agent rejection must reject the dispatch");

    agent_result?;
    assert!(matches!(error, NodeUpgradeBackendError::Rejected { .. }));
    assert!(error.to_string().contains("source policy rejected"));
    assert!(world.commands(&request).await?.is_empty());
    Ok(())
}

#[tokio::test]
async fn store_backend_rejects_a_command_owned_by_another_run()
-> Result<(), Box<dyn std::error::Error>> {
    let world = World::new().await?;
    let current_request = request("upgrade-1", &["node-1"])?;
    let conflicting = request("upgrade-2", &["node-1"])?;
    world
        .put_command(command_for(
            &conflicting,
            0,
            NodeUpgradeCommandState::Requested,
        ))
        .await?;

    let error = world
        .backend()?
        .apply(&current_request)
        .await
        .expect_err("a second run must not replace an active command");

    assert!(matches!(error, NodeUpgradeBackendError::Rejected { .. }));
    assert!(error.to_string().contains("upgrade-2"));
    Ok(())
}

#[tokio::test]
async fn store_backend_cannot_create_commands_after_losing_its_fence()
-> Result<(), Box<dyn std::error::Error>> {
    let world = World::new().await?;
    let request = request("upgrade-1", &["node-1"])?;
    let backend = world.backend()?;
    world.replace_leader().await?;

    let error = backend
        .apply(&request)
        .await
        .expect_err("stale leadership must stop dispatch");

    assert!(matches!(error, NodeUpgradeBackendError::Unavailable { .. }));
    assert!(world.commands(&request).await?.is_empty());
    Ok(())
}

enum AgentOutcome {
    Restarting,
    Rejected { node_id: NodeId },
}

struct World {
    cluster_id: ClusterId,
    keys: Keyspace,
    store: Arc<InMemoryStore>,
    fenced: Arc<FencedStore>,
    clock: Arc<StepClock>,
    _session: Box<dyn Session>,
}

impl World {
    async fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let cluster_id = ClusterId::new("dispatch-test")?;
        let keys = Keyspace::new(&cluster_id);
        let clock = Arc::new(StepClock::default());
        let store = Arc::new(InMemoryStore::new(clock.clone()));
        let session = store.session(Duration::from_secs(3_600)).await?;
        let outcome = store
            .put_cas(PutRequest {
                key: keys.leader(),
                value: b"leader-1".to_vec(),
                expected: ExpectedVersion::Missing,
                session: Some(SessionBinding {
                    session_id: session.id(),
                }),
            })
            .await?;
        let CasOutcome::Applied(leader) = outcome else {
            return Err("leader campaign conflicted".into());
        };
        let fenced = Arc::new(FencedStore::new(
            store.clone(),
            keys.leader(),
            LeadershipToken::from_campaign(
                LeaderIdentity {
                    node_id: NodeId::new("node-1")?,
                    instance_id: NodeInstanceId::new("leader-1")?,
                },
                session.id(),
                leader.version,
            ),
        ));
        Ok(Self {
            cluster_id,
            keys,
            store,
            fenced,
            clock,
            _session: session,
        })
    }

    fn backend(&self) -> Result<StoreNodeUpgradeBackend, Box<dyn std::error::Error>> {
        Ok(StoreNodeUpgradeBackend::new(
            &self.cluster_id,
            self.fenced.clone(),
            self.clock.clone(),
            StoreNodeUpgradeBackendSettings::new(Duration::from_secs(300), Duration::from_secs(1))?,
        ))
    }

    async fn drive_agents(
        &self,
        request: &NodeUpgradeRequest,
        outcome: AgentOutcome,
    ) -> Result<(), Box<dyn std::error::Error>> {
        for _attempt in 0..1_000 {
            tokio::task::yield_now().await;
            let commands = self.commands(request).await?;
            if commands.len() != request.targets.len() {
                continue;
            }
            if commands
                .values()
                .all(|command| command.state == NodeUpgradeCommandState::Requested)
            {
                for command in commands.values() {
                    let state = match &outcome {
                        AgentOutcome::Rejected { node_id } if node_id == &command.node_id => {
                            let mut failed = command.clone();
                            failed.state = NodeUpgradeCommandState::Failed;
                            failed.failure = Some(NodeUpgradeCommandFailure::Rejected {
                                message: "source policy rejected".to_string(),
                            });
                            self.replace_command(failed).await?;
                            continue;
                        }
                        AgentOutcome::Restarting | AgentOutcome::Rejected { .. } => {
                            NodeUpgradeCommandState::Staged
                        }
                    };
                    let mut staged = command.clone();
                    staged.state = state;
                    self.replace_command(staged).await?;
                }
                if matches!(&outcome, AgentOutcome::Rejected { .. }) {
                    return Ok(());
                }
            }
            let commands = self.commands(request).await?;
            if commands
                .values()
                .all(|command| command.state == NodeUpgradeCommandState::Released)
            {
                assert_eq!(commands.len(), request.targets.len());
                for command in commands.values() {
                    let mut restarting = command.clone();
                    restarting.state = NodeUpgradeCommandState::Restarting;
                    self.replace_command(restarting).await?;
                }
                return Ok(());
            }
        }
        Err("fake agents did not observe a complete command batch".into())
    }

    async fn commands(
        &self,
        request: &NodeUpgradeRequest,
    ) -> Result<BTreeMap<NodeId, NodeUpgradeCommand>, Box<dyn std::error::Error>> {
        let mut commands = BTreeMap::new();
        for target in &request.targets {
            let key = self.keys.node_upgrade_command(&target.node_id);
            if let Some(stored) = self.store.get(&key).await? {
                commands.insert(
                    target.node_id.clone(),
                    serde_json::from_slice(&stored.value)?,
                );
            }
        }
        Ok(commands)
    }

    async fn stored_values(
        &self,
        request: &NodeUpgradeRequest,
    ) -> Result<Vec<Vec<u8>>, Box<dyn std::error::Error>> {
        let mut values = Vec::new();
        for target in &request.targets {
            if let Some(stored) = self
                .store
                .get(&self.keys.node_upgrade_command(&target.node_id))
                .await?
            {
                values.push(stored.value);
            }
        }
        Ok(values)
    }

    async fn put_command(
        &self,
        command: NodeUpgradeCommand,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let outcome = self
            .store
            .put_cas(PutRequest {
                key: self.keys.node_upgrade_command(&command.node_id),
                value: serde_json::to_vec(&command)?,
                expected: ExpectedVersion::Missing,
                session: None,
            })
            .await?;
        if matches!(outcome, CasOutcome::Applied(_)) {
            Ok(())
        } else {
            Err("command create conflicted".into())
        }
    }

    async fn replace_command(
        &self,
        command: NodeUpgradeCommand,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let key = self.keys.node_upgrade_command(&command.node_id);
        let current = self.store.get(&key).await?.ok_or("command missing")?;
        let outcome = self
            .store
            .put_cas(PutRequest {
                key,
                value: serde_json::to_vec(&command)?,
                expected: ExpectedVersion::Exact(current.version),
                session: None,
            })
            .await?;
        if matches!(outcome, CasOutcome::Applied(_)) {
            Ok(())
        } else {
            Err("command update conflicted".into())
        }
    }

    async fn replace_leader(&self) -> Result<(), Box<dyn std::error::Error>> {
        let current = self
            .store
            .get(&self.keys.leader())
            .await?
            .ok_or("leader missing")?;
        let outcome = self
            .store
            .put_cas(PutRequest {
                key: self.keys.leader(),
                value: b"leader-2".to_vec(),
                expected: ExpectedVersion::Exact(current.version),
                session: None,
            })
            .await?;
        if matches!(outcome, CasOutcome::Applied(_)) {
            Ok(())
        } else {
            Err("leader replacement conflicted".into())
        }
    }
}

fn request(
    run_id: &str,
    node_ids: &[&str],
) -> Result<NodeUpgradeRequest, kernel_api::InvalidIdentifier> {
    Ok(NodeUpgradeRequest {
        run_id: UpgradeRunId::new(run_id)?,
        target_version: "2.0.0".to_string(),
        targets: node_ids
            .iter()
            .map(|node_id| {
                Ok(NodeUpgradeTarget {
                    node_id: NodeId::new(*node_id)?,
                    previous_instance_id: NodeInstanceId::new(format!("instance-{node_id}"))?,
                })
            })
            .collect::<Result<Vec<_>, kernel_api::InvalidIdentifier>>()?,
    })
}

fn command_for(
    request: &NodeUpgradeRequest,
    index: usize,
    state: NodeUpgradeCommandState,
) -> NodeUpgradeCommand {
    let target = request.targets.get(index).expect("fixture target exists");
    NodeUpgradeCommand {
        run_id: request.run_id.clone(),
        node_id: target.node_id.clone(),
        target_version: request.target_version.clone(),
        previous_instance_id: target.previous_instance_id.clone(),
        state,
        failure: None,
    }
}

#[derive(Default)]
struct StepClock {
    now: Mutex<MonotonicTime>,
    sleeps: Mutex<usize>,
}

impl StepClock {
    fn sleeps(&self) -> usize {
        *lock(&self.sleeps)
    }
}

#[async_trait]
impl Clock for StepClock {
    fn now(&self) -> MonotonicTime {
        *lock(&self.now)
    }

    async fn sleep_until(&self, deadline: MonotonicTime) {
        *lock(&self.now) = deadline;
        *lock(&self.sleeps) += 1;
        tokio::task::yield_now().await;
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}
