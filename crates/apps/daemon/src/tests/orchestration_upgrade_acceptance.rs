use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use clustertest::{
    FixtureInstanceId, FixtureNodeName, FixtureVersion, MaintenanceCompletion, MaintenanceFreeze,
    MaintenanceNodeRole, MaintenanceNodeSnapshot, MaintenanceTopology, SchedulingEligibility,
    SelectedRestartObservation, TargetRetention, UpgradeCluster, UpgradeFault, UpgradeObservation,
    scenarios,
};
use ingress::{BackendChange, IngressBackend, IngressBackendError};
use kernel_api::{
    ClusterId, Condition, ConditionReason, ConditionState, ConditionType, Generation, Node, NodeId,
    NodeInstanceId, NodeRole, Object, ObjectMeta, RESTART_TARGET_VERSION, ResourceKind,
    ResourceName, ResourceRevision, Timestamp, UpgradeMode, UpgradeOperation, UpgradePhase,
    UpgradeRun, UpgradeRunId, UpgradeRunSpec, UpgradeRunStatus,
};
use kernel_controller::{FencedStore, LeaderIdentity, LeadershipToken};
use kernel_store::{
    CasOutcome, Clock, ExpectedVersion, InMemoryStore, Keyspace, PutRequest, Session,
    SessionBinding, Store,
};
use upgrade::UpgradeSettings;

use super::build_backend::FakeBuildBackend;
use super::orchestration::{HarnessResult, put};
use super::orchestration_fixture::{ManualTimestampClock, NoopClock, node, settings};
use super::orchestration_upgrade_backend::{RecordingUpgradeBackend, maintained};
use crate::OperatorSuite;

#[tokio::test]
async fn shared_upgrade_scenarios_drive_composed_operators() -> HarnessResult<()> {
    let mut world = UpgradeAcceptanceWorld::new().await?;

    scenarios::rolling_upgrade_retries_and_restores_nodes_serially(&mut world).await?;
    scenarios::all_node_upgrade_restores_nodes_as_one_batch(&mut world).await?;
    Ok(())
}

struct UpgradeAcceptanceWorld {
    keys: Keyspace,
    store: Arc<InMemoryStore>,
    suite: OperatorSuite,
    backend: Arc<RecordingUpgradeBackend>,
    timestamp: Arc<ManualTimestampClock>,
    leader: NodeId,
    _session: Box<dyn Session>,
}

impl UpgradeAcceptanceWorld {
    async fn new() -> HarnessResult<Self> {
        let cluster_id = ClusterId::new("upgrade-acceptance")?;
        let keys = Keyspace::new(&cluster_id);
        let monotonic: Arc<dyn Clock> = Arc::new(NoopClock);
        let store = Arc::new(InMemoryStore::new(monotonic.clone()));
        let session = store.session(Duration::from_secs(30)).await?;
        let leader_id = NodeId::new("node-1")?;
        let campaign = store
            .put_cas(PutRequest {
                key: keys.leader(),
                value: b"upgrade-acceptance".to_vec(),
                expected: ExpectedVersion::Missing,
                session: Some(SessionBinding {
                    session_id: session.id(),
                }),
            })
            .await?;
        let CasOutcome::Applied(campaign) = campaign else {
            return Err("upgrade acceptance leadership campaign conflicted".into());
        };
        let fenced = Arc::new(FencedStore::new(
            store.clone(),
            keys.leader(),
            LeadershipToken::from_campaign(
                LeaderIdentity {
                    node_id: leader_id.clone(),
                    instance_id: NodeInstanceId::new("upgrade-acceptance")?,
                },
                session.id(),
                campaign.version,
            ),
        ));
        for index in 1..=3_u8 {
            let node_id = NodeId::new(format!("node-{index}"))?;
            let mut resource = node(&node_id, index)?;
            resource.spec.role = if index == 1 {
                NodeRole::Master
            } else {
                NodeRole::ControlPlane
            };
            resource.status.conditions.push(Condition {
                condition_type: ConditionType::ArtifactReplicationReady,
                state: ConditionState::True,
                reason: ConditionReason("PeerCopiesReady".to_string()),
                message: "retained artifacts are replicated".to_string(),
                observed_generation: resource.meta.generation,
                last_transition_time: Timestamp(10_000),
            });
            put(&store, &keys, "Node", &resource).await?;
            put_value(&store, keys.node_liveness(&node_id), b"live".to_vec()).await?;
        }

        let backend = Arc::new(RecordingUpgradeBackend::new(store.clone(), keys.clone()));
        let (mut backends, _) = FakeBuildBackend::operator_backends(Arc::new(NoopIngressBackend));
        backends.upgrades = Some(backend.clone());
        let timestamp = Arc::new(ManualTimestampClock::new(10_000));
        let mut operator_settings = settings()?;
        operator_settings.upgrade = Some(UpgradeSettings::new(
            Duration::from_millis(1),
            Duration::from_millis(1),
            3,
        )?);
        let suite = OperatorSuite::new(
            cluster_id,
            fenced,
            monotonic,
            timestamp.clone(),
            operator_settings,
            backends,
        )?;
        Ok(Self {
            keys,
            store,
            suite,
            backend,
            timestamp,
            leader: leader_id,
            _session: session,
        })
    }

    async fn run_upgrade(
        &self,
        run_name: &str,
        target: &FixtureVersion,
        operation: UpgradeOperation,
        mode: UpgradeMode,
        node_ids: Vec<NodeId>,
        failed_node: Option<NodeId>,
    ) -> HarnessResult<UpgradeObservation> {
        self.backend.fail_once_on(failed_node);
        let attempt_cursor = self.backend.attempt_count();
        let run_id = UpgradeRunId::new(run_name)?;
        put(
            &self.store,
            &self.keys,
            "UpgradeRun",
            &upgrade_run(run_id.clone(), target.as_str(), operation, mode, node_ids),
        )
        .await?;
        let completed = self.await_terminal(&run_id).await?;
        let topology = self.topology_snapshot().await?;
        Ok(UpgradeObservation {
            planned_nodes: completed
                .status
                .nodes
                .iter()
                .map(|node| FixtureNodeName::new(node.node_id.to_string()))
                .collect(),
            attempts: self.backend.attempts_since(attempt_cursor),
            completion: completion(completed.status.phase),
            target_retention: TargetRetention::RetainedUntilCompletion,
            final_freeze: freeze(&topology),
            final_nodes: topology.nodes,
        })
    }

    async fn await_terminal(&self, run_id: &UpgradeRunId) -> HarnessResult<UpgradeRun> {
        for _pass in 0..96 {
            self.suite.reconcile_snapshot().await?;
            self.timestamp.advance(1);
            let run = self.get_run(run_id).await?.ok_or_else(|| {
                format!("upgrade run `{run_id}` disappeared before reaching terminal state")
            })?;
            if matches!(
                run.status.phase,
                UpgradePhase::Completed | UpgradePhase::Failed | UpgradePhase::Canceled
            ) {
                self.suite.reconcile_snapshot().await?;
                return Ok(run);
            }
        }
        Err(format!("upgrade run `{run_id}` did not reach terminal state").into())
    }

    async fn get_run(&self, id: &UpgradeRunId) -> HarnessResult<Option<UpgradeRun>> {
        let key = self.keys.resource(
            &ResourceKind::new("UpgradeRun")?,
            &ResourceName::from(id.clone()),
        );
        self.store
            .get(&key)
            .await?
            .map(|stored| serde_json::from_slice(&stored.value).map_err(Into::into))
            .transpose()
    }

    async fn nodes(&self) -> HarnessResult<Vec<Node>> {
        self.store
            .list(&self.keys.resource_kind(&ResourceKind::new("Node")?))
            .await?
            .values
            .into_iter()
            .map(|stored| serde_json::from_slice(&stored.value).map_err(Into::into))
            .collect()
    }

    async fn topology_snapshot(&self) -> HarnessResult<MaintenanceTopology> {
        let nodes = self
            .nodes()
            .await?
            .into_iter()
            .map(|node| {
                let scheduling = if maintained(&node) {
                    SchedulingEligibility::Ineligible
                } else {
                    SchedulingEligibility::Eligible
                };
                Ok((
                    FixtureNodeName::new(node.meta.id.to_string()),
                    MaintenanceNodeSnapshot {
                        role: if node.spec.role.is_control_plane() {
                            MaintenanceNodeRole::Voter
                        } else {
                            MaintenanceNodeRole::Worker
                        },
                        version: FixtureVersion::new(node.status.version),
                        instance_id: FixtureInstanceId::new(node.status.instance_id.to_string()),
                        scheduling,
                    },
                ))
            })
            .collect::<HarnessResult<BTreeMap<_, _>>>()?;
        Ok(MaintenanceTopology {
            nodes,
            leader: FixtureNodeName::new(self.leader.to_string()),
        })
    }
}

#[async_trait]
impl UpgradeCluster for UpgradeAcceptanceWorld {
    type Error = Box<dyn std::error::Error + Send + Sync + 'static>;

    async fn topology(&mut self) -> Result<MaintenanceTopology, Self::Error> {
        self.topology_snapshot().await
    }

    async fn rolling_upgrade(
        &mut self,
        target: FixtureVersion,
        fault: UpgradeFault,
    ) -> Result<UpgradeObservation, Self::Error> {
        let UpgradeFault::FailFirstAttempt { node } = fault;
        self.run_upgrade(
            "rolling-upgrade",
            &target,
            UpgradeOperation::Upgrade,
            UpgradeMode::Rolling,
            Vec::new(),
            Some(NodeId::new(node.as_str())?),
        )
        .await
    }

    async fn all_node_upgrade(
        &mut self,
        target: FixtureVersion,
    ) -> Result<UpgradeObservation, Self::Error> {
        self.run_upgrade(
            "all-node-upgrade",
            &target,
            UpgradeOperation::Upgrade,
            UpgradeMode::AllNodes,
            Vec::new(),
            None,
        )
        .await
    }

    async fn restart_node(
        &mut self,
        node: &FixtureNodeName,
    ) -> Result<SelectedRestartObservation, Self::Error> {
        let node_id = NodeId::new(node.as_str())?;
        let attempt_cursor = self.backend.attempt_count();
        let observation = self
            .run_upgrade(
                "selected-node-restart",
                &FixtureVersion::new(RESTART_TARGET_VERSION),
                UpgradeOperation::Restart,
                UpgradeMode::Rolling,
                vec![node_id],
                None,
            )
            .await?;
        Ok(SelectedRestartObservation {
            planned_nodes: observation.planned_nodes,
            requested_nodes: self
                .backend
                .attempts_since(attempt_cursor)
                .into_iter()
                .map(|attempt| attempt.node)
                .collect(),
            completion: observation.completion,
            final_freeze: observation.final_freeze,
        })
    }
}

struct NoopIngressBackend;

#[async_trait]
impl IngressBackend for NoopIngressBackend {
    async fn apply(&self, _change: &BackendChange) -> Result<(), IngressBackendError> {
        Ok(())
    }

    async fn apply_blocklist(
        &self,
        _change: &ingress::IngressBlocklistChange,
    ) -> Result<(), ingress::IngressBackendError> {
        Ok(())
    }
}

fn upgrade_run(
    id: UpgradeRunId,
    target: &str,
    operation: UpgradeOperation,
    mode: UpgradeMode,
    node_ids: Vec<NodeId>,
) -> UpgradeRun {
    Object {
        meta: ObjectMeta {
            id,
            labels: BTreeMap::new(),
            annotations: BTreeMap::new(),
            revision: ResourceRevision::default(),
            generation: Generation(1),
            owner_refs: Vec::new(),
            finalizers: BTreeSet::new(),
            deletion_timestamp: None,
        },
        spec: UpgradeRunSpec {
            operation,
            target_version: target.to_string(),
            mode,
            node_ids,
        },
        status: UpgradeRunStatus {
            phase: UpgradePhase::Pending,
            nodes: Vec::new(),
            conditions: Vec::new(),
        },
    }
}

fn freeze(topology: &MaintenanceTopology) -> MaintenanceFreeze {
    if topology
        .nodes
        .values()
        .any(|node| node.scheduling == SchedulingEligibility::Ineligible)
    {
        MaintenanceFreeze::Present
    } else {
        MaintenanceFreeze::Cleared
    }
}

fn completion(phase: UpgradePhase) -> MaintenanceCompletion {
    if phase == UpgradePhase::Completed {
        MaintenanceCompletion::Succeeded
    } else {
        MaintenanceCompletion::Failed
    }
}

async fn put_value(
    store: &InMemoryStore,
    key: kernel_store::StoreKey,
    value: Vec<u8>,
) -> HarnessResult<()> {
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
        Err("upgrade acceptance seed write conflicted".into())
    }
}
