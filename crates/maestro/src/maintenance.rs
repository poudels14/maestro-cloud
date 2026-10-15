use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use maestro_control::AgentHub;
use maestro_store::Consensus;
use serde::Deserialize;
use serde_json::json;

use maestro_control::StoreWriter;
use maestro_core::{
    ClusterFreeze, DeploymentStatus, MaintenanceEvent, MaintenanceKind, MaintenancePhase,
    MaintenanceRun, MaintenanceStep, NodeId, NodeRecord, NodeRole, NodeState, NodeStepStatus,
};
use maestro_store::Mutation;
use serde_json::Value;

use crate::node_service::{ForwardingWriter, NodeClients, NodeRequest};

const LATEST: &str = "latest";
const DRAIN_TIMEOUT_MS: u64 = 10 * 60 * 1000;
const UPGRADE_TIMEOUT_MS: u64 = 6 * 60 * 60 * 1000;
const RESTART_TIMEOUT_MS: u64 = 5 * 60 * 1000;
const TICK_INTERVAL: Duration = Duration::from_secs(5);
const INFO_TIMEOUT: Duration = Duration::from_secs(5);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const IN_FLIGHT: [DeploymentStatus; 4] = [
    DeploymentStatus::Queued,
    DeploymentStatus::Building,
    DeploymentStatus::Deploying,
    DeploymentStatus::Draining,
];

pub trait MaintenanceNodes: Send + Sync + 'static {
    fn info(&self, node: &NodeRecord) -> impl Future<Output = Option<Value>> + Send;

    fn request(
        &self,
        node: &NodeRecord,
        kind: MaintenanceKind,
        run_id: &str,
    ) -> impl Future<Output = Result<(), String>> + Send;

    fn attached(&self, node_id: NodeId) -> bool;

    fn leader(&self) -> Option<NodeId>;
}

pub struct ClusterNodes {
    pub consensus: Arc<Consensus>,
    pub clients: NodeClients,
    pub hub: AgentHub,
}

pub type ClusterOrchestrator = Orchestrator<ForwardingWriter, ClusterNodes>;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpgradeRequest {
    #[serde(default)]
    target_version: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RestartRequest {
    #[serde(default)]
    node_id: Option<NodeId>,
}

pub struct Orchestrator<W: StoreWriter, N: MaintenanceNodes> {
    writer: Arc<W>,
    nodes: N,
}

pub enum Target {
    All,
    Node(NodeId),
}

impl<W: StoreWriter, N: MaintenanceNodes> Orchestrator<W, N> {
    pub fn new(writer: Arc<W>, nodes: N) -> Self {
        Self { writer, nodes }
    }

    pub async fn start(
        &self,
        kind: MaintenanceKind,
        target_version: String,
        target: Target,
    ) -> Result<MaintenanceRun, String> {
        let store = self.writer.state();
        let active = store
            .maintenance_run()
            .map_err(|error| error.to_string())?
            .is_some_and(|run| !run.is_terminal());
        let freeze = store.cluster_freeze().map_err(|error| error.to_string())?;
        let in_flight = IN_FLIGHT
            .iter()
            .map(|status| store.deployments_with_status(*status))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| error.to_string())?
            .concat();
        let nodes = store
            .nodes()
            .map_err(|error| error.to_string())?
            .into_iter()
            .filter(|node| node.state != NodeState::Removed)
            .collect::<Vec<_>>();
        let mut versions = Vec::new();
        for node in &nodes {
            let info = self.nodes.info(node).await;
            versions.push((node.clone(), info));
        }
        let offline = versions
            .iter()
            .find(|(_, info)| info.is_none())
            .map(|(node, _)| node.clone());
        let selected = versions
            .into_iter()
            .filter(|(node, _)| match target {
                Target::All => true,
                Target::Node(node_id) => node.id == node_id,
            })
            .map(|(node, info)| {
                let version = info.and_then(|info| info["version"].as_str().map(str::to_string));
                (node, version)
            })
            .collect::<Vec<_>>();
        let pending = selected
            .iter()
            .cloned()
            .filter(|(_, version)| match kind {
                MaintenanceKind::Upgrade => {
                    target_version == LATEST || version.as_deref() != Some(target_version.as_str())
                }
                MaintenanceKind::Restart => true,
            })
            .collect::<Vec<_>>();
        let voters = nodes
            .iter()
            .filter(|node| node.role == NodeRole::Voter)
            .count();
        let touches_voter = pending.iter().any(|(node, _)| node.role == NodeRole::Voter);
        let problem = [
            active.then(|| "another maintenance run is in progress".to_string()),
            freeze.map(|freeze| format!("cluster deploys are already frozen: {}", freeze.reason)),
            (!in_flight.is_empty()).then(|| {
                format!(
                    "wait for in-flight deployments to settle: {}",
                    in_flight
                        .iter()
                        .map(|deployment| format!("{}/{}", deployment.service_id, deployment.id))
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            }),
            offline.map(|node| format!("node {} is offline", node.name)),
            selected.is_empty().then(|| "no matching node".to_string()),
            (pending.is_empty() && !selected.is_empty())
                .then(|| format!("every node already runs {target_version}")),
            (voters == 2 && touches_voter).then(|| {
                "a cluster with exactly two voters would lose quorum; add a third voter first"
                    .to_string()
            }),
        ]
        .into_iter()
        .flatten()
        .next();
        match problem {
            Some(problem) => Err(problem),
            None => {
                let leader = self.nodes.leader();
                let mut steps = pending;
                steps.sort_by_key(|(node, _)| {
                    let rank = match (node.role, Some(node.id) == leader) {
                        (_, true) => 2,
                        (NodeRole::Voter, false) => 1,
                        (NodeRole::Worker, false) => 0,
                    };
                    (rank, node.id)
                });
                let now = now_ms();
                let run_id = format!("run-{now}");
                let reason = match kind {
                    MaintenanceKind::Upgrade => format!("rolling upgrade to {target_version}"),
                    MaintenanceKind::Restart => "rolling restart".to_string(),
                };
                let run = MaintenanceRun {
                    run_id: run_id.clone(),
                    kind,
                    target_version,
                    requested_at_ms: now,
                    updated_at_ms: now,
                    phase: MaintenancePhase::Draining,
                    phase_started_at_ms: now,
                    current_node_index: 0,
                    nodes: steps
                        .into_iter()
                        .map(|(node, version)| MaintenanceStep {
                            node_id: node.id,
                            node_name: node.name,
                            from_version: version,
                            status: NodeStepStatus::Pending,
                            started_at_ms: None,
                            requested_at_ms: None,
                            completed_at_ms: None,
                            error: None,
                        })
                        .collect(),
                    history: vec![MaintenanceEvent {
                        at_ms: now,
                        phase: MaintenancePhase::Draining,
                        node_id: None,
                        message: format!("started {reason}"),
                    }],
                    failure: None,
                };
                self.writer
                    .write(vec![
                        Mutation::SetMaintenanceRun {
                            run: Some(run.clone()),
                        },
                        Mutation::SetClusterFreeze {
                            freeze: Some(ClusterFreeze {
                                reason,
                                upgrade_run_id: Some(run_id),
                                at_ms: now,
                            }),
                        },
                    ])
                    .await
                    .map_err(|error| error.to_string())?;
                Ok(run)
            }
        }
    }

    pub async fn tick(&self) -> Result<(), String> {
        let run = self
            .writer
            .state()
            .maintenance_run()
            .map_err(|error| error.to_string())?
            .filter(|run| !run.is_terminal());
        match run {
            Some(run) => self.advance(run).await,
            None => Ok(()),
        }
    }

    async fn advance(&self, mut run: MaintenanceRun) -> Result<(), String> {
        let step = run.nodes[run.current_node_index].clone();
        let node = self
            .writer
            .state()
            .node(step.node_id)
            .map_err(|error| error.to_string())?
            .ok_or_else(|| format!("node {} no longer exists", step.node_name))?;
        let now = now_ms();
        let elapsed = now.saturating_sub(run.phase_started_at_ms);
        match run.phase {
            MaintenancePhase::Draining => {
                let mut mutations = Vec::new();
                if node.state != NodeState::Draining {
                    mutations.push(Mutation::PutNode {
                        node: NodeRecord {
                            state: NodeState::Draining,
                            ..node.clone()
                        },
                    });
                    run.nodes[run.current_node_index].status = NodeStepStatus::Draining;
                    run.nodes[run.current_node_index].started_at_ms = Some(now);
                }
                let empty = self
                    .writer
                    .state()
                    .assignments(node.id)
                    .map_err(|error| error.to_string())?
                    .is_empty();
                let elsewhere = self.other_schedulable(node.id)?;
                match (empty || !elsewhere, elapsed > DRAIN_TIMEOUT_MS) {
                    (true, _) => self.enter(
                        &mut run,
                        MaintenancePhase::Requested,
                        Some(node.id),
                        "drained",
                    ),
                    (false, true) => {
                        return self
                            .fail(run, &node, format!("draining {} timed out", node.name))
                            .await;
                    }
                    (false, false) => {}
                }
                mutations.push(Mutation::SetMaintenanceRun { run: Some(run) });
                self.write(mutations).await
            }
            MaintenancePhase::Requested => {
                run.nodes[run.current_node_index].status = NodeStepStatus::Upgrading;
                run.nodes[run.current_node_index].requested_at_ms = Some(now);
                self.enter(
                    &mut run,
                    MaintenancePhase::Verifying,
                    Some(node.id),
                    "requested",
                );
                self.write(vec![Mutation::SetMaintenanceRun {
                    run: Some(run.clone()),
                }])
                .await?;
                if let Err(error) = self.nodes.request(&node, run.kind, &run.run_id).await {
                    tracing::info!(node = %node.name, %error, "maintenance request did not complete; the node may be restarting");
                }
                Ok(())
            }
            MaintenancePhase::Verifying => {
                let requested_at = step.requested_at_ms.unwrap_or(run.phase_started_at_ms);
                let info = self.nodes.info(&node).await;
                let progress = info.as_ref().map(|info| &info["maintenance"]);
                let failed = progress.filter(|progress| {
                    progress["runId"].as_str() == Some(run.run_id.as_str())
                        && progress["stage"] == "failed"
                });
                let restarted = info.as_ref().is_some_and(|info| {
                    let started_at =
                        now.saturating_sub(info["uptimeMs"].as_u64().unwrap_or(u64::MAX));
                    started_at > requested_at
                });
                let done = match (run.kind, run.target_version.as_str()) {
                    (MaintenanceKind::Upgrade, LATEST) | (MaintenanceKind::Restart, _) => restarted,
                    (MaintenanceKind::Upgrade, target) => info
                        .as_ref()
                        .is_some_and(|info| info["version"].as_str() == Some(target)),
                } && self.nodes.attached(node.id);
                let timeout = match run.kind {
                    MaintenanceKind::Upgrade => UPGRADE_TIMEOUT_MS,
                    MaintenanceKind::Restart => RESTART_TIMEOUT_MS,
                };
                match (failed, done, elapsed > timeout) {
                    (Some(progress), _, _) => {
                        let error = progress["error"]
                            .as_str()
                            .unwrap_or("upgrade failed")
                            .to_string();
                        self.fail(run, &node, error).await
                    }
                    (None, true, _) => {
                        run.nodes[run.current_node_index].status = NodeStepStatus::Restoring;
                        self.enter(
                            &mut run,
                            MaintenancePhase::Restoring,
                            Some(node.id),
                            "verified",
                        );
                        self.write(vec![Mutation::SetMaintenanceRun { run: Some(run) }])
                            .await
                    }
                    (None, false, true) => {
                        self.fail(
                            run,
                            &node,
                            format!("{} did not come back in time", node.name),
                        )
                        .await
                    }
                    (None, false, false) => Ok(()),
                }
            }
            MaintenancePhase::Restoring => {
                run.nodes[run.current_node_index].status = NodeStepStatus::Succeeded;
                run.nodes[run.current_node_index].completed_at_ms = Some(now);
                let finished = run.current_node_index + 1 >= run.nodes.len();
                let mut mutations = vec![Mutation::PutNode {
                    node: NodeRecord {
                        state: NodeState::Active,
                        ..node.clone()
                    },
                }];
                match finished {
                    true => {
                        self.enter(
                            &mut run,
                            MaintenancePhase::Succeeded,
                            None,
                            "every node is done",
                        );
                        mutations.push(Mutation::SetClusterFreeze { freeze: None });
                    }
                    false => {
                        run.current_node_index += 1;
                        self.enter(
                            &mut run,
                            MaintenancePhase::Draining,
                            Some(node.id),
                            "restored",
                        );
                    }
                }
                mutations.push(Mutation::SetMaintenanceRun { run: Some(run) });
                self.write(mutations).await
            }
            MaintenancePhase::Succeeded | MaintenancePhase::Failed => Ok(()),
        }
    }

    async fn fail(
        &self,
        mut run: MaintenanceRun,
        node: &NodeRecord,
        error: String,
    ) -> Result<(), String> {
        tracing::warn!(node = %node.name, %error, "maintenance run failed");
        run.nodes[run.current_node_index].status = NodeStepStatus::Failed;
        run.nodes[run.current_node_index].error = Some(error.clone());
        run.failure = Some(format!(
            "{error}; remaining nodes skipped and the cluster unfrozen"
        ));
        self.enter(&mut run, MaintenancePhase::Failed, Some(node.id), &error);
        self.write(vec![
            Mutation::PutNode {
                node: NodeRecord {
                    state: NodeState::Active,
                    ..node.clone()
                },
            },
            Mutation::SetMaintenanceRun { run: Some(run) },
            Mutation::SetClusterFreeze { freeze: None },
        ])
        .await
    }

    fn enter(
        &self,
        run: &mut MaintenanceRun,
        phase: MaintenancePhase,
        node_id: Option<NodeId>,
        message: &str,
    ) {
        let now = now_ms();
        run.phase = phase;
        run.phase_started_at_ms = now;
        run.updated_at_ms = now;
        run.history.push(MaintenanceEvent {
            at_ms: now,
            phase,
            node_id,
            message: message.to_string(),
        });
    }

    fn other_schedulable(&self, node_id: NodeId) -> Result<bool, String> {
        Ok(self
            .writer
            .state()
            .nodes()
            .map_err(|error| error.to_string())?
            .iter()
            .any(|node| {
                node.id != node_id && node.is_schedulable() && self.nodes.attached(node.id)
            }))
    }

    async fn write(&self, mutations: Vec<Mutation>) -> Result<(), String> {
        self.writer
            .write(mutations)
            .await
            .map(|_| ())
            .map_err(|error| error.to_string())
    }
}

pub async fn run(orchestrator: Arc<ClusterOrchestrator>, consensus: Arc<Consensus>) {
    let mut ticker = tokio::time::interval(TICK_INTERVAL);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        ticker.tick().await;
        if consensus.is_leader()
            && let Err(error) = orchestrator.tick().await
        {
            tracing::warn!(%error, "maintenance tick failed");
        }
    }
}

pub async fn upgrade(
    State(orchestrator): State<Arc<ClusterOrchestrator>>,
    Json(request): Json<UpgradeRequest>,
) -> Response {
    let target_version = request
        .target_version
        .as_deref()
        .map(|version| version.trim().trim_start_matches('v'))
        .filter(|version| !version.is_empty())
        .unwrap_or(LATEST)
        .to_string();
    respond(
        orchestrator
            .start(MaintenanceKind::Upgrade, target_version, Target::All)
            .await,
    )
}

pub async fn restart(
    State(orchestrator): State<Arc<ClusterOrchestrator>>,
    Json(request): Json<RestartRequest>,
) -> Response {
    let target = match request.node_id {
        Some(node_id) => Target::Node(node_id),
        None => Target::All,
    };
    respond(
        orchestrator
            .start(MaintenanceKind::Restart, crate::VERSION.to_string(), target)
            .await,
    )
}

impl MaintenanceNodes for ClusterNodes {
    async fn info(&self, node: &NodeRecord) -> Option<Value> {
        tokio::time::timeout(
            INFO_TIMEOUT,
            self.clients.call(&node.endpoint, &NodeRequest::Info),
        )
        .await
        .ok()?
        .ok()
    }

    async fn request(
        &self,
        node: &NodeRecord,
        kind: MaintenanceKind,
        run_id: &str,
    ) -> Result<(), String> {
        let request = NodeRequest::Maintain {
            kind,
            run_id: run_id.to_string(),
        };
        tokio::time::timeout(REQUEST_TIMEOUT, self.clients.call(&node.endpoint, &request))
            .await
            .map_err(|_| "maintenance request timed out".to_string())?
            .map(|_| ())
            .map_err(|status| status.message().to_string())
    }

    fn attached(&self, node_id: NodeId) -> bool {
        self.hub.connected_nodes().contains(&node_id)
    }

    fn leader(&self) -> Option<NodeId> {
        self.consensus.current_leader().map(|(node_id, _)| node_id)
    }
}

fn respond(result: Result<MaintenanceRun, String>) -> Response {
    match result {
        Ok(run) => (StatusCode::ACCEPTED, Json(run)).into_response(),
        Err(error) => (StatusCode::CONFLICT, Json(json!({ "error": error }))).into_response(),
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_millis() as u64)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex;

    use maestro_core::{ByteSize, Millicores, NodeCapacity};
    use maestro_store::{ConsensusError, StateMachine};

    use super::*;

    struct TestWriter(StateMachine);

    impl StoreWriter for TestWriter {
        async fn write(&self, mutations: Vec<Mutation>) -> Result<u64, ConsensusError> {
            Ok(self.0.apply(&mutations)?)
        }

        fn state(&self) -> &StateMachine {
            &self.0
        }
    }

    #[derive(Default)]
    struct FakeNodes {
        versions: Mutex<HashMap<NodeId, String>>,
        failing: Option<NodeId>,
        requested: Mutex<Vec<NodeId>>,
        run_id: Mutex<String>,
    }

    impl MaintenanceNodes for FakeNodes {
        async fn info(&self, node: &NodeRecord) -> Option<Value> {
            let version = self.versions.lock().unwrap().get(&node.id).cloned();
            let failed =
                self.failing == Some(node.id) && self.requested.lock().unwrap().contains(&node.id);
            let maintenance = match failed {
                true => {
                    json!({ "runId": *self.run_id.lock().unwrap(), "stage": "failed", "error": "nixos-rebuild failed" })
                }
                false => Value::Null,
            };
            version.map(
                |version| json!({ "version": version, "uptimeMs": 0, "maintenance": maintenance }),
            )
        }

        async fn request(
            &self,
            node: &NodeRecord,
            kind: MaintenanceKind,
            run_id: &str,
        ) -> Result<(), String> {
            *self.run_id.lock().unwrap() = run_id.to_string();
            self.requested.lock().unwrap().push(node.id);
            if kind == MaintenanceKind::Upgrade && self.failing != Some(node.id) {
                self.versions
                    .lock()
                    .unwrap()
                    .insert(node.id, "3.1.0".to_string());
            }
            Ok(())
        }

        fn attached(&self, _node_id: NodeId) -> bool {
            true
        }

        fn leader(&self) -> Option<NodeId> {
            Some(NodeId(1))
        }
    }

    fn node(id: u64, role: NodeRole) -> NodeRecord {
        NodeRecord {
            id: NodeId(id),
            name: format!("node{id}"),
            role,
            endpoint: String::new(),
            subnet: None,
            runs_workloads: true,
            capacity: NodeCapacity {
                cpu: Millicores::new(4000),
                memory: ByteSize::new(8 << 30),
            },
            reserved: NodeCapacity {
                cpu: Millicores::new(1000),
                memory: ByteSize::new(1 << 30),
            },
            labels: Default::default(),
            state: NodeState::Active,
            wireguard: None,
        }
    }

    fn orchestrator(
        nodes: &[NodeRecord],
        fake: FakeNodes,
    ) -> (Orchestrator<TestWriter, FakeNodes>, tempfile::TempDir) {
        let directory = tempfile::tempdir().unwrap();
        let state = StateMachine::open(directory.path().join("state.redb")).unwrap();
        state
            .apply(
                &nodes
                    .iter()
                    .map(|node| Mutation::PutNode { node: node.clone() })
                    .collect::<Vec<_>>(),
            )
            .unwrap();
        nodes.iter().for_each(|node| {
            fake.versions
                .lock()
                .unwrap()
                .insert(node.id, "3.0.0".to_string());
        });
        (
            Orchestrator::new(Arc::new(TestWriter(state)), fake),
            directory,
        )
    }

    async fn finish(orchestrator: &Orchestrator<TestWriter, FakeNodes>) -> MaintenanceRun {
        for _ in 0..50 {
            orchestrator.tick().await.unwrap();
        }
        orchestrator
            .writer
            .state()
            .maintenance_run()
            .unwrap()
            .unwrap()
    }

    #[tokio::test]
    async fn upgrades_agents_then_followers_then_the_leader() {
        let nodes = [
            node(1, NodeRole::Voter),
            node(2, NodeRole::Voter),
            node(3, NodeRole::Voter),
            node(4, NodeRole::Worker),
        ];
        let (orchestrator, _directory) = orchestrator(&nodes, FakeNodes::default());
        let run = orchestrator
            .start(MaintenanceKind::Upgrade, "3.1.0".to_string(), Target::All)
            .await
            .unwrap();
        assert!(
            orchestrator
                .writer
                .state()
                .cluster_freeze()
                .unwrap()
                .is_some()
        );
        assert_eq!(
            run.nodes
                .iter()
                .map(|step| step.node_id.0)
                .collect::<Vec<_>>(),
            [4, 2, 3, 1]
        );
        let run = finish(&orchestrator).await;
        assert_eq!(run.phase, MaintenancePhase::Succeeded);
        assert_eq!(
            *orchestrator.nodes.requested.lock().unwrap(),
            [NodeId(4), NodeId(2), NodeId(3), NodeId(1)]
        );
        assert!(
            orchestrator
                .writer
                .state()
                .cluster_freeze()
                .unwrap()
                .is_none()
        );
        assert!(
            orchestrator
                .writer
                .state()
                .nodes()
                .unwrap()
                .iter()
                .all(|node| node.state == NodeState::Active)
        );
    }

    #[tokio::test]
    async fn upgrading_to_the_latest_release_reboots_every_node() {
        let nodes = [
            node(1, NodeRole::Voter),
            node(2, NodeRole::Voter),
            node(3, NodeRole::Voter),
        ];
        let (orchestrator, _directory) = orchestrator(&nodes, FakeNodes::default());
        orchestrator
            .start(MaintenanceKind::Upgrade, LATEST.to_string(), Target::All)
            .await
            .unwrap();
        let run = finish(&orchestrator).await;
        assert_eq!(run.phase, MaintenancePhase::Succeeded);
        assert_eq!(orchestrator.nodes.requested.lock().unwrap().len(), 3);
    }

    #[tokio::test]
    async fn stops_and_unfreezes_when_a_node_fails_to_upgrade() {
        let nodes = [
            node(1, NodeRole::Voter),
            node(2, NodeRole::Worker),
            node(3, NodeRole::Worker),
        ];
        let fake = FakeNodes {
            failing: Some(NodeId(2)),
            ..FakeNodes::default()
        };
        let (orchestrator, _directory) = orchestrator(&nodes, fake);
        orchestrator
            .start(MaintenanceKind::Upgrade, "3.1.0".to_string(), Target::All)
            .await
            .unwrap();
        let run = finish(&orchestrator).await;
        assert_eq!(run.phase, MaintenancePhase::Failed);
        assert_eq!(run.nodes[0].error.as_deref(), Some("nixos-rebuild failed"));
        assert_eq!(*orchestrator.nodes.requested.lock().unwrap(), [NodeId(2)]);
        assert!(
            orchestrator
                .writer
                .state()
                .cluster_freeze()
                .unwrap()
                .is_none()
        );
        assert_eq!(
            orchestrator
                .writer
                .state()
                .node(NodeId(2))
                .unwrap()
                .unwrap()
                .state,
            NodeState::Active
        );
    }

    #[tokio::test]
    async fn refuses_runs_that_would_break_quorum_or_overlap() {
        let (two_voters, _first) = orchestrator(
            &[node(1, NodeRole::Voter), node(2, NodeRole::Voter)],
            FakeNodes::default(),
        );
        let error = two_voters
            .start(MaintenanceKind::Restart, "3.0.0".to_string(), Target::All)
            .await
            .unwrap_err();
        assert!(error.contains("quorum"));

        let (single, _second) = orchestrator(&[node(1, NodeRole::Voter)], FakeNodes::default());
        let error = single
            .start(MaintenanceKind::Upgrade, "3.0.0".to_string(), Target::All)
            .await
            .unwrap_err();
        assert!(error.contains("already runs"));
        single
            .start(MaintenanceKind::Upgrade, "3.1.0".to_string(), Target::All)
            .await
            .unwrap();
        let error = single
            .start(MaintenanceKind::Restart, "3.0.0".to_string(), Target::All)
            .await
            .unwrap_err();
        assert!(error.contains("in progress"));
    }
}
