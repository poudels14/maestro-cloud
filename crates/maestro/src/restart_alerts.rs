use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use maestro_control::{AgentHub, ObservedReplicas};
use maestro_core::{ReplicaId, ReplicaPhase, ReplicaStatus, ServiceId, SlackCategory};
use maestro_store::Consensus;

use crate::slack::{SlackState, escape, notify};

const CHECK_INTERVAL: Duration = Duration::from_secs(2);
const WINDOW: Duration = Duration::from_secs(10 * 60);

#[derive(Clone, Debug, PartialEq, Eq)]
struct Restart {
    at: Instant,
    replica: ReplicaId,
    error: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Unstable {
    service: ServiceId,
    restarts: usize,
    replicas: BTreeSet<ReplicaId>,
    last_error: Option<String>,
}

#[derive(Default)]
struct Tracker {
    replicas: HashMap<ReplicaId, (u32, ReplicaPhase)>,
    restarts: HashMap<ServiceId, VecDeque<Restart>>,
    alerted: HashSet<ServiceId>,
}

pub async fn run_restart_alerts(
    state: SlackState,
    consensus: Arc<Consensus>,
    hub: Arc<AgentHub>,
    threshold: u32,
) {
    let mut ticker = tokio::time::interval(CHECK_INTERVAL);
    let mut tracker = Tracker::default();
    loop {
        ticker.tick().await;
        match consensus.is_leader() && threshold > 0 {
            false => tracker = Tracker::default(),
            true => {
                for unstable in tracker.observe(&hub.replicas(), threshold, Instant::now()) {
                    notify(
                        &state,
                        SlackCategory::Error,
                        &message(&state.cluster, &unstable),
                    )
                    .await;
                }
            }
        }
    }
}

impl Tracker {
    fn observe(
        &mut self,
        statuses: &[ReplicaStatus],
        threshold: u32,
        now: Instant,
    ) -> Vec<Unstable> {
        for status in statuses {
            let restarted = self
                .replicas
                .get(&status.replica_id)
                .map_or(0, |(restarts, phase)| {
                    let entered_restart = status.phase == ReplicaPhase::Restarting
                        && *phase != ReplicaPhase::Restarting;
                    status
                        .restarts
                        .saturating_sub(*restarts)
                        .max(u32::from(entered_restart))
                });
            let history = self.restarts.entry(status.service_id.clone()).or_default();
            (0..restarted).for_each(|_| {
                history.push_back(Restart {
                    at: now,
                    replica: status.replica_id.clone(),
                    error: status.last_error.clone(),
                })
            });
        }
        self.replicas = statuses
            .iter()
            .map(|status| (status.replica_id.clone(), (status.restarts, status.phase)))
            .collect();
        self.restarts.retain(|_, history| {
            while history
                .front()
                .is_some_and(|restart| now.duration_since(restart.at) >= WINDOW)
            {
                history.pop_front();
            }
            !history.is_empty()
        });
        let unstable = self
            .restarts
            .iter()
            .filter(|(_, history)| history.len() >= threshold as usize)
            .map(|(service, history)| Unstable {
                service: service.clone(),
                restarts: history.len(),
                replicas: history
                    .iter()
                    .map(|restart| restart.replica.clone())
                    .collect(),
                last_error: history.back().and_then(|restart| restart.error.clone()),
            })
            .collect::<Vec<_>>();
        let current = unstable
            .iter()
            .map(|unstable| unstable.service.clone())
            .collect::<HashSet<_>>();
        let fresh = unstable
            .into_iter()
            .filter(|unstable| !self.alerted.contains(&unstable.service))
            .collect();
        self.alerted = current;
        fresh
    }
}

fn message(cluster: &str, unstable: &Unstable) -> String {
    let replicas = unstable
        .replicas
        .iter()
        .map(|replica| format!("`{replica}`"))
        .collect::<Vec<_>>()
        .join(", ");
    let last_error = unstable
        .last_error
        .as_deref()
        .map(|error| format!("\n> last error: {}", escape(error).replace('\n', "\n> ")))
        .unwrap_or_default();
    format!(
        ":warning: *Service unstable* — `{}` restarted {} times in the last {} minutes\n> cluster: `{cluster}` · replicas: {replicas}{last_error}",
        unstable.service,
        unstable.restarts,
        WINDOW.as_secs() / 60,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use maestro_core::DeploymentId;

    fn status(replica: &str, phase: ReplicaPhase, restarts: u32) -> ReplicaStatus {
        ReplicaStatus {
            replica_id: ReplicaId::from(replica),
            service_id: ServiceId::from("api"),
            deployment_id: DeploymentId::from("d1"),
            phase,
            ip: None,
            restarts,
            consecutive_failures: 0,
            last_error: Some("exited with code 1".to_string()),
            drained: false,
        }
    }

    fn observe_all(tracker: &mut Tracker, steps: &[(ReplicaPhase, u32)], start: Instant) -> usize {
        steps
            .iter()
            .enumerate()
            .map(|(index, (phase, restarts))| {
                tracker
                    .observe(
                        &[status("api-0", *phase, *restarts)],
                        3,
                        start + Duration::from_secs(index as u64 * 10),
                    )
                    .len()
            })
            .sum()
    }

    #[test]
    fn alerts_once_when_restarts_reach_the_threshold() {
        let mut tracker = Tracker::default();
        let start = Instant::now();
        let alerts = observe_all(
            &mut tracker,
            &[
                (ReplicaPhase::Healthy, 0),
                (ReplicaPhase::Restarting, 1),
                (ReplicaPhase::Healthy, 1),
                (ReplicaPhase::Restarting, 2),
                (ReplicaPhase::Healthy, 2),
                (ReplicaPhase::Restarting, 3),
                (ReplicaPhase::Restarting, 4),
            ],
            start,
        );
        assert_eq!(alerts, 1);
        assert_eq!(tracker.restarts[&ServiceId::from("api")].len(), 4);
    }

    #[test]
    fn counts_restarts_after_the_counter_resets() {
        let mut tracker = Tracker::default();
        let alerts = observe_all(
            &mut tracker,
            &[
                (ReplicaPhase::Healthy, 1),
                (ReplicaPhase::Restarting, 1),
                (ReplicaPhase::Healthy, 1),
                (ReplicaPhase::Restarting, 1),
                (ReplicaPhase::Healthy, 1),
                (ReplicaPhase::Restarting, 1),
            ],
            Instant::now(),
        );
        assert_eq!(alerts, 1);
    }

    #[test]
    fn ignores_first_sight_and_unreachable_nodes() {
        let mut tracker = Tracker::default();
        let alerts = observe_all(
            &mut tracker,
            &[
                (ReplicaPhase::Restarting, 5),
                (ReplicaPhase::Failed, 5),
                (ReplicaPhase::Healthy, 5),
            ],
            Instant::now(),
        );
        assert_eq!(alerts, 0);
        assert!(tracker.restarts.is_empty());
    }

    #[test]
    fn rolling_out_a_new_deployment_is_not_a_restart() {
        let mut tracker = Tracker::default();
        let start = Instant::now();
        let replicas = |deployment: &str, phase: ReplicaPhase| {
            (0..3)
                .map(|index| ReplicaStatus {
                    deployment_id: DeploymentId::from(deployment),
                    last_error: None,
                    ..status(&format!("{deployment}-{index}"), phase, 0)
                })
                .collect::<Vec<_>>()
        };
        let rollout = [
            replicas("d1", ReplicaPhase::Healthy),
            [
                replicas("d1", ReplicaPhase::Healthy),
                replicas("d2", ReplicaPhase::Starting),
            ]
            .concat(),
            [
                replicas("d1", ReplicaPhase::Healthy),
                replicas("d2", ReplicaPhase::Healthy),
            ]
            .concat(),
            replicas("d2", ReplicaPhase::Healthy),
        ];
        let alerts = rollout
            .iter()
            .enumerate()
            .flat_map(|(index, statuses)| {
                tracker.observe(statuses, 3, start + Duration::from_secs(index as u64 * 10))
            })
            .count();
        assert_eq!(alerts, 0);
        assert!(tracker.restarts.is_empty());
    }

    #[test]
    fn counts_restarts_across_the_service() {
        let mut tracker = Tracker::default();
        let start = Instant::now();
        let replicas = |restarted: usize| {
            (0..3)
                .map(|index| match index < restarted {
                    true => status(&format!("d1-{index}"), ReplicaPhase::Restarting, 1),
                    false => status(&format!("d1-{index}"), ReplicaPhase::Healthy, 0),
                })
                .collect::<Vec<_>>()
        };
        let alerts = (0..=3)
            .map(|restarted| {
                tracker.observe(
                    &replicas(restarted),
                    3,
                    start + Duration::from_secs(restarted as u64 * 10),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            alerts.iter().map(Vec::len).collect::<Vec<_>>(),
            [0, 0, 0, 1]
        );
        assert_eq!(alerts[3][0].restarts, 3);
        assert_eq!(
            alerts[3][0].replicas,
            BTreeSet::from(["d1-0", "d1-1", "d1-2"].map(ReplicaId::from))
        );
    }

    #[test]
    fn rearms_once_restarts_leave_the_window() {
        let mut tracker = Tracker::default();
        let start = Instant::now();
        let steps = [
            (ReplicaPhase::Healthy, 0),
            (ReplicaPhase::Restarting, 1),
            (ReplicaPhase::Restarting, 2),
            (ReplicaPhase::Restarting, 3),
        ];
        assert_eq!(observe_all(&mut tracker, &steps, start), 1);
        let later = start + WINDOW + Duration::from_secs(60);
        assert!(
            tracker
                .observe(&[status("api-0", ReplicaPhase::Healthy, 3)], 3, later)
                .is_empty()
        );
        assert!(tracker.alerted.is_empty());
        let steps = [
            (ReplicaPhase::Restarting, 4),
            (ReplicaPhase::Restarting, 5),
            (ReplicaPhase::Restarting, 6),
        ];
        assert_eq!(observe_all(&mut tracker, &steps, later), 1);
    }

    #[test]
    fn formats_the_alert() {
        let text = message(
            "prod",
            &Unstable {
                service: ServiceId::from("api"),
                restarts: 3,
                replicas: BTreeSet::from([ReplicaId::from("api-0")]),
                last_error: Some("exited with code 137".to_string()),
            },
        );
        assert_eq!(
            text,
            ":warning: *Service unstable* — `api` restarted 3 times in the last 10 minutes\n> cluster: `prod` · replicas: `api-0`\n> last error: exited with code 137"
        );
    }
}

#[cfg(test)]
mod agent_tests {
    use super::*;
    use maestro_agent::{LocalPlanSource, Reconciler, ReconcilerSettings, StatusBoard};
    use maestro_core::{
        Assignment, ByteSize, DeploySpec, Deployment, DeploymentId, DeploymentStatus,
        EffectiveResources, Millicores, NodeId, ResolvedRequest, ServiceSpec,
    };
    use maestro_runtime::FakeRuntime;
    use maestro_store::{DiscoveryKeys, Mutation, SecretKey, StateMachine};
    use tempfile::TempDir;

    const NODE: NodeId = NodeId(1);
    const IMAGE: &str = "nginx:1.27";
    const THRESHOLD: u32 = 3;

    struct Cluster {
        _directory: TempDir,
        state: Arc<StateMachine>,
        runtime: Arc<FakeRuntime>,
        board: StatusBoard,
        reconciler: Reconciler<FakeRuntime, LocalPlanSource>,
        tracker: Tracker,
    }

    impl Cluster {
        fn new(stable_after: Duration) -> Self {
            let directory = tempfile::tempdir().unwrap();
            let state = Arc::new(StateMachine::open(directory.path().join("state.redb")).unwrap());
            let runtime = Arc::new(FakeRuntime::default());
            let board = StatusBoard::new();
            let reconciler = Reconciler::new(
                runtime.clone(),
                Arc::new(LocalPlanSource::new(
                    state.clone(),
                    Arc::new(SecretKey::from_bytes(&[5u8; 32])),
                    Arc::new(DiscoveryKeys::new("jwt-secret")),
                    NODE,
                )),
                board.clone(),
                ReconcilerSettings {
                    restart_base_delay: Duration::ZERO,
                    stable_after,
                    secrets_dir: directory.path().join("secrets"),
                    ..ReconcilerSettings::default()
                },
            );
            Self {
                _directory: directory,
                state,
                runtime,
                board,
                reconciler,
                tracker: Tracker::default(),
            }
        }

        fn deploy(&self, deployment: &str) {
            let spec = ServiceSpec {
                name: "API".to_string(),
                image: Some(IMAGE.to_string()),
                build: None,
                deploy: DeploySpec::default(),
                ingress: None,
                preview: None,
                network: None,
            };
            self.state
                .apply(&[
                    Mutation::PutService {
                        id: ServiceId::from("api"),
                        spec: spec.clone(),
                    },
                    Mutation::CreateDeployment {
                        deployment: Deployment {
                            id: DeploymentId::from(deployment),
                            service_id: ServiceId::from("api"),
                            spec,
                            status: DeploymentStatus::Deploying,
                            image: Some(IMAGE.to_string()),
                            failure: None,
                            created_at_ms: 1,
                            updated_at_ms: 1,
                            git_commit: None,
                            built_on: None,
                            version: None,
                            restart_of: None,
                        },
                    },
                ])
                .unwrap();
        }

        fn assign(&self, deployments: &[&str]) {
            let assignments = deployments
                .iter()
                .flat_map(|deployment| {
                    (0..3).map(|index| {
                        let deployment_id = DeploymentId::from(*deployment);
                        Assignment {
                            replica_id: ReplicaId::for_deployment(&deployment_id, index),
                            service_id: ServiceId::from("api"),
                            deployment_id,
                            node_id: NODE,
                            index,
                            image: IMAGE.to_string(),
                            resources: EffectiveResources {
                                cpu_limit: Millicores::new(1000),
                                memory_limit: ByteSize::new(4 << 30),
                                request: ResolvedRequest {
                                    cpu: Millicores::new(250),
                                    memory: ByteSize::new(512 << 20),
                                },
                            },
                            retiring_since_ms: None,
                        }
                    })
                })
                .collect();
            self.state
                .apply(&[Mutation::SetNodeAssignments {
                    node_id: NODE,
                    assignments,
                }])
                .unwrap();
        }

        async fn step(&mut self) -> Vec<Unstable> {
            self.reconciler.reconcile().await.unwrap();
            self.tracker
                .observe(&self.board.snapshot(), THRESHOLD, Instant::now())
        }

        async fn crash(&mut self, replica: &str) -> Vec<Unstable> {
            self.runtime.exit(replica, 1);
            let crashed = self.step().await;
            let relaunched = self.step().await;
            [crashed, relaunched].concat()
        }

        fn phase(&self, replica: &str) -> Option<ReplicaPhase> {
            self.board
                .get(&ReplicaId::from(replica))
                .map(|status| status.phase)
        }
    }

    #[tokio::test]
    async fn rolling_out_three_replicas_does_not_alert() {
        let mut cluster = Cluster::new(Duration::from_secs(60));
        cluster.deploy("d1");
        cluster.assign(&["d1"]);
        let mut alerts = cluster.step().await;
        cluster.deploy("d2");
        cluster.assign(&["d1", "d2"]);
        alerts.extend(cluster.step().await);
        cluster.assign(&["d2"]);
        alerts.extend(cluster.step().await);
        alerts.extend(cluster.step().await);

        assert!(alerts.is_empty());
        assert_eq!(cluster.phase("d1-0"), None);
        assert_eq!(cluster.phase("d2-2"), Some(ReplicaPhase::Healthy));
        assert!(cluster.tracker.restarts.is_empty());
    }

    #[tokio::test]
    async fn alerts_when_replicas_crash_across_the_service() {
        let mut cluster = Cluster::new(Duration::from_secs(60));
        cluster.deploy("d1");
        cluster.assign(&["d1"]);
        assert!(cluster.step().await.is_empty());

        assert!(cluster.crash("d1-0").await.is_empty());
        assert!(cluster.crash("d1-1").await.is_empty());
        assert_eq!(cluster.phase("d1-1"), Some(ReplicaPhase::Healthy));
        let alerts = cluster.crash("d1-2").await;

        assert_eq!(alerts.len(), 1);
        assert_eq!(alerts[0].service, ServiceId::from("api"));
        assert_eq!(alerts[0].restarts, 3);
        assert_eq!(
            alerts[0].replicas,
            BTreeSet::from(["d1-0", "d1-1", "d1-2"].map(ReplicaId::from))
        );
        assert_eq!(alerts[0].last_error.as_deref(), Some("exited with code 1"));
        assert!(cluster.crash("d1-0").await.is_empty());
    }

    #[tokio::test]
    async fn alerts_when_a_stable_replica_keeps_crashing() {
        let mut cluster = Cluster::new(Duration::ZERO);
        cluster.deploy("d1");
        cluster.assign(&["d1"]);
        cluster.step().await;

        let alerts = [
            cluster.crash("d1-0").await,
            cluster.crash("d1-0").await,
            cluster.crash("d1-0").await,
        ]
        .concat();

        assert_eq!(
            cluster
                .board
                .get(&ReplicaId::from("d1-0"))
                .unwrap()
                .restarts,
            1
        );
        assert_eq!(alerts.len(), 1);
        assert_eq!(alerts[0].restarts, 3);
    }
}
