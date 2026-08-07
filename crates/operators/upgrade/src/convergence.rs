use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use kernel_api::{
    AnnotationKey, Generation, Node, NodeId, NodeRole, NodeSpec, NodeStatus, Object, ObjectMeta,
    ResourceKind, ResourceName, ResourceRevision, Timestamp, UpgradeMode, UpgradeOperation,
    UpgradePhase, UpgradeRun, UpgradeRunId, UpgradeRunSpec, UpgradeRunStatus,
};
use kernel_controller::{
    Action, Backoff, ControllerRuntime, FencedStore, ReconcileContext, ReconcileError, Reconciler,
    RuntimeConfig, TimestampClock,
};
use kernel_store::{
    Clock, Compare, ExpectedVersion, Keyspace, Mutation, StoredValue, Transaction,
    TransactionOutcome,
};
use semver::Version;
use sha2::{Digest, Sha256};

use crate::plan_support::{add_duration, duration_between, parse_node_version};
use crate::snapshot::{ResourceMap, decode_kind};
use crate::{UpgradeError, UpgradeSettings};

const AUTO_UPGRADE_ID_PREFIX: &str = "auto-upgrade-";
const AUTO_UPGRADE_FINGERPRINT: &str = "upgrade.maestro.dev/auto-fingerprint";
const AUTO_UPGRADE_ATTEMPT: &str = "upgrade.maestro.dev/auto-attempt";
const MAXIMUM_RETRY_DELAY: Duration = Duration::from_secs(30 * 60);
const CONFLICT_RETRY: Duration = Duration::from_millis(100);

/// Leader-owned guard that creates rolling upgrades when nodes report mixed versions.
pub struct VersionConvergenceReconciler {
    keyspace: Keyspace,
    node_prefix: kernel_store::StorePrefix,
    timestamp_clock: Arc<dyn TimestampClock>,
    settings: UpgradeSettings,
}

impl VersionConvergenceReconciler {
    /// Constructs automatic version convergence for clusters with host upgrades enabled.
    pub fn new(
        cluster_id: kernel_api::ClusterId,
        timestamp_clock: Arc<dyn TimestampClock>,
        settings: UpgradeSettings,
    ) -> Result<Self, UpgradeError> {
        let keyspace = Keyspace::new(&cluster_id);
        let node_kind = ResourceKind::new("Node")?;
        Ok(Self {
            node_prefix: keyspace.resource_kind(&node_kind),
            keyspace,
            timestamp_clock,
            settings: settings.validate()?,
        })
    }

    /// Wraps convergence in the shared Node watch and resync runtime.
    pub fn runtime(
        self: Arc<Self>,
        store: Arc<FencedStore>,
        clock: Arc<dyn Clock>,
        config: RuntimeConfig,
    ) -> ControllerRuntime<Self> {
        ControllerRuntime::new(self.clone(), self.node_prefix.clone(), store, clock, config)
    }

    async fn converge(&self, context: &ReconcileContext) -> Result<Action, ReconcileError> {
        let snapshot = ConvergenceSnapshot::load(context.store(), &self.keyspace)
            .await
            .map_err(classify)?;
        match plan_convergence(&snapshot, self.settings, self.timestamp_clock.now())
            .map_err(classify)?
        {
            ConvergenceAction::Done => Ok(Action::Done),
            ConvergenceAction::Requeue(delay) => Ok(Action::Requeue(delay)),
            ConvergenceAction::Create { run, predecessor } => {
                let run_kind = ResourceKind::new("UpgradeRun")
                    .map_err(UpgradeError::from)
                    .map_err(classify)?;
                let run_key = self
                    .keyspace
                    .resource(&run_kind, &ResourceName::from(run.meta.id.clone()));
                let mut compares = snapshot.dependency_compares();
                if let Some(predecessor) = predecessor
                    && let Some(stored) = snapshot.runs.get(&predecessor)
                {
                    compares.push(Compare {
                        key: stored.stored.key.clone(),
                        expected: ExpectedVersion::Exact(stored.stored.version),
                    });
                }
                compares.push(Compare {
                    key: self.keyspace.maintenance_revision(),
                    expected: snapshot
                        .gate
                        .as_ref()
                        .map_or(ExpectedVersion::Missing, |gate| {
                            ExpectedVersion::Exact(gate.version)
                        }),
                });
                compares.push(Compare {
                    key: run_key.clone(),
                    expected: ExpectedVersion::Missing,
                });
                let encoded = serde_json::to_vec(&run)
                    .map_err(|error| UpgradeError::Serialize {
                        message: error.to_string(),
                    })
                    .map_err(classify)?;
                let outcome = context
                    .store()
                    .txn(Transaction {
                        compares,
                        mutations: vec![
                            Mutation::Put {
                                key: self.keyspace.maintenance_revision(),
                                value: run.meta.id.as_str().as_bytes().to_vec(),
                                session: None,
                            },
                            Mutation::Put {
                                key: run_key,
                                value: encoded,
                                session: None,
                            },
                        ],
                    })
                    .await
                    .map_err(ReconcileError::Infrastructure)?;
                match outcome {
                    TransactionOutcome::Applied { .. } => Ok(Action::Done),
                    TransactionOutcome::Conflict => Ok(Action::Requeue(CONFLICT_RETRY)),
                }
            }
        }
    }
}

#[async_trait]
impl Reconciler for VersionConvergenceReconciler {
    type Id = NodeId;
    type Spec = NodeSpec;
    type Status = NodeStatus;

    const KIND: &'static str = "Node";

    async fn reconcile(
        &self,
        resource: Object<Self::Id, Self::Spec, Self::Status>,
        context: ReconcileContext,
    ) -> Result<Action, ReconcileError> {
        if resource.meta.deletion_timestamp.is_some() || resource.spec.role != NodeRole::Master {
            return Ok(Action::Done);
        }
        self.converge(&context).await
    }
}

struct ConvergenceSnapshot {
    nodes: ResourceMap<NodeId, NodeSpec, NodeStatus>,
    runs: ResourceMap<UpgradeRunId, UpgradeRunSpec, UpgradeRunStatus>,
    gate: Option<StoredValue>,
}

impl ConvergenceSnapshot {
    async fn load(store: &FencedStore, keyspace: &Keyspace) -> Result<Self, UpgradeError> {
        let node_kind = ResourceKind::new("Node")?;
        let run_kind = ResourceKind::new("UpgradeRun")?;
        let listed_nodes = store.list(&keyspace.resource_kind(&node_kind)).await?;
        let nodes =
            decode_kind::<NodeId, NodeSpec, NodeStatus>(&listed_nodes.values, keyspace, "Node")?;
        let listed_runs = store.list(&keyspace.resource_kind(&run_kind)).await?;
        let runs = decode_kind::<UpgradeRunId, UpgradeRunSpec, UpgradeRunStatus>(
            &listed_runs.values,
            keyspace,
            "UpgradeRun",
        )?;
        Ok(Self {
            nodes,
            runs,
            gate: store.get(&keyspace.maintenance_revision()).await?,
        })
    }

    fn dependency_compares(&self) -> Vec<Compare> {
        self.nodes
            .values()
            .map(|node| Compare {
                key: node.stored.key.clone(),
                expected: ExpectedVersion::Exact(node.stored.version),
            })
            .collect()
    }
}

#[derive(Debug)]
enum ConvergenceAction {
    Done,
    Requeue(Duration),
    Create {
        run: Box<UpgradeRun>,
        predecessor: Option<UpgradeRunId>,
    },
}

fn plan_convergence(
    snapshot: &ConvergenceSnapshot,
    settings: UpgradeSettings,
    now: Timestamp,
) -> Result<ConvergenceAction, UpgradeError> {
    let nodes = snapshot
        .nodes
        .values()
        .map(|stored| &stored.resource)
        .filter(|node| node.meta.deletion_timestamp.is_none())
        .collect::<Vec<_>>();
    let runs = snapshot
        .runs
        .values()
        .map(|stored| &stored.resource)
        .filter(|run| run.meta.deletion_timestamp.is_none())
        .collect::<Vec<_>>();
    plan_observed_cluster(&nodes, &runs, settings, now)
}

fn plan_observed_cluster(
    nodes: &[&Node],
    runs: &[&UpgradeRun],
    settings: UpgradeSettings,
    now: Timestamp,
) -> Result<ConvergenceAction, UpgradeError> {
    if nodes.len() < 2 {
        return Ok(ConvergenceAction::Done);
    }
    let mut versions = BTreeMap::new();
    for node in nodes {
        versions.insert(node.meta.id.clone(), parse_node_version(node)?);
    }
    let Some(target) = versions.values().max().cloned() else {
        return Ok(ConvergenceAction::Done);
    };
    let targets = versions
        .iter()
        .filter_map(|(node_id, version)| (version < &target).then_some(node_id.clone()))
        .collect::<Vec<_>>();
    if targets.is_empty() {
        return Ok(ConvergenceAction::Done);
    }
    if runs.iter().any(|run| {
        !matches!(
            run.status.phase,
            UpgradePhase::Completed | UpgradePhase::Failed | UpgradePhase::Canceled
        )
    }) {
        return Ok(ConvergenceAction::Requeue(settings.observation_interval));
    }

    let fingerprint = convergence_fingerprint(nodes, &target);
    let latest = runs
        .iter()
        .copied()
        .filter(|run| auto_fingerprint(run) == Some(fingerprint.as_str()))
        .max_by_key(|run| run.meta.revision);
    let predecessor = latest.map(|run| run.meta.id.clone());
    let attempt = match latest {
        Some(run) if run.status.phase == UpgradePhase::Canceled => {
            return Ok(ConvergenceAction::Done);
        }
        Some(run) => {
            let previous = auto_attempt(run)?;
            if run.status.phase == UpgradePhase::Failed {
                let last_transition = run
                    .status
                    .conditions
                    .iter()
                    .map(|condition| condition.last_transition_time)
                    .max()
                    .unwrap_or(now);
                let backoff = Backoff::new(
                    settings.retry_delay,
                    settings.retry_delay.max(MAXIMUM_RETRY_DELAY),
                )?;
                let retry_at = add_duration(last_transition, backoff.delay(previous));
                let remaining = duration_between(now, retry_at);
                if !remaining.is_zero() {
                    return Ok(ConvergenceAction::Requeue(remaining));
                }
            }
            previous.saturating_add(1)
        }
        None => 0,
    };
    Ok(ConvergenceAction::Create {
        run: Box::new(new_auto_run(&fingerprint, attempt, target, targets)?),
        predecessor,
    })
}

fn convergence_fingerprint(nodes: &[&Node], target: &Version) -> String {
    let mut digest = Sha256::new();
    digest_field(&mut digest, target.to_string().as_bytes());
    for node in nodes {
        digest_field(&mut digest, node.meta.id.as_str().as_bytes());
        digest_field(&mut digest, node.status.version.as_bytes());
        digest_field(&mut digest, node.status.instance_id.as_str().as_bytes());
    }
    hex::encode(digest.finalize())
}

fn digest_field(digest: &mut Sha256, value: &[u8]) {
    digest.update(value.len().to_be_bytes());
    digest.update(value);
}

fn auto_fingerprint(run: &UpgradeRun) -> Option<&str> {
    annotation(run, AUTO_UPGRADE_FINGERPRINT)
}

fn auto_attempt(run: &UpgradeRun) -> Result<u32, UpgradeError> {
    let value = annotation(run, AUTO_UPGRADE_ATTEMPT).unwrap_or("0");
    value
        .parse()
        .map_err(|_| UpgradeError::InvalidAutoUpgradeAttempt {
            run_id: run.meta.id.clone(),
            value: value.to_string(),
        })
}

fn annotation<'a>(run: &'a UpgradeRun, name: &str) -> Option<&'a str> {
    run.meta
        .annotations
        .iter()
        .find_map(|(key, value)| (key.0 == name).then_some(value.as_str()))
}

fn new_auto_run(
    fingerprint: &str,
    attempt: u32,
    target: Version,
    node_ids: Vec<NodeId>,
) -> Result<UpgradeRun, UpgradeError> {
    let mut identity = Sha256::new();
    digest_field(&mut identity, fingerprint.as_bytes());
    digest_field(&mut identity, &attempt.to_be_bytes());
    let encoded = hex::encode(identity.finalize());
    let run_id = UpgradeRunId::new(format!(
        "{AUTO_UPGRADE_ID_PREFIX}{}",
        encoded.get(..32).unwrap_or(encoded.as_str())
    ))?;
    Ok(Object {
        meta: ObjectMeta {
            id: run_id,
            labels: BTreeMap::new(),
            annotations: BTreeMap::from([
                (
                    AnnotationKey(AUTO_UPGRADE_FINGERPRINT.to_string()),
                    fingerprint.to_string(),
                ),
                (
                    AnnotationKey(AUTO_UPGRADE_ATTEMPT.to_string()),
                    attempt.to_string(),
                ),
            ]),
            revision: ResourceRevision::default(),
            generation: Generation(1),
            owner_refs: Vec::new(),
            finalizers: BTreeSet::new(),
            deletion_timestamp: None,
        },
        spec: UpgradeRunSpec {
            operation: UpgradeOperation::Upgrade,
            target_version: target.to_string(),
            mode: UpgradeMode::Rolling,
            node_ids,
        },
        status: UpgradeRunStatus {
            phase: UpgradePhase::Pending,
            nodes: Vec::new(),
            conditions: Vec::new(),
        },
    })
}

fn classify(error: UpgradeError) -> ReconcileError {
    match error {
        UpgradeError::Controller(error) => ReconcileError::Infrastructure(error),
        error => ReconcileError::Terminal {
            reason: "VersionConvergenceFailed".to_string(),
            message: error.to_string(),
        },
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::panic)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::net::{IpAddr, Ipv4Addr};
    use std::time::Duration;

    use kernel_api::{
        Generation, NodeInstanceId, NodeRole, NodeSpec, NodeStatus, Object, ObjectMeta,
        ResourceRevision, Timestamp, UpgradePhase,
    };

    use super::{ConvergenceAction, convergence_fingerprint, new_auto_run, plan_observed_cluster};
    use crate::UpgradeSettings;

    fn settings() -> UpgradeSettings {
        UpgradeSettings::new(Duration::from_secs(5), Duration::from_secs(1), 10)
            .expect("settings must be valid")
    }

    fn node(id: &str, role: NodeRole, version: &str) -> kernel_api::Node {
        Object {
            meta: ObjectMeta {
                id: kernel_api::NodeId::new(id).expect("node ID must be valid"),
                labels: BTreeMap::new(),
                annotations: BTreeMap::new(),
                revision: ResourceRevision::default(),
                generation: Generation(1),
                owner_refs: Vec::new(),
                finalizers: BTreeSet::new(),
                deletion_timestamp: None,
            },
            spec: NodeSpec {
                hostname: format!("{id}.internal"),
                host_address: IpAddr::V4(Ipv4Addr::LOCALHOST),
                role,
                workload_network_mode: kernel_api::WorkloadNetworkMode::ClusterRouted,
                scheduling_labels: BTreeMap::new(),
            },
            status: NodeStatus {
                instance_id: NodeInstanceId::new(format!("instance-{id}"))
                    .expect("instance ID must be valid"),
                version: version.to_string(),
                last_seen: Timestamp(10_000),
                conditions: Vec::new(),
            },
        }
    }

    #[test]
    fn lower_version_master_is_automatically_targeted() {
        let master = node("master", NodeRole::Master, "0.6.42");
        let first = node("worker-1", NodeRole::Hybrid, "0.6.43");
        let second = node("worker-2", NodeRole::Hybrid, "0.6.43");
        let nodes = vec![&master, &first, &second];

        let action = plan_observed_cluster(&nodes, &[], settings(), Timestamp(10_000))
            .expect("observation must be valid");

        let ConvergenceAction::Create { run, .. } = action else {
            panic!("mixed versions must create an upgrade run");
        };
        assert_eq!(run.spec.target_version, "0.6.43");
        assert_eq!(run.spec.mode, kernel_api::UpgradeMode::Rolling);
        assert_eq!(run.spec.node_ids, vec![master.meta.id]);
    }

    #[test]
    fn every_lower_version_node_is_targeted_in_stable_order() {
        let master = node("master", NodeRole::Master, "0.6.44");
        let first = node("worker-b", NodeRole::Hybrid, "0.6.42");
        let second = node("worker-a", NodeRole::Hybrid, "0.6.43");
        let nodes = vec![&first, &master, &second];

        let action = plan_observed_cluster(&nodes, &[], settings(), Timestamp(10_000))
            .expect("observation must be valid");

        let ConvergenceAction::Create { run, .. } = action else {
            panic!("mixed versions must create an upgrade run");
        };
        assert_eq!(
            run.spec
                .node_ids
                .iter()
                .map(kernel_api::NodeId::as_str)
                .collect::<Vec<_>>(),
            vec!["worker-a", "worker-b"]
        );
    }

    #[test]
    fn equal_versions_do_not_create_an_upgrade() {
        let master = node("master", NodeRole::Master, "0.6.43");
        let worker = node("worker", NodeRole::Hybrid, "0.6.43");

        let action = plan_observed_cluster(&[&master, &worker], &[], settings(), Timestamp(10_000))
            .expect("observation must be valid");

        assert!(matches!(action, ConvergenceAction::Done));
    }

    #[test]
    fn active_maintenance_run_blocks_automatic_convergence() {
        let master = node("master", NodeRole::Master, "0.6.42");
        let worker = node("worker", NodeRole::Hybrid, "0.6.43");
        let nodes = vec![&master, &worker];
        let fingerprint = convergence_fingerprint(&nodes, &semver::Version::new(0, 6, 43));
        let run = new_auto_run(
            &fingerprint,
            0,
            semver::Version::new(0, 6, 43),
            vec![master.meta.id.clone()],
        )
        .expect("run must be valid");

        let action = plan_observed_cluster(&nodes, &[&run], settings(), Timestamp(10_000))
            .expect("observation must be valid");

        assert!(matches!(action, ConvergenceAction::Requeue(_)));
    }

    #[test]
    fn canceled_automatic_run_stays_canceled_for_the_same_observation() {
        let master = node("master", NodeRole::Master, "0.6.42");
        let worker = node("worker", NodeRole::Hybrid, "0.6.43");
        let nodes = vec![&master, &worker];
        let fingerprint = convergence_fingerprint(&nodes, &semver::Version::new(0, 6, 43));
        let mut run = new_auto_run(
            &fingerprint,
            0,
            semver::Version::new(0, 6, 43),
            vec![master.meta.id.clone()],
        )
        .expect("run must be valid");
        run.status.phase = UpgradePhase::Canceled;

        let action = plan_observed_cluster(&nodes, &[&run], settings(), Timestamp(10_000))
            .expect("observation must be valid");

        assert!(matches!(action, ConvergenceAction::Done));
    }
}
