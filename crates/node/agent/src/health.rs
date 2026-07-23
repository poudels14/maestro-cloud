use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use kernel_api::{
    Assignment, AssignmentId, AssignmentPhase, ClusterId, Deployment, HealthProbe, NodeId,
    ReplicaState, ResourceKind, ResourceName,
};
use kernel_store::{
    CasOutcome, Clock, ExpectedVersion, Keyspace, PutRequest, Store, StoreError, StoredValue,
    WatchCursor, WatchStart,
};
use tokio::sync::{Mutex, watch};

use crate::StatusClock;
use crate::health_probe::{HealthProbeTarget, HealthProber};
use crate::health_status::{HealthObservation, desired_health_status};

const ASSIGNMENT_KIND: &str = "Assignment";
const DEPLOYMENT_KIND: &str = "Deployment";
const REPLICA_STATE_KIND: &str = "ReplicaState";
const FNV_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
const FNV_PRIME: u64 = 0x100000001b3;
const MAX_CAS_ATTEMPTS: usize = 16;

/// Node-local health reconciliation cadence and ownership scope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HealthAgentSettings {
    /// Cluster whose replica readiness is reported.
    pub cluster_id: ClusterId,
    /// Local node; remote assignments are never probed or mutated.
    pub node_id: NodeId,
    /// Retry cadence for unhealthy probes and the event-loop resync tick.
    pub poll_interval: Duration,
}

/// Summary of one deterministic health pass.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct HealthReconcileReport {
    /// Local running assignments eligible for health evaluation.
    pub examined: usize,
    /// Network probes performed during this pass.
    pub probed: usize,
    /// Replicas made or retained ready.
    pub ready: usize,
    /// Failed probes still below their configured threshold.
    pub unhealthy: usize,
    /// Replicas at or beyond the terminal unhealthy threshold.
    pub terminal: usize,
    /// Assignments lacking a scheduler-owned ReplicaState.
    pub missing_replica_states: usize,
    /// Malformed resources skipped without stopping the health loop.
    pub malformed_resources: usize,
}

/// Store-driven health reconciler with stable probe staggering and injected time.
pub struct HealthAgent {
    store: Arc<dyn Store>,
    prober: Arc<dyn HealthProber>,
    settings: HealthAgentSettings,
    keyspace: Keyspace,
    assignment_kind: ResourceKind,
    deployment_kind: ResourceKind,
    replica_kind: ResourceKind,
    monotonic_clock: Arc<dyn Clock>,
    status_clock: Arc<dyn StatusClock>,
    schedules: Mutex<BTreeMap<AssignmentId, ProbeSchedule>>,
}

impl HealthAgent {
    /// Creates a health reconciler without spawning work.
    pub fn new(
        store: Arc<dyn Store>,
        prober: Arc<dyn HealthProber>,
        settings: HealthAgentSettings,
        monotonic_clock: Arc<dyn Clock>,
        status_clock: Arc<dyn StatusClock>,
    ) -> Result<Self, HealthAgentError> {
        if settings.poll_interval.is_zero() {
            return Err(HealthAgentError::ZeroPollInterval);
        }
        Ok(Self {
            keyspace: Keyspace::new(&settings.cluster_id),
            assignment_kind: ResourceKind::new(ASSIGNMENT_KIND)?,
            deployment_kind: ResourceKind::new(DEPLOYMENT_KIND)?,
            replica_kind: ResourceKind::new(REPLICA_STATE_KIND)?,
            store,
            prober,
            settings,
            monotonic_clock,
            status_clock,
            schedules: Mutex::new(BTreeMap::new()),
        })
    }

    /// Evaluates every due local replica and persists thresholded readiness evidence.
    pub async fn reconcile_once(&self) -> Result<HealthReconcileReport, HealthAgentError> {
        let (report, _cursor) = self.reconcile_with_cursor().await?;
        Ok(report)
    }

    /// Runs resource-watch and injected-clock driven health reconciliation until shutdown.
    pub async fn run(&self, mut shutdown: watch::Receiver<bool>) -> Result<(), HealthAgentError> {
        let mut poll_at = self
            .monotonic_clock
            .now()
            .saturating_add(self.settings.poll_interval);
        loop {
            if *shutdown.borrow() {
                return Ok(());
            }
            let (_report, cursor) = self.reconcile_with_cursor().await?;
            let mut events = self
                .store
                .watch(self.keyspace.resources(), WatchStart::After(cursor))?;
            loop {
                tokio::select! {
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow() {
                            return Ok(());
                        }
                    }
                    event = events.next() => {
                        match event {
                            Ok(_) | Err(StoreError::CursorExpired { .. }) => break,
                            Err(error) => return Err(error.into()),
                        }
                    }
                    () = self.monotonic_clock.sleep_until(poll_at) => {
                        poll_at = self
                            .monotonic_clock
                            .now()
                            .saturating_add(self.settings.poll_interval);
                        break;
                    }
                }
            }
        }
    }

    async fn reconcile_with_cursor(
        &self,
    ) -> Result<(HealthReconcileReport, WatchCursor), HealthAgentError> {
        let assignment_snapshot = self
            .store
            .list(&self.keyspace.resource_kind(&self.assignment_kind))
            .await?;
        let deployment_snapshot = self
            .store
            .list(&self.keyspace.resource_kind(&self.deployment_kind))
            .await?;
        let replica_snapshot = self
            .store
            .list(&self.keyspace.resource_kind(&self.replica_kind))
            .await?;
        let (assignments, malformed_assignments) = decode_resources(&assignment_snapshot.values);
        let (deployments, malformed_deployments) = decode_resources(&deployment_snapshot.values);
        let (replicas, malformed_replicas) = decode_resources(&replica_snapshot.values);
        let deployments = deployments
            .into_iter()
            .map(|deployment: Deployment| (deployment.meta.id.clone(), deployment))
            .collect::<BTreeMap<_, _>>();
        let replicas = replicas
            .into_iter()
            .map(|replica: ReplicaState| (replica.spec.assignment_id.clone(), replica))
            .collect::<BTreeMap<_, _>>();
        let assignments = assignments
            .into_iter()
            .filter(|assignment: &Assignment| {
                assignment.spec.node_id == self.settings.node_id
                    && assignment.meta.deletion_timestamp.is_none()
                    && assignment.status.phase == AssignmentPhase::Running
            })
            .collect::<Vec<_>>();
        let active_ids = assignments
            .iter()
            .map(|assignment| assignment.meta.id.clone())
            .collect::<BTreeSet<_>>();
        let mut report = HealthReconcileReport {
            examined: assignments.len(),
            malformed_resources: malformed_assignments
                .saturating_add(malformed_deployments)
                .saturating_add(malformed_replicas),
            ..Default::default()
        };
        for assignment in assignments {
            let Some(replica) = replicas.get(&assignment.meta.id) else {
                report.missing_replica_states = report.missing_replica_states.saturating_add(1);
                continue;
            };
            let Some(deployment) = deployments.get(&assignment.spec.deployment_id) else {
                continue;
            };
            if replica.status.phase == kernel_api::DeploymentPhase::Crashed {
                report.terminal = report.terminal.saturating_add(1);
                continue;
            }
            let health_check = deployment.spec.service.health_check.as_ref();
            if let Some(health_check) = health_check {
                if !self.probe_due(&assignment.meta.id).await {
                    continue;
                }
                let target = probe_target(assignment.spec.workload_address, &health_check.probe);
                let result = self.prober.probe(&target).await;
                report.probed = report.probed.saturating_add(1);
                match result {
                    Ok(()) => {
                        self.update_replica(
                            replica,
                            &assignment,
                            Some(health_check),
                            HealthObservation::Healthy,
                        )
                        .await?;
                        self.schedule_next(
                            assignment.meta.id.clone(),
                            ProbeOutcome::Healthy,
                            Duration::from_secs(u64::from(health_check.interval_secs.max(1))),
                        )
                        .await;
                        report.ready = report.ready.saturating_add(1);
                    }
                    Err(error) => {
                        self.update_replica(
                            replica,
                            &assignment,
                            Some(health_check),
                            HealthObservation::Unhealthy(&error.to_string()),
                        )
                        .await?;
                        self.schedule_next(
                            assignment.meta.id.clone(),
                            ProbeOutcome::Unhealthy,
                            self.settings.poll_interval,
                        )
                        .await;
                        if replica.status.healthcheck_failures.saturating_add(1)
                            >= health_check.unhealthy_threshold.max(1)
                        {
                            report.terminal = report.terminal.saturating_add(1);
                        } else {
                            report.unhealthy = report.unhealthy.saturating_add(1);
                        }
                    }
                }
            } else {
                self.update_replica(replica, &assignment, None, HealthObservation::NotConfigured)
                    .await?;
                report.ready = report.ready.saturating_add(1);
            }
        }
        self.schedules
            .lock()
            .await
            .retain(|assignment_id, _schedule| active_ids.contains(assignment_id));
        let cursor = std::cmp::min(
            std::cmp::min(assignment_snapshot.cursor, deployment_snapshot.cursor),
            replica_snapshot.cursor,
        );
        Ok((report, cursor))
    }

    async fn probe_due(&self, assignment_id: &AssignmentId) -> bool {
        let now = self.monotonic_clock.now();
        self.schedules
            .lock()
            .await
            .get(assignment_id)
            .is_none_or(|schedule| now >= schedule.next_probe)
    }

    async fn schedule_next(
        &self,
        assignment_id: AssignmentId,
        outcome: ProbeOutcome,
        interval: Duration,
    ) {
        let mut schedules = self.schedules.lock().await;
        let was_healthy = schedules
            .get(&assignment_id)
            .is_some_and(|schedule| schedule.outcome == ProbeOutcome::Healthy);
        let delay = if outcome == ProbeOutcome::Healthy && !was_healthy {
            healthy_stagger(
                assignment_id.as_str(),
                interval,
                self.settings.poll_interval,
            )
        } else {
            interval
        };
        schedules.insert(
            assignment_id,
            ProbeSchedule {
                next_probe: self.monotonic_clock.now().saturating_add(delay),
                outcome,
            },
        );
    }

    async fn update_replica(
        &self,
        replica: &ReplicaState,
        assignment: &Assignment,
        health_check: Option<&kernel_api::HealthCheckSpec>,
        observation: HealthObservation<'_>,
    ) -> Result<(), HealthAgentError> {
        let name = ResourceName::new(replica.meta.id.as_str())?;
        let key = self.keyspace.resource(&self.replica_kind, &name);
        for _attempt in 0..MAX_CAS_ATTEMPTS {
            let stored = self.store.get(&key).await?.ok_or_else(|| {
                HealthAgentError::ReplicaDisappeared {
                    replica_id: replica.meta.id.to_string(),
                }
            })?;
            let mut current: ReplicaState = decode_resource(&stored)?;
            if current.spec.assignment_id != assignment.meta.id {
                return Err(HealthAgentError::ReplicaReassigned {
                    replica_id: current.meta.id.to_string(),
                });
            }
            let desired = desired_health_status(
                &current,
                assignment,
                health_check,
                observation,
                self.status_clock.now(),
            );
            if current.status == desired {
                return Ok(());
            }
            current.status = desired;
            current.meta.revision = stored.version.resource_revision();
            let value = serde_json::to_vec(&current).map_err(|error| {
                HealthAgentError::SerializeResource {
                    message: error.to_string(),
                }
            })?;
            let result = self
                .store
                .put_cas(PutRequest {
                    key: key.clone(),
                    value,
                    expected: ExpectedVersion::Exact(stored.version),
                    session: None,
                })
                .await?;
            if matches!(result, CasOutcome::Applied(_)) {
                return Ok(());
            }
        }
        Err(HealthAgentError::Contention {
            replica_id: replica.meta.id.to_string(),
        })
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ProbeOutcome {
    Healthy,
    Unhealthy,
}

struct ProbeSchedule {
    next_probe: kernel_store::MonotonicTime,
    outcome: ProbeOutcome,
}

fn probe_target(address: std::net::IpAddr, probe: &HealthProbe) -> HealthProbeTarget {
    match probe {
        HealthProbe::Http { port, path } => HealthProbeTarget::Http {
            address,
            port: *port,
            path: path.clone(),
        },
        HealthProbe::Tcp { port } => HealthProbeTarget::Tcp {
            address,
            port: *port,
        },
    }
}

pub(crate) fn healthy_stagger(key: &str, interval: Duration, poll_interval: Duration) -> Duration {
    let tick_seconds = poll_interval.as_secs().max(1);
    let interval_seconds = interval.as_secs().max(tick_seconds);
    let slots = (interval_seconds / tick_seconds).max(1);
    let hash = key.as_bytes().iter().fold(FNV_OFFSET_BASIS, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(FNV_PRIME)
    });
    let slot = (hash % slots).saturating_add(1);
    Duration::from_secs(slot.saturating_mul(tick_seconds).min(interval_seconds))
}

fn decode_resources<Resource>(values: &[StoredValue]) -> (Vec<Resource>, usize)
where
    Resource: for<'de> serde::Deserialize<'de>,
{
    let decoded = values
        .iter()
        .map(|stored| serde_json::from_slice(&stored.value))
        .collect::<Vec<Result<Resource, _>>>();
    let malformed = decoded.iter().filter(|result| result.is_err()).count();
    (
        decoded.into_iter().filter_map(Result::ok).collect(),
        malformed,
    )
}

fn decode_resource<Resource>(stored: &StoredValue) -> Result<Resource, HealthAgentError>
where
    Resource: for<'de> serde::Deserialize<'de>,
{
    serde_json::from_slice(&stored.value).map_err(|error| HealthAgentError::MalformedResource {
        key: stored.key.to_string(),
        message: error.to_string(),
    })
}

/// Why a complete node-local health snapshot could not be reconciled.
#[derive(Debug, thiserror::Error)]
pub enum HealthAgentError {
    /// A configured resource kind or observed resource name was invalid.
    #[error(transparent)]
    InvalidIdentifier(#[from] kernel_api::InvalidIdentifier),
    /// A zero poll interval would create an unbounded reconciliation loop.
    #[error("health poll interval must be positive")]
    ZeroPollInterval,
    /// Store access or watch setup failed.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// A ReplicaState disappeared while its status was being updated.
    #[error("replica state `{replica_id}` disappeared before health status update")]
    ReplicaDisappeared { replica_id: String },
    /// A ReplicaState moved to another assignment during a probe.
    #[error("replica state `{replica_id}` was reassigned during health status update")]
    ReplicaReassigned { replica_id: String },
    /// A stored status target was malformed.
    #[error("malformed health resource at `{key}`: {message}")]
    MalformedResource { key: String, message: String },
    /// A health status could not be encoded.
    #[error("failed to serialize health resource: {message}")]
    SerializeResource { message: String },
    /// Repeated status conflicts exceeded the bounded retry budget.
    #[error("store contention prevented health update for replica `{replica_id}`")]
    Contention { replica_id: String },
}
