use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use kernel_api::{
    Assignment, AssignmentId, AssignmentPhase, ClusterId, Deployment, HealthProbe, NodeId,
    ReplicaState, ResourceKind, ResourceName, assignment_workload_address,
};
use kernel_store::{
    CasOutcome, Clock, ExpectedVersion, Keyspace, PutRequest, Store, StoreError, StoredValue,
    WatchCursor, WatchStart,
};
use tokio::sync::{Mutex, watch};

use crate::StatusClock;
use crate::health_probe::{HealthProbeTarget, HealthProber};
use crate::health_status::{HealthObservation, desired_health_status};
use crate::retry::{retryable_store_error, wait_for_store_retry_or_shutdown};

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
    /// Status targets that disappeared or changed ownership during this pass.
    pub stale_updates: usize,
}

impl HealthReconcileReport {
    fn accepted_update(&mut self, outcome: HealthUpdateOutcome) -> bool {
        match outcome {
            HealthUpdateOutcome::Current | HealthUpdateOutcome::Updated => true,
            HealthUpdateOutcome::Stale => {
                self.stale_updates = self.stale_updates.saturating_add(1);
                false
            }
            HealthUpdateOutcome::Malformed => {
                self.malformed_resources = self.malformed_resources.saturating_add(1);
                false
            }
        }
    }
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
        'reconcile: loop {
            if *shutdown.borrow() {
                return Ok(());
            }
            let cursor = match self.reconcile_with_cursor().await {
                Ok((_report, cursor)) => cursor,
                Err(error) if error.retryable() => {
                    tracing::warn!(
                        node_id = %self.settings.node_id,
                        error = %error,
                        "transient health reconciliation failure; retrying"
                    );
                    if wait_for_store_retry_or_shutdown(
                        self.monotonic_clock.as_ref(),
                        &mut shutdown,
                    )
                    .await
                    {
                        return Ok(());
                    }
                    poll_at = self
                        .monotonic_clock
                        .now()
                        .saturating_add(self.settings.poll_interval);
                    continue;
                }
                Err(error) => return Err(error),
            };
            let mut events = match self
                .store
                .watch(self.keyspace.resources(), WatchStart::After(cursor))
            {
                Ok(events) => events,
                Err(error) if retryable_store_error(&error) => {
                    tracing::warn!(
                        node_id = %self.settings.node_id,
                        error = %error,
                        "transient health watch setup failure; retrying"
                    );
                    if wait_for_store_retry_or_shutdown(
                        self.monotonic_clock.as_ref(),
                        &mut shutdown,
                    )
                    .await
                    {
                        return Ok(());
                    }
                    poll_at = self
                        .monotonic_clock
                        .now()
                        .saturating_add(self.settings.poll_interval);
                    continue;
                }
                Err(error) => return Err(error.into()),
            };
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
                            Err(error) if retryable_store_error(&error) => {
                                tracing::warn!(
                                    node_id = %self.settings.node_id,
                                    error = %error,
                                    "transient health watch failure; retrying"
                                );
                                if wait_for_store_retry_or_shutdown(
                                    self.monotonic_clock.as_ref(),
                                    &mut shutdown,
                                ).await {
                                    return Ok(());
                                }
                                poll_at = self
                                    .monotonic_clock
                                    .now()
                                    .saturating_add(self.settings.poll_interval);
                                continue 'reconcile;
                            }
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
        let (assignments, malformed_assignments) = decode_resources(
            &assignment_snapshot.values,
            ASSIGNMENT_KIND,
            &self.settings.node_id,
        );
        let (deployments, malformed_deployments) = decode_resources(
            &deployment_snapshot.values,
            DEPLOYMENT_KIND,
            &self.settings.node_id,
        );
        let (replicas, malformed_replicas) = decode_resources(
            &replica_snapshot.values,
            REPLICA_STATE_KIND,
            &self.settings.node_id,
        );
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
                let Some(address) = assignment_workload_address(&assignment) else {
                    continue;
                };
                let target = probe_target(address, &health_check.probe);
                let result = self.prober.probe(&target).await;
                report.probed = report.probed.saturating_add(1);
                match result {
                    Ok(()) => {
                        let outcome = self
                            .update_replica(
                                replica,
                                &assignment,
                                Some(health_check),
                                HealthObservation::Healthy,
                            )
                            .await?;
                        if report.accepted_update(outcome) {
                            self.schedule_next(
                                assignment.meta.id.clone(),
                                ProbeOutcome::Healthy,
                                Duration::from_secs(u64::from(health_check.interval_secs.max(1))),
                            )
                            .await;
                            report.ready = report.ready.saturating_add(1);
                        }
                    }
                    Err(error) => {
                        let outcome = self
                            .update_replica(
                                replica,
                                &assignment,
                                Some(health_check),
                                HealthObservation::Unhealthy(&error.to_string()),
                            )
                            .await?;
                        if report.accepted_update(outcome) {
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
                }
            } else {
                let outcome = self
                    .update_replica(replica, &assignment, None, HealthObservation::NotConfigured)
                    .await?;
                if report.accepted_update(outcome) {
                    report.ready = report.ready.saturating_add(1);
                }
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
    ) -> Result<HealthUpdateOutcome, HealthAgentError> {
        let name = ResourceName::new(replica.meta.id.as_str())?;
        let key = self.keyspace.resource(&self.replica_kind, &name);
        for _attempt in 0..MAX_CAS_ATTEMPTS {
            let Some(stored) = self.store.get(&key).await? else {
                return Ok(HealthUpdateOutcome::Stale);
            };
            let mut current: ReplicaState = match decode_resource(&stored) {
                Ok(current) => current,
                Err(error) => {
                    warn_malformed(REPLICA_STATE_KIND, &self.settings.node_id, &stored, &error);
                    return Ok(HealthUpdateOutcome::Malformed);
                }
            };
            if current.spec.assignment_id != assignment.meta.id {
                return Ok(HealthUpdateOutcome::Stale);
            }
            let desired = desired_health_status(
                &current,
                assignment,
                health_check,
                observation,
                self.status_clock.now(),
            );
            if current.status == desired {
                return Ok(HealthUpdateOutcome::Current);
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
                return Ok(HealthUpdateOutcome::Updated);
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HealthUpdateOutcome {
    Updated,
    Current,
    Stale,
    Malformed,
}

fn decode_resources<Resource>(
    values: &[StoredValue],
    kind: &'static str,
    node_id: &NodeId,
) -> (Vec<Resource>, usize)
where
    Resource: for<'de> serde::Deserialize<'de>,
{
    let mut decoded = Vec::new();
    let mut malformed = 0_usize;
    for stored in values {
        match serde_json::from_slice(&stored.value) {
            Ok(resource) => decoded.push(resource),
            Err(error) => {
                malformed = malformed.saturating_add(1);
                tracing::warn!(
                    kind,
                    node_id = %node_id,
                    resource_key = %stored.key,
                    error = %error,
                    "malformed health input resource was skipped"
                );
            }
        }
    }
    (decoded, malformed)
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

fn warn_malformed(
    kind: &'static str,
    node_id: &NodeId,
    stored: &StoredValue,
    error: &HealthAgentError,
) {
    tracing::warn!(
        kind,
        node_id = %node_id,
        resource_key = %stored.key,
        error = %error,
        "malformed health status target was skipped"
    );
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

impl HealthAgentError {
    fn retryable(&self) -> bool {
        match self {
            Self::Store(error) => retryable_store_error(error),
            Self::Contention { .. } => true,
            _ => false,
        }
    }
}
