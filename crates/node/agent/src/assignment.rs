use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use kernel_api::{
    Assignment, ClusterId, Deployment, DeploymentId, NodeId, ResourceKind, ResourceName,
};
use kernel_store::{
    CasOutcome, Clock, ExpectedVersion, Keyspace, PutRequest, Store, StoreError, StoredValue,
    WatchCursor, WatchStart,
};
use runtime::{
    AddressLease, AddressRequest, NetworkHandle, NetworkProvider, NetworkProviderError,
    NetworkSpec, RuntimeError, ShutdownRequest, WorkloadHandle, WorkloadRuntime, WorkloadState,
};
use tokio::sync::watch;

use crate::StatusClock;
use crate::assignment_plan::workload_spec;
use crate::assignment_status::{AssignmentOutcome, ConvergeFailure, desired_status};

const ASSIGNMENT_KIND: &str = "Assignment";
const DEPLOYMENT_KIND: &str = "Deployment";
const RUNTIME_RETRY_REASON: &str = "RuntimeRetry";
const MAX_CAS_ATTEMPTS: usize = 16;

/// Node-scoped assignment reconciliation settings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssignmentAgentSettings {
    /// Cluster whose assignment and runtime ownership labels are reconciled.
    pub cluster_id: ClusterId,
    /// Local node; assignments for other nodes are never mutated.
    pub node_id: NodeId,
    /// Node-local runtime bridge and exact host-owned IPAM range.
    pub network: NetworkSpec,
    /// Graceful workload shutdown deadline before forced termination.
    pub stop_timeout: Duration,
    /// Level-triggered full reconciliation interval.
    pub resync_interval: Duration,
}

/// Results of one complete desired/runtime-state comparison.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AssignmentReconcileReport {
    /// Active local assignments observed in the store snapshot.
    pub desired: usize,
    /// Assignments confirmed running after reconciliation.
    pub running: usize,
    /// Assignments left pending or failed with a status condition.
    pub unresolved: usize,
    /// Runtime workloads removed because no active local assignment owned them.
    pub garbage_collected: usize,
    /// Malformed resources skipped without crashing the agent loop.
    pub malformed_resources: usize,
}

/// Level-triggered node reconciler for assignment lifecycle, adoption, and garbage collection.
pub struct AssignmentAgent {
    store: Arc<dyn Store>,
    runtime: Arc<dyn WorkloadRuntime>,
    network: Arc<dyn NetworkProvider>,
    settings: AssignmentAgentSettings,
    keyspace: Keyspace,
    assignment_kind: ResourceKind,
    deployment_kind: ResourceKind,
    monotonic_clock: Arc<dyn Clock>,
    status_clock: Arc<dyn StatusClock>,
}

impl AssignmentAgent {
    /// Creates a node agent without starting background work.
    pub fn new(
        store: Arc<dyn Store>,
        runtime: Arc<dyn WorkloadRuntime>,
        network: Arc<dyn NetworkProvider>,
        settings: AssignmentAgentSettings,
        monotonic_clock: Arc<dyn Clock>,
        status_clock: Arc<dyn StatusClock>,
    ) -> Result<Self, AssignmentAgentError> {
        if settings.stop_timeout.is_zero() || settings.resync_interval.is_zero() {
            return Err(AssignmentAgentError::ZeroDeadline);
        }
        Ok(Self {
            keyspace: Keyspace::new(&settings.cluster_id),
            assignment_kind: ResourceKind::new(ASSIGNMENT_KIND)?,
            deployment_kind: ResourceKind::new(DEPLOYMENT_KIND)?,
            store,
            runtime,
            network,
            settings,
            monotonic_clock,
            status_clock,
        })
    }

    /// Reconciles one linearizable assignment snapshot and all owned runtime objects.
    pub async fn reconcile_once(&self) -> Result<AssignmentReconcileReport, AssignmentAgentError> {
        let (report, _cursor) = self.reconcile_with_cursor().await?;
        Ok(report)
    }

    /// Runs watch-driven reconciliation with periodic full resync until shutdown.
    pub async fn run(
        &self,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<(), AssignmentAgentError> {
        let mut resync_at = self
            .monotonic_clock
            .now()
            .saturating_add(self.settings.resync_interval);
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
                    () = self.monotonic_clock.sleep_until(resync_at) => {
                        resync_at = self
                            .monotonic_clock
                            .now()
                            .saturating_add(self.settings.resync_interval);
                        break;
                    }
                }
            }
        }
    }

    async fn reconcile_with_cursor(
        &self,
    ) -> Result<(AssignmentReconcileReport, WatchCursor), AssignmentAgentError> {
        let assignment_snapshot = self
            .store
            .list(&self.keyspace.resource_kind(&self.assignment_kind))
            .await?;
        let deployment_snapshot = self
            .store
            .list(&self.keyspace.resource_kind(&self.deployment_kind))
            .await?;
        let (assignments, malformed_assignments) = decode_assignments(&assignment_snapshot.values);
        let (deployments, malformed_deployments) = decode_deployments(&deployment_snapshot.values);
        let local = assignments
            .into_iter()
            .filter(|assignment| assignment.spec.node_id == self.settings.node_id)
            .collect::<Vec<_>>();
        let active = local
            .iter()
            .filter(|assignment| assignment.meta.deletion_timestamp.is_none())
            .collect::<Vec<_>>();
        let network = self.network.ensure_network(&self.settings.network).await?;
        let mut report = AssignmentReconcileReport {
            desired: active.len(),
            malformed_resources: malformed_assignments.saturating_add(malformed_deployments),
            ..Default::default()
        };
        for assignment in &active {
            let outcome = match deployments.get(&assignment.spec.deployment_id) {
                Some(deployment) => {
                    self.converge_assignment(assignment, deployment, &network)
                        .await
                }
                None => Err(ConvergeFailure::pending(
                    "DeploymentMissing",
                    format!(
                        "deployment `{}` is not present in the observed snapshot",
                        assignment.spec.deployment_id
                    ),
                )),
            };
            match outcome {
                Ok(handle) => {
                    self.update_status(assignment, AssignmentOutcome::Running(&handle))
                        .await?;
                    report.running = report.running.saturating_add(1);
                }
                Err(failure) => {
                    self.update_status(assignment, AssignmentOutcome::Unresolved(&failure))
                        .await?;
                    report.unresolved = report.unresolved.saturating_add(1);
                }
            }
        }

        if malformed_assignments == 0 {
            let active_ids = active
                .iter()
                .map(|assignment| assignment.meta.id.clone())
                .collect::<BTreeSet<_>>();
            let observed = self
                .runtime
                .list(&self.settings.cluster_id, &self.settings.node_id)
                .await?;
            for workload in observed {
                if !active_ids.contains(&workload.metadata.assignment_id) {
                    self.remove_workload(&workload.handle).await?;
                    report.garbage_collected = report.garbage_collected.saturating_add(1);
                }
            }
        }
        for assignment in local
            .iter()
            .filter(|assignment| assignment.meta.deletion_timestamp.is_some())
        {
            self.update_status(assignment, AssignmentOutcome::Stopped)
                .await?;
        }
        Ok((
            report,
            std::cmp::min(assignment_snapshot.cursor, deployment_snapshot.cursor),
        ))
    }

    async fn converge_assignment(
        &self,
        assignment: &Assignment,
        deployment: &Deployment,
        network: &NetworkHandle,
    ) -> Result<WorkloadHandle, ConvergeFailure> {
        let spec = workload_spec(&self.settings.cluster_id, assignment, deployment)?;
        let handle = self.runtime.create(&spec).await?;
        let lease = self
            .network
            .allocate_address(
                network,
                handle.workload_id(),
                AddressRequest::Exact(assignment.spec.workload_address),
            )
            .await?;
        self.network.attach(&handle, network, &lease).await?;
        self.runtime.start(&handle).await?;
        let status = self.runtime.status(&handle).await?;
        if status.state == WorkloadState::Running {
            Ok(handle)
        } else {
            Err(ConvergeFailure::pending(
                RUNTIME_RETRY_REASON,
                format!("runtime reported {:?} after start", status.state),
            ))
        }
    }

    async fn remove_workload(&self, handle: &WorkloadHandle) -> Result<(), AssignmentAgentError> {
        let attachments = self.network.inspect(handle).await?.attachments;
        let stop = self
            .runtime
            .stop(
                handle,
                ShutdownRequest {
                    timeout: self.settings.stop_timeout,
                },
            )
            .await;
        if matches!(stop, Err(RuntimeError::Timeout { .. })) {
            self.runtime.kill(handle).await?;
        } else {
            stop?;
        }
        for attachment in attachments {
            self.network
                .release_address(
                    &attachment.network,
                    &AddressLease {
                        workload_id: handle.workload_id().clone(),
                        address: attachment.address,
                    },
                )
                .await?;
            self.network.detach(handle, &attachment.network).await?;
        }
        self.runtime.remove(handle).await?;
        Ok(())
    }

    async fn update_status(
        &self,
        assignment: &Assignment,
        outcome: AssignmentOutcome<'_>,
    ) -> Result<(), AssignmentAgentError> {
        let resource_name = ResourceName::new(assignment.meta.id.as_str())?;
        let key = self
            .keyspace
            .resource(&self.assignment_kind, &resource_name);
        for _attempt in 0..MAX_CAS_ATTEMPTS {
            let stored = self.store.get(&key).await?.ok_or_else(|| {
                AssignmentAgentError::AssignmentDisappeared {
                    assignment_id: assignment.meta.id.to_string(),
                }
            })?;
            let mut current = decode_assignment(&stored)?;
            if current.spec.node_id != self.settings.node_id {
                return Err(AssignmentAgentError::AssignmentMoved {
                    assignment_id: current.meta.id.to_string(),
                });
            }
            let desired = desired_status(&current, outcome, self.status_clock.now());
            if current.status == desired {
                return Ok(());
            }
            current.status = desired;
            current.meta.revision = stored.version.resource_revision();
            let value = serde_json::to_vec(&current).map_err(|error| {
                AssignmentAgentError::SerializeResource {
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
        Err(AssignmentAgentError::Contention {
            assignment_id: assignment.meta.id.to_string(),
        })
    }
}

fn decode_assignments(values: &[StoredValue]) -> (Vec<Assignment>, usize) {
    let decoded = values
        .iter()
        .map(|stored| serde_json::from_slice(&stored.value))
        .collect::<Vec<Result<Assignment, _>>>();
    let malformed = decoded.iter().filter(|result| result.is_err()).count();
    (
        decoded.into_iter().filter_map(Result::ok).collect(),
        malformed,
    )
}

fn decode_deployments(values: &[StoredValue]) -> (BTreeMap<DeploymentId, Deployment>, usize) {
    let decoded = values
        .iter()
        .map(|stored| serde_json::from_slice(&stored.value))
        .collect::<Vec<Result<Deployment, _>>>();
    let malformed = decoded.iter().filter(|result| result.is_err()).count();
    (
        decoded
            .into_iter()
            .filter_map(Result::ok)
            .map(|deployment| (deployment.meta.id.clone(), deployment))
            .collect(),
        malformed,
    )
}

fn decode_assignment(stored: &StoredValue) -> Result<Assignment, AssignmentAgentError> {
    serde_json::from_slice(&stored.value).map_err(|error| {
        AssignmentAgentError::MalformedAssignment {
            key: stored.key.to_string(),
            message: error.to_string(),
        }
    })
}

/// Why node-local assignment reconciliation could not complete its snapshot.
#[derive(Debug, thiserror::Error)]
pub enum AssignmentAgentError {
    /// A configured resource kind or observed identity was invalid.
    #[error(transparent)]
    InvalidIdentifier(#[from] kernel_api::InvalidIdentifier),
    /// A zero deadline would create an unbounded loop or immediate forced shutdown.
    #[error("assignment stop and resync deadlines must be positive")]
    ZeroDeadline,
    /// Store access or watch setup failed.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// Runtime cleanup failed and will be retried by a later resync.
    #[error(transparent)]
    Runtime(#[from] RuntimeError),
    /// Network cleanup failed and will be retried by a later resync.
    #[error(transparent)]
    Network(#[from] NetworkProviderError),
    /// The assignment disappeared while its observed status was being committed.
    #[error("assignment `{assignment_id}` disappeared before status update")]
    AssignmentDisappeared { assignment_id: String },
    /// A scheduler mutation moved an assignment away from this node during reconciliation.
    #[error("assignment `{assignment_id}` moved to another node during status update")]
    AssignmentMoved { assignment_id: String },
    /// A stored assignment could not be decoded for its conditional status write.
    #[error("malformed Assignment resource at `{key}`: {message}")]
    MalformedAssignment { key: String, message: String },
    /// A status-bearing assignment could not be encoded.
    #[error("failed to serialize Assignment resource: {message}")]
    SerializeResource { message: String },
    /// Repeated concurrent status writes exhausted the bounded retry budget.
    #[error("store contention prevented status update for assignment `{assignment_id}`")]
    Contention { assignment_id: String },
}
