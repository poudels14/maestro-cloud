use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use kernel_api::{Assignment, Deployment, ReplicaState, ResourceKind, ResourceName};
use kernel_store::{
    CasOutcome, Clock, ExpectedVersion, Keyspace, PutRequest, Store, StoreError, WatchCursor,
    WatchStart,
};
use runtime::{
    AddressLease, AddressRequest, EventCursor, EventRequest, NetworkHandle, NetworkProvider,
    RuntimeError, ShutdownRequest, WorkloadHandle, WorkloadRuntime, WorkloadState,
};
use tokio::sync::watch;

use crate::StatusClock;
use crate::assignment_error::AssignmentAgentError;
#[cfg(unix)]
use crate::assignment_node_api::{active_node_api_workloads, mount_node_api};
#[cfg(not(unix))]
use crate::assignment_plan::node_api_user;
use crate::assignment_plan::{workload_id, workload_spec};
use crate::assignment_replica::ensure_replica;
use crate::assignment_resource::{
    decode_assignment, decode_assignments, decode_deployments, decode_replicas,
};
use crate::assignment_restart::{
    RestartReservation, finish_pending_restart, reserve_restart, restart_failure,
};
use crate::assignment_status::{
    AssignmentOutcome, ConvergeFailure, desired_status, runtime_status_message,
};
use crate::assignment_types::{
    AssignmentAgentSettings, AssignmentReconcileReport, ConvergedAssignment, earliest,
    monotonic_deadline,
};
use crate::secret_mount::SecretMountManager;
#[cfg(unix)]
use crate::{NodeApiServices, node_api_mount::NodeApiMountManager};

const ASSIGNMENT_KIND: &str = "Assignment";
const DEPLOYMENT_KIND: &str = "Deployment";
const REPLICA_STATE_KIND: &str = "ReplicaState";
const RUNTIME_RETRY_REASON: &str = "RuntimeRetry";
const RUNTIME_STREAM_RECONNECT_DELAY: Duration = Duration::from_secs(1);
const MAX_CAS_ATTEMPTS: usize = 16;

/// Level-triggered node reconciler for assignment lifecycle, adoption, and garbage collection.
pub struct AssignmentAgent {
    store: Arc<dyn Store>,
    runtime: Arc<dyn WorkloadRuntime>,
    network: Arc<dyn NetworkProvider>,
    settings: AssignmentAgentSettings,
    keyspace: Keyspace,
    assignment_kind: ResourceKind,
    deployment_kind: ResourceKind,
    replica_kind: ResourceKind,
    monotonic_clock: Arc<dyn Clock>,
    status_clock: Arc<dyn StatusClock>,
    secrets: SecretMountManager,
    #[cfg(unix)]
    node_api: NodeApiMountManager,
}

impl AssignmentAgent {
    /// Creates a node agent without starting background work.
    pub fn new(
        store: Arc<dyn Store>,
        runtime: Arc<dyn WorkloadRuntime>,
        network: Arc<dyn NetworkProvider>,
        settings: AssignmentAgentSettings,
        #[cfg(unix)] node_api_services: Option<NodeApiServices>,
        monotonic_clock: Arc<dyn Clock>,
        status_clock: Arc<dyn StatusClock>,
    ) -> Result<Self, AssignmentAgentError> {
        if settings.stop_timeout.is_zero()
            || settings.resync_interval.is_zero()
            || settings.restart_backoff_base.is_zero()
            || settings.restart_backoff_max < settings.restart_backoff_base
            || settings.reconcile_timeout.is_zero()
        {
            return Err(AssignmentAgentError::ZeroDeadline);
        }
        let secrets = SecretMountManager::new(settings.secrets_root.clone())?;
        #[cfg(unix)]
        let node_api = NodeApiMountManager::new(settings.node_api_root.clone(), node_api_services)?;
        Ok(Self {
            keyspace: Keyspace::new(&settings.cluster_id),
            assignment_kind: ResourceKind::new(ASSIGNMENT_KIND)?,
            deployment_kind: ResourceKind::new(DEPLOYMENT_KIND)?,
            replica_kind: ResourceKind::new(REPLICA_STATE_KIND)?,
            store,
            runtime,
            network,
            settings,
            monotonic_clock,
            status_clock,
            secrets,
            #[cfg(unix)]
            node_api,
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
        let mut runtime_cursor: Option<EventCursor> = None;
        let mut runtime_events = None;
        let mut runtime_reconnect_at = self.monotonic_clock.now();
        loop {
            if *shutdown.borrow() {
                #[cfg(unix)]
                self.node_api.shutdown_all().await?;
                return Ok(());
            }
            let reconcile_deadline = self
                .monotonic_clock
                .now()
                .saturating_add(self.settings.reconcile_timeout);
            let reconcile = tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        #[cfg(unix)]
                        self.node_api.shutdown_all().await?;
                        return Ok(());
                    }
                    continue;
                }
                result = self.reconcile_with_cursor() => Some(result),
                () = self.monotonic_clock.sleep_until(reconcile_deadline) => None,
            };
            let Some(reconcile) = reconcile else {
                continue;
            };
            let (report, cursor) = match reconcile {
                Ok(reconciled) => reconciled,
                Err(error) if error.retryable() => {
                    if self.wait_for_retry_or_shutdown(&mut shutdown).await {
                        #[cfg(unix)]
                        self.node_api.shutdown_all().await?;
                        return Ok(());
                    }
                    continue;
                }
                Err(error) => return Err(error),
            };
            let retry_at = report.requeue_at.map(|deadline| {
                monotonic_deadline(
                    self.monotonic_clock.as_ref(),
                    self.status_clock.as_ref(),
                    deadline,
                )
            });
            let mut events = match self
                .store
                .watch(self.keyspace.resources(), WatchStart::After(cursor))
            {
                Ok(events) => events,
                Err(error) if retryable_watch_error(&error) => {
                    if self.wait_for_retry_or_shutdown(&mut shutdown).await {
                        #[cfg(unix)]
                        self.node_api.shutdown_all().await?;
                        return Ok(());
                    }
                    continue;
                }
                Err(error) => return Err(error.into()),
            };
            if runtime_events.is_none() && self.monotonic_clock.now() >= runtime_reconnect_at {
                runtime_events = match self
                    .runtime
                    .events(EventRequest {
                        cluster_id: self.settings.cluster_id.clone(),
                        node_id: self.settings.node_id.clone(),
                        after: runtime_cursor.clone(),
                    })
                    .await
                {
                    Ok(events) => Some(events),
                    Err(_) => {
                        runtime_reconnect_at = self
                            .monotonic_clock
                            .now()
                            .saturating_add(RUNTIME_STREAM_RECONNECT_DELAY);
                        None
                    }
                };
            }
            loop {
                tokio::select! {
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow() {
                            #[cfg(unix)]
                            self.node_api.shutdown_all().await?;
                            return Ok(());
                        }
                    }
                    event = events.next() => {
                        match event {
                            Ok(_) => break,
                            Err(error) if retryable_watch_error(&error) => {
                                if self.wait_for_retry_or_shutdown(&mut shutdown).await {
                                    #[cfg(unix)]
                                    self.node_api.shutdown_all().await?;
                                    return Ok(());
                                }
                                break;
                            }
                            Err(error) => return Err(error.into()),
                        }
                    }
                    event = async {
                        match runtime_events.as_mut() {
                            Some(events) => events.next().await,
                            None => std::future::pending().await,
                        }
                    }, if runtime_events.is_some() && retry_at.is_none() => {
                        match event {
                            Ok(Some(event)) => {
                                runtime_cursor = Some(event.cursor);
                                break;
                            }
                            Ok(None) | Err(_) => {
                                runtime_events = None;
                                runtime_reconnect_at = self
                                    .monotonic_clock
                                    .now()
                                    .saturating_add(RUNTIME_STREAM_RECONNECT_DELAY);
                            }
                        }
                    }
                    () = self.monotonic_clock.sleep_until(runtime_reconnect_at), if runtime_events.is_none() => {
                        break;
                    }
                    () = async {
                        match retry_at {
                            Some(retry_at) => self.monotonic_clock.sleep_until(retry_at).await,
                            None => std::future::pending().await,
                        }
                    }, if retry_at.is_some() => {
                        break;
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

    async fn wait_for_retry_or_shutdown(&self, shutdown: &mut watch::Receiver<bool>) -> bool {
        let retry_at = self
            .monotonic_clock
            .now()
            .saturating_add(RUNTIME_STREAM_RECONNECT_DELAY);
        loop {
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return true;
                    }
                }
                () = self.monotonic_clock.sleep_until(retry_at) => return false,
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
        let replica_snapshot = self
            .store
            .list(&self.keyspace.resource_kind(&self.replica_kind))
            .await?;
        let (assignments, malformed_assignments) = decode_assignments(&assignment_snapshot.values);
        let (deployments, malformed_deployments) = decode_deployments(&deployment_snapshot.values);
        let (replicas, malformed_replicas) = decode_replicas(&replica_snapshot.values);
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
            malformed_resources: malformed_assignments
                .saturating_add(malformed_deployments)
                .saturating_add(malformed_replicas),
            ..Default::default()
        };
        for assignment in &active {
            let outcome = match deployments.get(&assignment.spec.deployment_id) {
                Some(deployment) => {
                    let replica = match replicas.get(&assignment.meta.id) {
                        Some(replica) => Some(replica.clone()),
                        None if malformed_replicas == 0 => {
                            let ensured = ensure_replica(
                                self.store.as_ref(),
                                &self.keyspace,
                                &self.assignment_kind,
                                &self.replica_kind,
                                assignment,
                            )
                            .await?;
                            if ensured.created {
                                report.replica_states_created =
                                    report.replica_states_created.saturating_add(1);
                            }
                            Some(ensured.replica)
                        }
                        None => None,
                    };
                    self.converge_assignment(assignment, deployment, replica.as_ref(), &network)
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
                Ok(converged) => {
                    self.update_status(assignment, AssignmentOutcome::Running(&converged.handle))
                        .await?;
                    report.running = report.running.saturating_add(1);
                    if converged.restarted {
                        report.restarted = report.restarted.saturating_add(1);
                    }
                }
                Err(failure) => {
                    report.requeue_at = earliest(report.requeue_at, failure.retry_at());
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
            let active_workloads = active
                .iter()
                .map(|assignment| assignment.meta.id.to_string())
                .collect::<BTreeSet<_>>();
            report.secret_mounts_collected = self.secrets.cleanup_stale(&active_workloads).await?;
            #[cfg(unix)]
            {
                let active_node_api_workloads = active_node_api_workloads(&active, &deployments);
                report.node_api_mounts_collected = self
                    .node_api
                    .cleanup_stale(&active_node_api_workloads)
                    .await?;
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
            std::cmp::min(
                std::cmp::min(assignment_snapshot.cursor, deployment_snapshot.cursor),
                replica_snapshot.cursor,
            ),
        ))
    }

    async fn converge_assignment(
        &self,
        assignment: &Assignment,
        deployment: &Deployment,
        replica: Option<&ReplicaState>,
        network: &NetworkHandle,
    ) -> Result<ConvergedAssignment, ConvergeFailure> {
        let workload_id = workload_id(assignment)?;
        let mut additional_mounts = Vec::new();
        let secret_mount = match deployment.spec.service.secrets.as_ref() {
            Some(secrets) => Some(self.secrets.materialize(&workload_id, secrets).await?),
            None => None,
        };
        additional_mounts.extend(secret_mount);
        #[cfg(unix)]
        if let Some(node_api_mount) =
            mount_node_api(&self.node_api, assignment, deployment, &workload_id).await?
        {
            additional_mounts.push(node_api_mount);
        }
        #[cfg(not(unix))]
        if node_api_user(deployment)?.is_some() {
            return Err(ConvergeFailure::failed(
                "NodeApiUnsupported",
                "node API workloads require a Unix host".to_owned(),
            ));
        }
        let spec = workload_spec(
            &self.settings.cluster_id,
            assignment,
            deployment,
            self.settings.network.gateway,
            additional_mounts,
        )?;
        let handle = self.runtime.create(&spec).await?;
        let before = self.runtime.status(&handle).await?;
        match before.state {
            WorkloadState::Created | WorkloadState::Running => {}
            WorkloadState::Stopped => {
                let replica = replica.ok_or_else(|| {
                    ConvergeFailure::pending(
                        "ReplicaStateMissing",
                        "an exited workload cannot restart until its ReplicaState is available"
                            .to_owned(),
                    )
                })?;
                match reserve_restart(
                    self.store.as_ref(),
                    &self.keyspace,
                    &self.replica_kind,
                    replica,
                    &assignment.meta.id,
                    deployment.spec.service.max_restarts,
                    self.settings.restart_backoff_base,
                    self.settings.restart_backoff_max,
                    self.status_clock.now(),
                )
                .await
                .map_err(restart_failure)?
                {
                    RestartReservation::Reserved { not_before }
                        if self.status_clock.now() < not_before =>
                    {
                        return Err(ConvergeFailure::pending_at(
                            "RestartBackoff",
                            format!("runtime restart is delayed until {}", not_before.0),
                            not_before,
                        ));
                    }
                    RestartReservation::Reserved { .. } => {}
                    RestartReservation::Exhausted { maximum } => {
                        return Err(ConvergeFailure::failed(
                            "RestartLimitReached",
                            format!(
                                "workload exhausted its restart limit of {} attempts",
                                maximum
                            ),
                        ));
                    }
                }
            }
            WorkloadState::Paused => {
                return Err(ConvergeFailure::pending(
                    RUNTIME_RETRY_REASON,
                    "runtime workload is paused".to_owned(),
                ));
            }
            WorkloadState::Failed => {
                return Err(ConvergeFailure::failed(
                    "RuntimeFailed",
                    runtime_status_message(&before),
                ));
            }
        }
        let needs_start = matches!(
            before.state,
            WorkloadState::Created | WorkloadState::Stopped
        );
        let lease = self
            .network
            .allocate_address(
                network,
                handle.workload_id(),
                AddressRequest::Exact(assignment.spec.workload_address),
            )
            .await?;
        self.network.attach(&handle, network, &lease).await?;
        if needs_start {
            self.runtime.start(&handle).await?;
        }
        let status = self.runtime.status(&handle).await?;
        if status.state == WorkloadState::Running {
            let restarted = match replica {
                Some(replica) => finish_pending_restart(
                    self.store.as_ref(),
                    &self.keyspace,
                    &self.replica_kind,
                    replica,
                    &assignment.meta.id,
                    self.status_clock.now(),
                )
                .await
                .map_err(restart_failure)?,
                None => false,
            };
            Ok(ConvergedAssignment { handle, restarted })
        } else {
            Err(ConvergeFailure::pending(
                RUNTIME_RETRY_REASON,
                runtime_status_message(&status),
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
        self.secrets.cleanup(handle.workload_id()).await?;
        #[cfg(unix)]
        self.node_api.cleanup(handle.workload_id()).await?;
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

fn retryable_watch_error(error: &StoreError) -> bool {
    matches!(
        error,
        StoreError::CursorExpired { .. }
            | StoreError::SessionExpired { .. }
            | StoreError::Unavailable { .. }
    )
}
