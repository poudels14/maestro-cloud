use std::collections::BTreeMap;
use std::net::IpAddr;
use std::path::PathBuf;
use std::time::Duration;

use kernel_api::{ClusterId, MaskedSecret, NodeId, ReplicaStateId, ServiceId, Timestamp};
use kernel_store::{Clock, MonotonicTime};
use runtime::{HostPortPublication, NetworkSpec, WorkloadHandle};

use crate::StatusClock;

/// How the node resolves service names for container workloads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkloadDns {
    /// Workloads do not receive an authoritative resolver.
    Disabled,
    /// Every workload uses one host-side resolver address.
    Static(IpAddr),
    /// A runtime-managed system Service supplies a delegated-network address.
    DelegatedService(ServiceId),
}

/// Node-scoped assignment reconciliation settings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssignmentAgentSettings {
    /// Cluster whose assignment and runtime ownership labels are reconciled.
    pub cluster_id: ClusterId,
    /// Local node; assignments for other nodes are never mutated.
    pub node_id: NodeId,
    /// Node-local runtime bridge and exact host-owned IPAM range.
    pub network: NetworkSpec,
    /// Resolver source selected for this node's networking backend.
    pub dns: WorkloadDns,
    /// Host ports granted only to daemon-owned system service identities.
    pub system_host_ports: BTreeMap<ServiceId, Vec<HostPortPublication>>,
    /// Graceful workload shutdown deadline before forced termination.
    pub stop_timeout: Duration,
    /// Level-triggered full reconciliation interval.
    pub resync_interval: Duration,
    /// Delay before the first restart attempt after an observed exit.
    pub restart_backoff_base: Duration,
    /// Maximum exponential delay between an exit and its restart attempt.
    pub restart_backoff_max: Duration,
    /// Maximum duration of one complete store/runtime reconciliation pass.
    pub reconcile_timeout: Duration,
    /// Volatile host directory containing per-workload secret files.
    pub secrets_root: PathBuf,
    /// Volatile host directory containing per-workload node API credentials and sockets.
    pub node_api_root: PathBuf,
}

/// Results of one complete desired/runtime-state comparison.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AssignmentReconcileReport {
    /// Active local assignments observed in the store snapshot.
    pub desired: usize,
    /// Assignments confirmed running after reconciliation.
    pub running: usize,
    /// Exited workloads restored during this reconciliation pass.
    pub restarted: usize,
    /// Missing ReplicaState resources created for scheduler-owned assignments.
    pub replica_states_created: usize,
    /// Earliest wall-clock deadline at which pending work should be retried.
    pub requeue_at: Option<Timestamp>,
    /// Assignments left pending or failed with a status condition.
    pub unresolved: usize,
    /// Runtime workloads removed because no active local assignment owned them.
    pub garbage_collected: usize,
    /// Orphaned provider-owned address reservations reclaimed from the desired snapshot.
    pub address_reservations_collected: usize,
    /// Stale per-workload secret directories zeroized and removed.
    pub secret_mounts_collected: usize,
    /// Stale per-workload node API credentials and listeners removed.
    pub node_api_mounts_collected: usize,
    /// Stale per-workload user-namespace allocations reclaimed by the runtime.
    pub user_namespaces_collected: usize,
    /// Malformed resources skipped without crashing the agent loop.
    pub malformed_resources: usize,
}

pub(crate) struct ConvergedAssignment {
    pub(crate) handle: WorkloadHandle,
    pub(crate) workload_address: IpAddr,
    pub(crate) reset_readiness: bool,
    pub(crate) restarted: bool,
    pub(crate) replica_id: Option<ReplicaStateId>,
    pub(crate) resolved_secrets: BTreeMap<String, MaskedSecret>,
}

pub(crate) fn earliest(
    current: Option<Timestamp>,
    candidate: Option<Timestamp>,
) -> Option<Timestamp> {
    match (current, candidate) {
        (Some(current), Some(candidate)) => Some(std::cmp::min(current, candidate)),
        (current, candidate) => current.or(candidate),
    }
}

pub(crate) fn monotonic_deadline(
    monotonic_clock: &dyn Clock,
    status_clock: &dyn StatusClock,
    deadline: Timestamp,
) -> MonotonicTime {
    let remaining = deadline.0.saturating_sub(status_clock.now().0);
    let delay = Duration::from_millis(u64::try_from(remaining).unwrap_or_default());
    monotonic_clock.now().saturating_add(delay)
}
