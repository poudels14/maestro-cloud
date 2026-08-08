use std::collections::BTreeMap;

use kernel_api::{
    Assignment, Condition, ConditionReason, ConditionState, ConditionType, DeploymentPhase,
    Generation, MaskedSecret, Object, ObjectMeta, OwnerReference, Ownership, ReplicaState,
    ReplicaStateId, ReplicaStateSpec, ReplicaStateStatus, ResourceId, ResourceKind, ResourceName,
    Timestamp, WorkloadId, WorkloadStopReason,
};
use kernel_store::{CasOutcome, ExpectedVersion, Keyspace, PutRequest, Store};

use crate::AssignmentAgentError;

const MAX_CAS_ATTEMPTS: usize = 16;

pub(crate) struct EnsuredReplica {
    pub(crate) replica: ReplicaState,
    pub(crate) created: bool,
}

pub(crate) async fn ensure_replica(
    store: &dyn Store,
    keyspace: &Keyspace,
    assignment_kind: &ResourceKind,
    replica_kind: &ResourceKind,
    assignment: &Assignment,
) -> Result<EnsuredReplica, AssignmentAgentError> {
    let replica_id = ReplicaStateId::new(assignment.meta.id.as_str())?;
    let key = keyspace.resource(replica_kind, &ResourceName::from(replica_id.clone()));
    let desired = initial_replica(replica_id, assignment, assignment_kind);
    let value =
        serde_json::to_vec(&desired).map_err(|error| AssignmentAgentError::SerializeResource {
            message: error.to_string(),
        })?;
    match store
        .put_cas(PutRequest {
            key: key.clone(),
            value,
            expected: ExpectedVersion::Missing,
            session: None,
        })
        .await?
    {
        CasOutcome::Applied(stored) => {
            let mut replica = desired;
            replica.meta.revision = stored.version.resource_revision();
            Ok(EnsuredReplica {
                replica,
                created: true,
            })
        }
        CasOutcome::Conflict { .. } => {
            let stored =
                store
                    .get(&key)
                    .await?
                    .ok_or_else(|| AssignmentAgentError::ReplicaDisappeared {
                        replica_id: desired.meta.id.to_string(),
                    })?;
            let mut current: ReplicaState =
                serde_json::from_slice(&stored.value).map_err(|error| {
                    AssignmentAgentError::MalformedReplica {
                        key: stored.key.to_string(),
                        message: error.to_string(),
                    }
                })?;
            if current.spec != desired.spec {
                return Err(AssignmentAgentError::ReplicaIdentityCollision {
                    replica_id: current.meta.id.to_string(),
                    assignment_id: assignment.meta.id.to_string(),
                });
            }
            current.meta.revision = stored.version.resource_revision();
            Ok(EnsuredReplica {
                replica: current,
                created: false,
            })
        }
    }
}

fn initial_replica(
    replica_id: ReplicaStateId,
    assignment: &Assignment,
    assignment_kind: &ResourceKind,
) -> ReplicaState {
    Object {
        meta: ObjectMeta {
            id: replica_id,
            labels: Default::default(),
            annotations: Default::default(),
            revision: Default::default(),
            generation: Generation(1),
            owner_refs: vec![OwnerReference {
                resource: ResourceId::new(
                    assignment_kind.clone(),
                    ResourceName::from(assignment.meta.id.clone()),
                ),
                ownership: Ownership::Controller,
            }],
            finalizers: Default::default(),
            deletion_timestamp: None,
        },
        spec: ReplicaStateSpec {
            service_id: assignment.spec.service_id.clone(),
            deployment_id: assignment.spec.deployment_id.clone(),
            assignment_id: assignment.meta.id.clone(),
            replica_index: assignment.spec.replica_index,
        },
        status: ReplicaStateStatus {
            phase: DeploymentPhase::Publishing,
            node_id: Some(assignment.spec.node_id.clone()),
            workload_id: None,
            healthcheck_failures: 0,
            resolved_secrets: None,
            conditions: Vec::new(),
        },
    }
}

pub(crate) async fn record_started(
    store: &dyn Store,
    key: kernel_store::StoreKey,
    assignment: &Assignment,
    workload_id: &WorkloadId,
    resolved_secrets: &BTreeMap<String, MaskedSecret>,
    reset_readiness: bool,
    now: Timestamp,
) -> Result<(), AssignmentAgentError> {
    let replica_id = ReplicaStateId::new(assignment.meta.id.as_str())?;
    for _attempt in 0..MAX_CAS_ATTEMPTS {
        let stored = store.get(&key).await?.ok_or_else(|| {
            AssignmentAgentError::ReplicaObservationDisappeared {
                replica_id: replica_id.to_string(),
            }
        })?;
        let mut current: ReplicaState = serde_json::from_slice(&stored.value).map_err(|error| {
            AssignmentAgentError::MalformedReplica {
                key: stored.key.to_string(),
                message: error.to_string(),
            }
        })?;
        if current.spec.assignment_id != assignment.meta.id {
            return Err(AssignmentAgentError::ReplicaIdentityCollision {
                replica_id: replica_id.to_string(),
                assignment_id: assignment.meta.id.to_string(),
            });
        }
        let phase = if reset_readiness
            || !matches!(
                current.status.phase,
                DeploymentPhase::PendingReady | DeploymentPhase::Ready
            ) {
            DeploymentPhase::PendingReady
        } else {
            current.status.phase
        };
        let runtime_reason = ConditionReason("WorkloadRunning".to_owned());
        let runtime_condition_current = current.status.conditions.iter().any(|condition| {
            condition.condition_type == ConditionType::RuntimeReady
                && condition.state == ConditionState::True
                && condition.reason == runtime_reason
        });
        if current.status.phase == phase
            && current.status.workload_id.as_ref() == Some(workload_id)
            && (!reset_readiness || current.status.healthcheck_failures == 0)
            && current.status.resolved_secrets.as_ref() == Some(resolved_secrets)
            && runtime_condition_current
        {
            return Ok(());
        }
        current.status.phase = phase;
        current.status.workload_id = Some(workload_id.clone());
        if reset_readiness {
            current.status.healthcheck_failures = 0;
            current
                .status
                .conditions
                .retain(|condition| condition.condition_type != ConditionType::HealthReady);
        }
        current.status.resolved_secrets = Some(resolved_secrets.clone());
        let last_transition_time = current
            .status
            .conditions
            .iter()
            .find(|condition| {
                condition.condition_type == ConditionType::RuntimeReady
                    && condition.state == ConditionState::True
                    && condition.reason == runtime_reason
            })
            .map_or(now, |condition| condition.last_transition_time);
        current
            .status
            .conditions
            .retain(|condition| condition.condition_type != ConditionType::RuntimeReady);
        current.status.conditions.push(Condition {
            condition_type: ConditionType::RuntimeReady,
            state: ConditionState::True,
            reason: runtime_reason,
            message: "runtime workload is running".to_owned(),
            observed_generation: assignment.meta.generation,
            last_transition_time,
        });
        current.meta.revision = stored.version.resource_revision();
        let value = serde_json::to_vec(&current).map_err(|error| {
            AssignmentAgentError::SerializeResource {
                message: error.to_string(),
            }
        })?;
        if matches!(
            store
                .put_cas(PutRequest {
                    key: key.clone(),
                    value,
                    expected: ExpectedVersion::Exact(stored.version),
                    session: None,
                })
                .await?,
            CasOutcome::Applied(_)
        ) {
            return Ok(());
        }
    }
    Err(AssignmentAgentError::ReplicaObservationContention {
        replica_id: replica_id.to_string(),
    })
}

pub(crate) async fn record_stopping(
    store: &dyn Store,
    key: kernel_store::StoreKey,
    assignment: &Assignment,
    reason: WorkloadStopReason,
    message: &str,
    now: Timestamp,
) -> Result<(), AssignmentAgentError> {
    record_stop_phase(
        store,
        key,
        assignment,
        DeploymentPhase::Stopping,
        reason,
        message,
        now,
    )
    .await
}

pub(crate) async fn record_stopped(
    store: &dyn Store,
    key: kernel_store::StoreKey,
    assignment: &Assignment,
    reason: WorkloadStopReason,
    message: &str,
    now: Timestamp,
) -> Result<(), AssignmentAgentError> {
    record_stop_phase(
        store,
        key,
        assignment,
        DeploymentPhase::Stopped,
        reason,
        message,
        now,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn record_stop_phase(
    store: &dyn Store,
    key: kernel_store::StoreKey,
    assignment: &Assignment,
    phase: DeploymentPhase,
    reason: WorkloadStopReason,
    message: &str,
    now: Timestamp,
) -> Result<(), AssignmentAgentError> {
    for _attempt in 0..MAX_CAS_ATTEMPTS {
        let Some(stored) = store.get(&key).await? else {
            return Ok(());
        };
        let mut current: ReplicaState = serde_json::from_slice(&stored.value).map_err(|error| {
            AssignmentAgentError::MalformedReplica {
                key: stored.key.to_string(),
                message: error.to_string(),
            }
        })?;
        if current.spec.assignment_id != assignment.meta.id {
            return Err(AssignmentAgentError::ReplicaIdentityCollision {
                replica_id: current.meta.id.to_string(),
                assignment_id: assignment.meta.id.to_string(),
            });
        }
        let condition_reason = ConditionReason(reason.condition_reason().to_owned());
        let previous = current
            .status
            .conditions
            .iter()
            .find(|condition| condition.condition_type == ConditionType::RuntimeReady);
        let last_transition_time = previous
            .filter(|condition| {
                condition.state == ConditionState::False && condition.reason == condition_reason
            })
            .map_or(now, |condition| condition.last_transition_time);
        current.status.phase = phase;
        if phase == DeploymentPhase::Stopped {
            current.status.workload_id = None;
        }
        current
            .status
            .conditions
            .retain(|condition| condition.condition_type != ConditionType::RuntimeReady);
        current.status.conditions.push(Condition {
            condition_type: ConditionType::RuntimeReady,
            state: ConditionState::False,
            reason: condition_reason,
            message: message.to_owned(),
            observed_generation: assignment.meta.generation,
            last_transition_time,
        });
        current.meta.revision = stored.version.resource_revision();
        let value = serde_json::to_vec(&current).map_err(|error| {
            AssignmentAgentError::SerializeResource {
                message: error.to_string(),
            }
        })?;
        if matches!(
            store
                .put_cas(PutRequest {
                    key: key.clone(),
                    value,
                    expected: ExpectedVersion::Exact(stored.version),
                    session: None,
                })
                .await?,
            CasOutcome::Applied(_)
        ) {
            return Ok(());
        }
    }
    Err(AssignmentAgentError::ReplicaObservationContention {
        replica_id: assignment.meta.id.to_string(),
    })
}
