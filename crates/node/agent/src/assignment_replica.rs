use std::collections::BTreeMap;

use kernel_api::{
    Assignment, DeploymentPhase, Generation, MaskedSecret, Object, ObjectMeta, OwnerReference,
    Ownership, ReplicaState, ReplicaStateId, ReplicaStateSpec, ReplicaStateStatus, ResourceId,
    ResourceKind, ResourceName, WorkloadId,
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
            restart_attempts: 0,
            restart_pending_attempt: None,
            restart_not_before: None,
            resolved_secrets: None,
            conditions: Vec::new(),
        },
    }
}

pub(crate) async fn record_started(
    store: &dyn Store,
    keyspace: &Keyspace,
    replica_kind: &ResourceKind,
    assignment: &Assignment,
    replica_id: &ReplicaStateId,
    workload_id: &WorkloadId,
    resolved_secrets: &BTreeMap<String, MaskedSecret>,
) -> Result<(), AssignmentAgentError> {
    let key = keyspace.resource(replica_kind, &ResourceName::from(replica_id.clone()));
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
        let phase = if current.status.phase == DeploymentPhase::Publishing {
            DeploymentPhase::PendingReady
        } else {
            current.status.phase
        };
        if current.status.phase == phase
            && current.status.workload_id.as_ref() == Some(workload_id)
            && current.status.resolved_secrets.as_ref() == Some(resolved_secrets)
        {
            return Ok(());
        }
        current.status.phase = phase;
        current.status.workload_id = Some(workload_id.clone());
        current.status.resolved_secrets = Some(resolved_secrets.clone());
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
