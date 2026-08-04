use std::time::Duration;

use kernel_api::{
    AssignmentId, Condition, ConditionReason, ConditionState, ConditionType, DeploymentPhase,
    ReplicaState, ResourceKind, ResourceName, Timestamp,
};
use kernel_store::{CasOutcome, ExpectedVersion, Keyspace, PutRequest, Store, StoreError};

use crate::assignment_status::ConvergeFailure;

const MAX_CAS_ATTEMPTS: usize = 16;

pub(crate) enum RestartReservation {
    Reserved { not_before: Timestamp },
    Exhausted { maximum: u32 },
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn reserve_restart(
    store: &dyn Store,
    keyspace: &Keyspace,
    kind: &ResourceKind,
    replica: &ReplicaState,
    assignment_id: &AssignmentId,
    maximum: Option<u32>,
    backoff_base: Duration,
    backoff_max: Duration,
    now: Timestamp,
) -> Result<RestartReservation, RestartTrackingError> {
    let name = ResourceName::new(replica.meta.id.as_str())?;
    let key = keyspace.resource(kind, &name);
    for _attempt in 0..MAX_CAS_ATTEMPTS {
        let stored =
            store
                .get(&key)
                .await?
                .ok_or_else(|| RestartTrackingError::ReplicaDisappeared {
                    replica_id: replica.meta.id.to_string(),
                })?;
        let mut current: ReplicaState = serde_json::from_slice(&stored.value).map_err(|error| {
            RestartTrackingError::MalformedReplica {
                replica_id: replica.meta.id.to_string(),
                message: error.to_string(),
            }
        })?;
        validate_assignment(&current, assignment_id)?;
        if current.status.restart_pending_attempt.is_some() {
            return Ok(RestartReservation::Reserved {
                not_before: current.status.restart_not_before.unwrap_or(now),
            });
        }
        if let Some(maximum) = maximum
            && current.status.restart_attempts >= maximum
        {
            if restart_limit_recorded(&current) {
                return Ok(RestartReservation::Exhausted { maximum });
            }
            current.status.phase = DeploymentPhase::Crashed;
            upsert_restart_condition(
                &mut current,
                ConditionState::False,
                "RestartLimitReached",
                format!(
                    "workload exhausted its restart limit of {} attempts",
                    maximum
                ),
                now,
            );
            current.meta.revision = stored.version.resource_revision();
            let value = serde_json::to_vec(&current).map_err(|error| {
                RestartTrackingError::SerializeReplica {
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
                return Ok(RestartReservation::Exhausted { maximum });
            }
            continue;
        }
        let next_attempt = current.status.restart_attempts.saturating_add(1);
        let not_before = restart_deadline(now, backoff_base, backoff_max, next_attempt);
        current.status.restart_attempts = next_attempt;
        current.status.restart_pending_attempt = Some(next_attempt);
        current.status.restart_not_before = Some(not_before);
        current.status.healthcheck_failures = 0;
        current.status.phase = DeploymentPhase::PendingReady;
        upsert_restart_condition(
            &mut current,
            ConditionState::Unknown,
            "RestartScheduled",
            format!(
                "runtime restart attempt {next_attempt} is reserved until {}",
                not_before.0
            ),
            now,
        );
        current.meta.revision = stored.version.resource_revision();
        let value = serde_json::to_vec(&current).map_err(|error| {
            RestartTrackingError::SerializeReplica {
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
            return Ok(RestartReservation::Reserved { not_before });
        }
    }
    Err(RestartTrackingError::Contention {
        replica_id: replica.meta.id.to_string(),
    })
}

fn restart_limit_recorded(replica: &ReplicaState) -> bool {
    replica.status.phase == DeploymentPhase::Crashed
        && replica.status.conditions.iter().any(|condition| {
            condition.condition_type == ConditionType::RuntimeRestart
                && condition.state == ConditionState::False
                && condition.reason.0 == "RestartLimitReached"
        })
}

pub(crate) async fn finish_pending_restart(
    store: &dyn Store,
    keyspace: &Keyspace,
    kind: &ResourceKind,
    replica: &ReplicaState,
    assignment_id: &AssignmentId,
    now: Timestamp,
) -> Result<bool, RestartTrackingError> {
    let name = ResourceName::new(replica.meta.id.as_str())?;
    let key = keyspace.resource(kind, &name);
    for _attempt in 0..MAX_CAS_ATTEMPTS {
        let stored =
            store
                .get(&key)
                .await?
                .ok_or_else(|| RestartTrackingError::ReplicaDisappeared {
                    replica_id: replica.meta.id.to_string(),
                })?;
        let mut current: ReplicaState = serde_json::from_slice(&stored.value).map_err(|error| {
            RestartTrackingError::MalformedReplica {
                replica_id: replica.meta.id.to_string(),
                message: error.to_string(),
            }
        })?;
        validate_assignment(&current, assignment_id)?;
        let Some(completed_attempt) = current.status.restart_pending_attempt else {
            return Ok(false);
        };
        current.status.restart_pending_attempt = None;
        current.status.restart_not_before = None;
        upsert_restart_condition(
            &mut current,
            ConditionState::True,
            "RestartSucceeded",
            format!("runtime restart attempt {completed_attempt} is running"),
            now,
        );
        current.meta.revision = stored.version.resource_revision();
        let value = serde_json::to_vec(&current).map_err(|error| {
            RestartTrackingError::SerializeReplica {
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
            return Ok(true);
        }
    }
    Err(RestartTrackingError::Contention {
        replica_id: replica.meta.id.to_string(),
    })
}

pub(crate) fn restart_deadline(
    now: Timestamp,
    backoff_base: Duration,
    backoff_max: Duration,
    attempt: u32,
) -> Timestamp {
    let exponent = attempt.saturating_sub(1).min(31);
    let factor = 1_u32.checked_shl(exponent).unwrap_or(u32::MAX);
    let delay = backoff_base.saturating_mul(factor).min(backoff_max);
    let milliseconds = i64::try_from(delay.as_millis()).unwrap_or(i64::MAX);
    Timestamp(now.0.saturating_add(milliseconds))
}

fn validate_assignment(
    replica: &ReplicaState,
    assignment_id: &AssignmentId,
) -> Result<(), RestartTrackingError> {
    if &replica.spec.assignment_id == assignment_id {
        Ok(())
    } else {
        Err(RestartTrackingError::ReplicaReassigned {
            replica_id: replica.meta.id.to_string(),
        })
    }
}

fn upsert_restart_condition(
    replica: &mut ReplicaState,
    state: ConditionState,
    reason: &str,
    message: String,
    now: Timestamp,
) {
    let previous = replica
        .status
        .conditions
        .iter()
        .find(|condition| condition.condition_type == ConditionType::RuntimeRestart);
    let last_transition_time = previous
        .filter(|condition| condition.state == state && condition.reason.0 == reason)
        .map_or(now, |condition| condition.last_transition_time);
    replica
        .status
        .conditions
        .retain(|condition| condition.condition_type != ConditionType::RuntimeRestart);
    replica.status.conditions.push(Condition {
        condition_type: ConditionType::RuntimeRestart,
        state,
        reason: ConditionReason(reason.to_owned()),
        message,
        observed_generation: replica.meta.generation,
        last_transition_time,
    });
}

pub(crate) fn restart_failure(error: RestartTrackingError) -> ConvergeFailure {
    let message = error.to_string();
    match error {
        RestartTrackingError::InvalidIdentifier(_)
        | RestartTrackingError::MalformedReplica { .. }
        | RestartTrackingError::SerializeReplica { .. } => {
            ConvergeFailure::failed("RestartStateRejected", message)
        }
        RestartTrackingError::Store(_)
        | RestartTrackingError::ReplicaDisappeared { .. }
        | RestartTrackingError::ReplicaReassigned { .. }
        | RestartTrackingError::Contention { .. } => {
            ConvergeFailure::pending("RestartStateUnavailable", message)
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum RestartTrackingError {
    #[error(transparent)]
    InvalidIdentifier(#[from] kernel_api::InvalidIdentifier),
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error("replica `{replica_id}` disappeared while reserving a restart")]
    ReplicaDisappeared { replica_id: String },
    #[error("replica `{replica_id}` moved to another assignment during restart")]
    ReplicaReassigned { replica_id: String },
    #[error("replica `{replica_id}` is malformed: {message}")]
    MalformedReplica { replica_id: String, message: String },
    #[error("failed to serialize restart state: {message}")]
    SerializeReplica { message: String },
    #[error("store contention prevented restart tracking for replica `{replica_id}`")]
    Contention { replica_id: String },
}
