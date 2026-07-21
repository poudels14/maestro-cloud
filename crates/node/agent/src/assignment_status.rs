use kernel_api::{
    Assignment, AssignmentPhase, AssignmentStatus, Condition, ConditionReason, ConditionState,
    ConditionType, Timestamp,
};
use runtime::{NetworkProviderError, RuntimeError, WorkloadHandle, WorkloadStatus};

use crate::assignment_plan::WorkloadPlanError;

const RUNTIME_READY_CONDITION: &str = "RuntimeReady";
const WORKLOAD_RUNNING_REASON: &str = "WorkloadRunning";
const RUNTIME_RETRY_REASON: &str = "RuntimeRetry";
const RUNTIME_REJECTED_REASON: &str = "RuntimeRejected";

#[derive(Clone, Copy)]
pub(crate) enum AssignmentOutcome<'a> {
    Running(&'a WorkloadHandle),
    Unresolved(&'a ConvergeFailure),
    Stopped,
}

pub(crate) struct ConvergeFailure {
    phase: AssignmentPhase,
    reason: &'static str,
    message: String,
    retry_at: Option<Timestamp>,
}

impl ConvergeFailure {
    pub(crate) fn pending(reason: &'static str, message: String) -> Self {
        Self {
            phase: AssignmentPhase::Pending,
            reason,
            message,
            retry_at: None,
        }
    }

    pub(crate) fn pending_at(reason: &'static str, message: String, retry_at: Timestamp) -> Self {
        Self {
            phase: AssignmentPhase::Pending,
            reason,
            message,
            retry_at: Some(retry_at),
        }
    }

    pub(crate) fn failed(reason: &'static str, message: String) -> Self {
        Self {
            phase: AssignmentPhase::Failed,
            reason,
            message,
            retry_at: None,
        }
    }

    pub(crate) fn retry_at(&self) -> Option<Timestamp> {
        self.retry_at
    }
}

impl From<WorkloadPlanError> for ConvergeFailure {
    fn from(error: WorkloadPlanError) -> Self {
        match error {
            WorkloadPlanError::ArtifactUnavailable => Self::pending(
                "ArtifactUnavailable",
                "deployment artifact is not available yet".to_owned(),
            ),
            _ => Self::failed(RUNTIME_REJECTED_REASON, error.to_string()),
        }
    }
}

impl From<RuntimeError> for ConvergeFailure {
    fn from(error: RuntimeError) -> Self {
        match error {
            RuntimeError::InvalidSpec { .. }
            | RuntimeError::Conflict { .. }
            | RuntimeError::Unsupported { .. }
            | RuntimeError::Rejected { .. } => {
                Self::failed(RUNTIME_REJECTED_REASON, error.to_string())
            }
            RuntimeError::NotFound { .. }
            | RuntimeError::Unavailable { .. }
            | RuntimeError::Stream { .. }
            | RuntimeError::Timeout { .. } => {
                Self::pending(RUNTIME_RETRY_REASON, error.to_string())
            }
        }
    }
}

impl From<NetworkProviderError> for ConvergeFailure {
    fn from(error: NetworkProviderError) -> Self {
        match error {
            NetworkProviderError::InvalidRange { .. } | NetworkProviderError::Rejected { .. } => {
                Self::failed(RUNTIME_REJECTED_REASON, error.to_string())
            }
            NetworkProviderError::NetworkNotFound { .. }
            | NetworkProviderError::AddressConflict { .. }
            | NetworkProviderError::Unavailable { .. } => {
                Self::pending(RUNTIME_RETRY_REASON, error.to_string())
            }
        }
    }
}

pub(crate) fn desired_status(
    assignment: &Assignment,
    outcome: AssignmentOutcome<'_>,
    now: Timestamp,
) -> AssignmentStatus {
    let (phase, workload_id, state, reason, message) = match outcome {
        AssignmentOutcome::Running(handle) => (
            AssignmentPhase::Running,
            Some(handle.workload_id().clone()),
            ConditionState::True,
            WORKLOAD_RUNNING_REASON,
            "runtime workload is running at its assigned address".to_owned(),
        ),
        AssignmentOutcome::Unresolved(failure) => (
            failure.phase,
            assignment.status.workload_id.clone(),
            ConditionState::False,
            failure.reason,
            failure.message.clone(),
        ),
        AssignmentOutcome::Stopped => (
            AssignmentPhase::Stopped,
            None,
            ConditionState::False,
            "WorkloadRemoved",
            "runtime workload and network reservation are removed".to_owned(),
        ),
    };
    let previous = assignment
        .status
        .conditions
        .iter()
        .find(|condition| condition.condition_type.0 == RUNTIME_READY_CONDITION);
    let last_transition_time = previous
        .filter(|condition| condition.state == state && condition.reason.0 == reason)
        .map_or(now, |condition| condition.last_transition_time);
    AssignmentStatus {
        phase,
        workload_id,
        conditions: vec![Condition {
            condition_type: ConditionType(RUNTIME_READY_CONDITION.to_owned()),
            state,
            reason: ConditionReason(reason.to_owned()),
            message,
            observed_generation: assignment.meta.generation,
            last_transition_time,
        }],
    }
}

pub(crate) fn runtime_status_message(status: &WorkloadStatus) -> String {
    let detail = status
        .detail
        .as_deref()
        .map_or(String::new(), |detail| format!(": {detail}"));
    let exit = status
        .exit_code
        .map_or(String::new(), |code| format!(" (exit code {code})"));
    format!("runtime reported {:?}{exit}{detail}", status.state)
}
