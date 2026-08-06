use kernel_api::{
    Assignment, AssignmentPhase, AssignmentStatus, Condition, ConditionReason, ConditionState,
    ConditionType, Timestamp,
};
use runtime::{NetworkProviderError, RuntimeError, WorkloadHandle, WorkloadStatus};

use crate::assignment_plan::WorkloadPlanError;
use crate::secret_mount::SecretMountError;
#[cfg(unix)]
use crate::{NodeApiMountError, NodeApiServerError};

const WORKLOAD_RUNNING_REASON: &str = "WorkloadRunning";
const RUNTIME_RETRY_REASON: &str = "RuntimeRetry";
const RUNTIME_REJECTED_REASON: &str = "RuntimeRejected";

#[derive(Clone, Copy)]
pub(crate) enum AssignmentOutcome<'a> {
    Running {
        handle: &'a WorkloadHandle,
        workload_address: std::net::IpAddr,
    },
    Unresolved(&'a ConvergeFailure),
    Stopped,
}

pub(crate) struct ConvergeFailure {
    phase: AssignmentPhase,
    class: FailureClass,
    reason: &'static str,
    message: String,
    retry_at: Option<Timestamp>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum FailureClass {
    Waiting,
    Retryable,
}

impl ConvergeFailure {
    pub(crate) fn pending(reason: &'static str, message: String) -> Self {
        Self {
            phase: AssignmentPhase::Pending,
            class: FailureClass::Waiting,
            reason,
            message,
            retry_at: None,
        }
    }

    pub(crate) fn pending_at(reason: &'static str, message: String, retry_at: Timestamp) -> Self {
        Self {
            phase: AssignmentPhase::Pending,
            class: FailureClass::Waiting,
            reason,
            message,
            retry_at: Some(retry_at),
        }
    }

    pub(crate) fn failed(reason: &'static str, message: String) -> Self {
        Self {
            phase: AssignmentPhase::Failed,
            class: FailureClass::Retryable,
            reason,
            message,
            retry_at: None,
        }
    }

    pub(crate) fn is_waiting(&self) -> bool {
        self.class == FailureClass::Waiting
    }

    pub(crate) fn with_retry(self, attempt: u32, retry_at: Timestamp) -> Self {
        Self {
            phase: AssignmentPhase::Pending,
            class: FailureClass::Waiting,
            reason: "RetryBackoff",
            message: format!(
                "{}; retry attempt {attempt} is scheduled for {}",
                self.message, retry_at.0
            ),
            retry_at: Some(retry_at),
        }
    }

    pub(crate) fn exhausted(self, maximum: u32) -> Self {
        Self {
            phase: AssignmentPhase::Failed,
            class: self.class,
            reason: "RetryLimitReached",
            message: format!(
                "{}; exhausted the retry limit of {maximum} attempts",
                self.message
            ),
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
            | RuntimeError::Timeout { .. } => Self::failed(RUNTIME_RETRY_REASON, error.to_string()),
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
                Self::failed(RUNTIME_RETRY_REASON, error.to_string())
            }
        }
    }
}

impl From<SecretMountError> for ConvergeFailure {
    fn from(error: SecretMountError) -> Self {
        match error {
            SecretMountError::InvalidRoot { .. }
            | SecretMountError::InvalidTarget { .. }
            | SecretMountError::InvalidKey { .. }
            | SecretMountError::InvalidFileName { .. }
            | SecretMountError::UnsafePath { .. }
            | SecretMountError::ContentConflict { .. }
            | SecretMountError::Encode { .. } => {
                Self::failed("SecretMountRejected", error.to_string())
            }
            SecretMountError::Task { .. } | SecretMountError::Io { .. } => {
                Self::failed("SecretMountUnavailable", error.to_string())
            }
        }
    }
}

#[cfg(unix)]
impl From<NodeApiMountError> for ConvergeFailure {
    fn from(error: NodeApiMountError) -> Self {
        match error {
            NodeApiMountError::InvalidRoot { .. }
            | NodeApiMountError::ServicesUnavailable
            | NodeApiMountError::UnsafePath { .. }
            | NodeApiMountError::InvalidCredential { .. }
            | NodeApiMountError::BindingConflict { .. }
            | NodeApiMountError::OwnerConflict { .. }
            | NodeApiMountError::Server(NodeApiServerError::PeerIdentityMismatch { .. }) => {
                Self::failed("NodeApiMountRejected", error.to_string())
            }
            NodeApiMountError::Random { .. }
            | NodeApiMountError::Task { .. }
            | NodeApiMountError::Server(_)
            | NodeApiMountError::Io { .. } => {
                Self::failed("NodeApiMountUnavailable", error.to_string())
            }
        }
    }
}

pub(crate) fn desired_status(
    assignment: &Assignment,
    outcome: AssignmentOutcome<'_>,
    now: Timestamp,
) -> AssignmentStatus {
    let (phase, workload_id, workload_address, state, reason, message) = match outcome {
        AssignmentOutcome::Running {
            handle,
            workload_address,
        } => (
            AssignmentPhase::Running,
            Some(handle.workload_id().clone()),
            Some(workload_address),
            ConditionState::True,
            WORKLOAD_RUNNING_REASON,
            "runtime workload is running at its assigned address".to_owned(),
        ),
        AssignmentOutcome::Unresolved(failure) => (
            failure.phase,
            assignment.status.workload_id.clone(),
            assignment.status.workload_address,
            ConditionState::False,
            failure.reason,
            failure.message.clone(),
        ),
        AssignmentOutcome::Stopped => (
            AssignmentPhase::Stopped,
            None,
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
        .find(|condition| condition.condition_type == ConditionType::RuntimeReady);
    let last_transition_time = previous
        .filter(|condition| condition.state == state && condition.reason.0 == reason)
        .map_or(now, |condition| condition.last_transition_time);
    AssignmentStatus {
        phase,
        workload_id,
        workload_address,
        conditions: vec![Condition {
            condition_type: ConditionType::RuntimeReady,
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
