use std::collections::HashMap;

use docker::errors::Error as DockerError;
use docker::models::{ContainerInspectResponse, ContainerStateStatusEnum, EventMessage};
use kernel_api::WorkloadId;

use crate::docker_config::{METADATA_LABEL, SPEC_LABEL, WORKLOAD_LABEL};
use crate::{
    EventCursor, ObservedWorkload, RuntimeError, RuntimeEvent, RuntimeEventKind, WorkloadHandle,
    WorkloadMetadata, WorkloadState, WorkloadStatus,
};

pub(crate) fn docker_handle(
    workload_id: WorkloadId,
    container_id: impl Into<String>,
) -> Result<WorkloadHandle, RuntimeError> {
    WorkloadHandle::new(workload_id, format!("docker/{}", container_id.into()))
}

pub(crate) fn container_id(handle: &WorkloadHandle) -> Result<&str, RuntimeError> {
    handle
        .backend_id()
        .strip_prefix("docker/")
        .ok_or_else(|| RuntimeError::Conflict {
            workload_id: handle.workload_id().clone(),
            message: "workload handle does not belong to the docker runtime".to_owned(),
        })
}

pub(crate) fn inspect_handle(
    inspect: &ContainerInspectResponse,
) -> Result<WorkloadHandle, RuntimeError> {
    let metadata = inspect_metadata(inspect)?;
    let container_id = inspect
        .id
        .clone()
        .ok_or_else(|| RuntimeError::Unavailable {
            message: "docker inspect response omitted the container ID".to_owned(),
        })?;
    docker_handle(metadata.workload_id, container_id)
}

pub(crate) fn validate_existing(
    inspect: &ContainerInspectResponse,
    workload_id: &WorkloadId,
    fingerprint: &str,
) -> Result<WorkloadHandle, RuntimeError> {
    let metadata = inspect_metadata(inspect)?;
    let labels = inspect_labels(inspect)?;
    let existing_fingerprint = labels
        .get(SPEC_LABEL)
        .ok_or_else(|| RuntimeError::Conflict {
            workload_id: workload_id.clone(),
            message: "existing docker container has no Maestro specification fingerprint"
                .to_owned(),
        })?;
    if metadata.workload_id != *workload_id || existing_fingerprint != fingerprint {
        Err(RuntimeError::Conflict {
            workload_id: workload_id.clone(),
            message: "docker container already exists with different ownership or specification"
                .to_owned(),
        })
    } else {
        inspect_handle(inspect)
    }
}

pub(crate) fn observed_workload(
    inspect: &ContainerInspectResponse,
) -> Result<ObservedWorkload, RuntimeError> {
    Ok(ObservedWorkload {
        handle: inspect_handle(inspect)?,
        metadata: inspect_metadata(inspect)?,
        status: workload_status(inspect),
    })
}

pub(crate) fn inspect_metadata(
    inspect: &ContainerInspectResponse,
) -> Result<WorkloadMetadata, RuntimeError> {
    let labels = inspect_labels(inspect)?;
    let encoded = labels
        .get(METADATA_LABEL)
        .ok_or_else(|| RuntimeError::Rejected {
            message: "docker container omitted Maestro ownership metadata".to_owned(),
        })?;
    serde_json::from_str(encoded).map_err(|error| RuntimeError::Rejected {
        message: format!("docker container has invalid Maestro ownership metadata: {error}"),
    })
}

pub(crate) fn workload_status(inspect: &ContainerInspectResponse) -> WorkloadStatus {
    let state = inspect.state.as_ref();
    let status = state.and_then(|state| state.status);
    let workload_state = match status {
        Some(ContainerStateStatusEnum::CREATED) => WorkloadState::Created,
        Some(ContainerStateStatusEnum::RUNNING) => WorkloadState::Running,
        Some(ContainerStateStatusEnum::PAUSED) => WorkloadState::Paused,
        Some(ContainerStateStatusEnum::EXITED | ContainerStateStatusEnum::STOPPING) => {
            WorkloadState::Stopped
        }
        Some(ContainerStateStatusEnum::RESTARTING) => WorkloadState::Running,
        Some(
            ContainerStateStatusEnum::DEAD
            | ContainerStateStatusEnum::REMOVING
            | ContainerStateStatusEnum::EMPTY,
        )
        | None => WorkloadState::Failed,
    };
    let exit_code = state
        .and_then(|state| state.exit_code)
        .and_then(i32_exit_code);
    let detail = state
        .and_then(|state| state.error.clone())
        .filter(|message| !message.is_empty());
    WorkloadStatus {
        state: workload_state,
        exit_code,
        detail,
    }
}

pub(crate) fn process_id(
    inspect: &ContainerInspectResponse,
    workload_id: &WorkloadId,
) -> Result<u32, RuntimeError> {
    let process_id = inspect
        .state
        .as_ref()
        .and_then(|state| state.pid)
        .ok_or_else(|| RuntimeError::Conflict {
            workload_id: workload_id.clone(),
            message: "docker container has no running process ID".to_owned(),
        })?;
    u32::try_from(process_id).map_err(|_| RuntimeError::Rejected {
        message: format!("docker reported invalid container process ID `{process_id}`"),
    })
}

pub(crate) fn runtime_event(message: EventMessage) -> Result<Option<RuntimeEvent>, RuntimeError> {
    let Some(kind) = message.action.as_deref().and_then(event_kind) else {
        return Ok(None);
    };
    let actor = message.actor.ok_or_else(|| RuntimeError::Stream {
        message: "docker lifecycle event omitted its actor".to_owned(),
    })?;
    let attributes = actor.attributes.ok_or_else(|| RuntimeError::Stream {
        message: "docker lifecycle event omitted ownership labels".to_owned(),
    })?;
    let workload_value = attributes
        .get(WORKLOAD_LABEL)
        .ok_or_else(|| RuntimeError::Stream {
            message: "docker lifecycle event omitted the Maestro workload label".to_owned(),
        })?;
    let workload_id =
        WorkloadId::new(workload_value.clone()).map_err(|error| RuntimeError::Stream {
            message: format!("docker lifecycle event has an invalid workload ID: {error}"),
        })?;
    let cursor = event_cursor(message.time_nano, message.time)?;
    let exit_code = if kind == RuntimeEventKind::Exited {
        attributes
            .get("exitCode")
            .and_then(|value| value.parse::<i32>().ok())
    } else {
        None
    };
    Ok(Some(RuntimeEvent {
        cursor,
        workload_id,
        kind,
        exit_code,
    }))
}

pub(crate) fn runtime_error(error: DockerError, workload_id: &WorkloadId) -> RuntimeError {
    let message = error.to_string();
    match error {
        DockerError::DockerResponseServerError {
            status_code: 404, ..
        } => RuntimeError::NotFound {
            workload_id: workload_id.clone(),
        },
        DockerError::DockerResponseServerError {
            status_code: 409, ..
        } => RuntimeError::Conflict {
            workload_id: workload_id.clone(),
            message,
        },
        DockerError::DockerResponseServerError {
            status_code: 400 | 422,
            ..
        } => RuntimeError::Rejected { message },
        _ => RuntimeError::Unavailable { message },
    }
}

pub(crate) fn stream_error(error: DockerError) -> RuntimeError {
    RuntimeError::Stream {
        message: error.to_string(),
    }
}

pub(crate) fn is_not_found(error: &DockerError) -> bool {
    matches!(
        error,
        DockerError::DockerResponseServerError {
            status_code: 404,
            ..
        }
    )
}

pub(crate) fn is_conflict(error: &DockerError) -> bool {
    matches!(
        error,
        DockerError::DockerResponseServerError {
            status_code: 409,
            ..
        }
    )
}

pub(crate) fn is_not_modified(error: &DockerError) -> bool {
    matches!(
        error,
        DockerError::DockerResponseServerError {
            status_code: 304,
            ..
        }
    )
}

fn inspect_labels(
    inspect: &ContainerInspectResponse,
) -> Result<&HashMap<String, String>, RuntimeError> {
    inspect
        .config
        .as_ref()
        .and_then(|config| config.labels.as_ref())
        .ok_or_else(|| RuntimeError::Rejected {
            message: "docker container omitted Maestro ownership labels".to_owned(),
        })
}

fn event_kind(action: &str) -> Option<RuntimeEventKind> {
    match action {
        "create" => Some(RuntimeEventKind::Created),
        "start" => Some(RuntimeEventKind::Started),
        "die" => Some(RuntimeEventKind::Exited),
        "destroy" => Some(RuntimeEventKind::Removed),
        _ => None,
    }
}

fn event_cursor(
    nanoseconds: Option<i64>,
    seconds: Option<i64>,
) -> Result<EventCursor, RuntimeError> {
    let total_nanoseconds =
        nanoseconds.or_else(|| seconds.and_then(|value| value.checked_mul(1_000_000_000)));
    let total_nanoseconds = total_nanoseconds.ok_or_else(|| RuntimeError::Stream {
        message: "docker lifecycle event omitted its timestamp".to_owned(),
    })?;
    let seconds = total_nanoseconds.div_euclid(1_000_000_000);
    let remainder = total_nanoseconds.rem_euclid(1_000_000_000);
    Ok(EventCursor::new(format!("{seconds}.{remainder:09}")))
}

fn i32_exit_code(value: i64) -> Option<i32> {
    i32::try_from(value).ok()
}
