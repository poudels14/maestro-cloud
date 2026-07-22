use std::collections::HashMap;
use std::time::Duration;

use containerd::services::v1::Container;
use containerd::tonic::{Code, Request, Status};
use kernel_api::WorkloadId;

use crate::{
    ObservedWorkload, RuntimeError, WorkloadHandle, WorkloadMetadata, WorkloadState, WorkloadStatus,
};

pub(crate) const CLUSTER_LABEL: &str = "com.maestro.cluster-id";
pub(crate) const NODE_LABEL: &str = "com.maestro.node-id";
pub(crate) const WORKLOAD_LABEL: &str = "com.maestro.workload-id";
pub(crate) const METADATA_LABEL: &str = "com.maestro.metadata";
pub(crate) const SPEC_LABEL: &str = "com.maestro.spec-sha256";

pub(crate) fn namespaced<T>(message: T, namespace: &str) -> Result<Request<T>, RuntimeError> {
    let value = namespace
        .parse()
        .map_err(|error| RuntimeError::InvalidSpec {
            message: format!("containerd namespace is invalid gRPC metadata: {error}"),
        })?;
    let mut request = Request::new(message);
    request.metadata_mut().insert("containerd-namespace", value);
    Ok(request)
}

pub(crate) fn namespaced_timeout<T>(
    message: T,
    namespace: &str,
    timeout: Duration,
) -> Result<Request<T>, RuntimeError> {
    let mut request = namespaced(message, namespace)?;
    request.set_timeout(timeout);
    Ok(request)
}

pub(crate) fn container_name(workload_id: &WorkloadId) -> String {
    format!("maestro-{}", workload_id.as_str())
}

pub(crate) fn containerd_handle(
    workload_id: WorkloadId,
    namespace: &str,
    container_id: &str,
) -> Result<WorkloadHandle, RuntimeError> {
    WorkloadHandle::new(
        workload_id,
        format!("containerd/{namespace}/{container_id}"),
    )
}

pub(crate) fn container_id<'a>(
    handle: &'a WorkloadHandle,
    namespace: &str,
) -> Result<&'a str, RuntimeError> {
    let prefix = format!("containerd/{namespace}/");
    handle
        .backend_id()
        .strip_prefix(&prefix)
        .ok_or_else(|| RuntimeError::Conflict {
            workload_id: handle.workload_id().clone(),
            message: "workload handle does not belong to this containerd namespace".to_owned(),
        })
}

pub(crate) fn metadata_labels(
    metadata: &WorkloadMetadata,
    fingerprint: String,
) -> Result<HashMap<String, String>, RuntimeError> {
    Ok(HashMap::from([
        (CLUSTER_LABEL.to_owned(), metadata.cluster_id.to_string()),
        (NODE_LABEL.to_owned(), metadata.node_id.to_string()),
        (WORKLOAD_LABEL.to_owned(), metadata.workload_id.to_string()),
        (
            METADATA_LABEL.to_owned(),
            serde_json::to_string(metadata).map_err(|error| RuntimeError::InvalidSpec {
                message: format!("failed to encode containerd ownership metadata: {error}"),
            })?,
        ),
        (SPEC_LABEL.to_owned(), fingerprint),
    ]))
}

pub(crate) fn container_metadata(container: &Container) -> Result<WorkloadMetadata, RuntimeError> {
    let encoded = container
        .labels
        .get(METADATA_LABEL)
        .ok_or_else(|| RuntimeError::Rejected {
            message: format!(
                "containerd container `{}` omitted Maestro ownership metadata",
                container.id
            ),
        })?;
    serde_json::from_str(encoded).map_err(|error| RuntimeError::Rejected {
        message: format!(
            "containerd container `{}` has invalid ownership metadata: {error}",
            container.id
        ),
    })
}

pub(crate) fn validate_existing(
    container: &Container,
    workload_id: &WorkloadId,
    fingerprint: &str,
    namespace: &str,
) -> Result<WorkloadHandle, RuntimeError> {
    let metadata = container_metadata(container)?;
    let existing_fingerprint = container.labels.get(SPEC_LABEL);
    if metadata.workload_id == *workload_id
        && existing_fingerprint.is_some_and(|existing| existing == fingerprint)
    {
        containerd_handle(workload_id.clone(), namespace, &container.id)
    } else {
        Err(RuntimeError::Conflict {
            workload_id: workload_id.clone(),
            message: "containerd container exists with different ownership or specification"
                .to_owned(),
        })
    }
}

pub(crate) fn observed_workload(
    container: &Container,
    namespace: &str,
    status: WorkloadStatus,
) -> Result<ObservedWorkload, RuntimeError> {
    let metadata = container_metadata(container)?;
    Ok(ObservedWorkload {
        handle: containerd_handle(metadata.workload_id.clone(), namespace, &container.id)?,
        metadata,
        status,
    })
}

pub(crate) fn task_status(process: Option<&containerd::types::v1::Process>) -> WorkloadStatus {
    let state =
        process.and_then(|process| containerd::types::v1::Status::try_from(process.status).ok());
    let workload_state = match state {
        None => WorkloadState::Created,
        Some(containerd::types::v1::Status::Created) => WorkloadState::Created,
        Some(containerd::types::v1::Status::Running) => WorkloadState::Running,
        Some(containerd::types::v1::Status::Stopped) => WorkloadState::Stopped,
        Some(containerd::types::v1::Status::Paused) => WorkloadState::Paused,
        Some(containerd::types::v1::Status::Pausing) => WorkloadState::Running,
        Some(containerd::types::v1::Status::Unknown) => WorkloadState::Failed,
    };
    let exit_code = process
        .filter(|_| workload_state == WorkloadState::Stopped)
        .and_then(|process| i32::try_from(process.exit_status).ok());
    WorkloadStatus {
        state: workload_state,
        exit_code,
        detail: None,
    }
}

pub(crate) fn task_container_id(process: &containerd::types::v1::Process) -> &str {
    if process.container_id.is_empty() {
        &process.id
    } else {
        &process.container_id
    }
}

pub(crate) fn runtime_status(error: Status, workload_id: &WorkloadId) -> RuntimeError {
    let message = error.message().to_owned();
    match error.code() {
        Code::NotFound => RuntimeError::NotFound {
            workload_id: workload_id.clone(),
        },
        Code::AlreadyExists | Code::FailedPrecondition | Code::Aborted => RuntimeError::Conflict {
            workload_id: workload_id.clone(),
            message,
        },
        Code::InvalidArgument | Code::OutOfRange | Code::PermissionDenied => {
            RuntimeError::Rejected { message }
        }
        _ => RuntimeError::Unavailable { message },
    }
}

pub(crate) fn is_not_found(error: &Status) -> bool {
    error.code() == Code::NotFound
}

pub(crate) fn is_already_exists(error: &Status) -> bool {
    error.code() == Code::AlreadyExists
}
