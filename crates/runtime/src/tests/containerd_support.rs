use containerd::services::v1::Container;
use containerd::types::v1::{Process, Status};

use crate::containerd_support::{metadata_labels, task_status, validate_existing};
use crate::{RuntimeError, WorkloadState};

use super::containerd_fixture::metadata;

#[test]
fn containerd_metadata_round_trips_into_an_adoptable_handle() {
    let metadata = metadata();
    let container = Container {
        id: "maestro-workload-1".to_owned(),
        labels: metadata_labels(&metadata, "fingerprint".to_owned()).unwrap(),
        ..Default::default()
    };
    let handle =
        validate_existing(&container, &metadata.workload_id, "fingerprint", "maestro").unwrap();
    assert_eq!(handle.backend_id(), "containerd/maestro/maestro-workload-1");
    assert!(matches!(
        validate_existing(&container, &metadata.workload_id, "different", "maestro"),
        Err(RuntimeError::Conflict { .. })
    ));
}

#[test]
fn containerd_task_status_preserves_runtime_state_and_exit_code() {
    assert_eq!(task_status(None).state, WorkloadState::Created);
    let running = Process {
        status: Status::Running as i32,
        pid: 42,
        ..Default::default()
    };
    assert_eq!(task_status(Some(&running)).state, WorkloadState::Running);
    let stopped = Process {
        status: Status::Stopped as i32,
        exit_status: 17,
        ..Default::default()
    };
    let status = task_status(Some(&stopped));
    assert_eq!(status.state, WorkloadState::Stopped);
    assert_eq!(status.exit_code, Some(17));
}
