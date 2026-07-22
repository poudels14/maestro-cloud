use containerd::services::v1::Container;
use containerd::types::v1::{Process, Status};

use crate::containerd_support::{
    metadata_labels, task_container_id, task_status, validate_existing,
};
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

#[test]
fn containerd_task_listing_accepts_service_and_shim_identity_shapes() {
    let service_shape = Process {
        id: "maestro-workload-1".to_owned(),
        ..Default::default()
    };
    assert_eq!(task_container_id(&service_shape), "maestro-workload-1");

    let explicit_shape = Process {
        container_id: "maestro-workload-2".to_owned(),
        id: "init".to_owned(),
        ..Default::default()
    };
    assert_eq!(task_container_id(&explicit_shape), "maestro-workload-2");
}
