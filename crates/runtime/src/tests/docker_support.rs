use std::collections::HashMap;

use docker::models::{
    ContainerConfig, ContainerInspectResponse, ContainerState, ContainerStateStatusEnum,
    EventActor, EventMessage,
};

use crate::docker_config::{METADATA_LABEL, SPEC_LABEL, WORKLOAD_LABEL, container_config};
use crate::docker_support::{observed_workload, runtime_event, validate_existing, workload_status};
use crate::{RuntimeEventKind, WorkloadState};

use super::docker_fixture::{container_spec, metadata};

#[test]
fn docker_inspection_recovers_ownership_status_and_exact_handle() {
    let desired = container_config(&container_spec()).unwrap();
    let inspect = inspection(
        desired.fingerprint.clone(),
        ContainerStateStatusEnum::RUNNING,
    );
    let handle =
        validate_existing(&inspect, &metadata().workload_id, &desired.fingerprint).unwrap();
    assert_eq!(handle.backend_id(), "docker/container-native-id");
    let observed = observed_workload(&inspect).unwrap();
    assert_eq!(observed.metadata, metadata());
    assert_eq!(observed.status.state, WorkloadState::Running);

    assert!(validate_existing(&inspect, &metadata().workload_id, "different-fingerprint").is_err());
}

#[test]
fn docker_status_and_events_translate_native_state_without_guessing() {
    let stopped = inspection("fingerprint".to_owned(), ContainerStateStatusEnum::EXITED);
    let status = workload_status(&stopped);
    assert_eq!(status.state, WorkloadState::Stopped);
    assert_eq!(status.exit_code, Some(23));

    let event = runtime_event(EventMessage {
        action: Some("die".to_owned()),
        actor: Some(EventActor {
            id: Some("container-native-id".to_owned()),
            attributes: Some(HashMap::from([
                (WORKLOAD_LABEL.to_owned(), "workload-1".to_owned()),
                ("exitCode".to_owned(), "23".to_owned()),
            ])),
        }),
        time_nano: Some(1_774_035_738_123_456_789),
        ..Default::default()
    })
    .unwrap()
    .unwrap();
    assert_eq!(event.kind, RuntimeEventKind::Exited);
    assert_eq!(event.exit_code, Some(23));
    assert_eq!(event.cursor.as_str(), "1774035738.123456789");
}

fn inspection(fingerprint: String, status: ContainerStateStatusEnum) -> ContainerInspectResponse {
    ContainerInspectResponse {
        id: Some("container-native-id".to_owned()),
        config: Some(ContainerConfig {
            labels: Some(HashMap::from([
                (
                    METADATA_LABEL.to_owned(),
                    serde_json::to_string(&metadata()).unwrap(),
                ),
                (SPEC_LABEL.to_owned(), fingerprint),
            ])),
            ..Default::default()
        }),
        state: Some(ContainerState {
            status: Some(status),
            pid: Some(321),
            exit_code: Some(23),
            ..Default::default()
        }),
        ..Default::default()
    }
}
