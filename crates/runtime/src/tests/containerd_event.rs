use containerd::events::{ContainerCreate, TaskExit};
use containerd::types::Envelope;
use prost::Message;
use prost_types::Any;

use crate::RuntimeEventKind;
use crate::containerd_event::decode_event;

#[test]
fn containerd_events_decode_primary_lifecycle_changes() {
    let created = envelope(
        "/containers/create",
        ContainerCreate {
            id: "maestro-workload-1".to_owned(),
            image: String::new(),
            runtime: None,
        },
    );
    let event = decode_event(&created).unwrap().unwrap();
    assert_eq!(event.container_id, "maestro-workload-1");
    assert_eq!(event.kind, RuntimeEventKind::Created);

    let exited = envelope(
        "/tasks/exit",
        TaskExit {
            container_id: "maestro-workload-1".to_owned(),
            id: String::new(),
            exit_status: 23,
            ..Default::default()
        },
    );
    let event = decode_event(&exited).unwrap().unwrap();
    assert_eq!(event.kind, RuntimeEventKind::Exited);
    assert_eq!(event.exit_code, Some(23));

    let service_shaped_exit = envelope(
        "/tasks/exit",
        TaskExit {
            container_id: "maestro-workload-1".to_owned(),
            id: "maestro-workload-1".to_owned(),
            exit_status: 24,
            ..Default::default()
        },
    );
    let event = decode_event(&service_shaped_exit).unwrap().unwrap();
    assert_eq!(event.kind, RuntimeEventKind::Exited);
    assert_eq!(event.exit_code, Some(24));
}

#[test]
fn containerd_events_ignore_exec_process_exits_and_unknown_topics() {
    let exec_exit = envelope(
        "/tasks/exit",
        TaskExit {
            container_id: "maestro-workload-1".to_owned(),
            id: "maestro-exec-1".to_owned(),
            ..Default::default()
        },
    );
    assert!(decode_event(&exec_exit).unwrap().is_none());
    assert!(decode_event(&Envelope::default()).unwrap().is_none());
}

fn envelope(topic: &str, event: impl Message) -> Envelope {
    Envelope {
        topic: topic.to_owned(),
        event: Some(Any {
            type_url: String::new(),
            value: event.encode_to_vec(),
        }),
        ..Default::default()
    }
}
