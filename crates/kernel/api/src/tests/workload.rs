use crate::{ArtifactArchiveId, BuildPhase, BuildSource, DeploymentPhase, NodeId, VolumeSource};

#[test]
fn tagged_enum_fields_follow_the_camel_case_wire_contract() {
    let build_source = BuildSource::Tarball {
        archive_id: ArtifactArchiveId::new("archive-1").expect("archive id"),
    };
    let volume_source = VolumeSource::HostPath {
        path: "/srv/data".to_string(),
        node_id: NodeId::new("node-1").expect("node id"),
    };

    assert_eq!(
        serde_json::to_value(build_source).expect("serialize build source"),
        serde_json::json!({"type": "tarball", "archiveId": "archive-1"})
    );
    assert_eq!(
        serde_json::to_value(volume_source).expect("serialize volume source"),
        serde_json::json!({"type": "hostPath", "path": "/srv/data", "nodeId": "node-1"})
    );
}

#[test]
fn deployment_transition_matrix_matches_the_harvested_lifecycle() {
    let phases = [
        DeploymentPhase::Queued,
        DeploymentPhase::Building,
        DeploymentPhase::PendingReady,
        DeploymentPhase::Ready,
        DeploymentPhase::Crashed,
        DeploymentPhase::Terminated,
        DeploymentPhase::Removed,
        DeploymentPhase::Draining,
        DeploymentPhase::Canceled,
    ];

    for current in phases {
        for target in phases {
            let expected = match target {
                DeploymentPhase::PendingReady => current == DeploymentPhase::Building,
                DeploymentPhase::Ready => matches!(
                    current,
                    DeploymentPhase::Building | DeploymentPhase::PendingReady
                ),
                DeploymentPhase::Crashed => !matches!(
                    current,
                    DeploymentPhase::Crashed
                        | DeploymentPhase::Canceled
                        | DeploymentPhase::Terminated
                ),
                DeploymentPhase::Draining => matches!(
                    current,
                    DeploymentPhase::Ready
                        | DeploymentPhase::PendingReady
                        | DeploymentPhase::Building
                ),
                DeploymentPhase::Terminated => current != DeploymentPhase::Terminated,
                DeploymentPhase::Queued
                | DeploymentPhase::Building
                | DeploymentPhase::Removed
                | DeploymentPhase::Canceled => true,
            };
            assert_eq!(current.can_transition_to(target), expected);
        }
    }
}

#[test]
fn build_terminal_phases_do_not_restart_themselves() {
    assert!(BuildPhase::Queued.can_transition_to(BuildPhase::Preparing));
    assert!(BuildPhase::Building.can_transition_to(BuildPhase::Succeeded));
    assert!(!BuildPhase::Succeeded.can_transition_to(BuildPhase::Building));
    assert!(!BuildPhase::Failed.can_transition_to(BuildPhase::Queued));
    assert!(BuildPhase::Canceled.can_transition_to(BuildPhase::Canceled));
}
