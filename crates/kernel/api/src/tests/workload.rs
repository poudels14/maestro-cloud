use std::collections::BTreeMap;

use crate::{
    ArtifactArchiveId, ArtifactTemplate, BuildPhase, BuildSource, BuildTemplate, DeploymentPhase,
    NodeId, ServiceId, VolumeSource, workload_hostname,
};

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
fn workload_hostname_preserves_the_replica_slot_within_one_dns_label() {
    let service_id = ServiceId::new(format!("api.{}", "a".repeat(70))).expect("service id");
    let hostname = workload_hostname(&service_id, 42);
    assert_eq!(hostname.len(), 63);
    assert!(hostname.ends_with("-42"));
    assert!(!hostname.contains('.'));
}

#[test]
fn build_artifact_discriminator_does_not_collide_with_its_source_field() {
    let artifact = ArtifactTemplate::Build {
        template: BuildTemplate {
            source: BuildSource::Git {
                repository: "https://example.test/repo.git".to_string(),
                revision: "main".to_string(),
            },
            dockerfile: "Dockerfile".to_string(),
            environment: BTreeMap::new(),
            secrets: BTreeMap::new(),
        },
    };
    let encoded = serde_json::to_value(&artifact).expect("serialize build artifact");
    assert_eq!(encoded.get("type"), Some(&serde_json::json!("build")));
    assert!(
        encoded
            .get("source")
            .is_some_and(serde_json::Value::is_object)
    );
    assert_eq!(
        serde_json::from_value::<ArtifactTemplate>(encoded).expect("deserialize build artifact"),
        artifact
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
