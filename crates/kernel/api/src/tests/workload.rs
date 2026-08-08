use std::collections::BTreeMap;

use crate::{
    ArtifactArchiveId, ArtifactTemplate, BuildPhase, BuildSource, BuildTemplate, DeploymentGoal,
    DeploymentPhase, DepotBuildConfig, ExecPolicy, NodeApiAccess, NodeId, PlacementConstraint,
    PreviewPolicy, ReplicaSpread, SecretMountSpec, SecretValue, ServiceId, ServiceSpec,
    VolumeSource, workload_hostname,
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
    assert_eq!(
        serde_json::to_value(VolumeSource::ReplicaManaged {
            name: "identity".to_owned()
        })
        .expect("serialize replica-managed volume"),
        serde_json::json!({"type": "replicaManaged", "name": "identity"})
    );
    assert_eq!(
        serde_json::to_value(DeploymentGoal::Remove).expect("serialize deployment goal"),
        serde_json::json!("remove")
    );
    assert_eq!(DeploymentGoal::default(), DeploymentGoal::Run);
}

#[test]
fn preview_policy_uses_explicit_lifecycle_units() {
    let policy = PreviewPolicy {
        close_grace_period_secs: 86_400,
        lifetime_secs: 604_800,
        replicas: 1,
        environment: BTreeMap::from([("PREVIEW".to_string(), "true".to_string())]),
        environment_source: None,
    };

    assert_eq!(
        serde_json::to_value(policy).expect("serialize preview policy"),
        serde_json::json!({
            "closeGracePeriodSecs": 86_400,
            "lifetimeSecs": 604_800,
            "replicas": 1,
            "environment": {"PREVIEW": "true"}
        })
    );
}

#[test]
fn replica_spread_defaults_to_stable_and_uses_an_explicit_wire_value() {
    assert_eq!(
        serde_json::to_value(PlacementConstraint::default()).expect("serialize default placement"),
        serde_json::json!({})
    );
    let placement = PlacementConstraint {
        replica_spread: ReplicaSpread::BestEffort,
        ..PlacementConstraint::default()
    };
    assert_eq!(
        serde_json::to_value(&placement).expect("serialize spread placement"),
        serde_json::json!({"replicaSpread": "bestEffort"})
    );
    assert_eq!(
        serde_json::from_value::<PlacementConstraint>(serde_json::json!({
            "replicaSpread": "bestEffort"
        }))
        .expect("deserialize spread placement"),
        placement
    );
}

#[test]
fn service_restart_attempts_default_to_ten_and_preserve_zero() {
    let mut encoded = serde_json::to_value(valid_service_spec()).expect("serialize service spec");
    encoded
        .as_object_mut()
        .expect("service object")
        .remove("maxRestartAttempts");
    let defaulted: ServiceSpec = serde_json::from_value(encoded).expect("default retry limit");
    assert_eq!(
        defaulted.max_restart_attempts,
        crate::DEFAULT_MAX_RESTART_ATTEMPTS
    );

    let mut encoded = serde_json::to_value(valid_service_spec()).expect("serialize service spec");
    encoded
        .as_object_mut()
        .expect("service object")
        .insert("maxRestartAttempts".to_owned(), serde_json::json!(0));
    let disabled: ServiceSpec = serde_json::from_value(encoded).expect("zero retry limit");
    assert_eq!(disabled.max_restart_attempts, 0);
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
            watch: false,
            registry: None,
            registry_repository: None,
            depot: None,
            environment: BTreeMap::new(),
            environment_source: None,
            secrets: BTreeMap::new(),
            secrets_source: None,
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
        DeploymentPhase::Preparing,
        DeploymentPhase::Building,
        DeploymentPhase::Publishing,
        DeploymentPhase::Starting,
        DeploymentPhase::PendingReady,
        DeploymentPhase::Retrying,
        DeploymentPhase::Ready,
        DeploymentPhase::Recovering,
        DeploymentPhase::Stopping,
        DeploymentPhase::Stopped,
        DeploymentPhase::Crashed,
        DeploymentPhase::Removed,
        DeploymentPhase::Draining,
        DeploymentPhase::Canceled,
    ];

    for current in phases {
        for target in phases {
            let expected = match target {
                DeploymentPhase::Preparing => {
                    matches!(current, DeploymentPhase::Queued | DeploymentPhase::Building)
                }
                DeploymentPhase::Building => matches!(
                    current,
                    DeploymentPhase::Queued | DeploymentPhase::Preparing
                ),
                DeploymentPhase::Publishing => matches!(
                    current,
                    DeploymentPhase::Preparing
                        | DeploymentPhase::Building
                        | DeploymentPhase::Starting
                        | DeploymentPhase::PendingReady
                        | DeploymentPhase::Retrying
                        | DeploymentPhase::Ready
                        | DeploymentPhase::Recovering
                        | DeploymentPhase::Stopped
                ),
                DeploymentPhase::Starting => matches!(
                    current,
                    DeploymentPhase::Building
                        | DeploymentPhase::Publishing
                        | DeploymentPhase::Recovering
                        | DeploymentPhase::Stopped
                        | DeploymentPhase::Retrying
                ),
                DeploymentPhase::PendingReady => matches!(
                    current,
                    DeploymentPhase::Building
                        | DeploymentPhase::Publishing
                        | DeploymentPhase::Starting
                        | DeploymentPhase::Retrying
                        | DeploymentPhase::Ready
                        | DeploymentPhase::Recovering
                        | DeploymentPhase::Stopped
                ),
                DeploymentPhase::Retrying => matches!(
                    current,
                    DeploymentPhase::Publishing
                        | DeploymentPhase::Starting
                        | DeploymentPhase::PendingReady
                        | DeploymentPhase::Ready
                        | DeploymentPhase::Recovering
                ),
                DeploymentPhase::Ready => matches!(
                    current,
                    DeploymentPhase::Building
                        | DeploymentPhase::Publishing
                        | DeploymentPhase::Starting
                        | DeploymentPhase::PendingReady
                        | DeploymentPhase::Retrying
                        | DeploymentPhase::Recovering
                ),
                DeploymentPhase::Recovering => matches!(
                    current,
                    DeploymentPhase::Publishing
                        | DeploymentPhase::Starting
                        | DeploymentPhase::PendingReady
                        | DeploymentPhase::Retrying
                        | DeploymentPhase::Ready
                        | DeploymentPhase::Stopping
                        | DeploymentPhase::Stopped
                ),
                DeploymentPhase::Stopping => matches!(
                    current,
                    DeploymentPhase::Publishing
                        | DeploymentPhase::Starting
                        | DeploymentPhase::PendingReady
                        | DeploymentPhase::Retrying
                        | DeploymentPhase::Ready
                        | DeploymentPhase::Recovering
                ),
                DeploymentPhase::Stopped => matches!(
                    current,
                    DeploymentPhase::Publishing
                        | DeploymentPhase::Starting
                        | DeploymentPhase::PendingReady
                        | DeploymentPhase::Retrying
                        | DeploymentPhase::Ready
                        | DeploymentPhase::Recovering
                        | DeploymentPhase::Stopping
                ),
                DeploymentPhase::Crashed => !matches!(
                    current,
                    DeploymentPhase::Crashed | DeploymentPhase::Canceled | DeploymentPhase::Removed
                ),
                DeploymentPhase::Draining => matches!(
                    current,
                    DeploymentPhase::Ready
                        | DeploymentPhase::Recovering
                        | DeploymentPhase::Stopping
                        | DeploymentPhase::Stopped
                        | DeploymentPhase::PendingReady
                        | DeploymentPhase::Retrying
                        | DeploymentPhase::Starting
                        | DeploymentPhase::Publishing
                        | DeploymentPhase::Building
                        | DeploymentPhase::Preparing
                ),
                DeploymentPhase::Queued | DeploymentPhase::Removed | DeploymentPhase::Canceled => {
                    true
                }
            };
            assert_eq!(current.can_transition_to(target), expected);
        }
    }
}

#[test]
fn build_transition_matrix_is_exhaustive() {
    let phases = [
        BuildPhase::Queued,
        BuildPhase::Preparing,
        BuildPhase::Building,
        BuildPhase::Succeeded,
        BuildPhase::Failed,
        BuildPhase::Canceled,
    ];

    for current in phases {
        for target in phases {
            let expected = current == target
                || matches!(
                    (current, target),
                    (
                        BuildPhase::Queued,
                        BuildPhase::Preparing | BuildPhase::Canceled
                    ) | (
                        BuildPhase::Preparing,
                        BuildPhase::Building | BuildPhase::Failed | BuildPhase::Canceled
                    ) | (
                        BuildPhase::Building,
                        BuildPhase::Succeeded | BuildPhase::Failed | BuildPhase::Canceled
                    )
                );
            assert_eq!(
                current.can_transition_to(target),
                expected,
                "{current:?} -> {target:?}"
            );
        }
    }
}

#[test]
fn service_admission_rejects_unsafe_runtime_shapes() {
    let mut spec = valid_service_spec();
    spec.exposed_ports = vec![8080, 8080];
    assert!(spec.validate().is_err());

    let mut spec = valid_service_spec();
    spec.environment
        .insert("BAD-KEY".to_string(), "x".to_string());
    assert!(spec.validate().is_err());

    let mut spec = valid_service_spec();
    spec.node_api = NodeApiAccess::IdentityAndTelemetry;
    assert!(spec.validate().is_err());

    let mut spec = valid_service_spec();
    spec.artifact = ArtifactTemplate::Build {
        template: BuildTemplate {
            source: BuildSource::Git {
                repository: "https://example.test/repo.git".to_string(),
                revision: "main".to_string(),
            },
            dockerfile: "../Dockerfile".to_string(),
            watch: false,
            registry: None,
            registry_repository: None,
            depot: None,
            environment: BTreeMap::new(),
            environment_source: None,
            secrets: BTreeMap::new(),
            secrets_source: None,
        },
    };
    assert!(spec.validate().is_err());

    let mut spec = valid_service_spec();
    spec.artifact = ArtifactTemplate::Build {
        template: BuildTemplate {
            source: BuildSource::Git {
                repository: "https://example.test/repo.git".to_string(),
                revision: "main".to_string(),
            },
            dockerfile: "Dockerfile".to_string(),
            watch: false,
            registry: Some("registry.example/team/".to_string()),
            registry_repository: None,
            depot: None,
            environment: BTreeMap::new(),
            environment_source: None,
            secrets: BTreeMap::new(),
            secrets_source: None,
        },
    };
    assert!(spec.validate().is_err());

    let ArtifactTemplate::Build { template } = &mut spec.artifact else {
        unreachable!("fixture is a build")
    };
    template.registry = None;
    template.registry_repository = Some(ServiceId::new("api").expect("service id"));
    assert!(spec.validate().is_err());

    let ArtifactTemplate::Build { template } = &mut spec.artifact else {
        unreachable!("fixture is a build")
    };
    template.registry_repository = None;
    template.depot = Some(DepotBuildConfig {
        project: "invalid project".to_owned(),
    });
    assert!(spec.validate().is_err());

    let mut spec = valid_service_spec();
    spec.secrets = Some(SecretMountSpec::Files {
        mount_path: "/run/secrets/etcd".to_string(),
        files: BTreeMap::new(),
    });
    assert!(spec.validate().is_err());

    let mut spec = valid_service_spec();
    spec.secrets = Some(SecretMountSpec::Files {
        mount_path: "/run/secrets/etcd".to_string(),
        files: BTreeMap::from([("../ca.pem".to_string(), SecretValue::new("certificate"))]),
    });
    assert!(spec.validate().is_err());
}

fn valid_service_spec() -> ServiceSpec {
    ServiceSpec {
        name: "API".to_string(),
        version: "1.0.0".to_string(),
        artifact: ArtifactTemplate::Image {
            reference: "registry.test/api:1.0.0".to_string(),
        },
        preview: None,
        command: None,
        replicas: 1,
        exposed_ports: vec![8080],
        health_check: None,
        max_restart_attempts: crate::DEFAULT_MAX_RESTART_ATTEMPTS,
        environment: BTreeMap::new(),
        environment_sources: Vec::new(),
        user: None,
        node_api: NodeApiAccess::Disabled,
        secrets: None,
        volumes: Vec::new(),
        placement: PlacementConstraint::default(),
        exec: ExecPolicy::Allowed,
    }
}
