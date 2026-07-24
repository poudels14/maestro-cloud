use kernel_api::{
    AnnotationKey, Assignment, AssignmentPhase, BuiltinKind, Deployment, DeploymentPhase,
    ReplicaState,
};
use serde_json::json;

use crate::legacy_fixtures::cluster_state;
use crate::legacy_node_tests::node_entries;
use crate::{LegacyEntry, LegacyPlanError, LegacySnapshot, plan_legacy_snapshot};

type TestResult = Result<(), Box<dyn std::error::Error>>;

const MASTER_SECRET: &str = "correct horse battery staple";

#[test]
fn cutover_plan_converts_scheduled_assignment_and_replica_state() -> TestResult {
    let mut entries = vec![
        service_info(),
        history_counter(),
        deployment_history(),
        json_entry(
            "/maetro/cluster/assignments/node-a",
            json!({
                "nodeId": "node-a",
                "generation": 7,
                "assignments": [{
                    "assignmentId": "assignment-1",
                    "placementEpoch": 3,
                    "serviceId": "api",
                    "deploymentId": "deploy-1",
                    "replicaIndex": 0,
                    "nodeId": "node-a",
                    "containerIp": "10.42.1.5",
                    "createdAtMs": 3_000
                }]
            }),
        ),
        json_entry(
            "/maetro/cluster/replica-states/node-a/assignment-1",
            json!({
                "serviceId": "api",
                "deploymentId": "deploy-1",
                "replicaIndex": 0,
                "status": "READY",
                "healthcheckFailures": 2,
                "restartAttempts": 4,
                "nodeId": "node-a",
                "assignmentId": "assignment-1",
                "endpoint": {
                    "containerIp": "10.42.1.5",
                    "containerHostname": "api-deploy",
                    "ingressContainerPort": 8080,
                    "gateway": {"hostIp": "10.0.0.5", "port": 443}
                },
                "error": "last transient failure"
            }),
        ),
    ];
    entries.extend(node_entries("node-a", "master", 10, 1));
    entries.extend(cluster_state());
    let snapshot = LegacySnapshot::new(entries)?;

    let plan = plan_legacy_snapshot(&snapshot, MASTER_SECRET)?;
    let assignment: Assignment = decode_write(&plan, BuiltinKind::Assignment)?;
    let replica: ReplicaState = decode_write(&plan, BuiltinKind::ReplicaState)?;

    assert_eq!(assignment.meta.id.as_str(), "assignment-1");
    assert_eq!(assignment.spec.node_id.as_str(), "node-a");
    assert_eq!(assignment.spec.placement_epoch, 3);
    assert_eq!(
        assignment
            .spec
            .workload_address
            .map(|address| address.to_string()),
        Some("10.42.1.5".to_owned())
    );
    assert_eq!(assignment.status.phase, AssignmentPhase::Running);
    assert_eq!(
        assignment.status.workload_id.as_ref().map(|id| id.as_str()),
        Some("assignment-1")
    );
    assert_eq!(
        assignment
            .meta
            .annotations
            .get(&AnnotationKey(
                "migration.maestro.dev/legacy-manifest-generation".to_owned()
            ))
            .map(String::as_str),
        Some("7")
    );
    assert_eq!(replica.status.phase, DeploymentPhase::Ready);
    assert_eq!(replica.status.healthcheck_failures, 2);
    assert_eq!(replica.status.restart_attempts, 4);
    assert!(replica.meta.annotations.contains_key(&AnnotationKey(
        "migration.maestro.dev/legacy-endpoint".to_owned()
    )));
    assert_eq!(
        replica
            .meta
            .annotations
            .get(&AnnotationKey(
                "migration.maestro.dev/legacy-error".to_owned()
            ))
            .map(String::as_str),
        Some("last transient failure")
    );
    Ok(())
}

#[test]
fn cutover_plan_rejects_orphan_and_mismatched_replica_state() -> TestResult {
    let orphan = LegacySnapshot::new(vec![
        service_info(),
        history_counter(),
        deployment_history(),
        json_entry(
            "/maetro/cluster/replica-states/node-a/assignment-1",
            replica_state("node-a", "assignment-1"),
        ),
    ])?;
    assert!(matches!(
        plan_legacy_snapshot(&orphan, MASTER_SECRET),
        Err(LegacyPlanError::DecodeLegacyState { .. })
    ));

    let mismatch = LegacySnapshot::new(vec![
        service_info(),
        history_counter(),
        deployment_history(),
        assignment_manifest(),
        json_entry(
            "/maetro/cluster/replica-states/node-a/assignment-1",
            replica_state("node-b", "assignment-1"),
        ),
    ])?;
    assert!(matches!(
        plan_legacy_snapshot(&mismatch, MASTER_SECRET),
        Err(LegacyPlanError::DecodeLegacyState { .. })
    ));
    Ok(())
}

#[test]
fn cutover_plan_rejects_assignment_references_outside_service_history() -> TestResult {
    let mut entries = vec![
        service_info(),
        history_counter(),
        deployment_history(),
        json_entry(
            "/maetro/cluster/assignments/node-a",
            json!({
                "nodeId": "node-a",
                "generation": 1,
                "assignments": [{
                    "assignmentId": "assignment-1",
                    "placementEpoch": 1,
                    "serviceId": "api",
                    "deploymentId": "missing",
                    "replicaIndex": 0,
                    "nodeId": "node-a",
                    "containerIp": "10.42.1.5",
                    "createdAtMs": 1
                }]
            }),
        ),
    ];
    entries.extend(node_entries("node-a", "master", 10, 1));
    entries.extend(node_entries("node-b", "worker", 11, 2));
    entries.extend(cluster_state());
    let snapshot = LegacySnapshot::new(entries)?;

    assert!(matches!(
        plan_legacy_snapshot(&snapshot, MASTER_SECRET),
        Err(LegacyPlanError::InvalidAssignment { .. })
    ));
    Ok(())
}

#[test]
fn cutover_plan_preserves_registry_free_image_placements() -> TestResult {
    let config = json!({
        "id": "web",
        "name": "Web",
        "version": "v1",
        "build": {
            "repo": "https://github.com/example/web.git",
            "branch": "main",
            "dockerfile": "Dockerfile"
        },
        "deploy": {"exposePorts": [8080]}
    });
    let mut entries = vec![
        json_entry(
            "/maetro/services/web/info",
            json!({"config": config.clone()}),
        ),
        LegacyEntry::new(
            "/maetro/services/web/deployments/history-next-index",
            b"1".to_vec(),
        ),
        json_entry(
            "/maetro/services/web/deployments/history/0000000000",
            json!({
                "id": "deploy-web",
                "createdAt": 1,
                "deployedAt": 2,
                "status": "READY",
                "config": config,
                "gitCommit": {"reference": "abc123", "message": "build"},
                "build": {
                    "dockerImageId": "sha256:built-image",
                    "sourceNodeId": "node-a"
                }
            }),
        ),
        json_entry(
            "/maetro/cluster/assignments/node-b",
            json!({
                "nodeId": "node-b",
                "generation": 9,
                "images": [{
                    "serviceId": "web",
                    "deploymentId": "deploy-web",
                    "image": "sha256:built-image",
                    "sourceNodeId": "node-a"
                }]
            }),
        ),
    ];
    entries.extend(node_entries("node-a", "master", 10, 1));
    entries.extend(node_entries("node-b", "worker", 11, 2));
    entries.extend(cluster_state());
    let snapshot = LegacySnapshot::new(entries)?;

    let plan = plan_legacy_snapshot(&snapshot, MASTER_SECRET)?;
    let deployment: Deployment = decode_write(&plan, BuiltinKind::Deployment)?;
    let raw = deployment
        .meta
        .annotations
        .get(&AnnotationKey(
            "migration.maestro.dev/legacy-image-assignments".to_owned(),
        ))
        .ok_or_else(|| std::io::Error::other("missing image placement annotation"))?;
    let placements: serde_json::Value = serde_json::from_str(raw)?;
    let placement = placements
        .as_array()
        .and_then(|placements| placements.first())
        .ok_or_else(|| std::io::Error::other("missing image placement"))?;
    assert_eq!(placement.get("nodeId"), Some(&json!("node-b")));
    assert_eq!(placement.get("manifestGeneration"), Some(&json!(9)));
    assert_eq!(placement.get("sourceNodeId"), Some(&json!("node-a")));
    Ok(())
}

fn assignment_manifest() -> LegacyEntry {
    json_entry(
        "/maetro/cluster/assignments/node-a",
        json!({
            "nodeId": "node-a",
            "generation": 1,
            "assignments": [{
                "assignmentId": "assignment-1",
                "placementEpoch": 1,
                "serviceId": "api",
                "deploymentId": "deploy-1",
                "replicaIndex": 0,
                "nodeId": "node-a",
                "containerIp": "10.42.1.5",
                "createdAtMs": 1
            }]
        }),
    )
}

fn replica_state(node_id: &str, assignment_id: &str) -> serde_json::Value {
    json!({
        "serviceId": "api",
        "deploymentId": "deploy-1",
        "replicaIndex": 0,
        "status": "PENDING_READY",
        "nodeId": node_id,
        "assignmentId": assignment_id
    })
}

fn service_info() -> LegacyEntry {
    json_entry(
        "/maetro/services/api/info",
        json!({"config": service_config()}),
    )
}

fn history_counter() -> LegacyEntry {
    LegacyEntry::new(
        "/maetro/services/api/deployments/history-next-index",
        b"1".to_vec(),
    )
}

fn deployment_history() -> LegacyEntry {
    json_entry(
        "/maetro/services/api/deployments/history/0000000000",
        json!({
            "id": "deploy-1",
            "createdAt": 1_000,
            "deployedAt": 2_000,
            "status": "READY",
            "config": service_config(),
            "build": {"dockerImageId": "registry.example/api@sha256:abc"}
        }),
    )
}

fn service_config() -> serde_json::Value {
    json!({
        "id": "api",
        "name": "API",
        "version": "v1",
        "image": "registry.example/api:latest",
        "deploy": {"exposePorts": [8080]}
    })
}

fn json_entry(key: &str, value: serde_json::Value) -> LegacyEntry {
    LegacyEntry::new(key, value.to_string().into_bytes())
}

fn decode_write<Value: serde::de::DeserializeOwned>(
    plan: &crate::MigrationPlan,
    kind: BuiltinKind,
) -> Result<Value, Box<dyn std::error::Error>> {
    let write = plan
        .writes()
        .iter()
        .find(|write| write.kind() == kind)
        .ok_or_else(|| std::io::Error::other(format!("missing {kind} write")))?;
    Ok(serde_json::from_slice(write.value())?)
}
